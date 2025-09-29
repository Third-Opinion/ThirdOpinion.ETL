#!/bin/bash

# ===================================================================
# REDEPLOY FHIR VIEWS SCRIPT
# ===================================================================
# This script allows you to redeploy FHIR materialized views by:
# 1. Dropping the existing view
# 2. Creating the new view from SQL file
# 3. Waiting for completion
# 4. Optionally refreshing the view with data
# ===================================================================

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration

CLUSTER_ID="prod-redshift-main-ue2"
DATABASE="dev"
SECRET_ARN="arn:aws:secretsmanager:us-east-2:442042533707:secret:redshift!prod-redshift-main-ue2-awsuser-yp5Lq4"
REGION="us-east-2"

# View definitions - using simple arrays instead of associative
VIEW_NAMES=(
    "fact_fhir_patients_view_v2"
    "fact_fhir_encounters_view_v2"
    "fact_fhir_observations_view_v2"
    "fact_fhir_practitioners_view_v1"
    "fact_fhir_document_references_view_v1"
    "fact_fhir_medication_requests_view_v1"
    "fact_fhir_conditions_view_v1"
    "fact_fhir_procedures_view_v1"
    "fact_fhir_diagnostic_reports_view_v1"
    "fact_fhir_allergies_view_v1"
)

# Function to print colored output
print_status() {
    echo -e "${2}${1}${NC}"
}

# Function to execute SQL and wait for completion
execute_sql() {
    local sql="$1"
    local description="$2"
    
    print_status "→ $description" "$BLUE"
    
    # For debugging: show first 200 chars of SQL
    if [ ${#sql} -gt 200 ]; then
        print_status "  SQL Preview: ${sql:0:200}..." "$YELLOW"
    fi
    
    # Execute the statement
    local statement_id=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --secret-arn "$SECRET_ARN" \
        --sql "$sql" \
        --region "$REGION" \
        --query Id \
        --output text)
    
    if [ -z "$statement_id" ]; then
        print_status "✗ Failed to submit statement: $description" "$RED"
        print_status "  Check AWS credentials and Redshift configuration" "$RED"
        return 1
    fi
    
    print_status "  Statement ID: $statement_id" "$YELLOW"
    
    # Wait for completion
    local status="SUBMITTED"
    local wait_count=0
    local max_wait=300  # 5 minutes timeout
    
    while [ "$status" != "FINISHED" ] && [ "$status" != "FAILED" ] && [ "$status" != "ABORTED" ] && [ $wait_count -lt $max_wait ]; do
        sleep 2
        status=$(aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query Status \
            --output text)
        
        # Show progress every 10 seconds
        if [ $((wait_count % 10)) -eq 0 ]; then
            echo -n "."
        fi
        
        wait_count=$((wait_count + 2))
    done
    
    echo  # New line after dots
    
    if [ "$status" = "FINISHED" ]; then
        print_status "✓ Completed: $description" "$GREEN"
        return 0
    else
        print_status "✗ Failed ($status): $description" "$RED"
        
        # Get error details from describe-statement
        local error_msg=$(aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query 'Error' \
            --output text)
        
        if [ "$error_msg" != "None" ] && [ ! -z "$error_msg" ]; then
            print_status "  Error: $error_msg" "$RED"
        fi
        
        # Also try to get QueryString to see what was executed
        local query_string=$(aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query 'QueryString' \
            --output text)
        
        if [ ! -z "$query_string" ] && [ ${#query_string} -gt 0 ]; then
            print_status "  Query executed (first 500 chars): ${query_string:0:500}" "$YELLOW"
        fi
        
        return 1
    fi
}

# Function to check if view exists
check_view_exists() {
    local view_name="$1"
    
    local sql="SELECT COUNT(*) FROM pg_views WHERE viewname = '${view_name}' AND schemaname = 'public';"
    
    local statement_id=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --secret-arn "$SECRET_ARN" \
        --sql "$sql" \
        --region "$REGION" \
        --query Id \
        --output text)
    
    if [ -z "$statement_id" ]; then
        return 1
    fi
    
    # Wait for completion
    local status="SUBMITTED"
    local timeout=30
    while [ "$status" != "FINISHED" ] && [ "$status" != "FAILED" ] && [ $timeout -gt 0 ]; do
        sleep 1
        status=$(aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query Status \
            --output text)
        timeout=$((timeout - 1))
    done
    
    if [ "$status" = "FINISHED" ]; then
        local count=$(aws redshift-data get-statement-result \
            --id "$statement_id" \
            --region "$REGION" \
            --query 'Records[0][0].longValue' \
            --output text)
        
        if [ "$count" = "1" ]; then
            return 0  # View exists
        fi
    fi
    
    return 1  # View doesn't exist
}

# Function to redeploy a single view
redeploy_view() {
    local view_name="$1"
    local sql_file="${view_name}.sql"
    
    print_status "\n========================================" "$BLUE"
    print_status "Redeploying: $view_name" "$BLUE"
    print_status "========================================" "$BLUE"
    
    # Check if SQL file exists
    if [ ! -f "$sql_file" ]; then
        print_status "✗ SQL file not found: $sql_file" "$RED"
        return 1
    fi
    
    # Step 1: Drop existing view
    print_status "\nStep 1: Dropping existing view (if exists)..." "$YELLOW"
    
    # Always try to drop, ignore if doesn't exist
    execute_sql "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping view $view_name"
    # Don't fail if drop fails - it might not exist
    
    # Step 2: Create new view
    print_status "\nStep 2: Creating new view from $sql_file..." "$YELLOW"
    
    # Read the SQL file
    if [ ! -r "$sql_file" ]; then
        print_status "✗ Cannot read SQL file: $sql_file" "$RED"
        return 1
    fi
    
    # Check file size
    local file_size=$(stat -f%z "$sql_file" 2>/dev/null || stat -c%s "$sql_file" 2>/dev/null)
    print_status "  SQL file size: $file_size bytes" "$YELLOW"
    
    # Read SQL content
    local create_sql=$(cat "$sql_file")
    
    if [ -z "$create_sql" ]; then
        print_status "✗ SQL file is empty: $sql_file" "$RED"
        return 1
    fi
    
    execute_sql "$create_sql" "Creating view $view_name"
    if [ $? -ne 0 ]; then
        print_status "✗ Failed to create view" "$RED"
        print_status "  Please check the SQL syntax in $sql_file" "$YELLOW"
        return 1
    fi
    
    # Step 3: Ask if user wants to refresh the view
    print_status "\nStep 3: Refresh view with data?" "$YELLOW"
    read -p "Do you want to refresh the materialized view now? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        execute_sql "REFRESH MATERIALIZED VIEW public.$view_name;" "Refreshing view $view_name"
        if [ $? -ne 0 ]; then
            print_status "✗ Failed to refresh view" "$RED"
            print_status "  View was created but not populated with data" "$YELLOW"
            return 1
        fi
    else
        print_status "  Skipping refresh (view created but empty)" "$YELLOW"
    fi
    
    print_status "\n✓ Successfully redeployed: $view_name" "$GREEN"
    return 0
}

# Function to display menu
display_menu() {
    echo
    print_status "========================================" "$BLUE"
    print_status "     FHIR VIEW REDEPLOYMENT TOOL" "$BLUE"
    print_status "========================================" "$BLUE"
    echo
    print_status "Available views:" "$YELLOW"
    
    local index=1
    for view_name in "${VIEW_NAMES[@]}"; do
        local status="[Not Found]"
        local color="$RED"
        
        if [ -f "${view_name}.sql" ]; then
            status="[SQL Ready]"
            color="$GREEN"
        fi
        
        printf "  %2d) %-45s %s\n" "$index" "$view_name" "$(echo -e "${color}${status}${NC}")"
        index=$((index + 1))
    done
    echo
    print_status "  A) Redeploy ALL views" "$YELLOW"
    print_status "  R) Refresh ALL existing views (no drop/create)" "$YELLOW"
    print_status "  Q) Quit" "$YELLOW"
    echo
}

# Function to get view by number
get_view_by_number() {
    local num=$1
    if [ $num -ge 1 ] && [ $num -le ${#VIEW_NAMES[@]} ]; then
        echo "${VIEW_NAMES[$((num-1))]}"
        return 0
    fi
    return 1
}

# Function to redeploy all views
redeploy_all_views() {
    print_status "\n========================================" "$BLUE"
    print_status "     REDEPLOYING ALL VIEWS" "$BLUE"
    print_status "========================================" "$BLUE"
    
    local success_count=0
    local fail_count=0
    local skip_count=0
    
    for view_name in "${VIEW_NAMES[@]}"; do
        if [ -f "${view_name}.sql" ]; then
            redeploy_view "$view_name"
            if [ $? -eq 0 ]; then
                success_count=$((success_count + 1))
            else
                fail_count=$((fail_count + 1))
            fi
        else
            print_status "⚠ Skipping $view_name (SQL file not found)" "$YELLOW"
            skip_count=$((skip_count + 1))
        fi
    done
    
    print_status "\n========================================" "$BLUE"
    print_status "SUMMARY:" "$BLUE"
    print_status "  Successful: $success_count" "$GREEN"
    print_status "  Failed: $fail_count" "$RED"
    print_status "  Skipped: $skip_count" "$YELLOW"
    print_status "========================================" "$BLUE"
}

# Function to refresh all existing views
refresh_all_views() {
    print_status "\n========================================" "$BLUE"
    print_status "     REFRESHING ALL EXISTING VIEWS" "$BLUE"
    print_status "========================================" "$BLUE"
    
    local success_count=0
    local fail_count=0
    
    for view_name in "${VIEW_NAMES[@]}"; do
        print_status "\nRefreshing: $view_name" "$YELLOW"
        execute_sql "REFRESH MATERIALIZED VIEW public.$view_name;" "Refreshing $view_name"
        if [ $? -eq 0 ]; then
            success_count=$((success_count + 1))
        else
            fail_count=$((fail_count + 1))
        fi
    done
    
    print_status "\n========================================" "$BLUE"
    print_status "REFRESH SUMMARY:" "$BLUE"
    print_status "  Successful: $success_count" "$GREEN"
    print_status "  Failed: $fail_count" "$RED"
    print_status "========================================" "$BLUE"
}

# Main script
main() {
    # Check AWS CLI is installed
    if ! command -v aws &> /dev/null; then
        print_status "Error: AWS CLI is not installed" "$RED"
        exit 1
    fi
    
    # Check AWS credentials
    aws sts get-caller-identity &> /dev/null
    if [ $? -ne 0 ]; then
        print_status "Error: AWS credentials not configured" "$RED"
        exit 1
    fi
    
    # Display configuration
    print_status "Configuration:" "$BLUE"
    print_status "  Cluster: $CLUSTER_ID" "$NC"
    print_status "  Database: $DATABASE" "$NC"
    print_status "  Region: $REGION" "$NC"
    
    while true; do
        display_menu
        
        read -p "Select option: " choice
        
        case $choice in
            [0-9]|[0-9][0-9])
                view_name=$(get_view_by_number $choice)
                if [ $? -eq 0 ]; then
                    redeploy_view "$view_name"
                else
                    print_status "Invalid selection: $choice" "$RED"
                fi
                ;;
            [Aa])
                redeploy_all_views
                ;;
            [Rr])
                refresh_all_views
                ;;
            [Qq])
                print_status "Goodbye!" "$GREEN"
                exit 0
                ;;
            *)
                print_status "Invalid selection: $choice" "$RED"
                ;;
        esac
        
        echo
        read -p "Press Enter to continue..."
    done
}

# Run main function
main "$@"