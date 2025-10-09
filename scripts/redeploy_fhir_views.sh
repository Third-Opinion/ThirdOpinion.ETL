#!/bin/bash

# ===================================================================
# REDEPLOY FHIR VIEWS SCRIPT - V2 WITH DEPENDENCY MANAGEMENT
# ===================================================================
# This script allows you to redeploy FHIR views with proper dependency ordering:
# 1. Drops all existing views (when doing bulk operations)
# 2. Creates new views from SQL files in dependency order
# 3. Waits for each dependency to complete before proceeding
# 4. Optionally refreshes views with data
#
# Usage:
#   ./redeploy_fhir_views.sh              # Normal mode with status checks
#   ./redeploy_fhir_views.sh --skip-status  # Skip status checks (faster menu)
#   ./redeploy_fhir_views.sh -s           # Short form
#
# Dependency hierarchy:
# - Base tables (no dependencies) are deployed first
# - Views that depend on other views are deployed after their dependencies
# ===================================================================

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
CLUSTER_ID="prod-redshift-main-ue2"
DATABASE="dev"
SECRET_ARN="arn:aws:secretsmanager:us-east-2:442042533707:secret:redshift!prod-redshift-main-ue2-awsuser-yp5Lq4"
REGION="us-east-2"

# Parse command line arguments
SKIP_STATUS_CHECK=false
if [ "$1" = "--skip-status" ] || [ "$1" = "-s" ]; then
    SKIP_STATUS_CHECK=true
    echo "Status checks disabled - menu will load faster"
fi

# Define dependency levels (views in same level can be deployed in parallel)
# Level 0: Base views with no dependencies on other views
DEPENDENCY_LEVEL_0=(
    "fact_fhir_practitioners_view_v1"  # Independent - practitioners data
)

# Level 1: Views that only depend on base tables
DEPENDENCY_LEVEL_1=(
    "fact_fhir_patients_view_v1"       # Depends on patient tables only
    "fact_fhir_allergy_intolerance_view_v1"  # Depends on allergy tables only
    "fact_fhir_care_plans_view_v1"     # Depends on care plan tables only
    "fact_fhir_medication_requests_view_v1"  # Depends on medication tables only
    "fact_fhir_procedures_view_v1"     # Depends on procedure tables only
    "fact_fhir_diagnostic_reports_view_v1"  # Depends on diagnostic report tables only
    "fact_fhir_document_references_view_v1"  # Depends on document reference tables only
)

# Level 2: Views that depend on Level 1 views
DEPENDENCY_LEVEL_2=(
    "fact_fhir_observations_view_v1"   # Depends on observation tables only (large view)
    "fact_fhir_conditions_view_v1"     # May depend on encounters
    "fact_fhir_encounters_view_v1"     # May depend on conditions/patients
)

# Level 3: Reporting views that depend on fact views
DEPENDENCY_LEVEL_3=(
    "rpt_fhir_hmu_patients_v1"         # Depends on patients, conditions, encounters
    "rpt_fhir_adt_medications_v1"      # Depends on medication views
    "rpt_fhir_psa_total_view_v1"       # Depends on observations
    "rpt_fhir_testosterone_total_view_v1"  # Depends on observations
)

# Combine all views in dependency order
VIEW_NAMES=()
for view in "${DEPENDENCY_LEVEL_0[@]}"; do VIEW_NAMES+=("$view"); done
for view in "${DEPENDENCY_LEVEL_1[@]}"; do VIEW_NAMES+=("$view"); done
for view in "${DEPENDENCY_LEVEL_2[@]}"; do VIEW_NAMES+=("$view"); done
for view in "${DEPENDENCY_LEVEL_3[@]}"; do VIEW_NAMES+=("$view"); done

# Function to print colored output
print_status() {
    echo -e "${2}${1}${NC}"
}

# Function to check if view SQL file exists (checks both directories)
view_file_exists() {
    local view_name="$1"
    if [ -f "views_deploy/${view_name}.sql" ] || [ -f "views/${view_name}.sql" ]; then
        return 0
    fi
    return 1
}

# Function to get view SQL file path (prefers deployment version)
get_view_file() {
    local view_name="$1"
    if [ -f "views_deploy/${view_name}.sql" ]; then
        echo "views_deploy/${view_name}.sql"
    elif view_file_exists "$view_name"; then
        echo "views/${view_name}.sql"
    else
        echo ""
    fi
}

# Function to get dependency level for a view
get_dependency_level() {
    local view_name="$1"

    for view in "${DEPENDENCY_LEVEL_0[@]}"; do
        if [ "$view" = "$view_name" ]; then
            echo "0"
            return
        fi
    done

    for view in "${DEPENDENCY_LEVEL_1[@]}"; do
        if [ "$view" = "$view_name" ]; then
            echo "1"
            return
        fi
    done

    for view in "${DEPENDENCY_LEVEL_2[@]}"; do
        if [ "$view" = "$view_name" ]; then
            echo "2"
            return
        fi
    done

    for view in "${DEPENDENCY_LEVEL_3[@]}"; do
        if [ "$view" = "$view_name" ]; then
            echo "3"
            return
        fi
    done

    echo "-1"  # Unknown
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

# Function to check if view exists and get stats
check_view_stats() {
    local view_name="$1"

    # Check if view exists
    local exists_sql="SELECT COUNT(*) FROM pg_views WHERE viewname = '${view_name}' AND schemaname = 'public';"

    local statement_id=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --secret-arn "$SECRET_ARN" \
        --sql "$exists_sql" \
        --region "$REGION" \
        --query Id \
        --output text)

    if [ -z "$statement_id" ]; then
        echo "ERROR|0|NULL"
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

    if [ "$status" != "FINISHED" ]; then
        echo "ERROR|0|NULL"
        return 1
    fi

    local count=$(aws redshift-data get-statement-result \
        --id "$statement_id" \
        --region "$REGION" \
        --query 'Records[0][0].longValue' \
        --output text)

    if [ "$count" != "1" ]; then
        echo "NOT_EXISTS|0|NULL"
        return 1
    fi

    # View exists, now get row count and max updated_at
    # First, check if the view has an updated_at or etl_updated_at column
    local has_updated_at=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --secret-arn "$SECRET_ARN" \
        --sql "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '${view_name}' AND column_name IN ('updated_at', 'etl_updated_at');" \
        --region "$REGION" \
        --query Id \
        --output text)

    # Wait for column check
    local check_status="SUBMITTED"
    local check_timeout=30
    while [ "$check_status" != "FINISHED" ] && [ "$check_status" != "FAILED" ] && [ $check_timeout -gt 0 ]; do
        sleep 1
        check_status=$(aws redshift-data describe-statement \
            --id "$has_updated_at" \
            --region "$REGION" \
            --query Status \
            --output text)
        check_timeout=$((check_timeout - 1))
    done

    local stats_sql
    if [ "$check_status" = "FINISHED" ]; then
        local col_exists=$(aws redshift-data get-statement-result \
            --id "$has_updated_at" \
            --region "$REGION" \
            --query 'Records[0][0].longValue' \
            --output text)

        if [ "$col_exists" -gt 0 ]; then
            # Try etl_updated_at first, then updated_at
            stats_sql="SELECT COUNT(*) as row_count, COALESCE(MAX(etl_updated_at), MAX(updated_at)) as max_updated_at FROM ${view_name};"
        else
            # No updated_at column, just get count
            stats_sql="SELECT COUNT(*) as row_count FROM ${view_name};"
        fi
    else
        # Default to trying updated_at
        stats_sql="SELECT COUNT(*) as row_count, MAX(updated_at) as max_updated_at FROM ${view_name};"
    fi

    statement_id=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --secret-arn "$SECRET_ARN" \
        --sql "$stats_sql" \
        --region "$REGION" \
        --query Id \
        --output text)

    if [ -z "$statement_id" ]; then
        echo "EXISTS|ERROR|NULL"
        return 0
    fi

    # Wait for completion
    status="SUBMITTED"
    timeout=60
    while [ "$status" != "FINISHED" ] && [ "$status" != "FAILED" ] && [ $timeout -gt 0 ]; do
        sleep 1
        status=$(aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query Status \
            --output text)
        timeout=$((timeout - 1))
    done

    if [ "$status" != "FINISHED" ]; then
        echo "EXISTS|ERROR|NULL"
        return 0
    fi

    # Extract row_count and max_updated_at
    # Get the full result as JSON
    local full_result=$(aws redshift-data get-statement-result \
        --id "$statement_id" \
        --region "$REGION" \
        --output json)

    # Check if we have jq available for robust JSON parsing
    if command -v jq &> /dev/null; then
        # Use jq for reliable JSON parsing
        local row_count=$(echo "$full_result" | jq -r '.Records[0][0].longValue // .Records[0][0].stringValue // "0"')

        # Check if there's a second column (max_updated_at)
        local has_second_col=$(echo "$full_result" | jq -r '.Records[0] | length')
        if [ "$has_second_col" -gt 1 ]; then
            local max_updated=$(echo "$full_result" | jq -r '.Records[0][1].stringValue // "NULL"')
        else
            local max_updated="N/A"
        fi
    else
        # Fallback to grep/sed parsing
        # Try to extract longValue for row count
        local row_count=$(echo "$full_result" | grep -o '"longValue"[[:space:]]*:[[:space:]]*[0-9]*' | head -1 | grep -o '[0-9]*$')

        # If no longValue, try stringValue for row count
        if [ -z "$row_count" ]; then
            row_count=$(echo "$full_result" | grep -o '"stringValue"[[:space:]]*:[[:space:]]*"[0-9]*"' | head -1 | grep -o '[0-9]*')
        fi

        # Extract second field for max_updated (skip first stringValue match)
        local max_updated=$(echo "$full_result" | grep -o '"stringValue"[[:space:]]*:[[:space:]]*"[^"]*"' | sed -n '2p' | sed 's/.*"\([^"]*\)".*/\1/')

        if [ -z "$row_count" ]; then
            row_count="0"
        fi
        if [ -z "$max_updated" ]; then
            max_updated="N/A"
        fi
    fi

    # Handle None/null values
    if [ "$row_count" = "None" ] || [ "$row_count" = "null" ]; then
        row_count="0"
    fi
    if [ "$max_updated" = "None" ] || [ "$max_updated" = "null" ]; then
        max_updated="NULL"
    fi

    echo "EXISTS|$row_count|$max_updated"
    return 0
}

# Function to check if view exists (backwards compatible)
check_view_exists() {
    local view_name="$1"
    local stats=$(check_view_stats "$view_name")
    local exists=$(echo "$stats" | cut -d'|' -f1)

    if [ "$exists" = "EXISTS" ]; then
        return 0
    fi
    return 1
}

# Function to drop all views in reverse dependency order
drop_all_views() {
    print_status "\n========================================" "$BLUE"
    print_status "     DROPPING ALL EXISTING VIEWS" "$BLUE"
    print_status "========================================" "$BLUE"

    # Drop in reverse order (highest dependency level first)
    print_status "\nDropping Level 3 views (Reporting)..." "$YELLOW"
    for view_name in "${DEPENDENCY_LEVEL_3[@]}"; do
        execute_sql "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping $view_name"
    done

    print_status "\nDropping Level 2 views (Complex fact views)..." "$YELLOW"
    for view_name in "${DEPENDENCY_LEVEL_2[@]}"; do
        execute_sql "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping $view_name"
    done

    print_status "\nDropping Level 1 views (Base fact views)..." "$YELLOW"
    for view_name in "${DEPENDENCY_LEVEL_1[@]}"; do
        execute_sql "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping $view_name"
    done

    print_status "\nDropping Level 0 views (Independent views)..." "$YELLOW"
    for view_name in "${DEPENDENCY_LEVEL_0[@]}"; do
        execute_sql "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping $view_name"
    done
}

# Function to create a single view
create_view() {
    local view_name="$1"
    local sql_file=$(get_view_file "$view_name")

    # Check if SQL file exists
    if [ -z "$sql_file" ]; then
        print_status "✗ SQL file not found for view: $view_name" "$RED"
        return 1
    fi

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
    return $?
}

# Function to deploy views by level
deploy_views_by_level() {
    local level="$1"
    shift
    local views=("$@")

    print_status "\n📊 Deploying Level $level views..." "$CYAN"

    local success_count=0
    local fail_count=0

    for view_name in "${views[@]}"; do
        if view_file_exists "$view_name"; then
            print_status "\nDeploying: $view_name" "$YELLOW"
            create_view "$view_name"
            if [ $? -eq 0 ]; then
                success_count=$((success_count + 1))
            else
                fail_count=$((fail_count + 1))
                print_status "  ⚠ Failed to create $view_name" "$RED"
            fi
        else
            print_status "  ⚠ Skipping $view_name (SQL file not found)" "$YELLOW"
        fi
    done

    print_status "  Level $level complete: $success_count successful, $fail_count failed" "$GREEN"
}

# Function to redeploy a single view
redeploy_view() {
    local view_name="$1"
    local sql_file=$(get_view_file "$view_name")

    print_status "\n========================================" "$BLUE"
    print_status "Redeploying: $view_name" "$BLUE"
    local level=$(get_dependency_level "$view_name")
    print_status "Dependency Level: $level" "$CYAN"
    print_status "========================================" "$BLUE"

    # Check if SQL file exists
    if [ -z "$sql_file" ]; then
        print_status "✗ SQL file not found for view: $view_name" "$RED"
        return 1
    fi

    # Step 1: Drop existing view
    print_status "\nStep 1: Dropping existing view (if exists)..." "$YELLOW"
    execute_sql "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping view $view_name"

    # Step 2: Create new view
    print_status "\nStep 2: Creating new view from $sql_file..." "$YELLOW"
    create_view "$view_name"
    if [ $? -ne 0 ]; then
        print_status "✗ Failed to create view" "$RED"
        print_status "  Please check the SQL syntax in $sql_file" "$YELLOW"
        return 1
    fi

    print_status "\n✓ Successfully redeployed: $view_name" "$GREEN"
    return 0
}

# Function to display menu with dependency information
display_menu() {
    echo
    print_status "========================================" "$BLUE"
    print_status "     FHIR VIEW REDEPLOYMENT TOOL" "$BLUE"
    print_status "     (Dependency-Aware Version)" "$BLUE"
    print_status "========================================" "$BLUE"
    echo

    print_status "📊 Views in Dependency Order:" "$YELLOW"
    echo

    # Display Level 0
    print_status "  Level 0 - Independent Views:" "$CYAN"
    local index=1
    for view_name in "${DEPENDENCY_LEVEL_0[@]}"; do
        local status="[Not Found]"
        local color="$RED"

        if view_file_exists "$view_name"; then
            if [ "$SKIP_STATUS_CHECK" = false ]; then
                local stats=$(check_view_stats "$view_name")
                local exists=$(echo "$stats" | cut -d'|' -f1)
                local row_count=$(echo "$stats" | cut -d'|' -f2)
                local max_updated=$(echo "$stats" | cut -d'|' -f3)

                if [ "$exists" = "EXISTS" ]; then
                    status="[Deployed: ${row_count} rows, max: ${max_updated}]"
                    color="$GREEN"
                elif [ "$exists" = "NOT_EXISTS" ]; then
                    status="[SQL Ready, Not Deployed]"
                    color="$YELLOW"
                else
                    status="[SQL Ready, Status Unknown]"
                    color="$YELLOW"
                fi
            else
                status="[SQL Ready]"
                color="$GREEN"
            fi
        fi

        printf "  %2d) %-40s %s\n" "$index" "$view_name" "$(echo -e "${color}${status}${NC}")"
        index=$((index + 1))
    done

    echo
    # Display Level 1
    print_status "  Level 1 - Base Fact Views:" "$CYAN"
    for view_name in "${DEPENDENCY_LEVEL_1[@]}"; do
        local status="[Not Found]"
        local color="$RED"

        if view_file_exists "$view_name"; then
            if [ "$SKIP_STATUS_CHECK" = false ]; then
                local stats=$(check_view_stats "$view_name")
                local exists=$(echo "$stats" | cut -d'|' -f1)
                local row_count=$(echo "$stats" | cut -d'|' -f2)
                local max_updated=$(echo "$stats" | cut -d'|' -f3)

                if [ "$exists" = "EXISTS" ]; then
                    status="[Deployed: ${row_count} rows, max: ${max_updated}]"
                    color="$GREEN"
                elif [ "$exists" = "NOT_EXISTS" ]; then
                    status="[SQL Ready, Not Deployed]"
                    color="$YELLOW"
                else
                    status="[SQL Ready, Status Unknown]"
                    color="$YELLOW"
                fi
            else
                status="[SQL Ready]"
                color="$GREEN"
            fi
        fi

        printf "  %2d) %-40s %s\n" "$index" "$view_name" "$(echo -e "${color}${status}${NC}")"
        index=$((index + 1))
    done

    echo
    # Display Level 2
    print_status "  Level 2 - Complex Fact Views:" "$CYAN"
    for view_name in "${DEPENDENCY_LEVEL_2[@]}"; do
        local status="[Not Found]"
        local color="$RED"

        if view_file_exists "$view_name"; then
            if [ "$SKIP_STATUS_CHECK" = false ]; then
                local stats=$(check_view_stats "$view_name")
                local exists=$(echo "$stats" | cut -d'|' -f1)
                local row_count=$(echo "$stats" | cut -d'|' -f2)
                local max_updated=$(echo "$stats" | cut -d'|' -f3)

                if [ "$exists" = "EXISTS" ]; then
                    status="[Deployed: ${row_count} rows, max: ${max_updated}]"
                    color="$GREEN"
                elif [ "$exists" = "NOT_EXISTS" ]; then
                    status="[SQL Ready, Not Deployed]"
                    color="$YELLOW"
                else
                    status="[SQL Ready, Status Unknown]"
                    color="$YELLOW"
                fi
            else
                status="[SQL Ready]"
                color="$GREEN"
            fi
        fi

        printf "  %2d) %-40s %s\n" "$index" "$view_name" "$(echo -e "${color}${status}${NC}")"
        index=$((index + 1))
    done

    echo
    # Display Level 3
    print_status "  Level 3 - Reporting Views:" "$CYAN"
    for view_name in "${DEPENDENCY_LEVEL_3[@]}"; do
        local status="[Not Found]"
        local color="$RED"

        if view_file_exists "$view_name"; then
            if [ "$SKIP_STATUS_CHECK" = false ]; then
                local stats=$(check_view_stats "$view_name")
                local exists=$(echo "$stats" | cut -d'|' -f1)
                local row_count=$(echo "$stats" | cut -d'|' -f2)
                local max_updated=$(echo "$stats" | cut -d'|' -f3)

                if [ "$exists" = "EXISTS" ]; then
                    status="[Deployed: ${row_count} rows, max: ${max_updated}]"
                    color="$GREEN"
                elif [ "$exists" = "NOT_EXISTS" ]; then
                    status="[SQL Ready, Not Deployed]"
                    color="$YELLOW"
                else
                    status="[SQL Ready, Status Unknown]"
                    color="$YELLOW"
                fi
            else
                status="[SQL Ready]"
                color="$GREEN"
            fi
        fi

        printf "  %2d) %-40s %s\n" "$index" "$view_name" "$(echo -e "${color}${status}${NC}")"
        index=$((index + 1))
    done

    echo
    print_status "🔧 BULK Operations:" "$YELLOW"
    print_status "  A) Redeploy ALL views (drops all, then creates in dependency order)" "$YELLOW"
    print_status "  F) Redeploy FACT views only" "$YELLOW"
    print_status "  R) Redeploy REPORTING views only" "$YELLOW"
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

# Function to redeploy all views with dependency management
redeploy_all_views() {
    print_status "\n========================================" "$BLUE"
    print_status "     REDEPLOYING ALL VIEWS" "$BLUE"
    print_status "   (With Dependency Management)" "$BLUE"
    print_status "========================================" "$BLUE"

    # Step 1: Drop all views first
    drop_all_views

    # Step 2: Deploy views in dependency order
    print_status "\n========================================" "$BLUE"
    print_status "     CREATING VIEWS IN DEPENDENCY ORDER" "$BLUE"
    print_status "========================================" "$BLUE"

    deploy_views_by_level 0 "${DEPENDENCY_LEVEL_0[@]}"
    deploy_views_by_level 1 "${DEPENDENCY_LEVEL_1[@]}"
    deploy_views_by_level 2 "${DEPENDENCY_LEVEL_2[@]}"
    deploy_views_by_level 3 "${DEPENDENCY_LEVEL_3[@]}"

    print_status "\n========================================" "$BLUE"
    print_status "✓ All views redeployed in dependency order" "$GREEN"
    print_status "========================================" "$BLUE"
}

# Function to redeploy fact views only
redeploy_fact_views() {
    print_status "\n========================================" "$BLUE"
    print_status "     REDEPLOYING FACT VIEWS" "$BLUE"
    print_status "========================================" "$BLUE"

    # Drop fact views in reverse order
    for view_name in "${DEPENDENCY_LEVEL_2[@]}"; do
        if [[ $view_name == fact_* ]]; then
            execute_sql "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping $view_name"
        fi
    done

    for view_name in "${DEPENDENCY_LEVEL_1[@]}"; do
        if [[ $view_name == fact_* ]]; then
            execute_sql "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping $view_name"
        fi
    done

    for view_name in "${DEPENDENCY_LEVEL_0[@]}"; do
        if [[ $view_name == fact_* ]]; then
            execute_sql "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping $view_name"
        fi
    done

    # Deploy fact views in dependency order
    print_status "\nCreating fact views in dependency order..." "$YELLOW"

    for view_name in "${DEPENDENCY_LEVEL_0[@]}"; do
        if [[ $view_name == fact_* ]] && view_file_exists "$view_name"; then
            create_view "$view_name"
        fi
    done

    for view_name in "${DEPENDENCY_LEVEL_1[@]}"; do
        if [[ $view_name == fact_* ]] && view_file_exists "$view_name"; then
            create_view "$view_name"
        fi
    done

    for view_name in "${DEPENDENCY_LEVEL_2[@]}"; do
        if [[ $view_name == fact_* ]] && view_file_exists "$view_name"; then
            create_view "$view_name"
        fi
    done
}

# Function to redeploy reporting views only
redeploy_reporting_views() {
    print_status "\n========================================" "$BLUE"
    print_status "     REDEPLOYING REPORTING VIEWS" "$BLUE"
    print_status "========================================" "$BLUE"

    # Drop and recreate reporting views
    for view_name in "${DEPENDENCY_LEVEL_3[@]}"; do
        if [[ $view_name == rpt_* ]]; then
            execute_sql "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping $view_name"
        fi
    done

    print_status "\nCreating reporting views..." "$YELLOW"

    for view_name in "${DEPENDENCY_LEVEL_3[@]}"; do
        if [[ $view_name == rpt_* ]] && view_file_exists "$view_name"; then
            create_view "$view_name"
        fi
    done
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
    echo
    print_status "📋 Dependency Management Enabled:" "$BLUE"
    print_status "  • Views are deployed in dependency order" "$NC"
    print_status "  • Bulk operations drop ALL views first" "$NC"
    print_status "  • Each level completes before the next begins" "$NC"

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
            [Ff])
                redeploy_fact_views
                ;;
            [Rr])
                redeploy_reporting_views
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