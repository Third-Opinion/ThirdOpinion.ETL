#!/bin/bash

# ===================================================================
# CREATE ALL FHIR MATERIALIZED VIEWS SCRIPT
# ===================================================================
# This script creates all FHIR materialized views in Redshift if they don't exist
# Uses AWS Redshift Data API for execution
# ===================================================================

# Configuration
AWS_PROFILE="${AWS_PROFILE:-to-prd-admin}"
CLUSTER_ID="prod-redshift-main-ue2"
DATABASE="dev"
SECRET_ARN="arn:aws:secretsmanager:us-east-2:442042533707:secret:redshift!prod-redshift-main-ue2-awsuser-yp5Lq4"
REGION="us-east-2"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to get record count from a view
get_record_count() {
    local view_name=$1
    
    STATEMENT_ID=$(aws --profile "$AWS_PROFILE" redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --secret-arn "$SECRET_ARN" \
        --sql "SELECT COUNT(*) FROM ${view_name}" \
        --region "$REGION" \
        --query 'Id' \
        --output text 2>/dev/null)
    
    if [ $? -ne 0 ]; then
        echo "N/A"
        return 1
    fi
    
    # Wait for completion
    while true; do
        STATUS=$(aws --profile "$AWS_PROFILE" redshift-data describe-statement \
            --id "$STATEMENT_ID" \
            --region "$REGION" \
            --query 'Status' \
            --output text 2>/dev/null)
        
        if [ "$STATUS" = "FINISHED" ]; then
            break
        elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "ABORTED" ]; then
            echo "N/A"
            return 1
        fi
        sleep 1
    done
    
    # Get the result
    COUNT=$(aws --profile "$AWS_PROFILE" redshift-data get-statement-result \
        --id "$STATEMENT_ID" \
        --region "$REGION" \
        --query 'Records[0][0].longValue' \
        --output text 2>/dev/null)
    
    if [ $? -eq 0 ] && [ "$COUNT" != "None" ] && [ "$COUNT" != "null" ]; then
        echo "$COUNT"
    else
        echo "N/A"
    fi
}

# Function to execute SQL and wait for completion
execute_sql_file() {
    local sql_file=$1
    local view_name=$2
    
    echo -e "${YELLOW}Creating view: ${view_name}${NC}"
    
    # Read the SQL file content
    if [ ! -f "$sql_file" ]; then
        echo -e "${RED}Error: SQL file not found: $sql_file${NC}"
        return 1
    fi
    
    # Execute the SQL
    STATEMENT_ID=$(aws --profile "$AWS_PROFILE" redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --secret-arn "$SECRET_ARN" \
        --sql "$(cat $sql_file)" \
        --region "$REGION" \
        --query 'Id' \
        --output text)
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}Error: Failed to execute SQL for $view_name${NC}"
        return 1
    fi
    
    echo "Statement ID: $STATEMENT_ID"
    
    # Wait for the statement to complete
    while true; do
        STATUS=$(aws --profile "$AWS_PROFILE" redshift-data describe-statement \
            --id "$STATEMENT_ID" \
            --region "$REGION" \
            --query 'Status' \
            --output text)
        
        case $STATUS in
            FINISHED)
                echo -e "${GREEN}✓ Successfully created: $view_name${NC}"
                # Get record count for newly created view
                echo -n "  Getting record count... "
                RECORD_COUNT=$(get_record_count "$view_name")
                echo -e "${GREEN}Records: $RECORD_COUNT${NC}"
                break
                ;;
            FAILED)
                ERROR_MSG=$(aws --profile "$AWS_PROFILE" redshift-data describe-statement \
                    --id "$STATEMENT_ID" \
                    --region "$REGION" \
                    --query 'Error' \
                    --output text)
                
                # Check if it's because view already exists
                if [[ "$ERROR_MSG" == *"already exists"* ]]; then
                    echo -e "${YELLOW}✓ View already exists: $view_name${NC}"
                    # Get record count for existing view
                    echo -n "  Getting record count... "
                    RECORD_COUNT=$(get_record_count "$view_name")
                    echo -e "${YELLOW}Records: $RECORD_COUNT${NC}"
                    return 0  # Treat as success
                else
                    echo -e "${RED}✗ Failed to create: $view_name${NC}"
                    echo -e "${RED}Error: $ERROR_MSG${NC}"
                    return 1
                fi
                ;;
            ABORTED)
                echo -e "${RED}✗ Query aborted: $view_name${NC}"
                return 1
                ;;
            *)
                echo -n "."
                sleep 2
                ;;
        esac
    done
    
    echo ""
    return 0
}

# Function to check if a view exists
check_view_exists() {
    local view_name=$1
    
    STATEMENT_ID=$(aws --profile "$AWS_PROFILE" redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --secret-arn "$SECRET_ARN" \
        --sql "SELECT COUNT(*) FROM pg_views WHERE viewname = '${view_name}'" \
        --region "$REGION" \
        --query 'Id' \
        --output text)
    
    # Wait for completion
    while true; do
        STATUS=$(aws --profile "$AWS_PROFILE" redshift-data describe-statement \
            --id "$STATEMENT_ID" \
            --region "$REGION" \
            --query 'Status' \
            --output text)
        
        if [ "$STATUS" = "FINISHED" ]; then
            break
        elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "ABORTED" ]; then
            return 1
        fi
        sleep 1
    done
    
    # Get the result
    COUNT=$(aws --profile "$AWS_PROFILE" redshift-data get-statement-result \
        --id "$STATEMENT_ID" \
        --region "$REGION" \
        --query 'Records[0][0].longValue' \
        --output text)
    
    if [ "$COUNT" -gt 0 ]; then
        return 0  # View exists
    else
        return 1  # View doesn't exist
    fi
}

# Main execution
echo "====================================================================="
echo "CREATING FHIR MATERIALIZED VIEWS IN REDSHIFT"
echo "====================================================================="
echo "Cluster: $CLUSTER_ID"
echo "Database: $DATABASE"
echo "Region: $REGION"
echo "====================================================================="
echo ""

# Array of views to create (in dependency order)
# Only create latest version (v2 when available, v1 otherwise)
declare -a VIEWS=(
    "views/fact_fhir_patients_view_v1.sql:fact_fhir_patients_view_v1"                     # v1 is latest
    "views/fact_fhir_encounters_view_v1.sql:fact_fhir_encounters_view_v1"                 # v1 is latest
    "views/fact_fhir_conditions_view_v1.sql:fact_fhir_conditions_view_v1"                 # only v1 exists
    "views/fact_fhir_diagnostic_reports_view_v1.sql:fact_fhir_diagnostic_reports_view_v1" # only v1 exists
    "views/fact_fhir_document_references_view_v1.sql:fact_fhir_document_references_view_v1" # only v1 exists
    "views/fact_fhir_medication_requests_view_v1.sql:fact_fhir_medication_requests_view_v1" # only v1 exists
    "views/fact_fhir_observations_view_v1.sql:fact_fhir_observations_view_v1"             # only v1 exists
    "views/fact_fhir_practitioners_view_v1.sql:fact_fhir_practitioners_view_v1"           # only v1 exists
    "views/fact_fhir_procedures_view_v1.sql:fact_fhir_procedures_view_v1"                 # only v1 exists
    "views/fact_fhir_allergy_intolerance_view_v1.sql:fact_fhir_allergy_intolerance_view_v1" # v1 is latest
    "views/fact_fhir_care_plans_view_v1.sql:fact_fhir_care_plans_view_v1"                 # v1 is latest
)

TOTAL=${#VIEWS[@]}
SUCCESS_COUNT=0
SKIP_COUNT=0
FAIL_COUNT=0

# Process each view
for i in "${!VIEWS[@]}"; do
    IFS=':' read -r SQL_FILE VIEW_NAME <<< "${VIEWS[$i]}"
    PROGRESS=$((i + 1))
    
    echo "[$PROGRESS/$TOTAL] Processing: $VIEW_NAME"
    echo "---------------------------------------------------------------------"
    
    # Check if view already exists
    if check_view_exists "$VIEW_NAME"; then
        echo -e "${YELLOW}⊘ View already exists, skipping: $VIEW_NAME${NC}"
        # Get record count for existing view
        echo -n "  Getting record count... "
        RECORD_COUNT=$(get_record_count "$VIEW_NAME")
        echo -e "${YELLOW}Records: $RECORD_COUNT${NC}"
        ((SKIP_COUNT++))
    else
        # Try to create the view
        if execute_sql_file "$SQL_FILE" "$VIEW_NAME"; then
            ((SUCCESS_COUNT++))
        else
            ((FAIL_COUNT++))
            echo -e "${RED}Failed to create view: $VIEW_NAME${NC}"
            
            # Ask if user wants to continue
            read -p "Continue with remaining views? (y/n): " -n 1 -r
            echo
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                echo "Aborting script execution."
                break
            fi
        fi
    fi
    
    echo ""
done

# Summary
echo "====================================================================="
echo "SUMMARY"
echo "====================================================================="
echo -e "${GREEN}Successfully created: $SUCCESS_COUNT views${NC}"
echo -e "${YELLOW}Skipped (already exist): $SKIP_COUNT views${NC}"
echo -e "${RED}Failed: $FAIL_COUNT views${NC}"
echo "====================================================================="

# Optional: Refresh all views
if [ true ]; then
    read -p "Do you want to refresh all materialized views now? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Refreshing materialized views..."
        
        for i in "${!VIEWS[@]}"; do
            IFS=':' read -r SQL_FILE VIEW_NAME <<< "${VIEWS[$i]}"
            
            echo "Refreshing: $VIEW_NAME"
            aws --profile "$AWS_PROFILE" redshift-data execute-statement \
                --cluster-identifier "$CLUSTER_ID" \
                --database "$DATABASE" \
                --secret-arn "$SECRET_ARN" \
                --sql "REFRESH MATERIALIZED VIEW ${VIEW_NAME}" \
                --region "$REGION" \
                --query 'Id' \
                --output text
        done
        
        echo -e "${GREEN}Refresh commands submitted successfully${NC}"
    fi
fi

exit $FAIL_COUNT