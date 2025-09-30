#!/bin/bash

# ===================================================================
# REFRESH ALL FHIR MATERIALIZED VIEWS SCRIPT
# ===================================================================
# This script refreshes all FHIR materialized views in Redshift
# Useful for updating views with latest data
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
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}====================================================================="
echo "REFRESHING FHIR MATERIALIZED VIEWS IN REDSHIFT"
echo "====================================================================="
echo "Cluster: $CLUSTER_ID"
echo "Database: $DATABASE"
echo "Region: $REGION"
echo "=====================================================================${NC}"
echo ""

# List of views to refresh (in dependency order)
# Only refresh latest version (v2 when available, v1 otherwise)
VIEWS=(
    "fact_fhir_patients_view_v1"           # v1 is latest
    "fact_fhir_encounters_view_v2"         # v2 is latest
    "fact_fhir_conditions_view_v1"         # only v1 exists
    "fact_fhir_diagnostic_reports_view_v1" # only v1 exists
    "fact_fhir_document_references_view_v1" # only v1 exists
    "fact_fhir_medication_requests_view_v1" # only v1 exists
    "fact_fhir_observations_view_v1"       # only v1 exists
    "fact_fhir_practitioners_view_v1"      # only v1 exists
    "fact_fhir_procedures_view_v1"         # only v1 exists
)

# Track statement IDs
declare -a STATEMENT_IDS=()
declare -a VIEW_NAMES=()

echo -e "${YELLOW}Submitting refresh statements...${NC}"
echo ""

# Submit all REFRESH statements
for view_name in "${VIEWS[@]}"; do
    echo -e "${BLUE}→ Refreshing: $view_name${NC}"
    
    # Execute the REFRESH statement
    STATEMENT_ID=$(aws redshift-data execute-statement \
        --profile "$AWS_PROFILE" \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --secret-arn "$SECRET_ARN" \
        --sql "REFRESH MATERIALIZED VIEW ${view_name}" \
        --region "$REGION" \
        --query 'Id' \
        --output text 2>/dev/null)
    
    if [ $? -eq 0 ]; then
        echo "  Statement ID: $STATEMENT_ID"
        STATEMENT_IDS+=("$STATEMENT_ID")
        VIEW_NAMES+=("$view_name")
    else
        echo -e "${RED}  ✗ Failed to submit${NC}"
    fi
done

echo ""
echo -e "${YELLOW}Waiting for refreshes to complete...${NC}"
echo -e "${YELLOW}Note: This may take several minutes depending on data volume${NC}"
echo ""

# Wait for all statements to complete
SUCCESS_COUNT=0
FAIL_COUNT=0
START_TIME=$(date +%s)

for i in "${!STATEMENT_IDS[@]}"; do
    STATEMENT_ID="${STATEMENT_IDS[$i]}"
    VIEW_NAME="${VIEW_NAMES[$i]}"
    
    echo -n "Refreshing $VIEW_NAME: "
    
    # Poll for completion
    while true; do
        STATUS=$(aws redshift-data describe-statement \
            --profile "$AWS_PROFILE" \
            --id "$STATEMENT_ID" \
            --region "$REGION" \
            --query 'Status' \
            --output text 2>/dev/null)
        
        case $STATUS in
            FINISHED)
                # Get execution time
                DURATION=$(aws redshift-data describe-statement \
                    --profile "$AWS_PROFILE" \
                    --id "$STATEMENT_ID" \
                    --region "$REGION" \
                    --query 'Duration' \
                    --output text 2>/dev/null)
                
                # Convert nanoseconds to seconds
                if [ ! -z "$DURATION" ] && [ "$DURATION" != "None" ]; then
                    DURATION_SEC=$(echo "scale=2; $DURATION / 1000000000" | bc)
                    echo -e "${GREEN}✓ Success (${DURATION_SEC}s)${NC}"
                else
                    echo -e "${GREEN}✓ Success${NC}"
                fi
                ((SUCCESS_COUNT++))
                break
                ;;
            FAILED)
                echo -e "${RED}✗ Failed${NC}"
                ERROR_MSG=$(aws redshift-data describe-statement \
                    --profile "$AWS_PROFILE" \
                    --id "$STATEMENT_ID" \
                    --region "$REGION" \
                    --query 'Error' \
                    --output text 2>/dev/null)
                echo -e "  ${RED}→ Error: $ERROR_MSG${NC}"
                ((FAIL_COUNT++))
                break
                ;;
            ABORTED)
                echo -e "${RED}✗ Aborted${NC}"
                ((FAIL_COUNT++))
                break
                ;;
            *)
                echo -n "."
                sleep 3
                ;;
        esac
    done
done

# Calculate total execution time
END_TIME=$(date +%s)
TOTAL_TIME=$((END_TIME - START_TIME))
TOTAL_MIN=$((TOTAL_TIME / 60))
TOTAL_SEC=$((TOTAL_TIME % 60))

echo ""
echo -e "${BLUE}====================================================================="
echo "SUMMARY"
echo "====================================================================="
echo -e "${GREEN}Successfully refreshed: $SUCCESS_COUNT views${NC}"
echo -e "${RED}Failed: $FAIL_COUNT views${NC}"
echo -e "Total execution time: ${TOTAL_MIN}m ${TOTAL_SEC}s"
echo -e "${BLUE}=====================================================================${NC}"

# Show timestamp of completion
echo -e "Completed at: $(date '+%Y-%m-%d %H:%M:%S')"

exit 0