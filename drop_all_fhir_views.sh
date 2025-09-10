#!/bin/bash

# ===================================================================
# DROP ALL FHIR MATERIALIZED VIEWS SCRIPT
# ===================================================================
# This script drops all FHIR materialized views in Redshift
# Uses CASCADE to handle dependencies
# ===================================================================

# Configuration
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

echo -e "${RED}====================================================================="
echo "WARNING: THIS WILL DROP ALL FHIR MATERIALIZED VIEWS"
echo "=====================================================================${NC}"
echo ""

# Confirmation
read -p "Are you sure you want to drop all FHIR materialized views? (yes/no): " -r
if [[ ! $REPLY == "yes" ]]; then
    echo "Aborting."
    exit 0
fi

echo ""
echo -e "${YELLOW}Dropping FHIR materialized views...${NC}"
echo ""

# List of views to drop (in reverse dependency order)
# Includes both v1 and v2 to ensure cleanup of old versions
VIEWS=(
    "fact_fhir_procedures_view_v1"
    "fact_fhir_practitioners_view_v1"
    "fact_fhir_observations_view_v1"
    "fact_fhir_medication_requests_view_v1"
    "fact_fhir_document_references_view_v1"
    "fact_fhir_diagnostic_reports_view_v1"
    "fact_fhir_conditions_view_v1"
    "fact_fhir_encounters_view_v2"
    "fact_fhir_encounters_view_v1"  # Drop v1 if exists (replaced by v2)
    "fact_fhir_patients_view_v2"
    "fact_fhir_patients_view_v1"    # Drop v1 if exists (replaced by v2)
)

# Track statement IDs
declare -a STATEMENT_IDS=()
declare -a VIEW_NAMES=()

# Submit all DROP statements
for view_name in "${VIEWS[@]}"; do
    echo -e "${BLUE}→ Dropping: $view_name${NC}"
    
    # Execute the DROP statement
    STATEMENT_ID=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --secret-arn "$SECRET_ARN" \
        --sql "DROP MATERIALIZED VIEW IF EXISTS ${view_name} CASCADE" \
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
echo -e "${YELLOW}Waiting for statements to complete...${NC}"
echo ""

# Wait for all statements to complete
SUCCESS_COUNT=0
FAIL_COUNT=0

for i in "${!STATEMENT_IDS[@]}"; do
    STATEMENT_ID="${STATEMENT_IDS[$i]}"
    VIEW_NAME="${VIEW_NAMES[$i]}"
    
    echo -n "Dropping $VIEW_NAME: "
    
    # Poll for completion
    while true; do
        STATUS=$(aws redshift-data describe-statement \
            --id "$STATEMENT_ID" \
            --region "$REGION" \
            --query 'Status' \
            --output text 2>/dev/null)
        
        case $STATUS in
            FINISHED)
                echo -e "${GREEN}✓ Dropped${NC}"
                ((SUCCESS_COUNT++))
                break
                ;;
            FAILED)
                echo -e "${RED}✗ Failed${NC}"
                ERROR_MSG=$(aws redshift-data describe-statement \
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
                sleep 2
                ;;
        esac
    done
done

echo ""
echo -e "${BLUE}====================================================================="
echo "SUMMARY"
echo "====================================================================="
echo -e "${GREEN}Successfully dropped: $SUCCESS_COUNT views${NC}"
echo -e "${RED}Failed: $FAIL_COUNT views${NC}"
echo -e "${BLUE}=====================================================================${NC}"

exit 0