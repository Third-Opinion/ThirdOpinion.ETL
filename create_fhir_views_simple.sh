
#!/bin/bash
set -e  # Exit on error

# ===================================================================
# CREATE FHIR MATERIALIZED VIEWS - SIMPLE VERSION
# ===================================================================
# This script creates all FHIR materialized views in Redshift
# It will skip views that already exist (CREATE will fail gracefully)
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

echo -e "${BLUE}====================================================================="
echo "CREATING FHIR MATERIALIZED VIEWS IN REDSHIFT"
echo "====================================================================="
echo "Cluster: $CLUSTER_ID"
echo "Database: $DATABASE"
echo "Region: $REGION"
echo "=====================================================================${NC}"
echo ""

# Array of SQL files to execute (in order)
# Only create latest version (v2 when available, v1 otherwise)
SQL_FILES=(
    "fact_fhir_patients_view_v1.sql"           # v1 is current version
    "fact_fhir_encounters_view_v2.sql"         # v2 available, skip v1
    "fact_fhir_conditions_view_v1.sql"         # only v1 exists
    "fact_fhir_diagnostic_reports_view_v1.sql" # only v1 exists
    "fact_fhir_document_references_view_v1.sql" # only v1 exists
    "fact_fhir_medication_requests_view_v1.sql" # only v1 exists
    "fact_fhir_observations_view_v1.sql"       # only v1 exists
    "fact_fhir_practitioners_view_v1.sql"      # only v1 exists
    "fact_fhir_procedures_view_v1.sql"         # only v1 exists
)

# Track statement IDs
declare -a STATEMENT_IDS=()
declare -a VIEW_NAMES=()

# Submit all SQL statements
echo -e "${YELLOW}Submitting SQL statements...${NC}"
echo ""

for sql_file in "${SQL_FILES[@]}"; do
    if [ ! -f "$sql_file" ]; then
        echo -e "${RED}✗ File not found: $sql_file${NC}"
        continue
    fi
    
    # Extract view name from file name
    view_name="${sql_file%.sql}"
    
    echo -e "${BLUE}→ Submitting: $view_name${NC}"
    
    # Execute the SQL statement
    STATEMENT_ID=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --secret-arn "$SECRET_ARN" \
        --sql "$(cat $sql_file)" \
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

# Wait for all statements to complete and check status
SUCCESS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

for i in "${!STATEMENT_IDS[@]}"; do
    STATEMENT_ID="${STATEMENT_IDS[$i]}"
    VIEW_NAME="${VIEW_NAMES[$i]}"
    
    echo -n "Checking $VIEW_NAME: "
    
    # Poll for completion
    while true; do
        STATUS=$(aws redshift-data describe-statement \
            --id "$STATEMENT_ID" \
            --region "$REGION" \
            --query 'Status' \
            --output text 2>/dev/null)
        
        case $STATUS in
            FINISHED)
                echo -e "${GREEN}✓ Success${NC}"
                ((SUCCESS_COUNT++))
                break
                ;;
            FAILED)
                # Get error message first
                ERROR_MSG=$(aws redshift-data describe-statement \
                    --id "$STATEMENT_ID" \
                    --region "$REGION" \
                    --query 'Error' \
                    --output text 2>/dev/null)
                
                # Check if it's because view already exists
                if [[ "$ERROR_MSG" == *"already exists"* ]]; then
                    echo -e "${YELLOW}✓ Already exists${NC}"
                    ((SKIP_COUNT++))
                else
                    echo -e "${RED}✗ Failed${NC}"
                    echo -e "  ${RED}→ Error: $ERROR_MSG${NC}"
                    ((FAIL_COUNT++))
                fi
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
echo -e "${GREEN}Successfully created: $SUCCESS_COUNT views${NC}"
echo -e "${YELLOW}Already existed: $SKIP_COUNT views${NC}"
echo -e "${RED}Failed: $FAIL_COUNT views${NC}"
echo -e "${BLUE}=====================================================================${NC}"

exit 0