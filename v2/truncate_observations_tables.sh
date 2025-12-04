#!/bin/bash

# Truncate all observation-related tables in Redshift
# This script executes the truncate SQL script against the test database

set -e

# Configuration
AWS_PROFILE="${AWS_PROFILE:-to-prod-admin}"
CLUSTER_ID="prod-redshift-main-ue2"
DATABASE_NAME="test"  # v2 uses "test" database
SECRET_ARN="arn:aws:secretsmanager:us-east-2:442042533707:secret:redshift!prod-redshift-main-ue2-awsuser-yp5Lq4"
REGION="us-east-2"
TRUNCATE_SCRIPT="${TRUNCATE_SCRIPT:-./v2/ddl/truncate_observations_tables.sql}"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}🗑️  TRUNCATING OBSERVATION TABLES (TEST DATABASE)${NC}"
echo -e "${BLUE}========================================================================${NC}"
echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo -e "  AWS Profile: ${AWS_PROFILE}"
echo -e "  Cluster ID: ${CLUSTER_ID}"
echo -e "  Database: ${DATABASE_NAME} ${YELLOW}(v2 uses 'test' database)${NC}"
echo -e "  Region: ${REGION}"
echo -e "  Script: ${TRUNCATE_SCRIPT}"
echo ""

# Check if script exists
if [ ! -f "$TRUNCATE_SCRIPT" ]; then
    echo -e "${RED}❌ Error: Truncate script not found: $TRUNCATE_SCRIPT${NC}"
    exit 1
fi

# Read SQL content
SQL_CONTENT=$(cat "$TRUNCATE_SCRIPT")

echo -e "${YELLOW}⚠️  WARNING: This will delete ALL data from observation tables!${NC}"
echo -e "${YELLOW}   Tables to truncate:${NC}"
echo -e "   - observation_codes"
echo -e "   - observation_categories"
echo -e "   - observation_interpretations"
echo -e "   - observation_reference_ranges"
echo -e "   - observation_components"
echo -e "   - observation_notes"
echo -e "   - observation_performers"
echo -e "   - observation_members"
echo -e "   - observation_derived_from"
echo -e "   - observations"
echo ""
echo -e "${YELLOW}   Note: Lookup tables (codes, categories, interpretations) are NOT truncated${NC}"
echo -e "${YELLOW}   (commented out in script - uncomment if you want to reset them too)${NC}"
echo ""

# Ask for confirmation (non-interactive mode - set CONFIRM=true to skip)
if [ "${CONFIRM:-false}" != "true" ]; then
    read -p "Are you sure you want to proceed? (yes/no): " confirmation
    if [ "$confirmation" != "yes" ]; then
        echo -e "${YELLOW}Cancelled.${NC}"
        exit 0
    fi
fi

# Execute SQL via Redshift Data API
echo -e "${BLUE}Executing truncate statements...${NC}"

STATEMENT_ID=$(AWS_PROFILE=$AWS_PROFILE aws redshift-data execute-statement \
    --cluster-identifier "$CLUSTER_ID" \
    --database "$DATABASE_NAME" \
    --secret-arn "$SECRET_ARN" \
    --sql "$SQL_CONTENT" \
    --region "$REGION" \
    --query "Id" \
    --output text 2>&1)

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to submit SQL${NC}"
    echo -e "${RED}   Error: $STATEMENT_ID${NC}"
    exit 1
fi

echo -e "   Statement ID: $STATEMENT_ID"

# Wait for statement to complete
STATUS="SUBMITTED"
ATTEMPTS=0
MAX_ATTEMPTS=30

while [ "$STATUS" != "FINISHED" ] && [ "$STATUS" != "FAILED" ] && [ "$STATUS" != "ABORTED" ] && [ $ATTEMPTS -lt $MAX_ATTEMPTS ]; do
    sleep 2
    STATUS=$(AWS_PROFILE=$AWS_PROFILE aws redshift-data describe-statement \
        --id "$STATEMENT_ID" \
        --region "$REGION" \
        --query "Status" \
        --output text 2>/dev/null || echo "ERROR")

    ((ATTEMPTS++))
    echo -ne "   Status: $STATUS (attempt $ATTEMPTS/$MAX_ATTEMPTS)\r"
done

echo ""  # New line after status updates

if [ "$STATUS" = "FINISHED" ]; then
    echo -e "${GREEN}✅ Successfully truncated all observation tables${NC}"
    exit 0
else
    echo -e "${RED}❌ Failed to truncate tables (Status: $STATUS)${NC}"

    # Get error message if available
    ERROR_MSG=$(AWS_PROFILE=$AWS_PROFILE aws redshift-data describe-statement \
        --id "$STATEMENT_ID" \
        --region "$REGION" \
        --query "Error" \
        --output text 2>/dev/null || echo "No error details available")

    if [ "$ERROR_MSG" != "None" ] && [ "$ERROR_MSG" != "No error details available" ]; then
        echo -e "${RED}   Error: $ERROR_MSG${NC}"
    fi

    exit 1
fi

