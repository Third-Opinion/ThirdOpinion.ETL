#!/bin/bash

# Deploy all DDL files from ./ddl to Redshift
# This script executes each SQL file to create tables in Redshift

set -e  # Exit on error

# Configuration
AWS_PROFILE="${AWS_PROFILE:-to-prd-admin}"
WORKGROUP_NAME="to-prd-redshift-serverless"
DATABASE_NAME="dev"
REGION="us-east-2"
DDL_DIR="./ddl"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}📊 DEPLOYING DDL TO REDSHIFT${NC}"
echo -e "${BLUE}========================================================================${NC}"
echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo -e "  AWS Profile: ${AWS_PROFILE}"
echo -e "  Workgroup: ${WORKGROUP_NAME}"
echo -e "  Database: ${DATABASE_NAME}"
echo -e "  Region: ${REGION}"
echo -e "  DDL Directory: ${DDL_DIR}"
echo ""

# Check if DDL directory exists
if [ ! -d "$DDL_DIR" ]; then
    echo -e "${RED}❌ Error: DDL directory not found: $DDL_DIR${NC}"
    exit 1
fi

# Count SQL files
SQL_FILE_COUNT=$(find "$DDL_DIR" -name "*.sql" -type f | wc -l | tr -d ' ')

if [ "$SQL_FILE_COUNT" -eq 0 ]; then
    echo -e "${RED}❌ Error: No SQL files found in $DDL_DIR${NC}"
    exit 1
fi

echo -e "${GREEN}Found $SQL_FILE_COUNT SQL files to deploy${NC}"
echo ""

# Track progress
DEPLOYED=0
FAILED=0

# Deploy each SQL file
for sql_file in "$DDL_DIR"/*.sql; do
    TABLE_NAME=$(basename "$sql_file" .sql)

    echo -e "${BLUE}📝 Deploying: ${TABLE_NAME}${NC}"

    # Read SQL content
    SQL_CONTENT=$(cat "$sql_file")

    # Execute SQL via Redshift Data API
    STATEMENT_ID=$(AWS_PROFILE=$AWS_PROFILE aws redshift-data execute-statement \
        --workgroup-name "$WORKGROUP_NAME" \
        --database "$DATABASE_NAME" \
        --sql "$SQL_CONTENT" \
        --region "$REGION" \
        --query "Id" \
        --output text 2>&1)

    if [ $? -ne 0 ]; then
        echo -e "${RED}   ❌ Failed to submit SQL${NC}"
        echo -e "${RED}   Error: $STATEMENT_ID${NC}"
        ((FAILED++))
        continue
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
        echo -e "${GREEN}   ✅ Successfully deployed table: $TABLE_NAME${NC}"
        ((DEPLOYED++))
    else
        echo -e "${RED}   ❌ Failed to deploy table: $TABLE_NAME (Status: $STATUS)${NC}"

        # Get error message if available
        ERROR_MSG=$(AWS_PROFILE=$AWS_PROFILE aws redshift-data describe-statement \
            --id "$STATEMENT_ID" \
            --region "$REGION" \
            --query "Error" \
            --output text 2>/dev/null || echo "No error details available")

        if [ "$ERROR_MSG" != "None" ] && [ "$ERROR_MSG" != "No error details available" ]; then
            echo -e "${RED}   Error: $ERROR_MSG${NC}"
        fi

        ((FAILED++))
    fi

    echo ""
done

# Summary
echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}📊 DEPLOYMENT SUMMARY${NC}"
echo -e "${BLUE}========================================================================${NC}"
echo -e "Total SQL files: $SQL_FILE_COUNT"
echo -e "${GREEN}✅ Successfully deployed: $DEPLOYED${NC}"
if [ $FAILED -gt 0 ]; then
    echo -e "${RED}❌ Failed: $FAILED${NC}"
fi
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}🎉 All DDL files deployed successfully!${NC}"
    exit 0
else
    echo -e "${YELLOW}⚠️  Some DDL files failed to deploy. Check errors above.${NC}"
    exit 1
fi
