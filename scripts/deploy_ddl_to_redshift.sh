#!/bin/bash

# Deploy all DDL files from ./ddl to Redshift
# This script executes each SQL file to create tables in Redshift

set -e  # Exit on error

# Configuration
AWS_PROFILE="${AWS_PROFILE:-to-prd-admin}"
CLUSTER_ID="prod-redshift-main-ue2"
DATABASE_NAME="dev"
SECRET_ARN="arn:aws:secretsmanager:us-east-2:442042533707:secret:redshift!prod-redshift-main-ue2-awsuser-yp5Lq4"
REGION="us-east-2"
DDL_DIR="${DDL_DIR:-./ddl}"

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
echo -e "  Cluster ID: ${CLUSTER_ID}"
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
MIGRATED=0

# ============================================================================
# SCHEMA MIGRATION FUNCTIONS
# ============================================================================

# Execute SQL and wait for completion
execute_sql() {
    local sql="$1"
    local description="$2"

    local statement_id=$(AWS_PROFILE=$AWS_PROFILE aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE_NAME" \
        --secret-arn "$SECRET_ARN" \
        --sql "$sql" \
        --region "$REGION" \
        --query "Id" \
        --output text 2>&1)

    if [ $? -ne 0 ]; then
        echo -e "${RED}   ❌ Failed to submit: $description${NC}"
        echo -e "${RED}   Error: $statement_id${NC}"
        return 1
    fi

    # Wait for completion
    local status="SUBMITTED"
    local attempts=0
    local max_attempts=30

    while [ "$status" != "FINISHED" ] && [ "$status" != "FAILED" ] && [ "$status" != "ABORTED" ] && [ $attempts -lt $max_attempts ]; do
        sleep 2
        status=$(AWS_PROFILE=$AWS_PROFILE aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query "Status" \
            --output text 2>/dev/null || echo "ERROR")
        ((attempts++))
    done

    if [ "$status" = "FINISHED" ]; then
        echo "$statement_id"
        return 0
    else
        local error_msg=$(AWS_PROFILE=$AWS_PROFILE aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query "Error" \
            --output text 2>/dev/null || echo "Unknown error")
        echo -e "${RED}   ❌ Failed: $description (Status: $status)${NC}"
        echo -e "${RED}   Error: $error_msg${NC}"
        return 1
    fi
}

# Check if table exists in Redshift
table_exists() {
    local table_name="$1"

    local sql="SELECT COUNT(DISTINCT table_name) FROM svv_columns WHERE table_schema = 'public' AND table_name = '$table_name';"

    local statement_id=$(AWS_PROFILE=$AWS_PROFILE aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE_NAME" \
        --secret-arn "$SECRET_ARN" \
        --sql "$sql" \
        --region "$REGION" \
        --query "Id" \
        --output text 2>&1)

    if [ $? -ne 0 ]; then
        return 1
    fi

    # Wait for query to complete
    sleep 3

    # Check status first
    local status=$(AWS_PROFILE=$AWS_PROFILE aws redshift-data describe-statement \
        --id "$statement_id" \
        --region "$REGION" \
        --query "Status" \
        --output text 2>/dev/null)

    if [ "$status" != "FINISHED" ]; then
        return 1
    fi

    local count=$(AWS_PROFILE=$AWS_PROFILE aws redshift-data get-statement-result \
        --id "$statement_id" \
        --region "$REGION" \
        --query "Records[0][0].longValue" \
        --output text 2>/dev/null)

    if [ "$count" = "1" ]; then
        return 0
    else
        return 1
    fi
}

# Get current table schema from Redshift
get_current_columns() {
    local table_name="$1"

    local sql="SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '$table_name' ORDER BY ordinal_position;"

    local statement_id=$(AWS_PROFILE=$AWS_PROFILE aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE_NAME" \
        --secret-arn "$SECRET_ARN" \
        --sql "$sql" \
        --region "$REGION" \
        --query "Id" \
        --output text 2>&1)

    if [ $? -ne 0 ]; then
        return 1
    fi

    sleep 2

    # Get results and format as: column_name|data_type|max_length
    AWS_PROFILE=$AWS_PROFILE aws redshift-data get-statement-result \
        --id "$statement_id" \
        --region "$REGION" \
        --output text 2>/dev/null | \
        grep "^RECORDS" | \
        awk '{if (NR % 5 == 1) printf "%s|", $2;
              if (NR % 5 == 2) printf "%s|", $2;
              if (NR % 5 == 3) {if ($2 == "None") printf "NULL|"; else printf "%s|", $2}
              if (NR % 5 == 4) {if ($2 == "None") printf "NULL|"; else printf "%s|", $2}
              if (NR % 5 == 0) {if ($2 == "None") printf "NULL\n"; else printf "%s\n", $2}}'
}

# Parse DDL file to extract column definitions
parse_ddl_columns() {
    local ddl_file="$1"

    # Extract column definitions from CREATE TABLE statement
    # Format: column_name TYPE constraints
    grep -v "^--" "$ddl_file" | \
        sed -n '/CREATE TABLE/,/^)/p' | \
        grep -v "CREATE TABLE" | \
        grep -v "DISTKEY\|SORTKEY\|DISTSTYLE\|^)$\|^);$" | \
        sed 's/^[[:space:]]*//' | \
        sed 's/,$//' | \
        grep -E "^[a-zA-Z_]" | \
        while IFS= read -r line; do
            # Extract column name (first word)
            col_name=$(echo "$line" | awk '{print $1}')
            # Extract data type (second word and optional params)
            # Handle VARCHAR(n), DECIMAL(p,s), etc.
            col_type=$(echo "$line" | awk '{
                if ($2 ~ /^[A-Z]+\(/) {
                    # Type with parameters like VARCHAR(255)
                    print $2
                } else {
                    # Simple type like INTEGER, TIMESTAMP, SUPER
                    print $2
                }
            }')

            # Only output if we have both name and type
            if [ -n "$col_name" ] && [ -n "$col_type" ]; then
                echo "${col_name}|${col_type}"
            fi
        done
}

# Compare schemas and generate ALTER statements for safe changes
compare_and_generate_alters() {
    local table_name="$1"
    local ddl_file="$2"

    echo -e "${BLUE}   ℹ️  Table exists, checking for schema changes...${NC}"

    # Get current schema
    local current_schema=$(get_current_columns "$table_name")
    if [ -z "$current_schema" ]; then
        echo -e "${YELLOW}   ⚠️  Could not retrieve current schema${NC}"
        return 1
    fi

    # Get desired schema from DDL
    local desired_schema=$(parse_ddl_columns "$ddl_file")
    if [ -z "$desired_schema" ]; then
        echo -e "${YELLOW}   ⚠️  Could not parse DDL file${NC}"
        return 1
    fi

    # Find new columns (in desired but not in current)
    local has_changes=0
    while IFS='|' read -r col_name col_type; do
        if ! echo "$current_schema" | grep -qi "^${col_name}|"; then
            echo -e "${GREEN}   ➕ New column detected: ${col_name} ${col_type}${NC}"

            # Generate ALTER TABLE statement
            local alter_sql="ALTER TABLE public.${table_name} ADD COLUMN ${col_name} ${col_type};"

            # Execute ALTER TABLE
            local statement_id=$(execute_sql "$alter_sql" "ADD COLUMN $col_name")
            if [ $? -eq 0 ]; then
                echo -e "${GREEN}   ✅ Added column: ${col_name}${NC}"
                echo -e "      Statement ID: $statement_id"
                has_changes=1
            else
                echo -e "${RED}   ❌ Failed to add column: ${col_name}${NC}"
                return 1
            fi
        fi
    done <<< "$desired_schema"

    # Check for increased VARCHAR sizes
    while IFS='|' read -r col_name col_type; do
        # Only check VARCHAR columns
        if echo "$col_type" | grep -qi "VARCHAR"; then
            # Extract size from desired DDL
            desired_size=$(echo "$col_type" | sed -E 's/VARCHAR\(([0-9]+)\)/\1/')

            # Get current size
            current_line=$(echo "$current_schema" | grep -i "^${col_name}|")
            if [ -n "$current_line" ]; then
                current_size=$(echo "$current_line" | cut -d'|' -f3)

                # Compare sizes (only if both are numeric)
                if [[ "$desired_size" =~ ^[0-9]+$ ]] && [[ "$current_size" =~ ^[0-9]+$ ]]; then
                    if [ "$desired_size" -gt "$current_size" ]; then
                        echo -e "${GREEN}   📏 VARCHAR size increase detected: ${col_name} (${current_size} → ${desired_size})${NC}"

                        # Generate ALTER TABLE statement
                        local alter_sql="ALTER TABLE public.${table_name} ALTER COLUMN ${col_name} TYPE VARCHAR(${desired_size});"

                        # Execute ALTER TABLE
                        local statement_id=$(execute_sql "$alter_sql" "ALTER COLUMN $col_name TYPE")
                        if [ $? -eq 0 ]; then
                            echo -e "${GREEN}   ✅ Increased VARCHAR size: ${col_name}${NC}"
                            echo -e "      Statement ID: $statement_id"
                            has_changes=1
                        else
                            echo -e "${RED}   ❌ Failed to increase VARCHAR size: ${col_name}${NC}"
                            return 1
                        fi
                    fi
                fi
            fi
        fi
    done <<< "$desired_schema"

    # Check for columns in DB but not in DDL (potential drops)
    while IFS='|' read -r col_name col_type col_len precision scale; do
        if ! echo "$desired_schema" | grep -qi "^${col_name}|"; then
            echo -e "${YELLOW}   ⚠️  Column exists in DB but not in DDL: ${col_name}${NC}"
            echo -e "      Consider manually dropping if no longer needed"
        fi
    done <<< "$current_schema"

    if [ $has_changes -eq 0 ]; then
        echo -e "${GREEN}   ✓ No schema changes needed${NC}"
    fi

    return 0
}

# Deploy each SQL file
for sql_file in "$DDL_DIR"/*.sql; do
    TABLE_NAME=$(basename "$sql_file" .sql)

    echo -e "${BLUE}📝 Deploying: ${TABLE_NAME}${NC}"

    # Check if table already exists
    if table_exists "$TABLE_NAME"; then
        # Table exists - check for schema changes and apply migrations
        if compare_and_generate_alters "$TABLE_NAME" "$sql_file"; then
            ((MIGRATED++))
            echo -e "${GREEN}   ✅ Schema migration completed for: $TABLE_NAME${NC}"
        else
            echo -e "${RED}   ❌ Schema migration failed for: $TABLE_NAME${NC}"
            ((FAILED++))
        fi
    else
        # Table doesn't exist - create it
        echo -e "${BLUE}   ℹ️  Table does not exist, creating...${NC}"

        # Read SQL content
        SQL_CONTENT=$(cat "$sql_file")

        # Execute SQL via Redshift Data API
        STATEMENT_ID=$(AWS_PROFILE=$AWS_PROFILE aws redshift-data execute-statement \
            --cluster-identifier "$CLUSTER_ID" \
            --database "$DATABASE_NAME" \
            --secret-arn "$SECRET_ARN" \
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
            echo -e "${GREEN}   ✅ Successfully created table: $TABLE_NAME${NC}"
            ((DEPLOYED++))
        else
            echo -e "${RED}   ❌ Failed to create table: $TABLE_NAME (Status: $STATUS)${NC}"

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
    fi

    echo ""
done

# Summary
echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}📊 DEPLOYMENT SUMMARY${NC}"
echo -e "${BLUE}========================================================================${NC}"
echo -e "Total SQL files: $SQL_FILE_COUNT"
echo -e "${GREEN}✅ Tables created: $DEPLOYED${NC}"
echo -e "${BLUE}🔄 Tables migrated: $MIGRATED${NC}"
if [ $FAILED -gt 0 ]; then
    echo -e "${RED}❌ Failed: $FAILED${NC}"
fi
echo ""

if [ $FAILED -eq 0 ]; then
    if [ $MIGRATED -gt 0 ] || [ $DEPLOYED -gt 0 ]; then
        echo -e "${GREEN}🎉 All DDL operations completed successfully!${NC}"
    else
        echo -e "${GREEN}✓ All tables are up to date${NC}"
    fi
    exit 0
else
    echo -e "${YELLOW}⚠️  Some operations failed. Check errors above.${NC}"
    exit 1
fi
