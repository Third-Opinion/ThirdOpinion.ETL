#!/bin/bash

# Deploy all DDL files from ./v2/ddl to Redshift (TEST database)
# This script executes each SQL file to create tables in Redshift
# Version: v2 - Uses "test" database instead of "dev"
# Version: v2.1 - Parallelized DDL execution for improved performance
# Version: v2.2 - Added better error handling for polling phase

set -e  # Exit on error (but we'll handle errors in the loop)

# Configuration
AWS_PROFILE="${AWS_PROFILE:-to-prod-admin}"
CLUSTER_ID="prod-redshift-main-ue2"
DATABASE_NAME="test"  # v2 uses "test" database
SECRET_ARN="arn:aws:secretsmanager:us-east-2:442042533707:secret:redshift!prod-redshift-main-ue2-awsuser-yp5Lq4"
REGION="us-east-2"
DDL_DIR="${DDL_DIR:-./v2/ddl}"  # v2 DDL directory
TABLE_POSTFIX="${TABLE_POSTFIX:-}"  # Optional postfix for table names (e.g., "_v2")

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}📊 DEPLOYING V2 DDL TO REDSHIFT (TEST DATABASE)${NC}"
echo -e "${BLUE}========================================================================${NC}"
echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo -e "  AWS Profile: ${AWS_PROFILE}"
echo -e "  Cluster ID: ${CLUSTER_ID}"
echo -e "  Database: ${DATABASE_NAME} ${YELLOW}(v2 uses 'test' database)${NC}"
echo -e "  Region: ${REGION}"
echo -e "  DDL Directory: ${DDL_DIR}"
if [ -n "$TABLE_POSTFIX" ]; then
    echo -e "  Table Postfix: ${TABLE_POSTFIX} ${YELLOW}(tables will be created with this suffix)${NC}"
else
    echo -e "  Table Postfix: ${YELLOW}(none - using base table names)${NC}"
fi
echo ""

# Check if DDL directory exists
if [ ! -d "$DDL_DIR" ]; then
    echo -e "${RED}❌ Error: DDL directory not found: $DDL_DIR${NC}"
    exit 1
fi

# Determine which files to deploy
if [ "${OBSERVATION_ONLY:-false}" = "true" ]; then
    SQL_FILES=("$DDL_DIR"/observation*.sql "$DDL_DIR"/codes.sql "$DDL_DIR"/categories.sql "$DDL_DIR"/interpretations.sql)
    echo -e "${YELLOW}   Filter: Observation tables only${NC}"
else
    SQL_FILES=("$DDL_DIR"/*.sql)
fi

# Count SQL files
SQL_FILE_COUNT=${#SQL_FILES[@]}

if [ "$SQL_FILE_COUNT" -eq 0 ]; then
    echo -e "${RED}❌ Error: No SQL files found${NC}"
    exit 1
fi

echo -e "${GREEN}Found $SQL_FILE_COUNT SQL files to deploy${NC}"
echo ""

# Track progress
DEPLOYED=0
FAILED=0
MIGRATED=0

# Parallel execution tracking
declare -A PENDING_STATEMENTS  # statement_id -> "table_name|operation|description"
declare -A COMPLETED_STATEMENTS  # statement_id -> status

# ============================================================================
# PARALLEL EXECUTION FUNCTIONS
# ============================================================================

# Submit SQL statement without waiting (returns statement ID)
submit_sql() {
    local sql="$1"
    local description="$2"

    # Use timeout to prevent hanging (30 seconds should be plenty for API call)
    local temp_err=$(mktemp 2>/dev/null || echo "/tmp/aws_err_$$")
    
    # Execute AWS CLI command with proper variable expansion
    local statement_id=$(timeout 30 env AWS_PROFILE="$AWS_PROFILE" aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE_NAME" \
        --secret-arn "$SECRET_ARN" \
        --sql "$sql" \
        --region "$REGION" \
        --query "Id" \
        --output text 2>"$temp_err")
    local exit_code=$?
    local error_output=$(cat "$temp_err" 2>/dev/null || echo "")
    rm -f "$temp_err"

    if [ $exit_code -eq 124 ]; then
        echo "TIMEOUT" >&2
        echo -e "${RED}   ❌ Timeout submitting: $description${NC}" >&2
        if [ -n "$error_output" ]; then
            echo -e "${RED}   Last output: $error_output${NC}" >&2
        fi
        return 1
    elif [ $exit_code -ne 0 ]; then
        echo -e "${RED}   ❌ Failed to submit: $description${NC}" >&2
        if [ -n "$statement_id" ] && [ "$statement_id" != "$error_output" ]; then
            echo -e "${RED}   Response: $statement_id${NC}" >&2
        fi
        if [ -n "$error_output" ]; then
            echo -e "${RED}   Error: $error_output${NC}" >&2
        fi
        echo -e "${RED}   Exit code: $exit_code${NC}" >&2
        return 1
    fi

    # Check if we got a valid statement ID (UUID format)
    if [ -z "$statement_id" ]; then
        echo -e "${RED}   ❌ Empty statement ID returned${NC}" >&2
        if [ -n "$error_output" ]; then
            echo -e "${RED}   Error output: $error_output${NC}" >&2
        fi
        return 1
    fi
    
    if [[ ! "$statement_id" =~ ^[a-f0-9-]{36}$ ]]; then
        echo -e "${RED}   ❌ Invalid statement ID format: '$statement_id'${NC}" >&2
        if [ -n "$error_output" ]; then
            echo -e "${RED}   Error output: $error_output${NC}" >&2
        fi
        return 1
    fi

    echo "$statement_id"
    return 0
}

# Poll a single statement until completion
poll_statement() {
    local statement_id="$1"
    local max_attempts="${2:-60}"  # Default 60 attempts (2 minutes)
    local attempts=0
    local status="SUBMITTED"

    while [ "$status" != "FINISHED" ] && [ "$status" != "FAILED" ] && [ "$status" != "ABORTED" ] && [ $attempts -lt $max_attempts ]; do
        sleep 2
        status=$(AWS_PROFILE=$AWS_PROFILE aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query "Status" \
            --output text 2>/dev/null || echo "ERROR")
        ((attempts++))
    done

    echo "$status"
    return 0
}

# Poll all pending statements in parallel
poll_all_statements() {
    # Disable exit-on-error for this function to handle errors gracefully
    set +e
    
    local all_finished=0
    local max_attempts=60
    local attempts=0
    local error_count=0
    local max_errors=10  # Allow some errors but not too many
    local pending_count=${#PENDING_STATEMENTS[@]}

    if [ $pending_count -eq 0 ]; then
        echo -e "${YELLOW}   ⚠️  No pending statements to poll${NC}"
        return 0
    fi

    echo -e "${BLUE}   ⏳ Polling ${pending_count} statement(s) for completion...${NC}"

    while [ $all_finished -eq 0 ] && [ $attempts -lt $max_attempts ] && [ $error_count -lt $max_errors ]; do
        all_finished=1
        local jobs_started=0
        
        # Check all statements in parallel
        for statement_id in "${!PENDING_STATEMENTS[@]}"; do
            if [ -z "${COMPLETED_STATEMENTS[$statement_id]}" ]; then
                # Check status in background
                (
                    temp_status_file="/tmp/poll_status_${statement_id}_$$"
                    status=$(env AWS_PROFILE="$AWS_PROFILE" aws redshift-data describe-statement \
                        --id "$statement_id" \
                        --region "$REGION" \
                        --query "Status" \
                        --output text 2>/dev/null || echo "ERROR")
                    echo "$status" > "$temp_status_file" 2>/dev/null
                ) &
                ((jobs_started++))
            fi
        done
        
        # Only wait if we started any background jobs
        if [ $jobs_started -gt 0 ]; then
            sleep 2
            # Wait for all status checks to complete (with timeout protection)
            wait 2>/dev/null || {
                echo -e "${YELLOW}   ⚠️  Warning: Some background jobs may have failed${NC}" >&2
                ((error_count++))
            }
        fi
        
        # Collect results from parallel checks
        local new_completions=0
        for statement_id in "${!PENDING_STATEMENTS[@]}"; do
            if [ -z "${COMPLETED_STATEMENTS[$statement_id]}" ]; then
                temp_status_file="/tmp/poll_status_${statement_id}_$$"
                if [ -f "$temp_status_file" ]; then
                    status=$(cat "$temp_status_file" 2>/dev/null || echo "ERROR")
                    rm -f "$temp_status_file" 2>/dev/null
                    
                    if [ "$status" = "FINISHED" ] || [ "$status" = "FAILED" ] || [ "$status" = "ABORTED" ]; then
                        COMPLETED_STATEMENTS[$statement_id]="$status"
                        ((new_completions++))
                        local info="${PENDING_STATEMENTS[$statement_id]}"
                        IFS='|' read -r table_name operation description <<< "$info"
                        if [ "$status" = "FINISHED" ]; then
                            echo -e "${GREEN}   ✓ Statement completed: ${table_name}${NC}"
                        else
                            echo -e "${RED}   ✗ Statement ${status}: ${table_name}${NC}"
                        fi
                    elif [ "$status" = "ERROR" ]; then
                        echo -e "${YELLOW}   ⚠️  Error checking status for ${statement_id:0:8}...${NC}" >&2
                        all_finished=0
                        ((error_count++))
                    else
                        all_finished=0
                    fi
                else
                    all_finished=0
                fi
            fi
        done

        ((attempts++))
        local completed_count=${#COMPLETED_STATEMENTS[@]}
        local total_count=${#PENDING_STATEMENTS[@]}
        
        if [ $all_finished -eq 0 ]; then
            echo -ne "   Progress: $completed_count/$total_count completed (attempt $attempts/$max_attempts)\r"
        elif [ $new_completions -gt 0 ]; then
            # Show progress even if all finished
            echo -e "${GREEN}   Progress: $completed_count/$total_count completed${NC}"
        fi
    done

    echo ""  # New line after progress updates
    
    # Show final status
    local final_completed=${#COMPLETED_STATEMENTS[@]}
    local final_total=${#PENDING_STATEMENTS[@]}
    if [ $final_completed -eq $final_total ]; then
        echo -e "${GREEN}   ✅ All ${final_total} statement(s) completed${NC}"
    else
        local remaining=$((final_total - final_completed))
        echo -e "${YELLOW}   ⚠️  ${remaining} statement(s) still pending after polling${NC}"
    fi
    
    # Check if we exited due to too many errors
    if [ $error_count -ge $max_errors ]; then
        echo -e "${RED}   ❌ Too many errors encountered during polling (${error_count}). Stopping.${NC}" >&2
        return 1
    fi
    
    # Check if we timed out
    if [ $attempts -ge $max_attempts ] && [ $all_finished -eq 0 ]; then
        echo -e "${YELLOW}   ⚠️  Timeout reached after ${max_attempts} attempts. Some statements may still be pending.${NC}" >&2
        local remaining=0
        for statement_id in "${!PENDING_STATEMENTS[@]}"; do
            if [ -z "${COMPLETED_STATEMENTS[$statement_id]}" ]; then
                ((remaining++))
            fi
        done
        if [ $remaining -gt 0 ]; then
            echo -e "${YELLOW}   ${remaining} statement(s) still pending. Check AWS console for status.${NC}" >&2
        fi
        return 1
    fi
    
    # Re-enable exit-on-error (caller will handle if needed)
    # Don't set -e here, let the caller decide
    return 0
}

# Execute SQL and wait for completion (kept for backward compatibility with migrations)
execute_sql() {
    local sql="$1"
    local description="$2"

    local statement_id=$(submit_sql "$sql" "$description")
    if [ $? -ne 0 ]; then
        return 1
    fi

    local status=$(poll_statement "$statement_id" 30)

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

# ============================================================================
# PHASE 1: Check table existence (PARALLEL)
# ============================================================================
echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}📋 PHASE 1: Checking table existence (parallel)${NC}"
echo -e "${BLUE}========================================================================${NC}"
echo ""

declare -A TABLE_EXISTS  # table_name -> "true" or "false"
declare -A TABLE_FILES   # table_name -> sql_file_path
declare -A EXISTENCE_STATEMENTS  # statement_id -> table_name

# Prepare table information
for sql_file in "${SQL_FILES[@]}"; do
    BASE_TABLE_NAME=$(basename "$sql_file" .sql)
    if [ -n "$TABLE_POSTFIX" ]; then
        TABLE_NAME="${BASE_TABLE_NAME}${TABLE_POSTFIX}"
    else
        TABLE_NAME="$BASE_TABLE_NAME"
    fi
    TABLE_FILES[$TABLE_NAME]="$sql_file"
done

# Submit all table existence queries in parallel
echo -e "${BLUE}Submitting ${#TABLE_FILES[@]} table existence check(s)...${NC}"
submitted_count=0
failed_submissions=0

# Test AWS CLI connectivity first (non-blocking, skip if it takes too long)
# Use set +e to prevent exit on error
set +e
echo -e "${BLUE}   Testing AWS CLI connectivity...${NC}"
test_output=$(timeout 5 env AWS_PROFILE="$AWS_PROFILE" aws sts get-caller-identity --region "$REGION" 2>&1)
test_exit=$?
set -e
if [ $test_exit -eq 124 ]; then
    echo -e "${YELLOW}   ⚠️  AWS CLI test timed out, continuing anyway...${NC}"
elif [ $test_exit -ne 0 ]; then
    echo -e "${YELLOW}   ⚠️  AWS CLI test failed (exit code: $test_exit), continuing anyway...${NC}"
    echo -e "${YELLOW}      Output: $test_output${NC}"
else
    echo -e "${GREEN}   ✓ AWS CLI connectivity OK${NC}"
fi

# Submit all table existence checks in parallel using background processes
set +e
echo -e "${BLUE}   Submitting all checks in parallel...${NC}"

# Submit all checks in parallel as background jobs
for table_name in "${!TABLE_FILES[@]}"; do
    (
        sql="SELECT COUNT(DISTINCT table_name) FROM svv_columns WHERE table_schema = 'public' AND table_name = '$table_name';"
        temp_file="/tmp/table_check_${table_name}_$$"
        temp_err="${temp_file}.err"
        
        # Submit directly using AWS CLI in background
        statement_id=$(timeout 30 env AWS_PROFILE="$AWS_PROFILE" aws redshift-data execute-statement \
            --cluster-identifier "$CLUSTER_ID" \
            --database "$DATABASE_NAME" \
            --secret-arn "$SECRET_ARN" \
            --sql "$sql" \
            --region "$REGION" \
            --query "Id" \
            --output text 2>"$temp_err")
        submit_exit_code=$?
        submit_error=$(cat "$temp_err" 2>/dev/null || echo "")
        rm -f "$temp_err"
        
        # Write result to temp file
        echo "${submit_exit_code}|${statement_id}|${submit_error}" > "$temp_file"
    ) &
    echo -e "${BLUE}   Started check for: ${table_name}${NC}"
done

# Wait for all background jobs to complete
echo -e "${BLUE}   Waiting for all submissions to complete...${NC}"
wait

# Collect results from temp files
echo -e "${BLUE}   Collecting results...${NC}"
for table_name in "${!TABLE_FILES[@]}"; do
    temp_file="/tmp/table_check_${table_name}_$$"
    if [ -f "$temp_file" ]; then
        IFS='|' read -r submit_exit_code statement_id submit_error < "$temp_file"
        rm -f "$temp_file"
        
        if [ $submit_exit_code -eq 0 ] && [ -n "$statement_id" ] && [[ "$statement_id" =~ ^[a-f0-9-]{36}$ ]]; then
            EXISTENCE_STATEMENTS[$statement_id]="$table_name"
            ((submitted_count++))
            echo -e "${GREEN}      ✓ ${table_name} submitted (ID: ${statement_id:0:8}...)${NC}"
        else
            ((failed_submissions++))
            echo -e "${RED}      ❌ Failed to submit check for ${table_name}${NC}"
            echo -e "${RED}         Exit code: $submit_exit_code${NC}"
            if [ -n "$statement_id" ] && [ "$statement_id" != "$submit_error" ]; then
                echo -e "${RED}         Response: $statement_id${NC}"
            fi
            if [ -n "$submit_error" ]; then
                echo -e "${RED}         Error: $submit_error${NC}"
            fi
        fi
    else
        ((failed_submissions++))
        echo -e "${RED}      ❌ No result file for ${table_name}${NC}"
    fi
done
set -e  # Re-enable exit on error
echo -e "${BLUE}   Submitted: $submitted_count, Failed: $failed_submissions${NC}"

# Wait for all existence checks to complete
if [ ${#EXISTENCE_STATEMENTS[@]} -gt 0 ]; then
    echo -e "${BLUE}Waiting for ${#EXISTENCE_STATEMENTS[@]} check(s) to complete...${NC}"
    
    all_finished=0
    attempts=0
    max_attempts=30
    declare -A EXISTENCE_RESULTS  # statement_id -> status
    
    set +e  # Don't exit on error during polling
    while [ $all_finished -eq 0 ] && [ $attempts -lt $max_attempts ]; do
        all_finished=1
        sleep 2
        
        # Check all statements in parallel
        for statement_id in "${!EXISTENCE_STATEMENTS[@]}"; do
            if [ -z "${EXISTENCE_RESULTS[$statement_id]}" ]; then
                # Check status in background
                (
                    temp_status_file="/tmp/status_${statement_id}_$$"
                    status_output=$(env AWS_PROFILE="$AWS_PROFILE" aws redshift-data describe-statement \
                        --id "$statement_id" \
                        --region "$REGION" \
                        --query "Status" \
                        --output text 2>&1)
                    status=$?
                    echo "${status}|${status_output}" > "$temp_status_file"
                ) &
            fi
        done
        
        # Wait for all status checks to complete
        wait
        
        # Collect results from parallel checks
        for statement_id in "${!EXISTENCE_STATEMENTS[@]}"; do
            if [ -z "${EXISTENCE_RESULTS[$statement_id]}" ]; then
                temp_status_file="/tmp/status_${statement_id}_$$"
                if [ -f "$temp_status_file" ]; then
                    IFS='|' read -r status status_output < "$temp_status_file"
                    rm -f "$temp_status_file"
                    
                    if [ $status -ne 0 ]; then
                        echo -e "${YELLOW}   ⚠️  Error checking status for ${statement_id:0:8}...: $status_output${NC}"
                        all_finished=0
                    else
                        if [ "$status_output" = "FINISHED" ] || [ "$status_output" = "FAILED" ] || [ "$status_output" = "ABORTED" ]; then
                            EXISTENCE_RESULTS[$statement_id]="$status_output"
                            table_name="${EXISTENCE_STATEMENTS[$statement_id]}"
                            echo -e "${BLUE}   ✓ ${table_name} check ${status_output} (ID: ${statement_id:0:8}...)${NC}"
                        else
                            all_finished=0
                        fi
                    fi
                else
                    all_finished=0
                fi
            fi
        done
        
        ((attempts++))
        if [ $all_finished -eq 0 ]; then
            completed=${#EXISTENCE_RESULTS[@]}
            total=${#EXISTENCE_STATEMENTS[@]}
            echo -ne "   Progress: $completed/$total completed (attempt $attempts/$max_attempts)\r"
        fi
    done
    set -e  # Re-enable exit on error
    echo ""  # New line after progress
    
    # Check if we timed out
    if [ $attempts -ge $max_attempts ] && [ $all_finished -eq 0 ]; then
        echo -e "${YELLOW}   ⚠️  Timeout reached. Some checks may not have completed.${NC}"
        for statement_id in "${!EXISTENCE_STATEMENTS[@]}"; do
            if [ -z "${EXISTENCE_RESULTS[$statement_id]}" ]; then
                table_name="${EXISTENCE_STATEMENTS[$statement_id]}"
                echo -e "${YELLOW}      Still pending: ${table_name} (ID: ${statement_id:0:8}...)${NC}"
            fi
        done
    fi
    
    # Process results in parallel
    echo -e "${BLUE}Processing results in parallel...${NC}"
    processed_count=0
    set +e  # Don't exit on error during result processing
    
    # Get all results in parallel
    for statement_id in "${!EXISTENCE_STATEMENTS[@]}"; do
        table_name="${EXISTENCE_STATEMENTS[$statement_id]}"
        status="${EXISTENCE_RESULTS[$statement_id]}"
        
        if [ -z "$status" ]; then
            echo -e "${YELLOW}   ⚠️  ${table_name}: No status available (ID: ${statement_id:0:8}...)${NC}"
            TABLE_EXISTS[$table_name]="false"
            continue
        fi
        
        if [ "$status" = "FINISHED" ]; then
            # Get result in background
            (
                temp_result_file="/tmp/result_${statement_id}_$$"
                count_output=$(env AWS_PROFILE="$AWS_PROFILE" aws redshift-data get-statement-result \
                    --id "$statement_id" \
                    --region "$REGION" \
                    --query "Records[0][0].longValue" \
                    --output text 2>&1)
                get_result_exit_code=$?
                echo "${get_result_exit_code}|${count_output}" > "$temp_result_file"
            ) &
        elif [ "$status" = "FAILED" ] || [ "$status" = "ABORTED" ]; then
            # Get error message in background
            (
                temp_error_file="/tmp/error_${statement_id}_$$"
                error_msg=$(env AWS_PROFILE="$AWS_PROFILE" aws redshift-data describe-statement \
                    --id "$statement_id" \
                    --region "$REGION" \
                    --query "Error" \
                    --output text 2>/dev/null || echo "Could not retrieve error")
                echo "${status}|${error_msg}" > "$temp_error_file"
            ) &
        else
            # Unexpected status - handle immediately
            echo -e "${YELLOW}   ⚠️  ${table_name}: Unexpected status '${status}'${NC}"
            TABLE_EXISTS[$table_name]="false"
        fi
    done
    
    # Wait for all result retrievals to complete
    wait
    
    # Collect and process all results
    for statement_id in "${!EXISTENCE_STATEMENTS[@]}"; do
        table_name="${EXISTENCE_STATEMENTS[$statement_id]}"
        status="${EXISTENCE_RESULTS[$statement_id]}"
        
        if [ "$status" = "FINISHED" ]; then
            temp_result_file="/tmp/result_${statement_id}_$$"
            if [ -f "$temp_result_file" ]; then
                IFS='|' read -r get_result_exit_code count_output < "$temp_result_file"
                rm -f "$temp_result_file"
                
                if [ $get_result_exit_code -eq 0 ] && [ -n "$count_output" ]; then
                    count="$count_output"
                    if [ "$count" = "1" ]; then
                        TABLE_EXISTS[$table_name]="true"
                        echo -e "${GREEN}   ✓ ${table_name} exists${NC}"
                    else
                        TABLE_EXISTS[$table_name]="false"
                        echo -e "${YELLOW}   ○ ${table_name} does not exist (count: $count)${NC}"
                    fi
                    ((processed_count++))
                else
                    echo -e "${RED}   ❌ Failed to get result for ${table_name}${NC}"
                    echo -e "${RED}      Error: $count_output${NC}"
                    TABLE_EXISTS[$table_name]="false"
                fi
            fi
        elif [ "$status" = "FAILED" ] || [ "$status" = "ABORTED" ]; then
            temp_error_file="/tmp/error_${statement_id}_$$"
            if [ -f "$temp_error_file" ]; then
                IFS='|' read -r error_status error_msg < "$temp_error_file"
                rm -f "$temp_error_file"
                echo -e "${RED}   ❌ ${table_name}: Check ${error_status} (ID: ${statement_id:0:8}...)${NC}"
                if [ "$error_msg" != "None" ] && [ -n "$error_msg" ] && [ "$error_msg" != "Could not retrieve error" ]; then
                    echo -e "${RED}      Error: $error_msg${NC}"
                fi
            else
                echo -e "${RED}   ❌ ${table_name}: Check ${status}${NC}"
            fi
            TABLE_EXISTS[$table_name]="false"
        fi
    done
    set -e  # Re-enable exit on error
    echo -e "${BLUE}   Processed: $processed_count/${#EXISTENCE_STATEMENTS[@]} results${NC}"
else
    echo -e "${YELLOW}   ⚠️  No table existence checks were submitted successfully${NC}"
    if [ $failed_submissions -gt 0 ]; then
        echo -e "${YELLOW}      $failed_submissions submission(s) failed${NC}"
    fi
    echo -e "${YELLOW}   Continuing with table creation (assuming tables don't exist)...${NC}"
    # Initialize all tables as non-existent so they will be created
    for table_name in "${!TABLE_FILES[@]}"; do
        TABLE_EXISTS[$table_name]="false"
    done
fi
echo ""

# ============================================================================
# PHASE 2: Prepare and submit all DDL operations in parallel
# ============================================================================
echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}📤 PHASE 2: Preparing DDL operations${NC}"
echo -e "${BLUE}========================================================================${NC}"
echo ""

# Clear pending statements
PENDING_STATEMENTS=()
COMPLETED_STATEMENTS=()

# Step 1: Analyze all tables in parallel to determine what operations are needed
echo -e "${BLUE}Analyzing all tables to determine required operations...${NC}"
declare -A DDL_OPERATIONS  # table_name -> "operation|sql|description" (multiple entries per table)

# Process all tables in parallel to collect operations
set +e
for table_name in "${!TABLE_FILES[@]}"; do
    (
        sql_file="${TABLE_FILES[$table_name]}"
        BASE_TABLE_NAME=$(basename "$sql_file" .sql)
        temp_file="/tmp/ddl_ops_${table_name}_$$"
        
        if [ "${TABLE_EXISTS[$table_name]}" = "true" ]; then
            # Table exists - analyze schema changes
            current_schema=$(get_current_columns "$table_name")
            desired_schema=$(parse_ddl_columns "$sql_file")
            
            if [ -z "$current_schema" ] || [ -z "$desired_schema" ]; then
                echo "SKIP|Could not parse schemas" > "$temp_file"
            else
                # Find new columns
                while IFS='|' read -r col_name col_type; do
                    if ! echo "$current_schema" | grep -qi "^${col_name}|"; then
                        alter_sql="ALTER TABLE public.${table_name} ADD COLUMN ${col_name} ${col_type};"
                        echo "ALTER|${alter_sql}|ADD COLUMN $col_name" >> "$temp_file"
                    fi
                done <<< "$desired_schema"
                
                # Check for VARCHAR size increases
                while IFS='|' read -r col_name col_type; do
                    if echo "$col_type" | grep -qi "VARCHAR"; then
                        desired_size=$(echo "$col_type" | sed -E 's/VARCHAR\(([0-9]+)\)/\1/')
                        current_line=$(echo "$current_schema" | grep -i "^${col_name}|")
                        if [ -n "$current_line" ]; then
                            current_size=$(echo "$current_line" | cut -d'|' -f3)
                            if [[ "$desired_size" =~ ^[0-9]+$ ]] && [[ "$current_size" =~ ^[0-9]+$ ]]; then
                                if [ "$desired_size" -gt "$current_size" ]; then
                                    alter_sql="ALTER TABLE public.${table_name} ALTER COLUMN ${col_name} TYPE VARCHAR(${desired_size});"
                                    echo "ALTER|${alter_sql}|ALTER COLUMN $col_name TYPE" >> "$temp_file"
                                fi
                            fi
                        fi
                    fi
                done <<< "$desired_schema"
                
                # If no operations, mark as no changes needed
                if [ ! -s "$temp_file" ]; then
                    echo "NONE|No schema changes needed" > "$temp_file"
                fi
            fi
        else
            # Table doesn't exist - prepare CREATE TABLE
            SQL_CONTENT=$(cat "$sql_file")
            if [ -n "$TABLE_POSTFIX" ]; then
                SQL_CONTENT=$(echo "$SQL_CONTENT" | sed "s/CREATE TABLE IF NOT EXISTS public\.${BASE_TABLE_NAME}/CREATE TABLE IF NOT EXISTS public.${TABLE_NAME}/g")
                SQL_CONTENT=$(echo "$SQL_CONTENT" | sed "s/CREATE TABLE public\.${BASE_TABLE_NAME}/CREATE TABLE public.${TABLE_NAME}/g")
                SQL_CONTENT=$(echo "$SQL_CONTENT" | sed "s/-- Table: ${BASE_TABLE_NAME}/-- Table: ${TABLE_NAME}/g")
            fi
            echo "CREATE|${SQL_CONTENT}|CREATE TABLE" > "$temp_file"
        fi
    ) &
done
wait
set -e

# Step 2: Collect all operations and submit them all in parallel
echo -e "${BLUE}Collecting operations and submitting in parallel...${NC}"
set +e
for table_name in "${!TABLE_FILES[@]}"; do
    temp_file="/tmp/ddl_ops_${table_name}_$$"
    if [ -f "$temp_file" ]; then
        while IFS='|' read -r operation sql description; do
            if [ "$operation" = "SKIP" ] || [ "$operation" = "NONE" ]; then
                if [ "$operation" = "NONE" ]; then
                    echo -e "${GREEN}   ✓ ${table_name}: $description${NC}"
                    TABLE_EXISTS[$table_name]="migrated"
                else
                    echo -e "${YELLOW}   ⚠️  ${table_name}: $description${NC}"
                fi
            else
                # Submit operation in background
                (
                    temp_result="/tmp/ddl_submit_${table_name}_${RANDOM}_$$"
                    statement_id=$(timeout 30 env AWS_PROFILE="$AWS_PROFILE" aws redshift-data execute-statement \
                        --cluster-identifier "$CLUSTER_ID" \
                        --database "$DATABASE_NAME" \
                        --secret-arn "$SECRET_ARN" \
                        --sql "$sql" \
                        --region "$REGION" \
                        --query "Id" \
                        --output text 2>"${temp_result}.err")
                    submit_exit=$?
                    submit_error=$(cat "${temp_result}.err" 2>/dev/null || echo "")
                    rm -f "${temp_result}.err"
                    
                    if [ $submit_exit -eq 0 ] && [ -n "$statement_id" ] && [[ "$statement_id" =~ ^[a-f0-9-]{36}$ ]]; then
                        echo "${statement_id}|${table_name}|${operation}|${description}" > "$temp_result"
                    else
                        echo "FAILED|${table_name}|${operation}|${description}|${submit_error}" > "$temp_result"
                    fi
                ) &
                
                if [ "$operation" = "CREATE" ]; then
                    echo -e "${BLUE}   📝 Submitting CREATE TABLE for: ${table_name}${NC}"
                else
                    echo -e "${BLUE}   📝 Submitting ALTER for: ${table_name} - $description${NC}"
                fi
            fi
        done < "$temp_file"
        rm -f "$temp_file"
    fi
done

# Wait for all submissions
echo -e "${BLUE}Waiting for all DDL submissions to complete...${NC}"
wait

# Collect submission results
for result_file in /tmp/ddl_submit_*_$$; do
    if [ -f "$result_file" ]; then
        IFS='|' read -r statement_id table_name operation description < "$result_file"
        if [ "$statement_id" != "FAILED" ]; then
            PENDING_STATEMENTS[$statement_id]="${table_name}|${operation}|${description}"
            echo -e "${GREEN}      ✓ ${table_name} submitted (ID: ${statement_id:0:8}...)${NC}"
        else
            IFS='|' read -r _ table_name operation description error_msg < "$result_file"
            echo -e "${RED}      ❌ Failed to submit ${table_name} - $description${NC}"
            echo -e "${RED}         Error: $error_msg${NC}"
            ((FAILED++))
        fi
        rm -f "$result_file"
    fi
done
set -e
echo ""

# ============================================================================
# PHASE 3: Wait for all statements to complete
# ============================================================================
if [ ${#PENDING_STATEMENTS[@]} -gt 0 ]; then
    echo -e "${BLUE}========================================================================${NC}"
    echo -e "${BLUE}⏳ PHASE 3: Waiting for ${#PENDING_STATEMENTS[@]} statement(s) to complete${NC}"
    echo -e "${BLUE}========================================================================${NC}"
    echo ""

    # Disable exit-on-error during polling to handle errors gracefully
    set +e
    poll_all_statements
    poll_exit_code=$?
    set -e  # Re-enable exit-on-error
    
    if [ $poll_exit_code -ne 0 ]; then
        echo -e "${YELLOW}   ⚠️  Polling encountered errors but continuing to process available results...${NC}"
    fi

    # ============================================================================
    # PHASE 4: Process results
    # ============================================================================
    echo -e "${BLUE}========================================================================${NC}"
    echo -e "${BLUE}📊 PHASE 4: Processing results${NC}"
    echo -e "${BLUE}========================================================================${NC}"
    echo ""

    # Disable exit-on-error during Phase 4 to handle all statements gracefully
    set +e

    # First, check status of any statements that weren't polled
    echo -e "${BLUE}   Checking final status of all statements...${NC}"
    for statement_id in "${!PENDING_STATEMENTS[@]}"; do
        if [ -z "${COMPLETED_STATEMENTS[$statement_id]}" ]; then
            # Statement wasn't polled, check its status now
            status=$(env AWS_PROFILE="$AWS_PROFILE" aws redshift-data describe-statement \
                --id "$statement_id" \
                --region "$REGION" \
                --query "Status" \
                --output text 2>/dev/null || echo "ERROR")
            if [ "$status" = "FINISHED" ] || [ "$status" = "FAILED" ] || [ "$status" = "ABORTED" ]; then
                COMPLETED_STATEMENTS[$statement_id]="$status"
            fi
        fi
    done

    # Now process all results
    for statement_id in "${!PENDING_STATEMENTS[@]}"; do
        status="${COMPLETED_STATEMENTS[$statement_id]}"
        info="${PENDING_STATEMENTS[$statement_id]}"
        
        # Parse info safely, handling empty fields
        IFS='|' read -r table_name operation description <<< "$info"
        # Ensure variables are set even if parsing fails
        table_name="${table_name:-unknown}"
        operation="${operation:-UNKNOWN}"
        description="${description:-}"

        if [ "$status" = "FINISHED" ]; then
            if [ "$operation" = "CREATE" ]; then
                echo -e "${GREEN}   ✅ Created table: $table_name${NC}"
                DEPLOYED=$((DEPLOYED + 1))
            else
                if [ -n "$description" ]; then
                    echo -e "${GREEN}   ✅ Migration: $table_name - $description${NC}"
                else
                    echo -e "${GREEN}   ✅ Migration: $table_name${NC}"
                fi
                # Count as migrated if this is the first successful migration for this table
                if [ "$operation" = "ALTER" ]; then
                    # We'll count migrations after all ALTERs are done
                    :
                fi
            fi
        elif [ "$status" = "FAILED" ] || [ "$status" = "ABORTED" ]; then
            if [ -n "$description" ]; then
                echo -e "${RED}   ❌ Failed: $table_name - $description (Status: ${status})${NC}"
            else
                echo -e "${RED}   ❌ Failed: $table_name (Status: ${status})${NC}"
            fi
            error_msg=$(AWS_PROFILE=$AWS_PROFILE aws redshift-data describe-statement \
                --id "$statement_id" \
                --region "$REGION" \
                --query "Error" \
                --output text 2>/dev/null || echo "No error details")
            if [ "$error_msg" != "None" ] && [ "$error_msg" != "No error details" ] && [ -n "$error_msg" ]; then
                echo -e "${RED}      Error: $error_msg${NC}"
            fi
            FAILED=$((FAILED + 1))
        else
            # Status is unknown or still pending
            if [ -n "$description" ]; then
                echo -e "${YELLOW}   ⚠️  Unknown/Pending: $table_name - $description (Status: ${status:-UNKNOWN})${NC}"
            else
                echo -e "${YELLOW}   ⚠️  Unknown/Pending: $table_name (Status: ${status:-UNKNOWN})${NC}"
            fi
            # Try to get current status one more time
            current_status=$(env AWS_PROFILE="$AWS_PROFILE" aws redshift-data describe-statement \
                --id "$statement_id" \
                --region "$REGION" \
                --query "Status" \
                --output text 2>/dev/null || echo "ERROR")
            if [ "$current_status" = "FINISHED" ]; then
                echo -e "${GREEN}      ✓ Actually completed: $table_name${NC}"
                if [ "$operation" = "CREATE" ]; then
                    DEPLOYED=$((DEPLOYED + 1))
                fi
            elif [ "$current_status" = "FAILED" ] || [ "$current_status" = "ABORTED" ]; then
                echo -e "${RED}      ✗ Actually failed: $table_name${NC}"
                error_msg=$(AWS_PROFILE=$AWS_PROFILE aws redshift-data describe-statement \
                    --id "$statement_id" \
                    --region "$REGION" \
                    --query "Error" \
                    --output text 2>/dev/null || echo "No error details")
                if [ "$error_msg" != "None" ] && [ "$error_msg" != "No error details" ] && [ -n "$error_msg" ]; then
                    echo -e "${RED}         Error: $error_msg${NC}"
                fi
                FAILED=$((FAILED + 1))
            else
                echo -e "${YELLOW}      Still in progress or error checking status${NC}"
                FAILED=$((FAILED + 1))
            fi
        fi
    done

    # Re-enable exit-on-error after Phase 4
    set -e

    # Count migrated tables (tables that had at least one successful ALTER, or no changes needed)
    for table_name in "${!TABLE_FILES[@]}"; do
        if [ "${TABLE_EXISTS[$table_name]}" = "migrated" ]; then
            ((MIGRATED++))
        elif [ "${TABLE_EXISTS[$table_name]}" = "pending_migration" ]; then
            # Check if at least one ALTER statement succeeded
            local has_successful_alter=0
            for statement_id in "${!PENDING_STATEMENTS[@]}"; do
                info="${PENDING_STATEMENTS[$statement_id]}"
                if [[ "$info" == "${table_name}|ALTER|"* ]]; then
                    if [ "${COMPLETED_STATEMENTS[$statement_id]}" = "FINISHED" ]; then
                        has_successful_alter=1
                        break
                    fi
                fi
            done
            if [ $has_successful_alter -eq 1 ]; then
                ((MIGRATED++))
            fi
        fi
    done
else
    echo -e "${GREEN}No DDL operations needed - all tables are up to date${NC}"
fi

echo ""

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

