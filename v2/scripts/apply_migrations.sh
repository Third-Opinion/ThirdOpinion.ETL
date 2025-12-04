#!/bin/bash

# Apply database migrations to Redshift
# Migrations are applied in timestamp order and tracked in schema_migrations table
# Version: 1.0

set -e

# Configuration
AWS_PROFILE="${AWS_PROFILE:-to-prod-admin}"
CLUSTER_ID="prod-redshift-main-ue2"
DATABASE_NAME="test"
SECRET_ARN="arn:aws:secretsmanager:us-east-2:442042533707:secret:redshift!prod-redshift-main-ue2-awsuser-yp5Lq4"
REGION="us-east-2"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-./v2/migrations}"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Parse arguments
SPECIFIC_MIGRATION=""
SHOW_STATUS=false
ROLLBACK_MODE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --migration)
            SPECIFIC_MIGRATION="$2"
            shift 2
            ;;
        --status)
            SHOW_STATUS=true
            shift
            ;;
        --rollback)
            ROLLBACK_MODE=true
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --migration FILE    Apply specific migration file"
            echo "  --status            Show migration status"
            echo "  --rollback          Rollback last migration (or specific if --migration provided)"
            echo "  --help              Show this help"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Execute SQL statement and wait for completion
execute_sql() {
    local sql="$1"
    local description="$2"
    
    local statement_id=$(env AWS_PROFILE="$AWS_PROFILE" aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE_NAME" \
        --secret-arn "$SECRET_ARN" \
        --sql "$sql" \
        --region "$REGION" \
        --query "Id" \
        --output text 2>&1)
    
    if [[ ! "$statement_id" =~ ^[a-f0-9-]{36}$ ]]; then
        echo -e "${RED}   ❌ Failed to submit: $description${NC}"
        echo -e "${RED}      Error: $statement_id${NC}"
        return 1
    fi
    
    # Poll for completion
    local status="SUBMITTED"
    local attempts=0
    local max_attempts=60
    
    while [ "$status" != "FINISHED" ] && [ "$status" != "FAILED" ] && [ "$status" != "ABORTED" ] && [ $attempts -lt $max_attempts ]; do
        sleep 2
        status=$(env AWS_PROFILE="$AWS_PROFILE" aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query "Status" \
            --output text 2>/dev/null || echo "ERROR")
        ((attempts++))
    done
    
    if [ "$status" = "FINISHED" ]; then
        return 0
    else
        local error_msg=$(env AWS_PROFILE="$AWS_PROFILE" aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query "Error" \
            --output text 2>/dev/null || echo "Unknown error")
        echo -e "${RED}   ❌ Failed: $description (Status: $status)${NC}"
        echo -e "${RED}      Error: $error_msg${NC}"
        return 1
    fi
}

# Get query result
get_query_result() {
    local statement_id="$1"
    local query="$2"
    
    env AWS_PROFILE="$AWS_PROFILE" aws redshift-data get-statement-result \
        --id "$statement_id" \
        --region "$REGION" \
        --query "$query" \
        --output text 2>/dev/null
}

# Check if migrations table exists
ensure_migrations_table() {
    echo -e "${BLUE}   Checking if schema_migrations table exists...${NC}"
    
    local check_sql="SELECT COUNT(*) FROM information_schema.tables 
                     WHERE table_schema = 'public' 
                     AND table_name = 'schema_migrations';"
    
    local statement_id=$(env AWS_PROFILE="$AWS_PROFILE" aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE_NAME" \
        --secret-arn "$SECRET_ARN" \
        --sql "$check_sql" \
        --region "$REGION" \
        --query "Id" \
        --output text 2>&1)
    
    sleep 3
    
    local count=$(get_query_result "$statement_id" "Records[0][0].longValue")
    
    if [ "$count" != "1" ]; then
        echo -e "${YELLOW}   Creating schema_migrations table...${NC}"
        
        if [ -f "./v2/ddl/schema_migrations.sql" ]; then
            local ddl=$(cat "./v2/ddl/schema_migrations.sql")
            execute_sql "$ddl" "Create schema_migrations table"
            echo -e "${GREEN}   ✅ Migration tracking table created${NC}"
        else
            echo -e "${RED}   ❌ Cannot find schema_migrations.sql DDL file${NC}"
            return 1
        fi
    else
        echo -e "${GREEN}   ✓ Migration tracking table exists${NC}"
    fi
}

# Get list of applied migrations
get_applied_migrations() {
    # Disable exit-on-error for this function
    set +e
    
    local sql="SELECT migration_id FROM public.schema_migrations 
               WHERE status = 'success' 
               ORDER BY migration_id;"
    
    local statement_id=$(env AWS_PROFILE="$AWS_PROFILE" aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE_NAME" \
        --secret-arn "$SECRET_ARN" \
        --sql "$sql" \
        --region "$REGION" \
        --query "Id" \
        --output text 2>&1)
    
    # Check if we got a valid statement ID
    if [[ ! "$statement_id" =~ ^[a-f0-9-]{36}$ ]]; then
        echo ""  # Return empty if query failed
        set -e
        return 0
    fi
    
    sleep 2
    
    # Extract migration IDs from results
    local result=$(env AWS_PROFILE="$AWS_PROFILE" aws redshift-data get-statement-result \
        --id "$statement_id" \
        --region "$REGION" \
        --output json 2>/dev/null | \
        jq -r '.Records[]?[0].stringValue?' 2>/dev/null || echo "")
    
    echo "$result"
    set -e
    return 0
}

# Calculate file checksum
calculate_checksum() {
    local file="$1"
    if command -v sha256sum &> /dev/null; then
        sha256sum "$file" | cut -d' ' -f1
    elif command -v shasum &> /dev/null; then
        shasum -a 256 "$file" | cut -d' ' -f1
    else
        echo "unknown"
    fi
}

# Record migration in tracking table
record_migration() {
    local migration_id="$1"
    local status="$2"
    local execution_time="$3"
    local error_msg="$4"
    local file_path="$5"
    local checksum="$6"
    
    local applied_by=$(env AWS_PROFILE="$AWS_PROFILE" aws sts get-caller-identity \
        --query 'Arn' \
        --output text 2>/dev/null | awk -F'/' '{print $NF}' || echo "unknown")
    
    # Escape single quotes in error message
    error_msg=$(echo "$error_msg" | sed "s/'/''/g")
    
    local sql="INSERT INTO public.schema_migrations 
               (migration_id, applied_at, applied_by, execution_time_seconds, status, error_message, checksum, migration_file_path)
               VALUES 
               ('$migration_id', GETDATE(), '$applied_by', $execution_time, '$status', '$error_msg', '$checksum', '$file_path');"
    
    execute_sql "$sql" "Record migration" >/dev/null 2>&1
}

# Extract Up section from migration file
extract_up_migration() {
    local migration_file="$1"
    
    # Check if file has Down Migration marker
    if grep -q "^-- Down Migration" "$migration_file"; then
        # Use awk to extract content between markers, skipping marker lines and separators
        awk '
            /^-- Up Migration/ { in_up=1; next }
            /^-- Down Migration/ { exit }
            in_up && !/^-- =/ && !/^$/ { print }
        ' "$migration_file"
    else
        # No Down section, treat entire file as Up migration (backward compatible)
        cat "$migration_file"
    fi
}

# Extract Down section from migration file
extract_down_migration() {
    local migration_file="$1"
    
    # Check if file has Down Migration marker
    if grep -q "^-- Down Migration" "$migration_file"; then
        # Use awk to extract content after Down marker, skipping marker lines and separators
        awk '
            /^-- Down Migration/ { in_down=1; next }
            in_down && !/^-- =/ && !/^$/ { print }
        ' "$migration_file"
    else
        # No Down section available
        return 1
    fi
}

# Apply a single migration (Up direction)
apply_migration() {
    local migration_file="$1"
    local migration_id=$(basename "$migration_file" .sql)
    
    echo -e "${BLUE}   Applying: ${migration_id}${NC}"
    
    local start_time=$(date +%s)
    local sql=$(extract_up_migration "$migration_file")
    local checksum=$(calculate_checksum "$migration_file")
    local file_path=$(realpath --relative-to="$(pwd)" "$migration_file" 2>/dev/null || echo "$migration_file")
    
    if execute_sql "$sql" "$migration_id"; then
        local end_time=$(date +%s)
        local execution_time=$((end_time - start_time))
        
        record_migration "$migration_id" "success" "$execution_time" "" "$file_path" "$checksum"
        echo -e "${GREEN}   ✅ Applied successfully (${execution_time}s)${NC}"
        return 0
    else
        local end_time=$(date +%s)
        local execution_time=$((end_time - start_time))
        local error_msg="Migration execution failed"
        
        record_migration "$migration_id" "failed" "$execution_time" "$error_msg" "$file_path" "$checksum"
        echo -e "${RED}   ❌ Migration failed${NC}"
        return 1
    fi
}

# Check if migration is rolled back
is_migration_rolled_back() {
    local migration_id="$1"
    
    set +e
    local sql="SELECT COUNT(*) FROM public.schema_migrations 
               WHERE migration_id = '$migration_id' 
               AND status = 'rolled_back';"
    
    local statement_id=$(env AWS_PROFILE="$AWS_PROFILE" aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE_NAME" \
        --secret-arn "$SECRET_ARN" \
        --sql "$sql" \
        --region "$REGION" \
        --query "Id" \
        --output text 2>&1)
    
    if [[ ! "$statement_id" =~ ^[a-f0-9-]{36}$ ]]; then
        set -e
        return 1
    fi
    
    sleep 2
    
    local count=$(get_query_result "$statement_id" "Records[0][0].longValue")
    set -e
    
    if [ "$count" = "1" ]; then
        return 0  # Already rolled back
    else
        return 1  # Not rolled back
    fi
}

# Rollback a single migration (Down direction)
rollback_migration() {
    local migration_file="$1"
    local migration_id=$(basename "$migration_file" .sql)
    
    echo -e "${YELLOW}   Rolling back: ${migration_id}${NC}"
    
    # Check if already rolled back
    if is_migration_rolled_back "$migration_id"; then
        echo -e "${YELLOW}   ⚠️  Migration ${migration_id} has already been rolled back${NC}"
        return 0
    fi
    
    # Check if Down migration exists
    if ! extract_down_migration "$migration_file" >/dev/null 2>&1; then
        echo -e "${RED}   ❌ No Down migration found for ${migration_id}${NC}"
        echo -e "${YELLOW}   ⚠️  Cannot rollback: migration file doesn't contain '-- Down Migration' section${NC}"
        return 1
    fi
    
    local start_time=$(date +%s)
    local sql=$(extract_down_migration "$migration_file")
    local file_path=$(realpath --relative-to="$(pwd)" "$migration_file" 2>/dev/null || echo "$migration_file")
    
    if execute_sql "$sql" "Rollback: $migration_id"; then
        local end_time=$(date +%s)
        local execution_time=$((end_time - start_time))
        
        # Update migration status to rolled_back
        local applied_by=$(env AWS_PROFILE="$AWS_PROFILE" aws sts get-caller-identity \
            --query 'Arn' \
            --output text 2>/dev/null | awk -F'/' '{print $NF}' || echo "unknown")
        
        local update_sql="UPDATE public.schema_migrations 
                         SET status = 'rolled_back',
                             applied_at = GETDATE(),
                             applied_by = '$applied_by'
                         WHERE migration_id = '$migration_id' 
                         AND status = 'success';"
        
        execute_sql "$update_sql" "Update migration status" >/dev/null 2>&1
        
        echo -e "${GREEN}   ✅ Rolled back successfully (${execution_time}s)${NC}"
        return 0
    else
        local end_time=$(date +%s)
        local execution_time=$((end_time - start_time))
        echo -e "${RED}   ❌ Rollback failed${NC}"
        return 1
    fi
}

# Get last applied migration
get_last_applied_migration() {
    local sql="SELECT migration_id FROM public.schema_migrations 
               WHERE status = 'success' 
               ORDER BY applied_at DESC 
               LIMIT 1;"
    
    local statement_id=$(env AWS_PROFILE="$AWS_PROFILE" aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE_NAME" \
        --secret-arn "$SECRET_ARN" \
        --sql "$sql" \
        --region "$REGION" \
        --query "Id" \
        --output text 2>&1)
    
    if [[ ! "$statement_id" =~ ^[a-f0-9-]{36}$ ]]; then
        return 1
    fi
    
    sleep 2
    
    local result=$(env AWS_PROFILE="$AWS_PROFILE" aws redshift-data get-statement-result \
        --id "$statement_id" \
        --region "$REGION" \
        --output json 2>/dev/null | \
        jq -r '.Records[0]?[0].stringValue?' 2>/dev/null || echo "")
    
    echo "$result"
}

# Show migration status
show_status() {
    echo -e "${BLUE}========================================================================${NC}"
    echo -e "${BLUE}📊 MIGRATION STATUS${NC}"
    echo -e "${BLUE}========================================================================${NC}"
    echo ""
    
    local sql="SELECT 
        migration_id,
        applied_at,
        applied_by,
        execution_time_seconds,
        status,
        error_message
    FROM public.schema_migrations
    ORDER BY applied_at DESC
    LIMIT 20;"
    
    # This would need proper formatting, simplified for now
    echo -e "${BLUE}Recent migrations:${NC}"
    echo "  (Use AWS Console or query tool to see full details)"
}

# Main execution
main() {
    echo -e "${BLUE}========================================================================${NC}"
    echo -e "${BLUE}🔄 DATABASE MIGRATIONS${NC}"
    echo -e "${BLUE}========================================================================${NC}"
    echo ""
    
    # Show status if requested
    if [ "$SHOW_STATUS" = true ]; then
        ensure_migrations_table
        show_status
        exit 0
    fi
    
    # Ensure migrations table exists
    ensure_migrations_table || exit 1
    
    # Check if migrations directory exists
    if [ ! -d "$MIGRATIONS_DIR" ]; then
        echo -e "${RED}❌ Migrations directory not found: $MIGRATIONS_DIR${NC}"
        exit 1
    fi
    
    # Handle rollback mode
    if [ "$ROLLBACK_MODE" = true ]; then
        echo -e "${BLUE}========================================================================${NC}"
        echo -e "${BLUE}⏪ ROLLBACK MIGRATIONS${NC}"
        echo -e "${BLUE}========================================================================${NC}"
        echo ""
        
        set +e
        local last_migration=$(get_last_applied_migration)
        set -e
        
        if [ -n "$SPECIFIC_MIGRATION" ]; then
            # Rollback specific migration
            local migration_file="$MIGRATIONS_DIR/$SPECIFIC_MIGRATION"
            if [ ! -f "$migration_file" ]; then
                echo -e "${RED}❌ Migration file not found: $migration_file${NC}"
                exit 1
            fi
            
            local migration_id=$(basename "$migration_file" .sql)
            
            # Check if migration was successfully applied
            local applied_migrations=$(get_applied_migrations)
            if ! echo "$applied_migrations" | grep -q "^${migration_id}$"; then
                echo -e "${YELLOW}⚠️  Migration ${migration_id} not found in applied migrations${NC}"
                echo -e "${YELLOW}   Cannot rollback: migration was never successfully applied${NC}"
                exit 1
            fi
            
            # Safety check: Only allow rolling back the last applied migration
            # This prevents breaking the migration sequence and creating gaps
            if [ "$migration_id" != "$last_migration" ]; then
                echo -e "${RED}❌ Cannot rollback ${migration_id}: it is not the latest applied migration${NC}"
                echo -e "${YELLOW}   Latest applied migration: ${last_migration}${NC}"
                echo ""
                echo -e "${YELLOW}   ⚠️  Safety Rule: You can only rollback migrations in reverse order${NC}"
                echo -e "${YELLOW}   This prevents breaking the migration sequence and dependency issues.${NC}"
                echo ""
                echo -e "${BLUE}   To rollback ${migration_id}, you must first rollback all newer migrations:${NC}"
                # Show which migrations need to be rolled back first
                local migration_files=($(find "$MIGRATIONS_DIR" -name "*.sql" -type f | sort))
                local target_found=false
                for file in "${migration_files[@]}"; do
                    local file_id=$(basename "$file" .sql)
                    if [ "$file_id" = "$migration_id" ]; then
                        target_found=true
                    elif [ "$target_found" = true ]; then
                        if echo "$applied_migrations" | grep -q "^${file_id}$"; then
                            echo -e "${BLUE}     - ${file_id}${NC}"
                        fi
                    fi
                done
                exit 1
            fi
            
            rollback_migration "$migration_file"
        else
            # Rollback last applied migration
            if [ -z "$last_migration" ]; then
                echo -e "${YELLOW}⚠️  No applied migrations found to rollback${NC}"
                exit 0
            fi
            
            echo -e "${BLUE}   Last applied migration: ${last_migration}${NC}"
            echo ""
            
            local migration_file="$MIGRATIONS_DIR/${last_migration}.sql"
            if [ ! -f "$migration_file" ]; then
                echo -e "${RED}❌ Migration file not found: $migration_file${NC}"
                echo -e "${YELLOW}   Migration was applied but file no longer exists${NC}"
                exit 1
            fi
            
            rollback_migration "$migration_file"
        fi
        
        exit 0
    fi
    
    # Get applied migrations (disable exit-on-error for this check)
    set +e
    echo -e "${BLUE}   Checking applied migrations...${NC}"
    local applied_migrations=$(get_applied_migrations)
    set -e
    
    if [ -n "$SPECIFIC_MIGRATION" ]; then
        # Apply specific migration
        local migration_file="$MIGRATIONS_DIR/$SPECIFIC_MIGRATION"
        if [ ! -f "$migration_file" ]; then
            echo -e "${RED}❌ Migration file not found: $migration_file${NC}"
            exit 1
        fi
        
        apply_migration "$migration_file"
    else
        # Find all migration files
        local migration_files=($(find "$MIGRATIONS_DIR" -name "*.sql" -type f | sort))
        
        if [ ${#migration_files[@]} -eq 0 ]; then
            echo -e "${YELLOW}⚠️  No migration files found in $MIGRATIONS_DIR${NC}"
            exit 0
        fi
        
        echo -e "${BLUE}   Found ${#migration_files[@]} migration file(s)${NC}"
        echo ""
        
        local applied_count=0
        local skipped_count=0
        local failed_count=0
        
        # Apply each migration
        set +e  # Disable exit-on-error for the loop
        for migration_file in "${migration_files[@]}"; do
            local migration_id=$(basename "$migration_file" .sql)
            
            # Check if already applied
            if echo "$applied_migrations" | grep -q "^${migration_id}$"; then
                echo -e "${YELLOW}   ⏭️  Skipping ${migration_id} (already applied)${NC}"
                ((skipped_count++))
                continue
            fi
            
            if apply_migration "$migration_file"; then
                ((applied_count++))
            else
                ((failed_count++))
                echo -e "${RED}   ❌ Migration failed, stopping${NC}"
                break
            fi
            echo ""
        done
        set -e  # Re-enable exit-on-error
        
        # Summary
        echo -e "${BLUE}========================================================================${NC}"
        echo -e "${BLUE}📊 SUMMARY${NC}"
        echo -e "${BLUE}========================================================================${NC}"
        echo -e "${GREEN}✅ Applied: $applied_count${NC}"
        echo -e "${YELLOW}⏭️  Skipped: $skipped_count${NC}"
        if [ $failed_count -gt 0 ]; then
            echo -e "${RED}❌ Failed: $failed_count${NC}"
        fi
    fi
}

# Run main function
main

