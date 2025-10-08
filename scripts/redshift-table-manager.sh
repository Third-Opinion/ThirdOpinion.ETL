#!/bin/bash
#
# redshift-table-manager.sh
# Manages Redshift table creation and schema verification via AWS CLI
#
# Usage:
#   ./redshift-table-manager.sh create <table-name> [--ddl-file <file>]
#   ./redshift-table-manager.sh verify <table-name>
#   ./redshift-table-manager.sh compare <table-name> [--ddl-file <file>]
#   ./redshift-table-manager.sh create-from-file <ddl-file>
#

set -e  # Exit on error

# ============================================================================
# CONFIGURATION - UPDATE THESE VALUES
# ============================================================================

# Use either CLUSTER_ID or WORKGROUP_NAME (not both)
CLUSTER_ID=""  # e.g., "prod-redshift-main-ue2"
WORKGROUP_NAME="hmu2-workgroup"  # e.g., "my-serverless-workgroup"

DATABASE="dev"
REGION="us-east-2"
AWS_PROFILE="${AWS_PROFILE:-to-prd-admin}"  # AWS profile to use
# DB_USER="your-username"  # Optional: if using username/password
SCHEMA="public"

# Default DDL directory
DDL_DIR="./ddl"

# Timeout settings
QUERY_TIMEOUT=60  # seconds

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

log_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

log_success() {
    echo -e "${GREEN}✅${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}⚠️${NC} $1"
}

log_error() {
    echo -e "${RED}❌${NC} $1"
}

log_debug() {
    echo -e "${CYAN}🔍${NC} $1"
}

log_git() {
    echo -e "${MAGENTA}🔀${NC} $1"
}

# Execute SQL and wait for result
execute_sql() {
    local sql="$1"
    local description="${2:-Executing SQL}"

    log_info "$description..."

    # Build execute-statement command based on configuration
    local query_id

    # Set AWS profile if configured
    local aws_opts=""
    if [ -n "$AWS_PROFILE" ]; then
        aws_opts="--profile $AWS_PROFILE"
    fi

    if [ -n "$WORKGROUP_NAME" ]; then
        query_id=$(aws $aws_opts redshift-data execute-statement \
            --workgroup-name "$WORKGROUP_NAME" \
            --database "$DATABASE" \
            --sql "$sql" \
            --region "$REGION" \
            --query 'Id' \
            --output text)
    elif [ -n "$CLUSTER_ID" ]; then
        query_id=$(aws $aws_opts redshift-data execute-statement \
            --cluster-identifier "$CLUSTER_ID" \
            --database "$DATABASE" \
            --sql "$sql" \
            --region "$REGION" \
            --query 'Id' \
            --output text)
    else
        log_error "Either CLUSTER_ID or WORKGROUP_NAME must be configured"
        return 1
    fi

    if [ -z "$query_id" ]; then
        log_error "Failed to submit query"
        return 1
    fi

    log_debug "Query ID: $query_id"

    # Wait for completion
    local elapsed=0
    while [ $elapsed -lt $QUERY_TIMEOUT ]; do
        local status=$(aws $aws_opts redshift-data describe-statement \
            --id "$query_id" \
            --region "$REGION" \
            --query 'Status' \
            --output text)

        case "$status" in
            FINISHED)
                log_success "Query completed successfully"
                echo "$query_id"
                return 0
                ;;
            FAILED|ABORTED)
                local error=$(aws $aws_opts redshift-data describe-statement \
                    --id "$query_id" \
                    --region "$REGION" \
                    --query 'Error' \
                    --output text)
                log_error "Query failed: $error"
                return 1
                ;;
            SUBMITTED|STARTED|PICKED)
                echo -n "."
                sleep 2
                elapsed=$((elapsed + 2))
                ;;
        esac
    done

    log_error "Query timed out after ${QUERY_TIMEOUT}s"
    return 1
}

# Get query results as JSON
get_query_results() {
    local query_id="$1"

    local aws_opts=""
    if [ -n "$AWS_PROFILE" ]; then
        aws_opts="--profile $AWS_PROFILE"
    fi

    aws $aws_opts redshift-data get-statement-result \
        --id "$query_id" \
        --region "$REGION" \
        --output json
}

# Check if table exists
table_exists() {
    local table_name="$1"

    local sql="SELECT COUNT(*) FROM pg_table_def WHERE schemaname = '$SCHEMA' AND tablename = '$table_name';"

    local query_id=$(execute_sql "$sql" "Checking if table $table_name exists" 2>&1 | tail -1)

    if [ -z "$query_id" ] || [ "$query_id" = "1" ]; then
        return 2
    fi

    local count=$(get_query_results "$query_id" 2>/dev/null | \
        jq -r '.Records[0][0].longValue // 0' 2>/dev/null)

    # Handle empty/invalid count
    if [ -z "$count" ] || ! [[ "$count" =~ ^[0-9]+$ ]]; then
        count=0
    fi

    if [ "$count" -gt 0 ]; then
        return 0  # Table exists
    else
        return 1  # Table doesn't exist
    fi
}

# Get table schema
get_table_schema() {
    local table_name="$1"

    local sql="
    SELECT
        tablename,
        \"column\" as column_name,
        type as data_type,
        encoding,
        distkey,
        sortkey,
        \"notnull\"
    FROM pg_table_def
    WHERE schemaname = '$SCHEMA'
    AND tablename = '$table_name'
    ORDER BY sortkey DESC, \"column\";
    "

    local query_id=$(execute_sql "$sql" "Fetching schema for $table_name" 2>&1 | tail -1)

    if [ -z "$query_id" ] || [ "$query_id" = "1" ]; then
        return 1
    fi

    get_query_results "$query_id" 2>/dev/null
}

# Get distribution style
get_dist_style() {
    local table_name="$1"

    local sql="
    SELECT
        CASE
            WHEN t.reldiststyle = 0 THEN 'EVEN'
            WHEN t.reldiststyle = 1 THEN 'KEY'
            WHEN t.reldiststyle = 8 THEN 'ALL'
            ELSE 'AUTO'
        END as diststyle
    FROM pg_class t
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = '$SCHEMA'
    AND t.relname = '$table_name';
    "

    local query_id=$(execute_sql "$sql" "Getting distribution style" 2>&1 | tail -1)

    if [ -z "$query_id" ] || [ "$query_id" = "1" ]; then
        echo "UNKNOWN"
        return 1
    fi

    get_query_results "$query_id" 2>/dev/null | jq -r '.Records[0][0].stringValue // "UNKNOWN"' 2>/dev/null
}

# Get actual DDL from Redshift - build from system catalogs
get_redshift_ddl() {
    local table_name="$1"

    # Build DDL from pg_table_def using LISTAGG with WITHIN GROUP
    local sql="
    SELECT
        'CREATE TABLE $SCHEMA.$table_name (' ||
        LISTAGG(
            \"column\" || ' ' ||
            type ||
            CASE WHEN \"notnull\" THEN ' NOT NULL' ELSE '' END ||
            CASE WHEN distkey THEN ' DISTKEY' ELSE '' END,
            ', '
        ) WITHIN GROUP (ORDER BY \"column\") ||
        ')' AS ddl
    FROM pg_table_def
    WHERE schemaname = '$SCHEMA'
    AND tablename = '$table_name';
    "

    local query_id=$(execute_sql "$sql" "Getting DDL from Redshift" 2>&1 | tail -1)

    if [ -z "$query_id" ] || [ "$query_id" = "1" ]; then
        return 1
    fi

    local base_ddl=$(get_query_results "$query_id" 2>/dev/null | jq -r '.Records[0][0].stringValue // ""' 2>/dev/null)

    # Get DISTSTYLE
    local diststyle=$(get_dist_style "$table_name")

    # Get SORTKEY using LISTAGG
    local sortkey_sql="
    SELECT
        'SORTKEY(' || LISTAGG(\"column\", ', ') WITHIN GROUP (ORDER BY sortkey) || ')' AS sortkey
    FROM pg_table_def
    WHERE schemaname = '$SCHEMA'
    AND tablename = '$table_name'
    AND sortkey > 0;
    "

    local sortkey_id=$(execute_sql "$sortkey_sql" "Getting SORTKEY" 2>&1 | tail -1)
    local sortkey=""
    if [ -n "$sortkey_id" ] && [ "$sortkey_id" != "1" ]; then
        sortkey=$(get_query_results "$sortkey_id" 2>/dev/null | jq -r '.Records[0][0].stringValue // ""' 2>/dev/null)
    fi

    # Combine all parts
    local full_ddl="$base_ddl"
    if [ "$diststyle" != "UNKNOWN" ] && [ -n "$diststyle" ]; then
        full_ddl="$full_ddl DISTSTYLE $diststyle"
    fi
    if [ -n "$sortkey" ]; then
        full_ddl="$full_ddl $sortkey"
    fi
    full_ddl="$full_ddl;"

    echo "$full_ddl"
}

# Extract table name from DDL
extract_table_name() {
    local ddl="$1"

    # Match: CREATE TABLE [public.]table_name
    echo "$ddl" | grep -i "CREATE TABLE" | \
        sed -E 's/.*CREATE TABLE (IF NOT EXISTS )?//I' | \
        sed -E 's/^(public\.)?([a-zA-Z0-9_]+).*/\2/' | \
        head -1
}

# Canonicalize DDL for comparison
canonicalize_ddl() {
    local ddl="$1"

    # Normalize the DDL:
    # 1. Remove comments
    # 2. Normalize whitespace (collapse multiple spaces/newlines to single space)
    # 3. Remove ENCODE specifications
    # 4. Normalize type names (character varying -> varchar, etc.)
    # 5. Remove extra commas and spaces
    # 6. Normalize DISTKEY/SORTKEY formatting

    echo "$ddl" | \
        sed 's/--.*$//' | \
        tr '\n' ' ' | \
        sed 's/  */ /g' | \
        sed -E 's/ ENCODE [a-z0-9]+//gi' | \
        sed -E 's/character varying/varchar/gi' | \
        sed -E 's/timestamp without time zone/timestamp/gi' | \
        sed -E 's/timestamp with time zone/timestamptz/gi' | \
        sed -E "s/'now'::text::timestamp with time zone/CURRENT_TIMESTAMP/gi" | \
        sed -E "s/DEFAULT \('now'[^)]*\)[^,)]*/DEFAULT CURRENT_TIMESTAMP/gi" | \
        sed -E 's/\( *([a-zA-Z0-9_]+) *,/(\1,/g' | \
        sed -E 's/, *([a-zA-Z0-9_]+) *\)/,\1)/g' | \
        sed -E 's/ distkey *,/ distkey/gi' | \
        sed -E 's/ +distkey/ distkey/gi' | \
        sed -E 's/\) *DISTSTYLE/) DISTSTYLE/gi' | \
        sed -E 's/SORTKEY *\(/SORTKEY(/gi' | \
        sed 's/ *; *$/;/' | \
        sed 's/  */ /g' | \
        tr '[:upper:]' '[:lower:]'
}

# Read DDL from file
read_ddl_file() {
    local file="$1"
    
    if [ ! -f "$file" ]; then
        log_error "DDL file not found: $file"
        return 1
    fi
    
    cat "$file"
}

# Find DDL file for table
find_ddl_file() {
    local table_name="$1"
    
    # Check multiple possible locations
    local possible_files=(
        "$DDL_DIR/${table_name}.sql"
        "$DDL_DIR/${table_name}-ddl.sql"
        "./${table_name}.sql"
        "./${table_name}-ddl.sql"
    )
    
    for file in "${possible_files[@]}"; do
        if [ -f "$file" ]; then
            echo "$file"
            return 0
        fi
    done
    
    return 1
}

# Get DDL for a table from file
get_table_ddl() {
    local table_name="$1"
    local ddl_file="$2"
    
    # If DDL file specified, use it
    if [ -n "$ddl_file" ]; then
        read_ddl_file "$ddl_file"
        return $?
    fi
    
    # Try to find DDL file
    local found_file=$(find_ddl_file "$table_name")
    if [ $? -eq 0 ]; then
        log_debug "Found DDL file: $found_file"
        read_ddl_file "$found_file"
        return $?
    fi
    
    # No DDL found
    log_error "No DDL found for table: $table_name"
    log_info "Options:"
    log_info "  1. Create DDL file: $DDL_DIR/${table_name}.sql"
    log_info "  2. Specify file: --ddl-file <file>"
    return 1
}

# ============================================================================
# GIT FUNCTIONS
# ============================================================================

# Check if in a git repository
check_git_repo() {
    if ! git rev-parse --git-dir > /dev/null 2>&1; then
        log_error "Not in a git repository"
        log_info "Initialize with: git init && git add . && git commit -m 'Initial commit'"
        return 1
    fi
    return 0
}

# Check if working directory is clean
check_git_clean() {
    if ! git diff-index --quiet HEAD -- 2>/dev/null; then
        log_warning "Working directory has uncommitted changes"
        return 1
    fi
    return 0
}

# Get current git commit SHA
get_git_sha() {
    git rev-parse HEAD 2>/dev/null
}

# Get short git commit SHA
get_git_sha_short() {
    git rev-parse --short HEAD 2>/dev/null
}

# Get git commit message
get_git_commit_message() {
    git log -1 --pretty=%B 2>/dev/null | tr -d "'" | head -c 1000
}

# Get git author
get_git_author() {
    git log -1 --pretty=%an 2>/dev/null
}

# Get git commit date
get_git_commit_date() {
    git log -1 --pretty=%cI 2>/dev/null
}

# Get git SHA for specific file
get_file_git_sha() {
    local file="$1"
    git log -1 --pretty=%H -- "$file" 2>/dev/null
}

# Get file content at specific commit
get_file_at_commit() {
    local file="$1"
    local commit="$2"
    git show "${commit}:${file}" 2>/dev/null
}

# Check if file has uncommitted changes
file_has_changes() {
    local file="$1"
    ! git diff --quiet -- "$file" 2>/dev/null
}

# Get list of changed DDL files
get_changed_ddl_files() {
    if check_git_repo; then
        git diff --name-only HEAD -- "$DDL_DIR/*.sql" 2>/dev/null
    fi
}

# ============================================================================
# DEPLOYMENT TRACKING
# ============================================================================

# Record deployment in Redshift
record_deployment() {
    local table_name="$1"
    local ddl_file="$2"
    local status="${3:-success}"
    local notes="$4"

    if ! check_git_repo; then
        log_warning "Not in git repo - skipping deployment tracking"
        return 0
    fi

    local git_sha=$(get_git_sha)
    local git_message=$(get_git_commit_message)
    local git_author=$(get_git_author)
    local git_date=$(get_git_commit_date)

    local aws_opts=""
    if [ -n "$AWS_PROFILE" ]; then
        aws_opts="--profile $AWS_PROFILE"
    fi

    local deployed_by=$(aws $aws_opts sts get-caller-identity --query 'Arn' --output text | awk -F'/' '{print $NF}')

    # Escape single quotes
    git_message=$(echo "$git_message" | sed "s/'/''/g")
    notes=$(echo "$notes" | sed "s/'/''/g")

    local insert_sql="
    INSERT INTO public.schema_deployments
    (table_name, git_commit_sha, git_commit_message, git_author, git_commit_date,
     deployed_by, ddl_file_path, deployment_status, deployment_notes)
    VALUES
    ('$table_name', '$git_sha', '$git_message', '$git_author', '$git_date',
     '$deployed_by', '$ddl_file', '$status', '$notes');
    "

    execute_sql "$insert_sql" "Recording deployment" 2>/dev/null

    if [ $? -eq 0 ]; then
        log_git "Deployment recorded: $(get_git_sha_short)"
    fi
}

# Get latest deployed version from Redshift
get_deployed_version() {
    local table_name="$1"

    local sql="
    SELECT
        git_commit_sha,
        git_commit_message,
        deployed_at,
        deployed_by
    FROM public.schema_deployments
    WHERE table_name = '$table_name'
    AND deployment_status = 'success'
    ORDER BY deployed_at DESC
    LIMIT 1;
    "

    local query_id=$(execute_sql "$sql" "Getting deployed version" 2>&1 | tail -1)

    if [ $? -eq 0 ] && [[ "$query_id" =~ ^[a-z0-9-]+$ ]]; then
        get_query_results "$query_id"
    fi
}

# Check if table needs deployment
needs_deployment() {
    local table_name="$1"
    local ddl_file="$2"

    if ! check_git_repo; then
        log_warning "Not in git repo - cannot check deployment status"
        return 0  # Assume needs deployment
    fi

    # Get current git SHA for the DDL file
    local current_sha=$(get_file_git_sha "$ddl_file")

    # Get deployed version
    local deployed_info=$(get_deployed_version "$table_name")

    if [ -z "$deployed_info" ]; then
        log_info "No deployment history found - needs deployment"
        return 0  # Needs deployment
    fi

    local deployed_sha=$(echo "$deployed_info" | jq -r '.Records[0][0].stringValue // ""')

    if [ "$current_sha" == "$deployed_sha" ]; then
        return 1  # Already deployed
    else
        return 0  # Needs deployment
    fi
}

# ============================================================================
# MAIN COMMANDS
# ============================================================================

cmd_create() {
    local table_name="$1"
    local ddl_file="$2"
    
    if [ -z "$table_name" ]; then
        log_error "Table name required"
        echo "Usage: $0 create <table-name> [--ddl-file <file>]"
        return 1
    fi
    
    log_info "Creating table: $table_name"
    echo "========================================"
    
    # Check if table exists
    if table_exists "$table_name"; then
        log_warning "Table $table_name already exists"
        read -p "Drop and recreate? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            log_info "Aborted"
            return 0
        fi
        
        log_info "Dropping existing table..."
        local drop_sql="DROP TABLE IF EXISTS $SCHEMA.$table_name CASCADE;"
        execute_sql "$drop_sql" "Dropping table"
        
        if [ $? -ne 0 ]; then
            log_error "Failed to drop table"
            return 1
        fi
    fi
    
    # Get DDL
    local ddl=$(get_table_ddl "$table_name" "$ddl_file")
    
    if [ $? -ne 0 ]; then
        return 1
    fi
    
    log_info "DDL to execute:"
    echo "----------------------------------------"
    echo "$ddl"
    echo "----------------------------------------"
    
    # Create table
    execute_sql "$ddl" "Creating table $table_name"
    
    if [ $? -ne 0 ]; then
        log_error "Failed to create table"
        return 1
    fi
    
    log_success "Table $table_name created successfully!"
    
    # Verify creation
    echo ""
    cmd_verify "$table_name"
}

cmd_create_from_file() {
    local ddl_file="$1"
    
    if [ -z "$ddl_file" ]; then
        log_error "DDL file required"
        echo "Usage: $0 create-from-file <ddl-file>"
        return 1
    fi
    
    if [ ! -f "$ddl_file" ]; then
        log_error "File not found: $ddl_file"
        return 1
    fi
    
    log_info "Reading DDL from: $ddl_file"
    
    # Read and extract table name
    local ddl=$(cat "$ddl_file")
    local table_name=$(extract_table_name "$ddl")
    
    if [ -z "$table_name" ]; then
        log_error "Could not extract table name from DDL"
        log_info "Make sure DDL contains: CREATE TABLE <table_name>"
        return 1
    fi
    
    log_info "Detected table name: $table_name"
    
    # Call create with this table name
    cmd_create "$table_name" "$ddl_file"
}

cmd_verify() {
    local table_name="$1"
    
    if [ -z "$table_name" ]; then
        log_error "Table name required"
        echo "Usage: $0 verify <table-name>"
        return 1
    fi
    
    log_info "Verifying table: $table_name"
    echo "========================================"
    
    # Check if exists
    if ! table_exists "$table_name"; then
        log_error "Table $table_name does not exist"
        return 1
    fi
    
    log_success "Table exists"
    
    # Get schema
    local schema_json=$(get_table_schema "$table_name")
    
    if [ $? -ne 0 ]; then
        log_error "Failed to get table schema"
        return 1
    fi
    
    # Get distribution style
    local diststyle=$(get_dist_style "$table_name")
    
    # Parse schema
    local distkey=$(echo "$schema_json" | jq -r '
        .Records[] | 
        select(.[4].booleanValue == true) | 
        .[1].stringValue
    ' | head -1)
    
    local sortkeys=$(echo "$schema_json" | jq -r '
        .Records[] | 
        select(.[5].longValue > 0) | 
        "\(.[5].longValue):\(.[1].stringValue)"
    ' | sort -n | cut -d: -f2 | paste -sd "," -)
    
    local column_count=$(echo "$schema_json" | jq '.Records | length')
    
    # Display results
    echo ""
    log_info "Table Details:"
    echo "  Distribution Style: $diststyle"
    echo "  Distribution Key:   ${distkey:-none}"
    echo "  Sort Keys:          ${sortkeys:-none}"
    echo "  Column Count:       $column_count"
    
    # Show columns
    echo ""
    log_info "Columns:"
    echo "$schema_json" | jq -r '
        .Records[] | 
        "  \(.[1].stringValue) - \(.[2].stringValue)\(
            if .[6].booleanValue then " NOT NULL" else "" end
        )\(
            if .[4].booleanValue then " (DISTKEY)" else "" end
        )\(
            if .[5].longValue > 0 then " (SORTKEY \(.[5].longValue))" else "" end
        )"
    '
    
    echo ""
    log_success "Table verification complete"
}

cmd_compare() {
    local table_name="$1"
    local ddl_file="$2"
    
    if [ -z "$table_name" ]; then
        log_error "Table name required"
        echo "Usage: $0 compare <table-name> [--ddl-file <file>]"
        return 1
    fi
    
    log_info "Comparing table schema: $table_name"
    echo "========================================"
    
    # Check if exists
    if ! table_exists "$table_name"; then
        log_error "Table $table_name does not exist"
        log_info "Run: $0 create $table_name"
        return 1
    fi
    
    # Get expected DDL
    local expected_ddl=$(get_table_ddl "$table_name" "$ddl_file")
    
    if [ $? -ne 0 ]; then
        return 1
    fi
    
    # Parse expected schema
    local expected_distkey=$(echo "$expected_ddl" | grep -i "DISTKEY" | sed 's/.*DISTKEY(\([^)]*\)).*/\1/')
    local expected_sortkeys=$(echo "$expected_ddl" | grep -i "SORTKEY" | sed 's/.*SORTKEY(\([^)]*\)).*/\1/' | tr -d ' ')
    
    # Get actual schema
    local schema_json=$(get_table_schema "$table_name")
    local diststyle=$(get_dist_style "$table_name")
    
    local actual_distkey=$(echo "$schema_json" | jq -r '
        .Records[] | 
        select(.[4].booleanValue == true) | 
        .[1].stringValue
    ' | head -1)
    
    local actual_sortkeys=$(echo "$schema_json" | jq -r '
        .Records[] | 
        select(.[5].longValue > 0) | 
        "\(.[5].longValue):\(.[1].stringValue)"
    ' | sort -n | cut -d: -f2 | paste -sd "," -)
    
    # Compare
    local differences=0
    
    echo ""
    log_info "Schema Comparison:"
    echo ""
    
    # Check DISTKEY
    if [ "$expected_distkey" != "$actual_distkey" ]; then
        log_error "DISTKEY mismatch:"
        echo "  Expected: $expected_distkey"
        echo "  Actual:   ${actual_distkey:-none}"
        differences=$((differences + 1))
    else
        log_success "DISTKEY matches: $expected_distkey"
    fi
    
    # Check SORTKEY
    if [ "$expected_sortkeys" != "$actual_sortkeys" ]; then
        log_error "SORTKEY mismatch:"
        echo "  Expected: $expected_sortkeys"
        echo "  Actual:   ${actual_sortkeys:-none}"
        differences=$((differences + 1))
    else
        log_success "SORTKEY matches: $expected_sortkeys"
    fi
    
    # Check DISTSTYLE
    local expected_diststyle="KEY"
    if [ -n "$expected_distkey" ]; then
        expected_diststyle="KEY"
    else
        expected_diststyle="EVEN"
    fi
    
    if [ "$diststyle" != "$expected_diststyle" ]; then
        log_error "DISTSTYLE mismatch:"
        echo "  Expected: $expected_diststyle"
        echo "  Actual:   $diststyle"
        differences=$((differences + 1))
    else
        log_success "DISTSTYLE matches: $diststyle"
    fi
    
    echo ""
    if [ $differences -eq 0 ]; then
        log_success "✨ Schema matches perfectly!"
        return 0
    else
        log_error "Found $differences difference(s)"
        log_warning "To fix: $0 create $table_name"
        return 1
    fi
}

cmd_list() {
    log_info "Available DDL files:"
    echo ""

    # List DDL files
    if [ -d "$DDL_DIR" ]; then
        local found=0
        for file in "$DDL_DIR"/*.sql; do
            if [ -f "$file" ]; then
                local basename=$(basename "$file" .sql)
                local table_name=$(basename "$basename" -ddl)

                # Check git status
                local git_status=""
                if check_git_repo 2>/dev/null; then
                    if file_has_changes "$file"; then
                        git_status="📝"
                    else
                        git_status="  "
                    fi
                fi

                # Check if exists in Redshift
                if table_exists "$table_name" 2>/dev/null; then
                    # Check if needs deployment
                    if needs_deployment "$table_name" "$file" 2>/dev/null; then
                        echo "  ⚠️  $git_status $table_name (needs deployment)"
                    else
                        echo "  ✅ $git_status $table_name (up to date)"
                    fi
                else
                    echo "  ⬜ $git_status $table_name (not deployed)"
                fi
                found=1
            fi
        done

        if [ $found -eq 0 ]; then
            echo "  (no .sql files found in $DDL_DIR)"
        fi

        echo ""
        log_info "Legend:"
        echo "  ✅ Deployed and up to date"
        echo "  ⚠️  Deployed but needs update"
        echo "  ⬜ Not deployed yet"
        echo "  📝 Has uncommitted changes"
    else
        log_error "DDL directory not found: $DDL_DIR"
        echo "Create it with: mkdir -p $DDL_DIR"
    fi
}

cmd_drop() {
    local table_name="$1"

    if [ -z "$table_name" ]; then
        log_error "Table name required"
        echo "Usage: $0 drop <table-name>"
        return 1
    fi

    log_warning "About to drop table: $table_name"
    read -p "Are you sure? (yes/N): " -r
    echo
    if [[ ! $REPLY == "yes" ]]; then
        log_info "Aborted"
        return 0
    fi

    local drop_sql="DROP TABLE IF EXISTS $SCHEMA.$table_name CASCADE;"
    execute_sql "$drop_sql" "Dropping table $table_name"

    if [ $? -ne 0 ]; then
        log_error "Failed to drop table"
        return 1
    fi

    log_success "Table $table_name dropped"
}

cmd_compare_ddl() {
    local table_name="$1"
    local ddl_file="$2"

    if [ -z "$table_name" ]; then
        log_error "Table name required"
        echo "Usage: $0 compare-ddl <table-name> [--ddl-file <file>]"
        return 1
    fi

    log_info "Comparing DDL for: $table_name"
    echo "========================================"

    # Check if exists
    if ! table_exists "$table_name"; then
        log_error "Table $table_name does not exist in Redshift"
        return 1
    fi

    # Get expected DDL from file
    local expected_ddl=$(get_table_ddl "$table_name" "$ddl_file")
    if [ $? -ne 0 ]; then
        return 1
    fi

    # Get actual DDL from Redshift
    local actual_ddl=$(get_redshift_ddl "$table_name")
    if [ $? -ne 0 ]; then
        log_error "Failed to retrieve DDL from Redshift"
        return 1
    fi

    # Canonicalize both DDLs
    local canonical_expected=$(canonicalize_ddl "$expected_ddl")
    local canonical_actual=$(canonicalize_ddl "$actual_ddl")

    echo ""
    log_info "Expected DDL (from file):"
    echo "----------------------------------------"
    echo "$expected_ddl"
    echo "----------------------------------------"

    echo ""
    log_info "Actual DDL (from Redshift):"
    echo "----------------------------------------"
    echo "$actual_ddl"
    echo "----------------------------------------"

    echo ""
    log_info "Canonical Expected:"
    echo "----------------------------------------"
    echo "$canonical_expected"
    echo "----------------------------------------"

    echo ""
    log_info "Canonical Actual:"
    echo "----------------------------------------"
    echo "$canonical_actual"
    echo "----------------------------------------"

    # Compare
    if [ "$canonical_expected" = "$canonical_actual" ]; then
        echo ""
        log_success "✨ DDL matches perfectly!"
        return 0
    else
        echo ""
        log_error "DDL mismatch detected"
        log_info "Showing diff:"
        echo ""
        diff <(echo "$canonical_expected") <(echo "$canonical_actual") || true
        return 1
    fi
}

cmd_deploy() {
    local table_name="$1"
    local ddl_file="$2"
    local force="${3:-false}"

    if [ -z "$table_name" ]; then
        log_error "Table name required"
        echo "Usage: $0 deploy <table-name> [--ddl-file <file>] [--force]"
        return 1
    fi

    log_info "Deploying table: $table_name"
    echo "========================================"

    # Check git
    if ! check_git_repo; then
        return 1
    fi

    # Find DDL file
    if [ -z "$ddl_file" ]; then
        ddl_file=$(find_ddl_file "$table_name")
        if [ $? -ne 0 ]; then
            log_error "No DDL file found for: $table_name"
            return 1
        fi
    fi

    log_info "DDL file: $ddl_file"

    # Check for uncommitted changes
    if file_has_changes "$ddl_file"; then
        log_error "File has uncommitted changes: $ddl_file"
        log_info "Commit changes first:"
        log_info "  git add $ddl_file"
        log_info "  git commit -m 'Update $table_name schema'"
        return 1
    fi

    # Get git info
    local git_sha=$(get_git_sha)
    local git_sha_short=$(get_git_sha_short)
    local git_message=$(get_git_commit_message)
    local git_author=$(get_git_author)

    log_git "Git SHA: $git_sha_short"
    log_git "Author: $git_author"
    log_git "Message: $git_message"

    # Check if already deployed
    if [ "$force" != "true" ] && ! needs_deployment "$table_name" "$ddl_file"; then
        log_success "Table $table_name is already at latest version ($git_sha_short)"
        log_info "Use --force to redeploy"
        return 0
    fi

    # Read DDL
    local ddl=$(read_ddl_file "$ddl_file")

    if [ $? -ne 0 ]; then
        return 1
    fi

    # Check if table exists
    if table_exists "$table_name"; then
        log_warning "Table $table_name already exists"

        if [ "$force" != "true" ]; then
            read -p "Drop and recreate? (y/N): " -n 1 -r
            echo
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                log_info "Aborted"
                return 0
            fi
        fi

        log_info "Dropping existing table..."
        local drop_sql="DROP TABLE IF EXISTS $SCHEMA.$table_name CASCADE;"
        execute_sql "$drop_sql" "Dropping table"

        if [ $? -ne 0 ]; then
            log_error "Failed to drop table"
            record_deployment "$table_name" "$ddl_file" "failed" "Drop failed"
            return 1
        fi
    fi

    log_info "DDL to deploy:"
    echo "----------------------------------------"
    echo "$ddl"
    echo "----------------------------------------"

    # Deploy
    execute_sql "$ddl" "Creating table $table_name"

    if [ $? -ne 0 ]; then
        log_error "Failed to create table"
        record_deployment "$table_name" "$ddl_file" "failed" "Create failed"
        return 1
    fi

    log_success "Table $table_name deployed successfully!"

    # Record deployment
    record_deployment "$table_name" "$ddl_file" "success" "Deployed from Git $git_sha_short"

    echo ""
    cmd_status "$table_name"
}

cmd_status() {
    local table_name="$1"

    if [ -z "$table_name" ]; then
        log_error "Table name required"
        echo "Usage: $0 status <table-name>"
        return 1
    fi

    log_info "Deployment status: $table_name"
    echo "========================================"

    # Check if table exists
    if ! table_exists "$table_name"; then
        log_error "Table does not exist in Redshift"
        return 1
    fi

    log_success "Table exists in Redshift"

    # Check git
    if ! check_git_repo; then
        log_warning "Not in git repo - cannot check version"
        return 0
    fi

    # Find DDL file
    local ddl_file=$(find_ddl_file "$table_name")
    if [ $? -ne 0 ]; then
        log_warning "No DDL file found"
        return 0
    fi

    # Get current git info
    local current_sha=$(get_file_git_sha "$ddl_file")
    local current_sha_short=$(git rev-parse --short "$current_sha" 2>/dev/null)

    # Get deployed info
    local deployed_info=$(get_deployed_version "$table_name")

    echo ""
    log_info "Version Information:"

    if [ -z "$deployed_info" ]; then
        log_warning "No deployment history found"
        echo "  Current Git SHA: $current_sha_short"
        echo "  Status: Not tracked"
    else
        local deployed_sha=$(echo "$deployed_info" | jq -r '.Records[0][0].stringValue // ""')
        local deployed_sha_short=$(git rev-parse --short "$deployed_sha" 2>/dev/null)
        local deployed_message=$(echo "$deployed_info" | jq -r '.Records[0][1].stringValue // ""')
        local deployed_at=$(echo "$deployed_info" | jq -r '.Records[0][2].stringValue // ""')
        local deployed_by=$(echo "$deployed_info" | jq -r '.Records[0][3].stringValue // ""')

        echo "  Deployed SHA:  $deployed_sha_short"
        echo "  Deployed At:   $deployed_at"
        echo "  Deployed By:   $deployed_by"
        echo "  Message:       $deployed_message"
        echo ""
        echo "  Current SHA:   $current_sha_short"

        if [ "$current_sha" == "$deployed_sha" ]; then
            log_success "✨ Up to date"
        else
            log_warning "⚠️  DDL has changed - needs deployment"
            log_info "Run: $0 deploy $table_name"
        fi
    fi
}

cmd_history() {
    local table_name="$1"

    if [ -z "$table_name" ]; then
        log_error "Table name required"
        echo "Usage: $0 history <table-name>"
        return 1
    fi

    log_info "Deployment history: $table_name"
    echo "========================================"

    local sql="
    SELECT
        LEFT(git_commit_sha, 8) as sha,
        LEFT(git_commit_message, 50) as message,
        git_author,
        deployed_at,
        deployed_by,
        deployment_status
    FROM public.schema_deployments
    WHERE table_name = '$table_name'
    ORDER BY deployed_at DESC
    LIMIT 20;
    "

    local query_id=$(execute_sql "$sql" "Fetching deployment history" 2>&1 | tail -1)

    if [ $? -ne 0 ] || ! [[ "$query_id" =~ ^[a-z0-9-]+$ ]]; then
        return 1
    fi

    local results=$(get_query_results "$query_id")

    echo ""
    echo "$results" | jq -r '
        .Records[] |
        "SHA:     \(.[0].stringValue)
Message: \(.[1].stringValue)
Author:  \(.[2].stringValue)
Date:    \(.[3].stringValue)
By:      \(.[4].stringValue)
Status:  \(.[5].stringValue)
----------------------------------------"
    '
}

cmd_list_deployments() {
    log_info "All deployments"
    echo "========================================"

    local sql="
    WITH latest_deployments AS (
        SELECT
            table_name,
            git_commit_sha,
            deployed_at,
            ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY deployed_at DESC) as rn
        FROM public.schema_deployments
        WHERE deployment_status = 'success'
    )
    SELECT
        table_name,
        LEFT(git_commit_sha, 8) as sha,
        deployed_at
    FROM latest_deployments
    WHERE rn = 1
    ORDER BY table_name;
    "

    local query_id=$(execute_sql "$sql" "Fetching all deployments" 2>&1 | tail -1)

    if [ $? -ne 0 ] || ! [[ "$query_id" =~ ^[a-z0-9-]+$ ]]; then
        return 1
    fi

    local results=$(get_query_results "$query_id")

    echo ""
    printf "%-30s %-10s %-25s\n" "TABLE" "GIT SHA" "DEPLOYED AT"
    echo "---------------------------------------------------------------------"

    echo "$results" | jq -r '
        .Records[] |
        "\(.[0].stringValue)\t\(.[1].stringValue)\t\(.[2].stringValue)"
    ' | while IFS=$'\t' read -r table sha date; do
        printf "%-30s %-10s %-25s\n" "$table" "$sha" "$date"
    done
}

cmd_diff_git() {
    local table_name="$1"

    if [ -z "$table_name" ]; then
        log_error "Table name required"
        echo "Usage: $0 diff-git <table-name>"
        return 1
    fi

    if ! check_git_repo; then
        return 1
    fi

    # Find DDL file
    local ddl_file=$(find_ddl_file "$table_name")
    if [ $? -ne 0 ]; then
        log_error "No DDL file found"
        return 1
    fi

    # Get deployed version
    local deployed_info=$(get_deployed_version "$table_name")

    if [ -z "$deployed_info" ]; then
        log_warning "No deployment history - cannot show diff"
        return 1
    fi

    local deployed_sha=$(echo "$deployed_info" | jq -r '.Records[0][0].stringValue // ""')
    local current_sha=$(get_file_git_sha "$ddl_file")

    if [ "$current_sha" == "$deployed_sha" ]; then
        log_success "No changes - DDL is up to date"
        return 0
    fi

    log_info "Showing git diff between deployed and current version"
    echo "========================================"

    git diff "$deployed_sha" "$current_sha" -- "$ddl_file"
}

cmd_compare_all() {
    log_info "Comparing all DDL files with Redshift"
    echo "========================================"

    if [ ! -d "$DDL_DIR" ]; then
        log_error "DDL directory not found: $DDL_DIR"
        return 1
    fi

    # Count total files
    local total_files=$(find "$DDL_DIR" -name "*.sql" -type f | wc -l | tr -d ' ')

    if [ "$total_files" -eq 0 ]; then
        log_warning "No SQL files found in $DDL_DIR"
        return 0
    fi

    log_info "Found $total_files DDL files to compare"
    echo ""

    local match_count=0
    local mismatch_count=0
    local missing_count=0
    local error_count=0
    local current=0

    for file in "$DDL_DIR"/*.sql; do
        if [ ! -f "$file" ]; then
            continue
        fi

        current=$((current + 1))
        local basename=$(basename "$file" .sql)
        local table_name=$(basename "$basename" -ddl)

        # Check if table exists
        if ! table_exists "$table_name" 2>/dev/null; then
            echo "[$current/$total_files] ⬜ $table_name - Not in Redshift"
            missing_count=$((missing_count + 1))
            continue
        fi

        # Get expected DDL from file
        local expected_ddl=$(read_ddl_file "$file" 2>/dev/null)
        if [ $? -ne 0 ]; then
            echo "[$current/$total_files] ❌ $table_name - Error reading DDL file"
            error_count=$((error_count + 1))
            continue
        fi

        # Get actual DDL from Redshift
        local actual_ddl=$(get_redshift_ddl "$table_name" 2>/dev/null)
        if [ $? -ne 0 ]; then
            echo "[$current/$total_files] ❌ $table_name - Error getting Redshift DDL"
            error_count=$((error_count + 1))
            continue
        fi

        # Canonicalize both DDLs
        local canonical_expected=$(canonicalize_ddl "$expected_ddl")
        local canonical_actual=$(canonicalize_ddl "$actual_ddl")

        # Compare
        if [ "$canonical_expected" = "$canonical_actual" ]; then
            echo "[$current/$total_files] ✅ $table_name - Schema matches"
            match_count=$((match_count + 1))
        else
            echo "[$current/$total_files] ⚠️  $table_name - Schema MISMATCH"
            mismatch_count=$((mismatch_count + 1))
        fi
    done

    echo ""
    echo "========================================"
    log_info "Comparison Summary:"
    echo "  Total files:     $total_files"
    echo "  Matches:         $match_count"
    echo "  Mismatches:      $mismatch_count"
    echo "  Not in Redshift: $missing_count"
    echo "  Errors:          $error_count"

    if [ $mismatch_count -eq 0 ] && [ $error_count -eq 0 ]; then
        log_success "All existing tables match their DDL files!"
        return 0
    else
        log_warning "Some tables have mismatches or errors"
        log_info "Use: $0 compare-ddl <table-name> to see details"
        return 1
    fi
}

cmd_deploy_all() {
    local force="${1:-false}"

    log_info "Deploying all tables from $DDL_DIR"
    echo "========================================"

    if ! check_git_repo; then
        return 1
    fi

    if [ ! -d "$DDL_DIR" ]; then
        log_error "DDL directory not found: $DDL_DIR"
        return 1
    fi

    # Count total files
    local total_files=$(find "$DDL_DIR" -name "*.sql" -type f | wc -l | tr -d ' ')

    if [ "$total_files" -eq 0 ]; then
        log_warning "No SQL files found in $DDL_DIR"
        return 0
    fi

    log_info "Found $total_files DDL files to process"
    echo ""

    local success_count=0
    local skip_count=0
    local fail_count=0
    local current=0

    for file in "$DDL_DIR"/*.sql; do
        if [ ! -f "$file" ]; then
            continue
        fi

        current=$((current + 1))
        local basename=$(basename "$file" .sql)
        local table_name=$(basename "$basename" -ddl)

        echo ""
        log_info "[$current/$total_files] Processing: $table_name"
        echo "----------------------------------------"

        # Check for uncommitted changes
        if file_has_changes "$file"; then
            log_warning "Skipping $table_name - uncommitted changes"
            log_info "Commit first: git add $file && git commit"
            skip_count=$((skip_count + 1))
            continue
        fi

        # Check if deployment needed (unless force)
        if [ "$force" != "true" ]; then
            if ! needs_deployment "$table_name" "$file" 2>/dev/null; then
                log_success "$table_name already up to date"
                skip_count=$((skip_count + 1))
                continue
            fi
        fi

        # Deploy the table
        if [ "$force" == "true" ]; then
            cmd_deploy "$table_name" "$file" "true" 2>&1 | grep -E "(✅|❌|⚠️)" || true
        else
            cmd_deploy "$table_name" "$file" "false" 2>&1 | grep -E "(✅|❌|⚠️)" || true
        fi

        if [ $? -eq 0 ]; then
            success_count=$((success_count + 1))
        else
            fail_count=$((fail_count + 1))
            log_error "Failed to deploy $table_name"
        fi
    done

    echo ""
    echo "========================================"
    log_info "Deployment Summary:"
    echo "  Total files:     $total_files"
    echo "  Deployed:        $success_count"
    echo "  Skipped:         $skip_count"
    echo "  Failed:          $fail_count"

    if [ $fail_count -eq 0 ]; then
        log_success "All deployments completed successfully!"
        return 0
    else
        log_warning "Some deployments failed - check output above"
        return 1
    fi
}

# ============================================================================
# MAIN
# ============================================================================

main() {
    local command="$1"
    [ $# -gt 0 ] && shift

    # Parse arguments
    local table_name=""
    local ddl_file=""
    local force=false

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --ddl-file)
                ddl_file="$2"
                shift 2
                ;;
            --force)
                force=true
                shift
                ;;
            *)
                if [ -z "$table_name" ]; then
                    table_name="$1"
                fi
                shift
                ;;
        esac
    done

    # Show help if no command provided
    case "$command" in
        help|--help|-h|"")
            cat <<EOF
Redshift Table Manager (Git-Integrated)
========================================

Usage: $0 <command> [arguments] [options]

Commands:
  # Git-based deployment (recommended)
  deploy <table> [--ddl-file <file>] [--force]  Deploy table from Git
  deploy-all [--force]                           Deploy all tables from ./ddl directory
  status <table>                                 Show deployment status
  history <table>                                Show deployment history
  diff-git <table>                               Show git diff between deployed and current
  deployments                                    Show all deployed tables

  # Schema verification
  compare <table> [--ddl-file <file>]            Compare single table DDL vs Redshift (metadata)
  compare-ddl <table> [--ddl-file <file>]        Compare single table DDL vs Redshift (full canonicalized)
  compare-all                                    Compare all DDL files vs Redshift tables

  # Legacy commands
  create <table> [--ddl-file <file>]             Create a table
  create-from-file <ddl-file>                    Create table from DDL file (auto-detects name)
  verify <table>                                 Verify table exists and show schema
  list                                           List all tables and status
  drop <table>                                   Drop a table
  help                                           Show this help message

Options:
  --ddl-file <file>   Use specific DDL file
  --force             Force deployment (skip checks)

Examples:
  # Deploy table (must be committed to Git)
  git add ddl/conditions.sql
  git commit -m "Add conditions table"
  $0 deploy conditions

  # Deploy all tables
  $0 deploy-all

  # Force redeploy all tables (drops and recreates)
  $0 deploy-all --force

  # Check deployment status
  $0 status conditions

  # View deployment history
  $0 history conditions

  # See git diff between deployed and current
  $0 diff-git conditions

  # Compare all DDL files with Redshift
  $0 compare-all

  # Compare single table in detail
  $0 compare-ddl conditions

  # List all tables
  $0 list

  # Force redeploy single table
  $0 deploy conditions --force

Workflow:
  1. Edit DDL file: ddl/conditions.sql
  2. Test locally
  3. Commit to Git: git commit -m "Update schema"
  4. Deploy: $0 deploy conditions
  5. Verify: $0 status conditions

Configuration:
  Edit the script to set:
    - CLUSTER_ID or WORKGROUP_NAME
    - DATABASE
    - REGION
    - DDL_DIR (default: ./ddl)

DDL File Locations (checked in order):
  1. ./ddl/<table-name>.sql
  2. ./ddl/<table-name>-ddl.sql
  3. ./<table-name>.sql
  4. ./<table-name>-ddl.sql

Prerequisites:
  - Git repository initialized
  - AWS CLI configured (aws configure)
  - jq installed (brew install jq)
  - DDL files committed to Git

EOF
            exit 0
            ;;
    esac

    # Check for required tools
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI not found. Please install it first."
        exit 1
    fi

    if ! command -v jq &> /dev/null; then
        log_error "jq not found. Please install it first."
        echo "  macOS: brew install jq"
        echo "  Linux: sudo apt-get install jq"
        exit 1
    fi

    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured"
        echo "Run: aws configure"
        exit 1
    fi

    case "$command" in
        deploy)
            if [ "$force" == "true" ]; then
                cmd_deploy "$table_name" "$ddl_file" "true"
            else
                cmd_deploy "$table_name" "$ddl_file"
            fi
            ;;
        deploy-all)
            if [ "$force" == "true" ]; then
                cmd_deploy_all "true"
            else
                cmd_deploy_all "false"
            fi
            ;;
        status)
            cmd_status "$table_name"
            ;;
        history)
            cmd_history "$table_name"
            ;;
        diff-git)
            cmd_diff_git "$table_name"
            ;;
        diff)
            log_warning "Command 'diff' has been renamed to 'diff-git'"
            log_info "Usage: $0 diff-git <table-name>"
            cmd_diff_git "$table_name"
            ;;
        deployments)
            cmd_list_deployments
            ;;
        compare)
            cmd_compare "$table_name" "$ddl_file"
            ;;
        compare-ddl)
            cmd_compare_ddl "$table_name" "$ddl_file"
            ;;
        compare-all)
            cmd_compare_all
            ;;
        create)
            cmd_create "$table_name" "$ddl_file"
            ;;
        create-from-file)
            cmd_create_from_file "$table_name"
            ;;
        verify)
            cmd_verify "$table_name"
            ;;
        list)
            cmd_list
            ;;
        drop)
            cmd_drop "$table_name"
            ;;
        *)
            log_error "Unknown command: $command"
            echo "Run '$0 help' for usage"
            exit 1
            ;;
    esac
}

main "$@"