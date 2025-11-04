#!/bin/bash

# ===================================================================
# REDEPLOY FHIR VIEWS SCRIPT - V2 WITH DEPENDENCY MANAGEMENT
# ===================================================================
# This script allows you to redeploy FHIR views with proper dependency ordering:
# 1. Drops all existing views (when doing bulk operations)
# 2. Creates new views from SQL files in dependency order
# 3. Waits for each dependency to complete before proceeding
# 4. Optionally refreshes views with data
#
# Usage:
#   ./redeploy_fhir_views.sh              # Fast mode (status checks skipped by default)
#   ./redeploy_fhir_views.sh --with-status  # Show deployment status (slower menu)
#   ./redeploy_fhir_views.sh -w           # Short form for with-status
#
# New Features:
#   - Select individual views by number (e.g., "9") to get action menu
#   - Action menu allows: Drop+Create, Create if Missing, Refresh, Drop Only, View SQL
#   - Group operations still available (L0-L4, A, F, R)
#
# Dependency hierarchy:
# - Base tables (no dependencies) are deployed first
# - Views that depend on other views are deployed after their dependencies
# ===================================================================

# Ensure we're running from project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT" || {
    echo "Error: Cannot change to project root directory"
    exit 1
}

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
CLUSTER_ID="prod-redshift-main-ue2"
DATABASE="dev"
SECRET_ARN="arn:aws:secretsmanager:us-east-2:442042533707:secret:redshift!prod-redshift-main-ue2-awsuser-yp5Lq4"
REGION="us-east-2"

# Parse command line arguments
# Default to SKIP status checks for faster loading
SKIP_STATUS_CHECK=true
if [ "$1" = "--with-status" ] || [ "$1" = "-w" ]; then
    SKIP_STATUS_CHECK=false
    echo "Status checks enabled - menu will show detailed view statistics"
else
    echo "💡 Tip: Use --with-status or -w to see deployment status (slower)"
fi

# Define dependency levels (views in same level can be deployed in parallel)
# Level 0: Base views with no dependencies on other views
DEPENDENCY_LEVEL_0=(
    "fact_fhir_practitioners_view_v1"  # Independent - practitioners data
)

# Level 1: Views that only depend on base tables
DEPENDENCY_LEVEL_1=(
    "fact_fhir_patients_view_v1"       # Depends on patient tables only
    "fact_fhir_allergy_intolerance_view_v1"  # Depends on allergy tables only
    "fact_fhir_care_plans_view_v1"     # Depends on care plan tables only
    "fact_fhir_medication_requests_view_v1"  # Depends on medication tables only
    "fact_fhir_procedures_view_v1"     # Depends on procedure tables only
    "fact_fhir_diagnostic_reports_view_v1"  # Depends on diagnostic report tables only
    "fact_fhir_document_references_view_v1"  # Depends on document reference tables only
)

# Level 2: Views that depend on Level 1 views
DEPENDENCY_LEVEL_2=(
    "fact_fhir_observations_view_v1"   # Depends on observation tables only (large view)
    "fact_fhir_conditions_view_v1"     # May depend on encounters
    "fact_fhir_encounters_view_v1"     # May depend on conditions/patients
)

# Level 3: Base reporting view that other reports depend on
DEPENDENCY_LEVEL_3=(
    "rpt_fhir_hmu_patients_v1"         # Depends on patients, conditions, encounters
)

# Level 4: Key reporting views - Medication and primary observation reports
DEPENDENCY_LEVEL_4=(
    "rpt_fhir_medication_requests_adt_meds_hmu_view_v1"  # Depends on medication views and hmu_patients
    "rpt_fhir_observations_psa_total_hmu_v1"  # Depends on observations and hmu_patients
    "rpt_fhir_observations_testosterone_total_hmu_v1"  # Depends on observations and hmu_patients
    "rpt_fhir_observations_adt_treatment_hmu_v1"  # Depends on observations and hmu_patients
)

# Level 5: Additional observation reporting views
DEPENDENCY_LEVEL_5=(
    "rpt_fhir_observations_absolute_neutrophil_count_hmu_v1"  # Depends on observations and hmu_patients
    "rpt_fhir_observations_platelet_count_hmu_v1"  # Depends on observations and hmu_patients
    "rpt_fhir_observations_hemoglobin_hmu_v1"  # Depends on observations and hmu_patients
    "rpt_fhir_observations_creatinine_hmu_v1"  # Depends on observations and hmu_patients
    "rpt_fhir_observations_egfr_hmu_v1"  # Depends on observations and hmu_patients
    "rpt_fhir_observations_alt_hmu_v1"  # Depends on observations and hmu_patients
    "rpt_fhir_observations_ast_hmu_v1"  # Depends on observations and hmu_patients
    "rpt_fhir_observations_total_bilirubin_hmu_v1"  # Depends on observations and hmu_patients
    "rpt_fhir_observations_serum_albumin_hmu_v1"  # Depends on observations and hmu_patients
    "rpt_fhir_observations_serum_potassium_hmu_v1"  # Depends on observations and hmu_patients
    "rpt_fhir_observations_hba1c_hmu_v1"  # Depends on observations and hmu_patients
    "rpt_fhir_observations_bmi_hmu_v1"  # Depends on observations and hmu_patients
    "rpt_fhir_observations_cd4_count_hmu_v1"  # Depends on observations and hmu_patients
    "rpt_fhir_observations_hiv_viral_load_hmu_v1"  # Depends on observations and hmu_patients
    "rpt_fhir_observations_psa_progression_hmu_v1"  # Depends on observations and hmu_patients
)

# Level 6: Condition reporting views
DEPENDENCY_LEVEL_6=(
    "rpt_fhir_conditions_additional_malignancy_hmu_v1"  # Depends on conditions and hmu_patients
    "rpt_fhir_conditions_active_liver_disease_hmu_v1"  # Depends on conditions and hmu_patients
    "rpt_fhir_conditions_cns_metastases_hmu_v1"  # Depends on conditions and hmu_patients
    "rpt_fhir_conditions_hsms_hmu_v1"  # Depends on conditions and hmu_patients
)

# Level 7: Union views that depend on multiple Level 5/6 views
DEPENDENCY_LEVEL_7=(
    "rpt_fhir_observations_labs_union_hmu_v1"  # Depends on all Level 5 observation views
    "rpt_fhir_conditions_union_hmu_v1"  # Depends on all Level 6 condition views
    "rpt_fhir_conditions_hsms_inferred_hmu_v1"  # Depends on adt_treatment (L4) and psa_progression (L5)
    "rpt_fhir_mcrpc_patients_hmu_v1"  # Depends on adt_treatment (L4) and psa_progression (L5)
)

# Level 8: Additional reporting views that depend on multiple levels
DEPENDENCY_LEVEL_8=(
    "rpt_fhir_diagnostic_reports_afterdx_hmu_v1"  # Depends on Level 1 diagnostic reports and Level 3 hmu_patients
)

# Combine all views in dependency order
VIEW_NAMES=()
for view in "${DEPENDENCY_LEVEL_0[@]}"; do VIEW_NAMES+=("$view"); done
for view in "${DEPENDENCY_LEVEL_1[@]}"; do VIEW_NAMES+=("$view"); done
for view in "${DEPENDENCY_LEVEL_2[@]}"; do VIEW_NAMES+=("$view"); done
for view in "${DEPENDENCY_LEVEL_3[@]}"; do VIEW_NAMES+=("$view"); done
for view in "${DEPENDENCY_LEVEL_4[@]}"; do VIEW_NAMES+=("$view"); done
for view in "${DEPENDENCY_LEVEL_5[@]}"; do VIEW_NAMES+=("$view"); done
for view in "${DEPENDENCY_LEVEL_6[@]}"; do VIEW_NAMES+=("$view"); done
for view in "${DEPENDENCY_LEVEL_7[@]}"; do VIEW_NAMES+=("$view"); done
for view in "${DEPENDENCY_LEVEL_8[@]}"; do VIEW_NAMES+=("$view"); done

# Function to print colored output
print_status() {
    echo -e "${2}${1}${NC}"
}

# Function to check if view SQL file exists (checks both directories)
view_file_exists() {
    local view_name="$1"
    if [ -f "views_deploy/${view_name}.sql" ] || [ -f "views/${view_name}.sql" ]; then
        return 0
    fi
    return 1
}

# Function to get view SQL file path (prefers deployment version)
get_view_file() {
    local view_name="$1"
    if [ -f "views_deploy/${view_name}.sql" ]; then
        echo "views_deploy/${view_name}.sql"
    elif view_file_exists "$view_name"; then
        echo "views/${view_name}.sql"
    else
        echo ""
    fi
}

# Function to get dependency level for a view
get_dependency_level() {
    local view_name="$1"

    for view in "${DEPENDENCY_LEVEL_0[@]}"; do
        if [ "$view" = "$view_name" ]; then
            echo "0"
            return
        fi
    done

    for view in "${DEPENDENCY_LEVEL_1[@]}"; do
        if [ "$view" = "$view_name" ]; then
            echo "1"
            return
        fi
    done

    for view in "${DEPENDENCY_LEVEL_2[@]}"; do
        if [ "$view" = "$view_name" ]; then
            echo "2"
            return
        fi
    done

    for view in "${DEPENDENCY_LEVEL_3[@]}"; do
        if [ "$view" = "$view_name" ]; then
            echo "3"
            return
        fi
    done

    for view in "${DEPENDENCY_LEVEL_4[@]}"; do
        if [ "$view" = "$view_name" ]; then
            echo "4"
            return
        fi
    done

    for view in "${DEPENDENCY_LEVEL_5[@]}"; do
        if [ "$view" = "$view_name" ]; then
            echo "5"
            return
        fi
    done

    for view in "${DEPENDENCY_LEVEL_6[@]}"; do
        if [ "$view" = "$view_name" ]; then
            echo "6"
            return
        fi
    done

    for view in "${DEPENDENCY_LEVEL_7[@]}"; do
        if [ "$view" = "$view_name" ]; then
            echo "7"
            return
        fi
    done

    for view in "${DEPENDENCY_LEVEL_8[@]}"; do
        if [ "$view" = "$view_name" ]; then
            echo "8"
            return
        fi
    done

    echo "-1"  # Unknown
}

# Function to execute SQL and wait for completion (silent version for DROP commands)
execute_sql_silent() {
    local sql="$1"
    local description="$2"

    # Execute the statement (don't print anything)
    local statement_id=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --secret-arn "$SECRET_ARN" \
        --sql "$sql" \
        --region "$REGION" \
        --query Id \
        --output text 2>/dev/null)

    if [ -z "$statement_id" ]; then
        return 1
    fi

    # Wait for completion silently
    local status="SUBMITTED"
    local wait_count=0
    local max_wait=60  # 1 minute timeout for drops

    while [ "$status" != "FINISHED" ] && [ "$status" != "FAILED" ] && [ "$status" != "ABORTED" ] && [ $wait_count -lt $max_wait ]; do
        sleep 1
        status=$(aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query Status \
            --output text 2>/dev/null)
        wait_count=$((wait_count + 1))
    done

    if [ "$status" = "FINISHED" ]; then
        return 0
    else
        return 1
    fi
}

# Function to execute SQL and wait for completion
execute_sql() {
    local sql="$1"
    local description="$2"

    print_status "→ $description" "$BLUE"

    # For debugging: show first 200 chars of SQL
    if [ ${#sql} -gt 200 ]; then
        print_status "  SQL Preview: ${sql:0:200}..." "$YELLOW"
    fi

    # Execute the statement
    local statement_id=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --secret-arn "$SECRET_ARN" \
        --sql "$sql" \
        --region "$REGION" \
        --query Id \
        --output text)

    if [ -z "$statement_id" ]; then
        print_status "✗ Failed to submit statement: $description" "$RED"
        print_status "  Check AWS credentials and Redshift configuration" "$RED"
        return 1
    fi

    print_status "  Statement ID: $statement_id" "$YELLOW"

    # Wait for completion
    local status="SUBMITTED"
    local wait_count=0
    local max_wait=300  # 5 minutes timeout

    while [ "$status" != "FINISHED" ] && [ "$status" != "FAILED" ] && [ "$status" != "ABORTED" ] && [ $wait_count -lt $max_wait ]; do
        sleep 2
        status=$(aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query Status \
            --output text)

        # Show progress every 10 seconds
        if [ $((wait_count % 10)) -eq 0 ]; then
            echo -n "."
        fi

        wait_count=$((wait_count + 2))
    done

    echo  # New line after dots

    if [ "$status" = "FINISHED" ]; then
        print_status "✓ Completed: $description" "$GREEN"
        return 0
    else
        print_status "✗ Failed ($status): $description" "$RED"

        # Get error details from describe-statement
        local error_msg=$(aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query 'Error' \
            --output text)

        if [ "$error_msg" != "None" ] && [ ! -z "$error_msg" ]; then
            print_status "  Error: $error_msg" "$RED"
        fi

        # Also try to get QueryString to see what was executed
        local query_string=$(aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query 'QueryString' \
            --output text)

        if [ ! -z "$query_string" ] && [ ${#query_string} -gt 0 ]; then
            print_status "  Query executed (first 500 chars): ${query_string:0:500}" "$YELLOW"
        fi

        return 1
    fi
}

# Function to check if view exists and get stats
check_view_stats() {
    local view_name="$1"

    # Check if view exists
    local exists_sql="SELECT COUNT(*) FROM pg_views WHERE viewname = '${view_name}' AND schemaname = 'public';"

    local statement_id=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --secret-arn "$SECRET_ARN" \
        --sql "$exists_sql" \
        --region "$REGION" \
        --query Id \
        --output text)

    if [ -z "$statement_id" ]; then
        echo "ERROR|0|NULL"
        return 1
    fi

    # Wait for completion
    local status="SUBMITTED"
    local timeout=30
    while [ "$status" != "FINISHED" ] && [ "$status" != "FAILED" ] && [ $timeout -gt 0 ]; do
        sleep 1
        status=$(aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query Status \
            --output text)
        timeout=$((timeout - 1))
    done

    if [ "$status" != "FINISHED" ]; then
        echo "ERROR|0|NULL"
        return 1
    fi

    local count=$(aws redshift-data get-statement-result \
        --id "$statement_id" \
        --region "$REGION" \
        --query 'Records[0][0].longValue' \
        --output text)

    if [ "$count" != "1" ]; then
        echo "NOT_EXISTS|0|NULL"
        return 1
    fi

    # View exists, now get row count and max updated_at
    # First, check if the view has an updated_at or etl_updated_at column
    local has_updated_at=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --secret-arn "$SECRET_ARN" \
        --sql "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '${view_name}' AND column_name IN ('updated_at', 'etl_updated_at');" \
        --region "$REGION" \
        --query Id \
        --output text)

    # Wait for column check
    local check_status="SUBMITTED"
    local check_timeout=30
    while [ "$check_status" != "FINISHED" ] && [ "$check_status" != "FAILED" ] && [ $check_timeout -gt 0 ]; do
        sleep 1
        check_status=$(aws redshift-data describe-statement \
            --id "$has_updated_at" \
            --region "$REGION" \
            --query Status \
            --output text)
        check_timeout=$((check_timeout - 1))
    done

    local stats_sql
    if [ "$check_status" = "FINISHED" ]; then
        local col_exists=$(aws redshift-data get-statement-result \
            --id "$has_updated_at" \
            --region "$REGION" \
            --query 'Records[0][0].longValue' \
            --output text)

        if [ "$col_exists" -gt 0 ]; then
            # Try etl_updated_at first, then updated_at
            stats_sql="SELECT COUNT(*) as row_count, COALESCE(MAX(etl_updated_at), MAX(updated_at)) as max_updated_at FROM ${view_name};"
        else
            # No updated_at column, just get count
            stats_sql="SELECT COUNT(*) as row_count FROM ${view_name};"
        fi
    else
        # Default to trying updated_at
        stats_sql="SELECT COUNT(*) as row_count, MAX(updated_at) as max_updated_at FROM ${view_name};"
    fi

    statement_id=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --secret-arn "$SECRET_ARN" \
        --sql "$stats_sql" \
        --region "$REGION" \
        --query Id \
        --output text)

    if [ -z "$statement_id" ]; then
        echo "EXISTS|ERROR|NULL"
        return 0
    fi

    # Wait for completion
    status="SUBMITTED"
    timeout=60
    while [ "$status" != "FINISHED" ] && [ "$status" != "FAILED" ] && [ $timeout -gt 0 ]; do
        sleep 1
        status=$(aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query Status \
            --output text)
        timeout=$((timeout - 1))
    done

    if [ "$status" != "FINISHED" ]; then
        echo "EXISTS|ERROR|NULL"
        return 0
    fi

    # Extract row_count and max_updated_at
    # Get the full result as JSON
    local full_result=$(aws redshift-data get-statement-result \
        --id "$statement_id" \
        --region "$REGION" \
        --output json)

    # Check if we have jq available for robust JSON parsing
    if command -v jq &> /dev/null; then
        # Use jq for reliable JSON parsing
        local row_count=$(echo "$full_result" | jq -r '.Records[0][0].longValue // .Records[0][0].stringValue // "0"')

        # Check if there's a second column (max_updated_at)
        local has_second_col=$(echo "$full_result" | jq -r '.Records[0] | length')
        if [ "$has_second_col" -gt 1 ]; then
            local max_updated=$(echo "$full_result" | jq -r '.Records[0][1].stringValue // "NULL"')
        else
            local max_updated="N/A"
        fi
    else
        # Fallback to grep/sed parsing
        # Try to extract longValue for row count
        local row_count=$(echo "$full_result" | grep -o '"longValue"[[:space:]]*:[[:space:]]*[0-9]*' | head -1 | grep -o '[0-9]*$')

        # If no longValue, try stringValue for row count
        if [ -z "$row_count" ]; then
            row_count=$(echo "$full_result" | grep -o '"stringValue"[[:space:]]*:[[:space:]]*"[0-9]*"' | head -1 | grep -o '[0-9]*')
        fi

        # Extract second field for max_updated (skip first stringValue match)
        local max_updated=$(echo "$full_result" | grep -o '"stringValue"[[:space:]]*:[[:space:]]*"[^"]*"' | sed -n '2p' | sed 's/.*"\([^"]*\)".*/\1/')

        if [ -z "$row_count" ]; then
            row_count="0"
        fi
        if [ -z "$max_updated" ]; then
            max_updated="N/A"
        fi
    fi

    # Handle None/null values
    if [ "$row_count" = "None" ] || [ "$row_count" = "null" ]; then
        row_count="0"
    fi
    if [ "$max_updated" = "None" ] || [ "$max_updated" = "null" ]; then
        max_updated="NULL"
    fi

    echo "EXISTS|$row_count|$max_updated"
    return 0
}

# Function to check if view exists (backwards compatible)
check_view_exists() {
    local view_name="$1"
    local stats=$(check_view_stats "$view_name")
    local exists=$(echo "$stats" | cut -d'|' -f1)

    if [ "$exists" = "EXISTS" ]; then
        return 0
    fi
    return 1
}

# Function to drop all views in reverse dependency order
drop_all_views() {
    print_status "\n========================================" "$BLUE"
    print_status "     DROPPING ALL EXISTING VIEWS" "$BLUE"
    print_status "========================================" "$BLUE"

    # Drop in reverse order (highest dependency level first)
    print_status "\nDropping Level 8 views (Additional reporting views)..." "$YELLOW"
    for view_name in "${DEPENDENCY_LEVEL_8[@]}"; do
        execute_sql_silent "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
        execute_sql_silent "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
    done

    print_status "\nDropping Level 7 views (Union views)..." "$YELLOW"
    for view_name in "${DEPENDENCY_LEVEL_7[@]}"; do
        execute_sql_silent "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
        execute_sql_silent "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
    done

    print_status "\nDropping Level 6 views (Condition reporting)..." "$YELLOW"
    for view_name in "${DEPENDENCY_LEVEL_6[@]}"; do
        execute_sql_silent "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
        execute_sql_silent "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
    done

    print_status "\nDropping Level 5 views (Additional observation reporting)..." "$YELLOW"
    for view_name in "${DEPENDENCY_LEVEL_5[@]}"; do
        execute_sql_silent "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
        execute_sql_silent "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
    done

    print_status "\nDropping Level 4 views (Key reporting)..." "$YELLOW"
    for view_name in "${DEPENDENCY_LEVEL_4[@]}"; do
        execute_sql_silent "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
        execute_sql_silent "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
    done

    print_status "\nDropping Level 3 views (Base reporting)..." "$YELLOW"
    for view_name in "${DEPENDENCY_LEVEL_3[@]}"; do
        execute_sql_silent "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
        execute_sql_silent "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
    done

    print_status "\nDropping Level 2 views (Complex fact views)..." "$YELLOW"
    for view_name in "${DEPENDENCY_LEVEL_2[@]}"; do
        execute_sql_silent "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
        execute_sql_silent "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
    done

    print_status "\nDropping Level 1 views (Base fact views)..." "$YELLOW"
    for view_name in "${DEPENDENCY_LEVEL_1[@]}"; do
        execute_sql_silent "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
        execute_sql_silent "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
    done

    print_status "\nDropping Level 0 views (Independent views)..." "$YELLOW"
    for view_name in "${DEPENDENCY_LEVEL_0[@]}"; do
        execute_sql_silent "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
        execute_sql_silent "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
    done

    print_status "✓ All views dropped" "$GREEN"
}

# Function to create a single view
create_view() {
    local view_name="$1"
    local sql_file=$(get_view_file "$view_name")

    # Check if SQL file exists
    if [ -z "$sql_file" ]; then
        print_status "✗ SQL file not found for view: $view_name" "$RED"
        return 1
    fi

    # Read the SQL file
    if [ ! -r "$sql_file" ]; then
        print_status "✗ Cannot read SQL file: $sql_file" "$RED"
        return 1
    fi

    # Check file size
    local file_size=$(stat -f%z "$sql_file" 2>/dev/null || stat -c%s "$sql_file" 2>/dev/null)
    print_status "  SQL file size: $file_size bytes" "$YELLOW"

    # Read SQL content
    local create_sql=$(cat "$sql_file")

    if [ -z "$create_sql" ]; then
        print_status "✗ SQL file is empty: $sql_file" "$RED"
        return 1
    fi

    execute_sql "$create_sql" "Creating view $view_name"
    return $?
}

# Function to deploy views by level
deploy_views_by_level() {
    local level="$1"
    shift
    local views=("$@")

    print_status "\n📊 Deploying Level $level views..." "$CYAN"

    local success_count=0
    local fail_count=0

    for view_name in "${views[@]}"; do
        if view_file_exists "$view_name"; then
            print_status "\nDeploying: $view_name" "$YELLOW"
            create_view "$view_name"
            if [ $? -eq 0 ]; then
                success_count=$((success_count + 1))
            else
                fail_count=$((fail_count + 1))
                print_status "  ⚠ Failed to create $view_name" "$RED"
            fi
        else
            print_status "  ⚠ Skipping $view_name (SQL file not found)" "$YELLOW"
        fi
    done

    print_status "  Level $level complete: $success_count successful, $fail_count failed" "$GREEN"
}

# Function to refresh a materialized view
refresh_view() {
    local view_name="$1"

    print_status "\n→ Refreshing view: $view_name" "$YELLOW"

    # Check if view exists and is materialized (Redshift uses svv_tables)
    local check_sql="SELECT COUNT(*) FROM svv_tables WHERE table_name = '${view_name}' AND table_schema = 'public' AND table_type = 'MATERIALIZED VIEW';"

    local statement_id=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --secret-arn "$SECRET_ARN" \
        --sql "$check_sql" \
        --region "$REGION" \
        --query Id \
        --output text 2>&1)

    if [ -z "$statement_id" ] || [[ "$statement_id" == *"error"* ]] || [[ "$statement_id" == *"Error"* ]]; then
        print_status "  ✗ Failed to execute check query: $statement_id" "$RED"
        return 1
    fi

    # Wait for check to complete with better status tracking
    local status="SUBMITTED"
    local timeout=60
    local check_count=0
    while [ "$status" != "FINISHED" ] && [ "$status" != "FAILED" ] && [ "$status" != "ABORTED" ] && [ $timeout -gt 0 ]; do
        sleep 2
        check_count=$((check_count + 1))

        local describe_output=$(aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            2>&1)

        status=$(echo "$describe_output" | grep -o '"Status": "[^"]*"' | cut -d'"' -f4)

        if [ -z "$status" ]; then
            print_status "  ⚠ Could not get statement status (attempt $check_count)" "$YELLOW"
            status="UNKNOWN"
        fi

        timeout=$((timeout - 2))
    done

    if [ "$status" = "FAILED" ] || [ "$status" = "ABORTED" ]; then
        local error_msg=$(aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query Error \
            --output text 2>/dev/null)
        print_status "  ✗ Check query failed: $error_msg" "$RED"
        return 1
    fi

    if [ "$status" != "FINISHED" ]; then
        print_status "  ✗ Check query timed out (status: $status)" "$RED"
        return 1
    fi

    # Get result
    local result_output=$(aws redshift-data get-statement-result \
        --id "$statement_id" \
        --region "$REGION" \
        2>&1)

    local is_materialized=$(echo "$result_output" | grep -o '"longValue": [0-9]*' | head -1 | awk '{print $2}')

    if [ -z "$is_materialized" ]; then
        # Try alternative parsing
        is_materialized=$(echo "$result_output" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('Records', [[{}]])[0][0].get('longValue', 0))" 2>/dev/null || echo "0")
    fi

    if [ "$is_materialized" != "1" ]; then
        print_status "  ⊙ View is not materialized (regular view), skipping refresh" "$YELLOW"
        return 2  # Return special code for "skipped (not materialized)"
    fi

    # Refresh the materialized view
    print_status "  → Executing REFRESH MATERIALIZED VIEW..." "$CYAN"
    execute_sql "REFRESH MATERIALIZED VIEW public.$view_name;" "Refreshing materialized view $view_name"
    return $?
}

# Function to deploy views by level with mode (1=drop+create, 2=create if missing, 3=refresh if present, 4=deploy all)
deploy_level_with_mode() {
    local level="$1"
    local mode="$2"
    shift 2
    local views=("$@")

    local level_name=""
    case $level in
        0) level_name="Independent Views" ;;
        1) level_name="Base Fact Views" ;;
        2) level_name="Complex Fact Views" ;;
        3) level_name="Base Reporting View" ;;
        4) level_name="Key Reporting Views" ;;
        5) level_name="Additional Observation Reports" ;;
        6) level_name="Condition Reports" ;;
        7) level_name="Union Views" ;;
        8) level_name="Additional Reporting Views" ;;
    esac

    print_status "\n========================================" "$BLUE"
    case $mode in
        1)
            print_status "     DROP & CREATE - Level $level ($level_name)" "$BLUE"
            print_status "========================================" "$BLUE"

            # Drop views in this level
            for view_name in "${views[@]}"; do
                execute_sql_silent "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
                execute_sql_silent "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
            done

            # Create views
            local success_count=0
            local fail_count=0
            for view_name in "${views[@]}"; do
                if view_file_exists "$view_name"; then
                    print_status "\nCreating: $view_name" "$YELLOW"
                    create_view "$view_name"
                    if [ $? -eq 0 ]; then
                        success_count=$((success_count + 1))
                    else
                        fail_count=$((fail_count + 1))
                    fi
                fi
            done
            print_status "\n✓ Level $level complete: $success_count successful, $fail_count failed" "$GREEN"
            ;;

        2)
            print_status "     CREATE IF MISSING - Level $level ($level_name)" "$BLUE"
            print_status "========================================" "$BLUE"

            local success_count=0
            local fail_count=0
            local skipped_count=0

            for view_name in "${views[@]}"; do
                if view_file_exists "$view_name"; then
                    if check_view_exists "$view_name"; then
                        print_status "  ⊙ Skipping $view_name (already exists)" "$YELLOW"
                        skipped_count=$((skipped_count + 1))
                    else
                        print_status "\nCreating: $view_name" "$YELLOW"
                        create_view "$view_name"
                        if [ $? -eq 0 ]; then
                            success_count=$((success_count + 1))
                        else
                            fail_count=$((fail_count + 1))
                        fi
                    fi
                fi
            done
            print_status "\n✓ Level $level complete: $success_count created, $skipped_count skipped, $fail_count failed" "$GREEN"
            ;;

        3)
            print_status "     REFRESH IF PRESENT - Level $level ($level_name)" "$BLUE"
            print_status "========================================" "$BLUE"

            local success_count=0
            local fail_count=0
            local skipped_count=0

            for view_name in "${views[@]}"; do
                if check_view_exists "$view_name"; then
                    refresh_view "$view_name"
                    local result=$?
                    if [ $result -eq 0 ]; then
                        success_count=$((success_count + 1))
                    elif [ $result -eq 2 ]; then
                        skipped_count=$((skipped_count + 1))
                    else
                        fail_count=$((fail_count + 1))
                    fi
                else
                    print_status "  ⊙ Skipping $view_name (does not exist)" "$YELLOW"
                    skipped_count=$((skipped_count + 1))
                fi
            done
            print_status "\n✓ Level $level complete: $success_count refreshed, $skipped_count skipped, $fail_count failed" "$GREEN"
            ;;

        4)
            print_status "     DEPLOY ALL - Level $level ($level_name)" "$BLUE"
            print_status "     (Create if missing, Refresh if present)" "$BLUE"
            print_status "========================================" "$BLUE"

            local created_count=0
            local refreshed_count=0
            local skipped_count=0
            local fail_count=0

            for view_name in "${views[@]}"; do
                if view_file_exists "$view_name"; then
                    if check_view_exists "$view_name"; then
                        print_status "\nRefreshing existing: $view_name" "$YELLOW"
                        refresh_view "$view_name"
                        local result=$?
                        if [ $result -eq 0 ]; then
                            refreshed_count=$((refreshed_count + 1))
                        elif [ $result -eq 2 ]; then
                            skipped_count=$((skipped_count + 1))
                        else
                            fail_count=$((fail_count + 1))
                        fi
                    else
                        print_status "\nCreating new: $view_name" "$YELLOW"
                        create_view "$view_name"
                        if [ $? -eq 0 ]; then
                            created_count=$((created_count + 1))
                        else
                            fail_count=$((fail_count + 1))
                        fi
                    fi
                fi
            done
            print_status "\n✓ Level $level complete: $created_count created, $refreshed_count refreshed, $skipped_count skipped (not materialized), $fail_count failed" "$GREEN"
            ;;

        5)
            print_status "     DROP ONLY - Level $level ($level_name)" "$BLUE"
            print_status "========================================" "$BLUE"

            local dropped_count=0
            local fail_count=0

            for view_name in "${views[@]}"; do
                print_status "\nDropping: $view_name" "$YELLOW"

                # Try dropping as materialized view first
                execute_sql_silent "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
                local mat_result=$?

                # Then try dropping as regular view
                execute_sql_silent "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
                local view_result=$?

                if [ $mat_result -eq 0 ] || [ $view_result -eq 0 ]; then
                    dropped_count=$((dropped_count + 1))
                    print_status "  ✓ Dropped $view_name" "$GREEN"
                else
                    fail_count=$((fail_count + 1))
                    print_status "  ✗ Failed to drop $view_name" "$RED"
                fi
            done
            print_status "\n✓ Level $level complete: $dropped_count dropped, $fail_count failed" "$GREEN"
            ;;
    esac
}

# Function to show level-specific menu
deploy_level_menu() {
    local level="$1"
    shift
    local views=("$@")

    local level_name=""
    case $level in
        0) level_name="Independent Views" ;;
        1) level_name="Base Fact Views" ;;
        2) level_name="Complex Fact Views" ;;
        3) level_name="Base Reporting View" ;;
        4) level_name="Key Reporting Views" ;;
        5) level_name="Additional Observation Reports" ;;
        6) level_name="Condition Reports" ;;
        7) level_name="Union Views" ;;
        8) level_name="Additional Reporting Views" ;;
    esac

    echo
    print_status "========================================" "$CYAN"
    print_status "  Level $level - $level_name" "$CYAN"
    print_status "========================================" "$CYAN"
    echo
    print_status "Select deployment mode:" "$YELLOW"
    print_status "  1) Drop and Create all views in this level" "$YELLOW"
    print_status "  2) Create views if missing (skip existing)" "$YELLOW"
    print_status "  3) Refresh materialized views if present" "$YELLOW"
    print_status "  4) Deploy all (create if missing, refresh if present)" "$YELLOW"
    print_status "  5) Drop only (remove all views in this level)" "$YELLOW"
    print_status "  B) Back to main menu" "$YELLOW"
    echo

    read -p "Select mode: " mode

    case $mode in
        1|2|3|4|5)
            deploy_level_with_mode "$level" "$mode" "${views[@]}"
            ;;
        [Bb])
            return 0
            ;;
        *)
            print_status "Invalid selection" "$RED"
            ;;
    esac
}

# Function to show view action menu
view_action_menu() {
    local view_name="$1"
    local sql_file=$(get_view_file "$view_name")
    local level=$(get_dependency_level "$view_name")

    echo
    print_status "========================================" "$CYAN"
    print_status "  View: $view_name" "$CYAN"
    print_status "  Level: $level | SQL: $sql_file" "$CYAN"
    print_status "========================================" "$CYAN"
    echo
    print_status "Select action:" "$YELLOW"
    print_status "  1) Drop and Create (full redeploy)" "$YELLOW"
    print_status "  2) Create if missing" "$YELLOW"
    print_status "  3) Refresh if present (materialized views only)" "$YELLOW"
    print_status "  4) Drop only" "$YELLOW"
    print_status "  5) View SQL file" "$YELLOW"
    print_status "  B) Back to main menu" "$YELLOW"
    echo

    read -p "Select action: " action

    case $action in
        1)
            redeploy_view "$view_name"
            ;;
        2)
            if check_view_exists "$view_name"; then
                print_status "  ⊙ View already exists, skipping" "$YELLOW"
            else
                print_status "\nCreating view: $view_name" "$YELLOW"
                # Drop first (just in case)
                execute_sql_silent "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
                execute_sql_silent "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
                create_view "$view_name"
            fi
            ;;
        3)
            if check_view_exists "$view_name"; then
                refresh_view "$view_name"
            else
                print_status "  ✗ View does not exist, cannot refresh" "$RED"
            fi
            ;;
        4)
            print_status "\nDropping view: $view_name" "$YELLOW"
            execute_sql_silent "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
            execute_sql_silent "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
            print_status "  ✓ View dropped" "$GREEN"
            ;;
        5)
            if [ -n "$sql_file" ] && [ -f "$sql_file" ]; then
                print_status "\nSQL File: $sql_file" "$CYAN"
                print_status "----------------------------------------" "$CYAN"
                head -50 "$sql_file"
                print_status "\n... (showing first 50 lines)" "$YELLOW"
            else
                print_status "  ✗ SQL file not found" "$RED"
            fi
            ;;
        [Bb])
            return 0
            ;;
        *)
            print_status "Invalid selection" "$RED"
            ;;
    esac
}

# Function to redeploy a single view (full drop and create)
redeploy_view() {
    local view_name="$1"
    local sql_file=$(get_view_file "$view_name")

    print_status "\n========================================" "$BLUE"
    print_status "Redeploying: $view_name" "$BLUE"
    local level=$(get_dependency_level "$view_name")
    print_status "Dependency Level: $level" "$CYAN"
    print_status "========================================" "$BLUE"

    # Check if SQL file exists
    if [ -z "$sql_file" ]; then
        print_status "✗ SQL file not found for view: $view_name" "$RED"
        return 1
    fi

    # Step 1: Drop existing view (detect if regular view or materialized view)
    print_status "\nStep 1: Dropping existing view (if exists)..." "$YELLOW"

    # Try to drop both types silently (one will succeed, one will fail - that's OK)
    execute_sql_silent "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
    execute_sql_silent "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"

    print_status "✓ Drop completed (if view existed)" "$GREEN"

    # Step 2: Create new view
    print_status "\nStep 2: Creating new view from $sql_file..." "$YELLOW"
    create_view "$view_name"
    if [ $? -ne 0 ]; then
        print_status "✗ Failed to create view" "$RED"
        print_status "  Please check the SQL syntax in $sql_file" "$YELLOW"
        return 1
    fi

    print_status "\n✓ Successfully redeployed: $view_name" "$GREEN"
    return 0
}

# Function to display menu with dependency information
display_menu() {
    echo
    print_status "========================================" "$BLUE"
    print_status "     FHIR VIEW REDEPLOYMENT TOOL" "$BLUE"
    print_status "     (Dependency-Aware Version)" "$BLUE"
    print_status "========================================" "$BLUE"
    echo

    print_status "📊 Views in Dependency Order:" "$YELLOW"
    echo

    # Display Level 0
    print_status "  Level 0 - Independent Views:" "$CYAN"
    local index=1
    for view_name in "${DEPENDENCY_LEVEL_0[@]}"; do
        local status="[Not Found]"
        local color="$RED"

        if view_file_exists "$view_name"; then
            if [ "$SKIP_STATUS_CHECK" = false ]; then
                local stats=$(check_view_stats "$view_name")
                local exists=$(echo "$stats" | cut -d'|' -f1)
                local row_count=$(echo "$stats" | cut -d'|' -f2)
                local max_updated=$(echo "$stats" | cut -d'|' -f3)

                if [ "$exists" = "EXISTS" ]; then
                    status="[Deployed: ${row_count} rows, max: ${max_updated}]"
                    color="$GREEN"
                elif [ "$exists" = "NOT_EXISTS" ]; then
                    status="[SQL Ready, Not Deployed]"
                    color="$YELLOW"
                else
                    status="[SQL Ready, Status Unknown]"
                    color="$YELLOW"
                fi
            else
                status="[SQL Ready]"
                color="$GREEN"
            fi
        fi

        printf "  %2d) %-40s %s\n" "$index" "$view_name" "$(echo -e "${color}${status}${NC}")"
        index=$((index + 1))
    done

    echo
    # Display Level 1
    print_status "  Level 1 - Base Fact Views:" "$CYAN"
    for view_name in "${DEPENDENCY_LEVEL_1[@]}"; do
        local status="[Not Found]"
        local color="$RED"

        if view_file_exists "$view_name"; then
            if [ "$SKIP_STATUS_CHECK" = false ]; then
                local stats=$(check_view_stats "$view_name")
                local exists=$(echo "$stats" | cut -d'|' -f1)
                local row_count=$(echo "$stats" | cut -d'|' -f2)
                local max_updated=$(echo "$stats" | cut -d'|' -f3)

                if [ "$exists" = "EXISTS" ]; then
                    status="[Deployed: ${row_count} rows, max: ${max_updated}]"
                    color="$GREEN"
                elif [ "$exists" = "NOT_EXISTS" ]; then
                    status="[SQL Ready, Not Deployed]"
                    color="$YELLOW"
                else
                    status="[SQL Ready, Status Unknown]"
                    color="$YELLOW"
                fi
            else
                status="[SQL Ready]"
                color="$GREEN"
            fi
        fi

        printf "  %2d) %-40s %s\n" "$index" "$view_name" "$(echo -e "${color}${status}${NC}")"
        index=$((index + 1))
    done

    echo
    # Display Level 2
    print_status "  Level 2 - Complex Fact Views:" "$CYAN"
    for view_name in "${DEPENDENCY_LEVEL_2[@]}"; do
        local status="[Not Found]"
        local color="$RED"

        if view_file_exists "$view_name"; then
            if [ "$SKIP_STATUS_CHECK" = false ]; then
                local stats=$(check_view_stats "$view_name")
                local exists=$(echo "$stats" | cut -d'|' -f1)
                local row_count=$(echo "$stats" | cut -d'|' -f2)
                local max_updated=$(echo "$stats" | cut -d'|' -f3)

                if [ "$exists" = "EXISTS" ]; then
                    status="[Deployed: ${row_count} rows, max: ${max_updated}]"
                    color="$GREEN"
                elif [ "$exists" = "NOT_EXISTS" ]; then
                    status="[SQL Ready, Not Deployed]"
                    color="$YELLOW"
                else
                    status="[SQL Ready, Status Unknown]"
                    color="$YELLOW"
                fi
            else
                status="[SQL Ready]"
                color="$GREEN"
            fi
        fi

        printf "  %2d) %-40s %s\n" "$index" "$view_name" "$(echo -e "${color}${status}${NC}")"
        index=$((index + 1))
    done

    echo
    # Display Level 3
    print_status "  Level 3 - Base Reporting View:" "$CYAN"
    for view_name in "${DEPENDENCY_LEVEL_3[@]}"; do
        local status="[Not Found]"
        local color="$RED"

        if view_file_exists "$view_name"; then
            if [ "$SKIP_STATUS_CHECK" = false ]; then
                local stats=$(check_view_stats "$view_name")
                local exists=$(echo "$stats" | cut -d'|' -f1)
                local row_count=$(echo "$stats" | cut -d'|' -f2)
                local max_updated=$(echo "$stats" | cut -d'|' -f3)

                if [ "$exists" = "EXISTS" ]; then
                    status="[Deployed: ${row_count} rows, max: ${max_updated}]"
                    color="$GREEN"
                elif [ "$exists" = "NOT_EXISTS" ]; then
                    status="[SQL Ready, Not Deployed]"
                    color="$YELLOW"
                else
                    status="[SQL Ready, Status Unknown]"
                    color="$YELLOW"
                fi
            else
                status="[SQL Ready]"
                color="$GREEN"
            fi
        fi

        printf "  %2d) %-40s %s\n" "$index" "$view_name" "$(echo -e "${color}${status}${NC}")"
        index=$((index + 1))
    done

    echo
    # Display Level 4
    print_status "  Level 4 - Key Reporting Views:" "$CYAN"
    for view_name in "${DEPENDENCY_LEVEL_4[@]}"; do
        local status="[Not Found]"
        local color="$RED"

        if view_file_exists "$view_name"; then
            if [ "$SKIP_STATUS_CHECK" = false ]; then
                local stats=$(check_view_stats "$view_name")
                local exists=$(echo "$stats" | cut -d'|' -f1)
                local row_count=$(echo "$stats" | cut -d'|' -f2)
                local max_updated=$(echo "$stats" | cut -d'|' -f3)

                if [ "$exists" = "EXISTS" ]; then
                    status="[Deployed: ${row_count} rows, max: ${max_updated}]"
                    color="$GREEN"
                elif [ "$exists" = "NOT_EXISTS" ]; then
                    status="[SQL Ready, Not Deployed]"
                    color="$YELLOW"
                else
                    status="[SQL Ready, Status Unknown]"
                    color="$YELLOW"
                fi
            else
                status="[SQL Ready]"
                color="$GREEN"
            fi
        fi

        printf "  %2d) %-40s %s\n" "$index" "$view_name" "$(echo -e "${color}${status}${NC}")"
        index=$((index + 1))
    done

    echo
    # Display Level 5
    print_status "  Level 5 - Additional Observation Reports:" "$CYAN"
    for view_name in "${DEPENDENCY_LEVEL_5[@]}"; do
        local status="[Not Found]"
        local color="$RED"

        if view_file_exists "$view_name"; then
            if [ "$SKIP_STATUS_CHECK" = false ]; then
                local stats=$(check_view_stats "$view_name")
                local exists=$(echo "$stats" | cut -d'|' -f1)
                local row_count=$(echo "$stats" | cut -d'|' -f2)
                local max_updated=$(echo "$stats" | cut -d'|' -f3)

                if [ "$exists" = "EXISTS" ]; then
                    status="[Deployed: ${row_count} rows, max: ${max_updated}]"
                    color="$GREEN"
                elif [ "$exists" = "NOT_EXISTS" ]; then
                    status="[SQL Ready, Not Deployed]"
                    color="$YELLOW"
                else
                    status="[SQL Ready, Status Unknown]"
                    color="$YELLOW"
                fi
            else
                status="[SQL Ready]"
                color="$GREEN"
            fi
        fi

        printf "  %2d) %-40s %s\n" "$index" "$view_name" "$(echo -e "${color}${status}${NC}")"
        index=$((index + 1))
    done

    echo
    # Display Level 6
    print_status "  Level 6 - Condition Reports:" "$CYAN"
    for view_name in "${DEPENDENCY_LEVEL_6[@]}"; do
        local status="[Not Found]"
        local color="$RED"

        if view_file_exists "$view_name"; then
            if [ "$SKIP_STATUS_CHECK" = false ]; then
                local stats=$(check_view_stats "$view_name")
                local exists=$(echo "$stats" | cut -d'|' -f1)
                local row_count=$(echo "$stats" | cut -d'|' -f2)
                local max_updated=$(echo "$stats" | cut -d'|' -f3)

                if [ "$exists" = "EXISTS" ]; then
                    status="[Deployed: ${row_count} rows, max: ${max_updated}]"
                    color="$GREEN"
                elif [ "$exists" = "NOT_EXISTS" ]; then
                    status="[SQL Ready, Not Deployed]"
                    color="$YELLOW"
                else
                    status="[SQL Ready, Status Unknown]"
                    color="$YELLOW"
                fi
            else
                status="[SQL Ready]"
                color="$GREEN"
            fi
        fi

        printf "  %2d) %-40s %s\n" "$index" "$view_name" "$(echo -e "${color}${status}${NC}")"
        index=$((index + 1))
    done

    echo
    # Display Level 7
    print_status "  Level 7 - Union Views:" "$CYAN"
    for view_name in "${DEPENDENCY_LEVEL_7[@]}"; do
        local status="[Not Found]"
        local color="$RED"

        if view_file_exists "$view_name"; then
            if [ "$SKIP_STATUS_CHECK" = false ]; then
                local stats=$(check_view_stats "$view_name")
                local exists=$(echo "$stats" | cut -d'|' -f1)
                local row_count=$(echo "$stats" | cut -d'|' -f2)
                local max_updated=$(echo "$stats" | cut -d'|' -f3)

                if [ "$exists" = "EXISTS" ]; then
                    status="[Deployed: ${row_count} rows, max: ${max_updated}]"
                    color="$GREEN"
                elif [ "$exists" = "NOT_EXISTS" ]; then
                    status="[SQL Ready, Not Deployed]"
                    color="$YELLOW"
                else
                    status="[SQL Ready, Status Unknown]"
                    color="$YELLOW"
                fi
            else
                status="[SQL Ready]"
                color="$GREEN"
            fi
        fi

        printf "  %2d) %-40s %s\n" "$index" "$view_name" "$(echo -e "${color}${status}${NC}")"
        index=$((index + 1))
    done

    echo
    # Display Level 8
    print_status "  Level 8 - Additional Reporting Views:" "$CYAN"
    for view_name in "${DEPENDENCY_LEVEL_8[@]}"; do
        local status="[Not Found]"
        local color="$RED"

        if view_file_exists "$view_name"; then
            if [ "$SKIP_STATUS_CHECK" = false ]; then
                local stats=$(check_view_stats "$view_name")
                local exists=$(echo "$stats" | cut -d'|' -f1)
                local row_count=$(echo "$stats" | cut -d'|' -f2)
                local max_updated=$(echo "$stats" | cut -d'|' -f3)

                if [ "$exists" = "EXISTS" ]; then
                    status="[Deployed: ${row_count} rows, max: ${max_updated}]"
                    color="$GREEN"
                elif [ "$exists" = "NOT_EXISTS" ]; then
                    status="[SQL Ready, Not Deployed]"
                    color="$YELLOW"
                else
                    status="[SQL Ready, Status Unknown]"
                    color="$YELLOW"
                fi
            else
                status="[SQL Ready]"
                color="$GREEN"
            fi
        fi

        printf "  %2d) %-40s %s\n" "$index" "$view_name" "$(echo -e "${color}${status}${NC}")"
        index=$((index + 1))
    done

    echo
    print_status "📊 LEVEL Operations:" "$YELLOW"
    print_status "  L0) Deploy Level 0 views (with mode options)" "$YELLOW"
    print_status "  L1) Deploy Level 1 views (with mode options)" "$YELLOW"
    print_status "  L2) Deploy Level 2 views (with mode options)" "$YELLOW"
    print_status "  L3) Deploy Level 3 views (with mode options)" "$YELLOW"
    print_status "  L4) Deploy Level 4 views (with mode options)" "$YELLOW"
    print_status "  L5) Deploy Level 5 views (with mode options)" "$YELLOW"
    print_status "  L6) Deploy Level 6 views (with mode options)" "$YELLOW"
    print_status "  L7) Deploy Level 7 views (with mode options)" "$YELLOW"
    print_status "  L8) Deploy Level 8 views (with mode options)" "$YELLOW"
    echo
    print_status "🔧 BULK Operations:" "$YELLOW"
    print_status "  A) Redeploy ALL views (drops all, then creates in dependency order)" "$YELLOW"
    print_status "  D) Deploy ALL views (create if missing, refresh if present)" "$YELLOW"
    print_status "  F) Redeploy FACT views only" "$YELLOW"
    print_status "  R) Redeploy REPORTING views only" "$YELLOW"
    print_status "  Q) Quit" "$YELLOW"
    echo
}

# Function to get view by number
get_view_by_number() {
    local num=$1
    if [ $num -ge 1 ] && [ $num -le ${#VIEW_NAMES[@]} ]; then
        echo "${VIEW_NAMES[$((num-1))]}"
        return 0
    fi
    return 1
}

# Function to redeploy all views with dependency management
redeploy_all_views() {
    print_status "\n========================================" "$BLUE"
    print_status "     REDEPLOYING ALL VIEWS" "$BLUE"
    print_status "   (With Dependency Management)" "$BLUE"
    print_status "========================================" "$BLUE"

    # Step 1: Drop all views first
    drop_all_views

    # Step 2: Deploy views in dependency order
    print_status "\n========================================" "$BLUE"
    print_status "     CREATING VIEWS IN DEPENDENCY ORDER" "$BLUE"
    print_status "========================================" "$BLUE"

    deploy_views_by_level 0 "${DEPENDENCY_LEVEL_0[@]}"
    deploy_views_by_level 1 "${DEPENDENCY_LEVEL_1[@]}"
    deploy_views_by_level 2 "${DEPENDENCY_LEVEL_2[@]}"
    deploy_views_by_level 3 "${DEPENDENCY_LEVEL_3[@]}"
    deploy_views_by_level 4 "${DEPENDENCY_LEVEL_4[@]}"
    deploy_views_by_level 5 "${DEPENDENCY_LEVEL_5[@]}"
    deploy_views_by_level 6 "${DEPENDENCY_LEVEL_6[@]}"
    deploy_views_by_level 7 "${DEPENDENCY_LEVEL_7[@]}"
    deploy_views_by_level 8 "${DEPENDENCY_LEVEL_8[@]}"

    print_status "\n========================================" "$BLUE"
    print_status "✓ All views redeployed in dependency order" "$GREEN"
    print_status "========================================" "$BLUE"
}

# Function to deploy all views (create if missing, refresh if present)
deploy_all_views() {
    print_status "\n========================================" "$BLUE"
    print_status "     DEPLOYING ALL VIEWS" "$BLUE"
    print_status "   (Create if missing, Refresh if present)" "$BLUE"
    print_status "========================================" "$BLUE"

    # Deploy views in dependency order using mode 4
    deploy_level_with_mode 0 4 "${DEPENDENCY_LEVEL_0[@]}"
    deploy_level_with_mode 1 4 "${DEPENDENCY_LEVEL_1[@]}"
    deploy_level_with_mode 2 4 "${DEPENDENCY_LEVEL_2[@]}"
    deploy_level_with_mode 3 4 "${DEPENDENCY_LEVEL_3[@]}"
    deploy_level_with_mode 4 4 "${DEPENDENCY_LEVEL_4[@]}"
    deploy_level_with_mode 5 4 "${DEPENDENCY_LEVEL_5[@]}"
    deploy_level_with_mode 6 4 "${DEPENDENCY_LEVEL_6[@]}"
    deploy_level_with_mode 7 4 "${DEPENDENCY_LEVEL_7[@]}"
    deploy_level_with_mode 8 4 "${DEPENDENCY_LEVEL_8[@]}"

    print_status "\n========================================" "$BLUE"
    print_status "✓ All views deployed in dependency order" "$GREEN"
    print_status "========================================" "$BLUE"
}

# Function to redeploy fact views only
redeploy_fact_views() {
    print_status "\n========================================" "$BLUE"
    print_status "     REDEPLOYING FACT VIEWS" "$BLUE"
    print_status "========================================" "$BLUE"

    # Drop fact views in reverse order
    for view_name in "${DEPENDENCY_LEVEL_2[@]}"; do
        if [[ $view_name == fact_* ]]; then
            execute_sql "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
            execute_sql "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
        fi
    done

    for view_name in "${DEPENDENCY_LEVEL_1[@]}"; do
        if [[ $view_name == fact_* ]]; then
            execute_sql "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
            execute_sql "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
        fi
    done

    for view_name in "${DEPENDENCY_LEVEL_0[@]}"; do
        if [[ $view_name == fact_* ]]; then
            execute_sql "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
            execute_sql "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
        fi
    done

    # Deploy fact views in dependency order
    print_status "\nCreating fact views in dependency order..." "$YELLOW"

    for view_name in "${DEPENDENCY_LEVEL_0[@]}"; do
        if [[ $view_name == fact_* ]] && view_file_exists "$view_name"; then
            create_view "$view_name"
        fi
    done

    for view_name in "${DEPENDENCY_LEVEL_1[@]}"; do
        if [[ $view_name == fact_* ]] && view_file_exists "$view_name"; then
            create_view "$view_name"
        fi
    done

    for view_name in "${DEPENDENCY_LEVEL_2[@]}"; do
        if [[ $view_name == fact_* ]] && view_file_exists "$view_name"; then
            create_view "$view_name"
        fi
    done
}

# Function to redeploy reporting views only
redeploy_reporting_views() {
    print_status "\n========================================" "$BLUE"
    print_status "     REDEPLOYING REPORTING VIEWS" "$BLUE"
    print_status "========================================" "$BLUE"

    # Drop reporting views in reverse order (Level 6, 5, 4, 3)
    print_status "\nDropping Level 6 reporting views..." "$YELLOW"
    for view_name in "${DEPENDENCY_LEVEL_6[@]}"; do
        if [[ $view_name == rpt_* ]]; then
            execute_sql_silent "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
            execute_sql_silent "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
        fi
    done

    print_status "\nDropping Level 5 reporting views..." "$YELLOW"
    for view_name in "${DEPENDENCY_LEVEL_5[@]}"; do
        if [[ $view_name == rpt_* ]]; then
            execute_sql_silent "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
            execute_sql_silent "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
        fi
    done

    print_status "\nDropping Level 4 reporting views..." "$YELLOW"
    for view_name in "${DEPENDENCY_LEVEL_4[@]}"; do
        if [[ $view_name == rpt_* ]]; then
            execute_sql_silent "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
            execute_sql_silent "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
        fi
    done

    print_status "\nDropping Level 3 reporting views..." "$YELLOW"
    for view_name in "${DEPENDENCY_LEVEL_3[@]}"; do
        if [[ $view_name == rpt_* ]]; then
            execute_sql_silent "DROP MATERIALIZED VIEW IF EXISTS public.$view_name CASCADE;" "Dropping materialized view $view_name"
            execute_sql_silent "DROP VIEW IF EXISTS public.$view_name CASCADE;" "Dropping regular view $view_name"
        fi
    done

    print_status "\nCreating reporting views in dependency order..." "$YELLOW"

    # Create Level 3 first (base reporting)
    for view_name in "${DEPENDENCY_LEVEL_3[@]}"; do
        if [[ $view_name == rpt_* ]] && view_file_exists "$view_name"; then
            create_view "$view_name"
        fi
    done

    # Then create Level 4 (key reporting)
    for view_name in "${DEPENDENCY_LEVEL_4[@]}"; do
        if [[ $view_name == rpt_* ]] && view_file_exists "$view_name"; then
            create_view "$view_name"
        fi
    done

    # Then create Level 5 (additional observation reporting)
    for view_name in "${DEPENDENCY_LEVEL_5[@]}"; do
        if [[ $view_name == rpt_* ]] && view_file_exists "$view_name"; then
            create_view "$view_name"
        fi
    done

    # Finally create Level 6 (condition reporting)
    for view_name in "${DEPENDENCY_LEVEL_6[@]}"; do
        if [[ $view_name == rpt_* ]] && view_file_exists "$view_name"; then
            create_view "$view_name"
        fi
    done
}

# Main script
main() {
    # Check AWS CLI is installed
    if ! command -v aws &> /dev/null; then
        print_status "Error: AWS CLI is not installed" "$RED"
        exit 1
    fi

    # Check AWS credentials
    aws sts get-caller-identity &> /dev/null
    if [ $? -ne 0 ]; then
        print_status "Error: AWS credentials not configured" "$RED"
        exit 1
    fi

    # Display configuration
    print_status "Configuration:" "$BLUE"
    print_status "  Cluster: $CLUSTER_ID" "$NC"
    print_status "  Database: $DATABASE" "$NC"
    print_status "  Region: $REGION" "$NC"
    echo
    print_status "📋 Dependency Management Enabled:" "$BLUE"
    print_status "  • Views are deployed in dependency order" "$NC"
    print_status "  • Bulk operations drop ALL views first" "$NC"
    print_status "  • Each level completes before the next begins" "$NC"

    while true; do
        display_menu

        read -p "Select option: " choice

        case $choice in
            [0-9]|[0-9][0-9])
                view_name=$(get_view_by_number $choice)
                if [ $? -eq 0 ]; then
                    view_action_menu "$view_name"
                else
                    print_status "Invalid selection: $choice" "$RED"
                fi
                ;;
            [Ll]0)
                deploy_level_menu 0 "${DEPENDENCY_LEVEL_0[@]}"
                ;;
            [Ll]1)
                deploy_level_menu 1 "${DEPENDENCY_LEVEL_1[@]}"
                ;;
            [Ll]2)
                deploy_level_menu 2 "${DEPENDENCY_LEVEL_2[@]}"
                ;;
            [Ll]3)
                deploy_level_menu 3 "${DEPENDENCY_LEVEL_3[@]}"
                ;;
            [Ll]4)
                deploy_level_menu 4 "${DEPENDENCY_LEVEL_4[@]}"
                ;;
            [Ll]5)
                deploy_level_menu 5 "${DEPENDENCY_LEVEL_5[@]}"
                ;;
            [Ll]6)
                deploy_level_menu 6 "${DEPENDENCY_LEVEL_6[@]}"
                ;;
            [Ll]7)
                deploy_level_menu 7 "${DEPENDENCY_LEVEL_7[@]}"
                ;;
            [Ll]8)
                deploy_level_menu 8 "${DEPENDENCY_LEVEL_8[@]}"
                ;;
            [Aa])
                redeploy_all_views
                ;;
            [Dd])
                deploy_all_views
                ;;
            [Ff])
                redeploy_fact_views
                ;;
            [Rr])
                redeploy_reporting_views
                ;;
            [Qq])
                print_status "Goodbye!" "$GREEN"
                exit 0
                ;;
            *)
                print_status "Invalid selection: $choice" "$RED"
                ;;
        esac

        echo
        read -p "Press Enter to continue..."
    done
}

# Run main function
main "$@"