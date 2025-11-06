#!/bin/bash

# ===================================================================
# POPULATE MANAGED VIEWS TABLE
# ===================================================================
# This script populates the managed_views table with all views
# defined in redeploy_fhir_views.sh, including their dependency
# levels and metadata.
#
# Usage:
#   ./populate_managed_views.sh
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

# Configuration - matches redeploy_fhir_views.sh
CLUSTER_ID="prod-redshift-main-ue2"
DATABASE="dev"
SECRET_ARN="arn:aws:secretsmanager:us-east-2:442042533707:secret:redshift!prod-redshift-main-ue2-awsuser-yp5Lq4"
REGION="us-east-2"

# Function to print colored output
print_status() {
    echo -e "${2}${1}${NC}"
}

# Function to execute SQL and wait for completion
execute_sql() {
    local sql="$1"
    local description="$2"

    print_status "→ $description" "$BLUE"

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
        return 1
    fi

    # Wait for completion
    local status="SUBMITTED"
    local wait_count=0
    local max_wait=60

    while [ "$status" != "FINISHED" ] && [ "$status" != "FAILED" ] && [ "$status" != "ABORTED" ] && [ $wait_count -lt $max_wait ]; do
        sleep 2
        status=$(aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query Status \
            --output text)
        wait_count=$((wait_count + 2))
    done

    if [ "$status" = "FINISHED" ]; then
        print_status "✓ Completed: $description" "$GREEN"
        return 0
    else
        print_status "✗ Failed ($status): $description" "$RED"

        # Get error details
        local error_msg=$(aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query 'Error' \
            --output text)

        if [ "$error_msg" != "None" ] && [ ! -z "$error_msg" ]; then
            print_status "  Error: $error_msg" "$RED"
        fi

        return 1
    fi
}

# Function to determine view category
get_view_category() {
    local view_name="$1"
    if [[ $view_name == fact_* ]]; then
        echo "FACT"
    elif [[ $view_name == rpt_* ]]; then
        echo "REPORTING"
    else
        echo "OTHER"
    fi
}

# Function to determine view type from SQL file
get_view_type() {
    local view_name="$1"
    local sql_file=""

    if [ -f "views_deploy/${view_name}.sql" ]; then
        sql_file="views_deploy/${view_name}.sql"
    elif [ -f "views/${view_name}.sql" ]; then
        sql_file="views/${view_name}.sql"
    else
        echo "UNKNOWN"
        return
    fi

    # Check if SQL contains "CREATE MATERIALIZED VIEW"
    if grep -q "CREATE MATERIALIZED VIEW" "$sql_file"; then
        echo "MATERIALIZED"
    else
        echo "REGULAR"
    fi
}

# Function to get SQL file path
get_sql_file_path() {
    local view_name="$1"
    if [ -f "views_deploy/${view_name}.sql" ]; then
        echo "views_deploy/${view_name}.sql"
    elif [ -f "views/${view_name}.sql" ]; then
        echo "views/${view_name}.sql"
    else
        echo "NOT_FOUND"
    fi
}

# Function to insert/update view record
upsert_view() {
    local view_name="$1"
    local level="$2"
    local view_type=$(get_view_type "$view_name")
    local view_category=$(get_view_category "$view_name")
    local sql_file=$(get_sql_file_path "$view_name")

    # Escape single quotes for SQL
    view_name_escaped="${view_name//\'/\'\'}"
    sql_file_escaped="${sql_file//\'/\'\'}"

    local sql="
    BEGIN TRANSACTION;

    -- Delete if exists
    DELETE FROM public.managed_views WHERE view_name = '${view_name_escaped}';

    -- Insert new record
    INSERT INTO public.managed_views (
        view_name,
        dependency_level,
        view_type,
        view_category,
        sql_file_path,
        last_updated,
        created_at
    ) VALUES (
        '${view_name_escaped}',
        ${level},
        '${view_type}',
        '${view_category}',
        '${sql_file_escaped}',
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    );

    COMMIT;
    "

    execute_sql "$sql" "Upserting view: $view_name (Level $level)"
}

# Main script
main() {
    print_status "\n========================================" "$BLUE"
    print_status "  POPULATE MANAGED VIEWS TABLE" "$BLUE"
    print_status "========================================" "$BLUE"
    echo

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

    print_status "Configuration:" "$BLUE"
    print_status "  Cluster: $CLUSTER_ID" "$NC"
    print_status "  Database: $DATABASE" "$NC"
    print_status "  Region: $REGION" "$NC"
    echo

    # Define all views by level (copied from redeploy_fhir_views.sh)

    # Level 0: Independent views
    declare -a LEVEL_0=(
        "fact_fhir_practitioners_view_v1"
    )

    # Level 1: Base fact views
    declare -a LEVEL_1=(
        "fact_fhir_patients_view_v1"
        "fact_fhir_allergy_intolerance_view_v1"
        "fact_fhir_care_plans_view_v1"
        "fact_fhir_medication_requests_view_v1"
        "fact_fhir_procedures_view_v1"
        "fact_fhir_diagnostic_reports_view_v1"
        "fact_fhir_document_references_view_v1"
    )

    # Level 2: Complex fact views
    declare -a LEVEL_2=(
        "fact_fhir_observations_view_v1"
        "fact_fhir_observations_vital_signs_view_v1"
        "fact_fhir_conditions_view_v1"
        "fact_fhir_encounters_view_v1"
    )

    # Level 3: Base reporting view
    declare -a LEVEL_3=(
        "rpt_fhir_hmu_patients_v1"
    )

    # Level 4: Key reporting views
    declare -a LEVEL_4=(
        "rpt_fhir_medication_requests_adt_meds_hmu_view_v1"
        "rpt_fhir_observations_psa_total_hmu_v1"
        "rpt_fhir_observations_testosterone_total_hmu_v1"
        "rpt_fhir_observations_adt_treatment_hmu_v1"
    )

    # Level 5: Additional observation reporting views
    declare -a LEVEL_5=(
        "rpt_fhir_observations_absolute_neutrophil_count_hmu_v1"
        "rpt_fhir_observations_platelet_count_hmu_v1"
        "rpt_fhir_observations_hemoglobin_hmu_v1"
        "rpt_fhir_observations_creatinine_hmu_v1"
        "rpt_fhir_observations_egfr_hmu_v1"
        "rpt_fhir_observations_alt_hmu_v1"
        "rpt_fhir_observations_ast_hmu_v1"
        "rpt_fhir_observations_total_bilirubin_hmu_v1"
        "rpt_fhir_observations_serum_albumin_hmu_v1"
        "rpt_fhir_observations_serum_potassium_hmu_v1"
        "rpt_fhir_observations_hba1c_hmu_v1"
        "rpt_fhir_observations_bmi_hmu_v1"
        "rpt_fhir_observations_cd4_count_hmu_v1"
        "rpt_fhir_observations_hiv_viral_load_hmu_v1"
        "rpt_fhir_observations_psa_progression_hmu_v1"
    )

    # Level 6: Condition reporting views
    declare -a LEVEL_6=(
        "rpt_fhir_conditions_additional_malignancy_hmu_v1"
        "rpt_fhir_conditions_active_liver_disease_hmu_v1"
        "rpt_fhir_conditions_cardiac_hmu_v1"
        "rpt_fhir_conditions_cns_metastases_hmu_v1"
        "rpt_fhir_conditions_hsms_hmu_v1"
    )

    # Level 7: Union views
    declare -a LEVEL_7=(
        "rpt_fhir_observations_labs_union_hmu_v1"
        "rpt_fhir_conditions_union_hmu_v1"
        "rpt_fhir_conditions_hsms_inferred_hmu_v1"
        "rpt_fhir_mcrpc_patients_hmu_v1"
    )

    # Level 8: Additional reporting views
    declare -a LEVEL_8=(
        "rpt_fhir_diagnostic_reports_afterdx_hmu_v1"
    )

    print_status "📊 Processing views by level..." "$CYAN"
    echo

    # Process each level
    print_status "Level 0: Independent Views" "$YELLOW"
    for view_name in "${LEVEL_0[@]}"; do
        upsert_view "$view_name" 0
    done

    print_status "\nLevel 1: Base Fact Views" "$YELLOW"
    for view_name in "${LEVEL_1[@]}"; do
        upsert_view "$view_name" 1
    done

    print_status "\nLevel 2: Complex Fact Views" "$YELLOW"
    for view_name in "${LEVEL_2[@]}"; do
        upsert_view "$view_name" 2
    done

    print_status "\nLevel 3: Base Reporting View" "$YELLOW"
    for view_name in "${LEVEL_3[@]}"; do
        upsert_view "$view_name" 3
    done

    print_status "\nLevel 4: Key Reporting Views" "$YELLOW"
    for view_name in "${LEVEL_4[@]}"; do
        upsert_view "$view_name" 4
    done

    print_status "\nLevel 5: Additional Observation Reports" "$YELLOW"
    for view_name in "${LEVEL_5[@]}"; do
        upsert_view "$view_name" 5
    done

    print_status "\nLevel 6: Condition Reports" "$YELLOW"
    for view_name in "${LEVEL_6[@]}"; do
        upsert_view "$view_name" 6
    done

    print_status "\nLevel 7: Union Views" "$YELLOW"
    for view_name in "${LEVEL_7[@]}"; do
        upsert_view "$view_name" 7
    done

    print_status "\nLevel 8: Additional Reporting Views" "$YELLOW"
    for view_name in "${LEVEL_8[@]}"; do
        upsert_view "$view_name" 8
    done

    print_status "\n========================================" "$GREEN"
    print_status "✓ All views processed successfully" "$GREEN"
    print_status "========================================" "$GREEN"
    echo

    # Show summary
    local summary_sql="
    SELECT
        dependency_level,
        view_category,
        view_type,
        COUNT(*) as view_count
    FROM public.managed_views
    GROUP BY dependency_level, view_category, view_type
    ORDER BY dependency_level, view_category, view_type;
    "

    print_status "📊 Summary:" "$CYAN"
    execute_sql "$summary_sql" "Generating summary report"
}

# Run main function
main "$@"
