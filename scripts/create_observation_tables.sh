#!/bin/bash

# Create missing observation tables manually using Redshift Data API
# This works around HMUObservation job failures

CLUSTER_ID="to-prd-redshift"
DATABASE="hmu-healthlake-database"
SECRET_ARN="arn:aws:secretsmanager:us-east-2:442042533707:secret:redshift!prod-redshift-main-ue2-awsuser-yp5Lq4"
REGION="us-east-2"

execute_sql() {
    local sql="$1"
    local description="$2"

    echo "→ $description"
    local statement_id=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --secret-arn "$SECRET_ARN" \
        --sql "$sql" \
        --region "$REGION" \
        --query Id \
        --output text)

    if [ -z "$statement_id" ]; then
        echo "✗ Failed to submit statement"
        return 1
    fi

    echo "  Statement ID: $statement_id"

    # Wait for completion
    local status="SUBMITTED"
    while [ "$status" = "SUBMITTED" ] || [ "$status" = "STARTED" ] || [ "$status" = "PICKED" ]; do
        sleep 2
        status=$(aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query Status \
            --output text)
        echo -n "."
    done
    echo ""

    if [ "$status" = "FINISHED" ]; then
        echo "✓ Completed: $description"
        return 0
    else
        echo "✗ Failed ($status): $description"
        local error=$(aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query Error \
            --output text)
        echo "  Error: $error"
        return 1
    fi
}

echo "========================================="
echo "Creating Observation Tables in Redshift"
echo "========================================="
echo ""

# observation_codes table
execute_sql "CREATE TABLE IF NOT EXISTS public.observation_codes (
    observation_id VARCHAR(255),
    code_code VARCHAR(50),
    code_system VARCHAR(255),
    code_display VARCHAR(255),
    code_text VARCHAR(500),
    meta_last_updated TIMESTAMP
) SORTKEY (observation_id, code_code);" "Creating observation_codes table"

echo ""

# observation_notes table
execute_sql "CREATE TABLE IF NOT EXISTS public.observation_notes (
    observation_id VARCHAR(255),
    note_text VARCHAR(MAX),
    note_author_reference VARCHAR(255),
    note_time TIMESTAMP,
    meta_last_updated TIMESTAMP
) SORTKEY (observation_id);" "Creating observation_notes table"

echo ""

# observation_reference_ranges table
execute_sql "CREATE TABLE IF NOT EXISTS public.observation_reference_ranges (
    observation_id VARCHAR(255),
    low_value DECIMAL(18,6),
    low_unit VARCHAR(50),
    low_system VARCHAR(255),
    low_code VARCHAR(50),
    high_value DECIMAL(18,6),
    high_unit VARCHAR(50),
    high_system VARCHAR(255),
    high_code VARCHAR(50),
    type_text VARCHAR(500),
    applies_to_code VARCHAR(50),
    applies_to_system VARCHAR(255),
    applies_to_display VARCHAR(255),
    age_low DECIMAL(18,6),
    age_high DECIMAL(18,6),
    text VARCHAR(1000),
    meta_last_updated TIMESTAMP
) SORTKEY (observation_id);" "Creating observation_reference_ranges table"

echo ""

# observation_components table
execute_sql "CREATE TABLE IF NOT EXISTS public.observation_components (
    observation_id VARCHAR(255),
    component_code_code VARCHAR(50),
    component_code_system VARCHAR(255),
    component_code_display VARCHAR(255),
    component_value_quantity DECIMAL(18,6),
    component_value_unit VARCHAR(50),
    component_value_system VARCHAR(255),
    component_value_code VARCHAR(50),
    component_value_string VARCHAR(MAX),
    component_value_boolean BOOLEAN,
    component_value_integer INTEGER,
    component_value_datetime TIMESTAMP,
    component_value_period_start TIMESTAMP,
    component_value_period_end TIMESTAMP,
    component_data_absent_reason_code VARCHAR(50),
    component_data_absent_reason_system VARCHAR(255),
    component_data_absent_reason_display VARCHAR(255),
    meta_last_updated TIMESTAMP
) SORTKEY (observation_id, component_code_code);" "Creating observation_components table"

echo ""

# observation_derived_from table
execute_sql "CREATE TABLE IF NOT EXISTS public.observation_derived_from (
    observation_id VARCHAR(255),
    derived_from_reference VARCHAR(255),
    meta_last_updated TIMESTAMP
) SORTKEY (observation_id, derived_from_reference);" "Creating observation_derived_from table"

echo ""
echo "========================================="
echo "All observation tables created!"
echo "========================================="
