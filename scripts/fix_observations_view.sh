#!/bin/bash

CLUSTER_ID="prod-redshift-main-ue2"
DATABASE="dev"
SECRET_ARN="arn:aws:secretsmanager:us-east-2:442042533707:secret:redshift!prod-redshift-main-ue2-awsuser-yp5Lq4"
REGION="us-east-2"

echo "Step 1: Dropping fact_fhir_observations_view_v1 from all schemas..."

# Try dropping from default schema
aws redshift-data execute-statement \
    --cluster-identifier "$CLUSTER_ID" \
    --database "$DATABASE" \
    --secret-arn "$SECRET_ARN" \
    --sql "DROP MATERIALIZED VIEW IF EXISTS fact_fhir_observations_view_v1 CASCADE;" \
    --region "$REGION" >/dev/null 2>&1

sleep 2

# Try dropping from public schema explicitly
statement_id=$(aws redshift-data execute-statement \
    --cluster-identifier "$CLUSTER_ID" \
    --database "$DATABASE" \
    --secret-arn "$SECRET_ARN" \
    --sql "DROP MATERIALIZED VIEW IF EXISTS public.fact_fhir_observations_view_v1 CASCADE;" \
    --region "$REGION" \
    --query Id \
    --output text)

echo "Dropping view, Statement ID: $statement_id"
sleep 5

echo ""
echo "Step 2: Creating fact_fhir_observations_view_v1 in public schema..."

# Read the SQL file - skip first 36 lines (comments) and replace schema
SQL=$(sed '1,36d' /Users/ken/code/ThirdOpinion/ThirdOpinion.ETL/views/fact_fhir_observations_view_v1.sql | sed 's/CREATE MATERIALIZED VIEW fact_fhir_observations_view_v1/CREATE MATERIALIZED VIEW public.fact_fhir_observations_view_v1/')

statement_id=$(aws redshift-data execute-statement \
    --cluster-identifier "$CLUSTER_ID" \
    --database "$DATABASE" \
    --secret-arn "$SECRET_ARN" \
    --sql "$SQL" \
    --region "$REGION" \
    --query Id \
    --output text)

echo "Statement ID: $statement_id"
echo "Waiting for completion (this may take several minutes)..."

status="SUBMITTED"
count=0
while [ "$status" = "SUBMITTED" ] || [ "$status" = "STARTED" ] || [ "$status" = "PICKED" ]; do
    sleep 10
    status=$(aws redshift-data describe-statement \
        --id "$statement_id" \
        --region "$REGION" \
        --query Status \
        --output text 2>/dev/null)
    count=$((count + 1))
    if [ $((count % 6)) -eq 0 ]; then
        echo "  Still running... Status: $status ($(date +%H:%M:%S))"
    fi
done

if [ "$status" = "FINISHED" ]; then
    echo "✓ Successfully created public.fact_fhir_observations_view_v1"
else
    echo "✗ Failed to create view with status: $status"
    aws redshift-data describe-statement \
        --id "$statement_id" \
        --region "$REGION" \
        --query 'Error' \
        --output text
fi
