#!/bin/bash

CLUSTER_ID="prod-redshift-main-ue2"
DATABASE="dev"
SECRET_ARN="arn:aws:secretsmanager:us-east-2:442042533707:secret:redshift!prod-redshift-main-ue2-awsuser-yp5Lq4"
REGION="us-east-2"

echo "Creating fact_fhir_observations_view_v1..."

SQL=$(cat /Users/ken/code/ThirdOpinion/ThirdOpinion.ETL/views/fact_fhir_observations_view_v1.sql)

statement_id=$(aws redshift-data execute-statement \
    --cluster-identifier "$CLUSTER_ID" \
    --database "$DATABASE" \
    --secret-arn "$SECRET_ARN" \
    --sql "$SQL" \
    --region "$REGION" \
    --query Id \
    --output text)

echo "Statement ID: $statement_id"
echo "Waiting for completion..."

status="SUBMITTED"
while [ "$status" = "SUBMITTED" ] || [ "$status" = "STARTED" ] || [ "$status" = "PICKED" ]; do
    sleep 5
    status=$(aws redshift-data describe-statement \
        --id "$statement_id" \
        --region "$REGION" \
        --query Status \
        --output text)
    echo "  Status: $status ($(date +%H:%M:%S))"
done

if [ "$status" = "FINISHED" ]; then
    echo "✓ Successfully created fact_fhir_observations_view_v1"
else
    echo "✗ Failed to create view"
    aws redshift-data describe-statement \
        --id "$statement_id" \
        --region "$REGION" \
        --query 'Error' \
        --output text
fi
