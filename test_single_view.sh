#!/bin/bash

# Test creating a single view to see the actual error

CLUSTER_ID="prod-redshift-main-ue2"
DATABASE="dev"
SECRET_ARN="arn:aws:secretsmanager:us-east-2:442042533707:secret:redshift!prod-redshift-main-ue2-awsuser-yp5Lq4"
REGION="us-east-2"

SQL_FILE="fact_fhir_patients_view_v2.sql"

echo "Testing with: $SQL_FILE"
echo "----------------------------------------"

# Execute and capture the statement ID
STATEMENT_ID=$(aws redshift-data execute-statement \
    --cluster-identifier "$CLUSTER_ID" \
    --database "$DATABASE" \
    --secret-arn "$SECRET_ARN" \
    --sql "$(cat $SQL_FILE)" \
    --region "$REGION" \
    --query 'Id' \
    --output text)

echo "Statement ID: $STATEMENT_ID"

# Wait a moment
sleep 3

# Get the status and any error
echo "Checking status..."
STATUS_JSON=$(aws redshift-data describe-statement \
    --id "$STATEMENT_ID" \
    --region "$REGION" \
    --output json)

STATUS=$(echo "$STATUS_JSON" | jq -r '.Status')
ERROR_MSG=$(echo "$STATUS_JSON" | jq -r '.Error // empty')

echo "Status: $STATUS"

if [ "$STATUS" = "FAILED" ]; then
    if [[ "$ERROR_MSG" == *"already exists"* ]]; then
        echo "Result: View already exists (success)"
        exit 0
    else
        echo "Error: $ERROR_MSG"
        exit 1
    fi
elif [ "$STATUS" = "FINISHED" ]; then
    echo "Result: Successfully created"
    exit 0
else
    echo "Result: $STATUS"
    exit 2
fi