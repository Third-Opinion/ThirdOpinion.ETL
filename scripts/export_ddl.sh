#!/bin/bash

# Script to export DDL for all tables in the dev database
# Each table will be exported to a separate .ddl file

set -euo pipefail

# Configuration
PROFILE="to-prd-admin"
CLUSTER_ID="prod-redshift-main-ue2"
DATABASE="dev"
SCHEMA="public"
OUTPUT_DIR="ddl_exports"

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo "🚀 Starting DDL export for all tables in $DATABASE database..."
echo "📁 Output directory: $OUTPUT_DIR"
echo ""

# Get list of tables
echo "📋 Getting list of tables..."
TABLES=$(aws redshift-data execute-statement \
    --cluster-identifier "$CLUSTER_ID" \
    --database "$DATABASE" \
    --sql "SELECT tablename FROM pg_tables WHERE schemaname = '$SCHEMA' ORDER BY tablename;" \
    --profile "$PROFILE" \
    --query 'Id' \
    --output text)

# Wait for query to complete
echo "⏳ Waiting for table list query to complete..."
while true; do
    STATUS=$(aws redshift-data describe-statement --id "$TABLES" --profile "$PROFILE" --query 'Status' --output text)
    if [ "$STATUS" = "FINISHED" ]; then
        break
    elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "ABORTED" ]; then
        echo "❌ Query failed with status: $STATUS"
        exit 1
    fi
    sleep 2
done

# Get the results
TABLE_LIST=$(aws redshift-data get-statement-result \
    --id "$TABLES" \
    --profile "$PROFILE" \
    --query 'Records[*][0].stringValue' \
    --output text)

echo "📊 Found $(echo "$TABLE_LIST" | wc -w) tables to export"
echo ""

# Counter for progress
COUNT=0
TOTAL=$(echo "$TABLE_LIST" | wc -w)

# Export DDL for each table
for table in $TABLE_LIST; do
    COUNT=$((COUNT + 1))
    echo "[$COUNT/$TOTAL] Exporting DDL for: $table"
    
    # Use Redshift's SHOW TABLE command to get DDL
    DDL_QUERY="SHOW TABLE $SCHEMA.$table"
    
    # Execute DDL query
    DDL_ID=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --sql "$DDL_QUERY" \
        --profile "$PROFILE" \
        --query 'Id' \
        --output text)
    
    # Wait for query to complete
    QUERY_FAILED=false
    while true; do
        STATUS=$(aws redshift-data describe-statement --id "$DDL_ID" --profile "$PROFILE" --query 'Status' --output text)
        if [ "$STATUS" = "FINISHED" ]; then
            break
        elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "ABORTED" ]; then
            echo "❌ DDL query failed for $table with status: $STATUS"
            ERROR_MSG=$(aws redshift-data describe-statement --id "$DDL_ID" --profile "$PROFILE" --query 'Error' --output text)
            echo "   Error: $ERROR_MSG"
            QUERY_FAILED=true
            break
        fi
        sleep 1
    done

    # Skip if query failed
    if [ "$QUERY_FAILED" = true ]; then
        continue
    fi

    # Get the DDL result - SHOW TABLE returns multiple rows, concatenate them
    DDL_RESULT=$(aws redshift-data get-statement-result \
        --id "$DDL_ID" \
        --profile "$PROFILE" \
        --query 'Records[*][0].stringValue' \
        --output text | tr '\t' '\n')

    # Write DDL to file
    echo "$DDL_RESULT" > "$OUTPUT_DIR/${table}.ddl"

    echo "✅ Exported: $OUTPUT_DIR/${table}.ddl"
done

echo ""
echo "🎉 DDL export completed!"
echo "📁 All DDL files saved to: $OUTPUT_DIR/"
echo "📊 Total tables exported: $TOTAL"
