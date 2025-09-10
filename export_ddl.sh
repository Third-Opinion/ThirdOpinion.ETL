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
    
    # Create DDL query for this table
    DDL_QUERY="
    SELECT 
        'CREATE TABLE ' || '$SCHEMA' || '.' || '$table' || ' (' || 
        LISTAGG(
            column_name || ' ' || data_type || 
            CASE 
                WHEN character_maximum_length IS NOT NULL THEN '(' || character_maximum_length || ')'
                WHEN numeric_precision IS NOT NULL AND numeric_scale IS NOT NULL THEN '(' || numeric_precision || ',' || numeric_scale || ')'
                WHEN numeric_precision IS NOT NULL THEN '(' || numeric_precision || ')'
                ELSE ''
            END ||
            CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END,
            ', '
        ) WITHIN GROUP (ORDER BY ordinal_position) || ');' as ddl
    FROM information_schema.columns 
    WHERE table_schema = '$SCHEMA' 
      AND table_name = '$table'
    GROUP BY table_schema, table_name;
    "
    
    # Execute DDL query
    DDL_ID=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --sql "$DDL_QUERY" \
        --profile "$PROFILE" \
        --query 'Id' \
        --output text)
    
    # Wait for query to complete
    while true; do
        STATUS=$(aws redshift-data describe-statement --id "$DDL_ID" --profile "$PROFILE" --query 'Status' --output text)
        if [ "$STATUS" = "FINISHED" ]; then
            break
        elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "ABORTED" ]; then
            echo "❌ DDL query failed for $table with status: $STATUS"
            continue
        fi
        sleep 1
    done
    
    # Get the DDL result
    DDL_RESULT=$(aws redshift-data get-statement-result \
        --id "$DDL_ID" \
        --profile "$PROFILE" \
        --query 'Records[0][0].stringValue' \
        --output text)
    
    # Write DDL to file
    echo "$DDL_RESULT" > "$OUTPUT_DIR/${table}.ddl"
    
    echo "✅ Exported: $OUTPUT_DIR/${table}.ddl"
done

echo ""
echo "🎉 DDL export completed!"
echo "📁 All DDL files saved to: $OUTPUT_DIR/"
echo "📊 Total tables exported: $TOTAL"
