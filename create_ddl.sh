#!/bin/bash
set -euo pipefail

# Configuration
CLUSTER_ID="prod-redshift-main-ue2"
DATABASE="dev"
REGION="us-east-2"
OUTPUT_FILE="redshift_all_tables_ddl.sql"
# Set your AWS profile here if needed, or leave empty to use the default
PROFILE=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Redshift DDL Generator ===${NC}"
echo "Cluster: $CLUSTER_ID"
echo "Database: $DATABASE"
echo "Region: $REGION"
echo "Output file: $OUTPUT_FILE"
echo ""

# Function to wait for statement completion
wait_for_completion() {
    local statement_id=$1
    local description=$2
    
    echo -e "${YELLOW}Executing: $description${NC}"
    echo "Statement ID: $statement_id"
    
    while true; do
        STATUS=$(aws redshift-data describe-statement --id $statement_id --region $REGION --profile "$PROFILE" --query 'Status' --output text)
        echo -n "."
        
        if [ "$STATUS" = "FINISHED" ]; then
            echo -e " ${GREEN}COMPLETED${NC}"
            break
        elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "ABORTED" ]; then
            echo -e " ${RED}FAILED${NC}"
            echo "Error details:"
            aws redshift-data describe-statement --id $statement_id --region $REGION --profile "$PROFILE" --query 'Error' --output text
            exit 1
        fi
        
        sleep 1
    done
}

# Initialize output file
echo "-- Redshift DDL Export" > $OUTPUT_FILE
echo "-- Generated on: $(date)" >> $OUTPUT_FILE
echo "-- Cluster: $CLUSTER_ID" >> $OUTPUT_FILE
echo "-- Database: $DATABASE" >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE

# Step 1: Get all tables
echo -e "${GREEN}Step 1: Getting list of all tables...${NC}"

# Using list-tables for a more direct approach
TABLE_LIST=$(aws redshift-data list-tables \
    --cluster-identifier "$CLUSTER_ID" \
    --database "$DATABASE" \
    --region "$REGION" \
    --profile "$PROFILE" \
    --output json)

if [ -z "$TABLE_LIST" ]; then
    echo -e "${RED}Failed to retrieve table list from Redshift.${NC}"
    exit 1
fi

TABLES_TO_PROCESS=$(echo "$TABLE_LIST" | jq -r '.Tables[] | select(.Schema | IN("pg_catalog", "information_schema", "pg_toast") | not) | .Schema + " " + .Name')

TABLE_COUNT=$(echo "$TABLES_TO_PROCESS" | wc -l | xargs)
echo -e "${GREEN}Found $TABLE_COUNT tables${NC}"

# Step 2: Generate DDL for each table
echo -e "${GREEN}Step 2: Generating DDL for each table...${NC}"

PROCESSED_COUNT=0
echo "$TABLES_TO_PROCESS" | while read -r SCHEMA TABLE; do
    PROCESSED_COUNT=$((PROCESSED_COUNT + 1))
    
    echo -e "${YELLOW}Processing ($PROCESSED_COUNT/$TABLE_COUNT): ${SCHEMA}.${TABLE}${NC}"
    
    # DDL query for individual table
    DDL_QUERY="
WITH table_info AS (
    SELECT 
        n.nspname AS schemaname,
        c.relname AS tablename,
        a.attname AS columnname,
        CASE 
            WHEN t.typname = 'varchar' THEN 'VARCHAR'
            WHEN t.typname = 'bpchar' THEN 'CHAR'
            WHEN t.typname = 'int4' THEN 'INTEGER'
            WHEN t.typname = 'int8' THEN 'BIGINT'
            WHEN t.typname = 'int2' THEN 'SMALLINT'
            WHEN t.typname = 'bool' THEN 'BOOLEAN'
            WHEN t.typname = 'float8' THEN 'DOUBLE PRECISION'
            WHEN t.typname = 'float4' THEN 'REAL'
            WHEN t.typname = 'numeric' THEN 'NUMERIC'
            WHEN t.typname = 'date' THEN 'DATE'
            WHEN t.typname = 'timestamp' THEN 'TIMESTAMP'
            WHEN t.typname = 'timestamptz' THEN 'TIMESTAMPTZ'
            ELSE UPPER(t.typname)
        END AS datatype,
        CASE 
            WHEN t.typname IN ('varchar', 'bpchar') AND a.atttypmod > 0 THEN a.atttypmod - 4
            ELSE NULL
        END AS columnlength,
        CASE 
            WHEN t.typname = 'numeric' AND a.atttypmod > 0 THEN (a.atttypmod - 4) >> 16 & 65535
            ELSE NULL
        END AS precision,
        CASE 
            WHEN t.typname = 'numeric' AND a.atttypmod > 0 THEN (a.atttypmod - 4) & 65535
            ELSE NULL
        END AS scale,
        a.attnum AS columnposition,
        a.attnotnull AS notnull,
        CASE WHEN a.attisdistkey THEN a.attname ELSE NULL END AS distkey,
        CASE WHEN a.attsortkey > 0 THEN a.attname ELSE NULL END AS sortkey,
        a.attsortkey AS sortkeyorder
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON c.oid = a.attrelid
    JOIN pg_type t ON a.atttypid = t.oid
    WHERE c.relkind = 'r'
    AND a.attnum > 0
    AND NOT a.attisdropped
    AND n.nspname = '$SCHEMA'
    AND c.relname = '$TABLE'
),
dist_sort_keys AS (
    SELECT 
        schemaname,
        tablename,
        LISTAGG(CASE WHEN distkey IS NOT NULL THEN distkey END, ', ') WITHIN GROUP (ORDER BY columnposition) AS distkeys,
        LISTAGG(CASE WHEN sortkey IS NOT NULL THEN sortkey END, ', ') WITHIN GROUP (ORDER BY sortkeyorder) AS sortkeys
    FROM table_info
    GROUP BY schemaname, tablename
)
SELECT 
    'CREATE TABLE ' || QUOTE_IDENT('$SCHEMA') || '.' || QUOTE_IDENT('$TABLE') || ' (' || CHR(10) ||
    '    ' || LISTAGG(
        QUOTE_IDENT(columnname) || ' ' || 
        CASE 
            WHEN datatype IN ('VARCHAR', 'CHAR') AND columnlength IS NOT NULL 
                THEN datatype || '(' || columnlength || ')'
            WHEN datatype = 'NUMERIC' AND precision IS NOT NULL 
                THEN datatype || '(' || precision || CASE WHEN scale > 0 THEN ',' || scale ELSE '' END || ')'
            ELSE datatype
        END ||
        CASE WHEN notnull THEN ' NOT NULL' ELSE '' END,
        ',' || CHR(10) || '    '
    ) WITHIN GROUP (ORDER BY columnposition) || CHR(10) ||
    ')' ||
    CASE 
        WHEN dsk.distkeys IS NOT NULL AND dsk.distkeys != '' 
            THEN CHR(10) || 'DISTKEY(' || dsk.distkeys || ')'
        ELSE ''
    END ||
    CASE 
        WHEN dsk.sortkeys IS NOT NULL AND dsk.sortkeys != '' 
            THEN CHR(10) || 'SORTKEY(' || dsk.sortkeys || ')'
        ELSE ''
    END ||
    ';' AS ddl_statement
FROM table_info ti
JOIN dist_sort_keys dsk ON ti.schemaname = dsk.schemaname AND ti.tablename = dsk.tablename
GROUP BY ti.schemaname, ti.tablename, dsk.distkeys, dsk.sortkeys;"

    # Execute DDL query
    DDL_STATEMENT_ID=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --sql "$DDL_QUERY" \
        --region "$REGION" \
        --profile "$PROFILE" \
        --query 'Id' --output text)

    wait_for_completion "$DDL_STATEMENT_ID" "Generating DDL for ${SCHEMA}.${TABLE}"

    # Get DDL result and append to file
    DDL_RESULT=$(aws redshift-data get-statement-result \
        --id "$DDL_STATEMENT_ID" \
        --region "$REGION" \
        --profile "$PROFILE" \
        --query 'Records[0][0].stringValue' --output text)

    if [ "$DDL_RESULT" != "None" ] && [ "$DDL_RESULT" != "" ]; then
        echo "" >> $OUTPUT_FILE
        echo "-- Table: ${SCHEMA}.${TABLE}" >> $OUTPUT_FILE
        echo "$DDL_RESULT" >> $OUTPUT_FILE
        echo "" >> $OUTPUT_FILE
        echo -e "${GREEN}✓ DDL generated for ${SCHEMA}.${TABLE}${NC}"
    else
        echo -e "${RED}✗ Failed to generate DDL for ${SCHEMA}.${TABLE}${NC}"
        echo "-- ERROR: Could not generate DDL for ${SCHEMA}.${TABLE}" >> $OUTPUT_FILE
    fi
    
    # Small delay to avoid overwhelming the API
    sleep 0.5
done

echo ""
echo -e "${GREEN}=== DDL Generation Complete ===${NC}"
echo "Output saved to: $OUTPUT_FILE"
echo "Total tables processed: $TABLE_COUNT"
echo ""
echo "You can now review the DDL file:"
echo "cat $OUTPUT_FILE"