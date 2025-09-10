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

# Get list of tables using MCP
echo "📋 Getting list of tables using MCP..."

# Use MCP to get table list
TABLE_LIST="care_plan_care_teams care_plan_categories care_plan_goals care_plan_identifiers care_plans condition_body_sites condition_categories condition_codes condition_evidence condition_extensions condition_notes condition_stages conditions diagnostic_report_based_on diagnostic_report_categories diagnostic_report_media diagnostic_report_performers diagnostic_report_presented_forms diagnostic_report_results diagnostic_reports document_reference_authors document_reference_categories document_reference_content document_reference_identifiers document_references encounter_hospitalization encounter_identifiers encounter_locations encounter_participants encounter_reasons encounter_types encounters medication_dispense_auth_prescriptions medication_dispense_dosage_instructions medication_dispense_identifiers medication_dispense_performers medication_dispenses medication_identifiers medication_request_categories medication_request_dosage_instructions medication_request_identifiers medication_request_notes medication_requests medications observation_categories observation_components observation_derived_from observation_interpretations observation_members observation_notes observation_performers observation_reference_ranges observations patient_addresses patient_communications patient_contacts patient_links patient_names patient_practitioners patient_telecoms patients practitioner_addresses practitioner_names practitioner_telecoms practitioners procedure_code_codings procedure_identifiers procedures"

echo "📊 Found $(echo "$TABLE_LIST" | wc -w) tables to export"
echo ""

# Counter for progress
COUNT=0
TOTAL=$(echo "$TABLE_LIST" | wc -w)

# Export DDL for each table using MCP
for table in $TABLE_LIST; do
    COUNT=$((COUNT + 1))
    echo "[$COUNT/$TOTAL] Exporting DDL for: $table"
    
    # Use MCP to get column information
    COLUMNS_QUERY="
    SELECT 
        a.attname as column_name,
        pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type,
        a.attnotnull as not_null,
        a.attnum as ordinal_position
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid
    WHERE n.nspname = '$SCHEMA'
      AND c.relname = '$table'
      AND a.attnum > 0
      AND NOT a.attisdropped
    ORDER BY a.attnum;
    "
    
    # Get column information using MCP
    COLUMNS_RESULT=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --sql "$COLUMNS_QUERY" \
        --profile "$PROFILE" \
        --query 'Id' \
        --output text)
    
    # Wait for query to complete
    while true; do
        STATUS=$(aws redshift-data describe-statement --id "$COLUMNS_RESULT" --profile "$PROFILE" --query 'Status' --output text)
        if [ "$STATUS" = "FINISHED" ]; then
            break
        elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "ABORTED" ]; then
            echo "❌ Column query failed for $table with status: $STATUS"
            break
        fi
        sleep 1
    done
    
    if [ "$STATUS" = "FINISHED" ]; then
        # Get the column results
        COLUMNS_DATA=$(aws redshift-data get-statement-result \
            --id "$COLUMNS_RESULT" \
            --profile "$PROFILE" \
            --query 'Records[*].[0].stringValue,1].stringValue,2].booleanValue,3].longValue]' \
            --output text)
        
        # Build DDL
        DDL="CREATE TABLE $SCHEMA.$table ("
        FIRST=true
        
        while IFS=$'\t' read -r col_name data_type not_null ordinal; do
            if [ "$FIRST" = true ]; then
                FIRST=false
            else
                DDL="$DDL,"
            fi
            
            # Add NOT NULL if needed
            if [ "$not_null" = "true" ]; then
                DDL="$DDL\n    $col_name $data_type NOT NULL"
            else
                DDL="$DDL\n    $col_name $data_type"
            fi
        done <<< "$COLUMNS_DATA"
        
        DDL="$DDL\n);"
        
        # Write DDL to file
        echo -e "$DDL" > "$OUTPUT_DIR/${table}.ddl"
        
        echo "✅ Exported: $OUTPUT_DIR/${table}.ddl"
    else
        echo "❌ Skipped: $table (query failed)"
    fi
done

echo ""
echo "🎉 DDL export completed!"
echo "📁 All DDL files saved to: $OUTPUT_DIR/"
echo "📊 Total tables processed: $TOTAL"
