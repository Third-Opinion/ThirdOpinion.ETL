#!/bin/bash

# ===================================================================
# Fix Script for LISTAGG DISTINCT with COUNT DISTINCT Error in Redshift
# ===================================================================
# This script documents the files that need to be fixed to resolve
# the Redshift error: "Using LISTAGG aggregate functions with other 
# distinct aggregate function not supported"
#
# The solution is to refactor these views to use CTEs (Common Table 
# Expressions) to separate LISTAGG operations from COUNT DISTINCT operations
# ===================================================================

echo "====================================================================="
echo "Files that need to be refactored to fix LISTAGG/COUNT DISTINCT issue:"
echo "====================================================================="
echo ""
echo "The following files use both LISTAGG(DISTINCT ...) and COUNT(DISTINCT ...)"
echo "in the same SELECT statement, which Redshift doesn't support:"
echo ""
echo "1. fact_fhir_document_references_view_v1.sql"
echo "2. fact_fhir_encounters_view_v2.sql"
echo "3. fact_fhir_medication_requests_view_v1.sql"
echo ""
echo "Files that are already correctly using CTEs to separate aggregations:"
echo "- fact_fhir_conditions_view_v1.sql (uses CTEs)"
echo "- fact_fhir_diagnostic_reports_view_v1.sql (uses CTEs)"
echo "- fact_fhir_observations_view_v1.sql (uses CTEs)"
echo "- fact_fhir_patients_view_v2.sql (uses CTEs)"
echo "- fact_fhir_practitioners_view_v1.sql (uses CTEs)"
echo "- fact_fhir_procedures_view_v1.sql (uses CTEs)"
echo ""
echo "====================================================================="
echo "SOLUTION PATTERN:"
echo "====================================================================="
echo ""
echo "Move all LISTAGG operations into CTEs before the main SELECT, like this:"
echo ""
echo "WITH aggregated_data AS ("
echo "    SELECT"
echo "        id,"
echo "        LISTAGG(DISTINCT value, ',') AS aggregated_values"
echo "    FROM table"
echo "    GROUP BY id"
echo "),"
echo "count_data AS ("
echo "    SELECT"
echo "        id,"
echo "        COUNT(DISTINCT value) AS distinct_count"
echo "    FROM table"
echo "    GROUP BY id"
echo ")"
echo "SELECT"
echo "    main_table.*,"
echo "    ad.aggregated_values,"
echo "    cd.distinct_count"
echo "FROM main_table"
echo "    LEFT JOIN aggregated_data ad ON main_table.id = ad.id"
echo "    LEFT JOIN count_data cd ON main_table.id = cd.id;"
echo ""
echo "====================================================================="

# Check if user wants to see the specific errors in each file
read -p "Do you want to see the specific problematic lines in each file? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo ""
    echo "=== fact_fhir_document_references_view_v1.sql ==="
    echo "Lines with LISTAGG(DISTINCT...):"
    grep -n "LISTAGG.*DISTINCT" fact_fhir_document_references_view_v1.sql | head -5
    echo "Lines with COUNT(DISTINCT...):"
    grep -n "COUNT.*DISTINCT" fact_fhir_document_references_view_v1.sql | head -5
    
    echo ""
    echo "=== fact_fhir_encounters_view_v2.sql ==="
    echo "Lines with LISTAGG(DISTINCT...):"
    grep -n "LISTAGG.*DISTINCT" fact_fhir_encounters_view_v2.sql | head -5
    echo "Lines with COUNT(DISTINCT...):"
    grep -n "COUNT.*DISTINCT" fact_fhir_encounters_view_v2.sql | head -5
    
    echo ""
    echo "=== fact_fhir_medication_requests_view_v1.sql ==="
    echo "Lines with LISTAGG(DISTINCT...):"
    grep -n "LISTAGG.*DISTINCT" fact_fhir_medication_requests_view_v1.sql | head -5
    echo "Lines with COUNT(DISTINCT...):"
    grep -n "COUNT.*DISTINCT" fact_fhir_medication_requests_view_v1.sql | head -5
fi

echo ""
echo "====================================================================="
echo "NEXT STEPS:"
echo "====================================================================="
echo "1. Refactor the 3 problematic SQL files to use CTEs"
echo "2. Move all LISTAGG operations into separate CTEs"
echo "3. Keep COUNT(DISTINCT...) in separate CTEs or main SELECT"
echo "4. Test each view after refactoring"
echo "5. Deploy the fixed views to Redshift"
echo "====================================================================="