#!/bin/bash

echo "====================================================================="
echo "VALIDATING SQL FILES FOR REDSHIFT COMPATIBILITY"
echo "====================================================================="
echo ""

# Track issues
ISSUES_FOUND=0

# SQL files to check
SQL_FILES=(
    "fact_fhir_patients_view_v1.sql"
    "fact_fhir_encounters_view_v1.sql"
    "fact_fhir_conditions_view_v1.sql"
    "fact_fhir_diagnostic_reports_view_v1.sql"
    "fact_fhir_document_references_view_v1.sql"
    "fact_fhir_medication_requests_view_v1.sql"
    "fact_fhir_observations_view_v1.sql"
    "fact_fhir_practitioners_view_v1.sql"
    "fact_fhir_procedures_view_v1.sql"
)

for sql_file in "${SQL_FILES[@]}"; do
    echo "Checking: $sql_file"
    echo "----------------------------------------"
    
    if [ ! -f "$sql_file" ]; then
        echo "  ❌ File not found!"
        ((ISSUES_FOUND++))
        continue
    fi
    
    # Check for AUTO REFRESH YES
    if grep -q "AUTO REFRESH YES" "$sql_file"; then
        echo "  ⚠️  Has AUTO REFRESH YES (should be NO)"
        ((ISSUES_FOUND++))
    fi
    
    # Check for missing tables
    tables=$(grep -o "FROM public\.[a-z_]*" "$sql_file" | sed 's/FROM public\.//' | sort -u)
    for table in $tables; do
        if [ ! -f "ddl_exports/${table}.ddl" ]; then
            echo "  ❌ References non-existent table: $table"
            ((ISSUES_FOUND++))
        fi
    done
    
    # Check for LISTAGG DISTINCT with COUNT DISTINCT in same SELECT/CTE
    if grep -q "LISTAGG.*DISTINCT" "$sql_file"; then
        # Check if there's a COUNT DISTINCT in the same CTE
        awk '/WITH .* AS \(/,/\),/ {
            if (/LISTAGG.*DISTINCT/ && /COUNT.*DISTINCT/) {
                print "  ⚠️  Has LISTAGG DISTINCT and COUNT DISTINCT in same CTE (line " NR ")"
                exit 1
            }
        }' "$sql_file"
        if [ $? -eq 1 ]; then
            ((ISSUES_FOUND++))
        fi
    fi
    
    # Check for syntax issues
    # Check for trailing commas before FROM
    if grep -q ",\s*FROM" "$sql_file"; then
        line=$(grep -n ",\s*FROM" "$sql_file" | head -1)
        echo "  ⚠️  Possible trailing comma before FROM: $line"
        ((ISSUES_FOUND++))
    fi
    
    # Check if view creation statement is present
    if ! grep -q "CREATE MATERIALIZED VIEW" "$sql_file"; then
        echo "  ❌ Missing CREATE MATERIALIZED VIEW statement"
        ((ISSUES_FOUND++))
    fi
    
    # All clear for this file
    if [ $ISSUES_FOUND -eq 0 ]; then
        echo "  ✅ No issues found"
    fi
    
    echo ""
done

echo "====================================================================="
echo "SUMMARY"
echo "====================================================================="
if [ $ISSUES_FOUND -eq 0 ]; then
    echo "✅ All SQL files are valid"
else
    echo "⚠️  Found $ISSUES_FOUND issues that need to be fixed"
fi
echo "====================================================================="

exit $ISSUES_FOUND