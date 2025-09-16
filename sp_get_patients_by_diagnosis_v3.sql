-- Version 3: Using cursor to return results that can be displayed
CREATE OR REPLACE PROCEDURE sp_get_patients_by_diagnosis_v3(
    code_system_json VARCHAR(MAX),  -- JSON string like '[{"code":"C61","system":"ICD10"},{"code":"44558993","system":"SNOMED"}]'
    encounter_since DATE,            -- Filter for encounters since this date
    result_cursor INOUT REFCURSOR    -- Cursor to return results
)
AS $$
DECLARE
    sql_query VARCHAR(MAX);
    where_clause VARCHAR(MAX);
    i INTEGER;
    array_length INTEGER;
    json_element VARCHAR(500);
    code_val VARCHAR(100);
    system_val VARCHAR(100);
BEGIN
    -- Get array length using JSON_ARRAY_LENGTH on the string
    SELECT JSON_ARRAY_LENGTH(code_system_json) INTO array_length;
    
    -- Build WHERE clause by iterating through array indices
    where_clause := '';
    i := 0;
    
    -- Loop through the JSON array
    WHILE i < array_length LOOP
        -- Extract each element as a string
        json_element := JSON_EXTRACT_ARRAY_ELEMENT_TEXT(code_system_json, i);
        
        -- Extract code and system from the element
        code_val := JSON_EXTRACT_PATH_TEXT(json_element, 'code');
        system_val := JSON_EXTRACT_PATH_TEXT(json_element, 'system');
        
        IF where_clause != '' THEN
            where_clause := where_clause || ' OR ';
        END IF;
        
        IF system_val = 'ICD10' THEN
            where_clause := where_clause || '(c.code_code = ''' || code_val || ''' AND c.code_system = ''http://hl7.org/fhir/sid/icd-10-cm'')';
        ELSIF system_val = 'SNOMED' THEN
            where_clause := where_clause || '(c.code_code = ''' || code_val || ''' AND c.code_system = ''http://snomed.info/sct'')';
        ELSE
            where_clause := where_clause || '(c.code_code = ''' || code_val || ''' AND c.code_system = ''' || system_val || ''')';
        END IF;
        
        i := i + 1;
    END LOOP;
    
    -- Build the query
    sql_query := '
    WITH target_patients AS (
        SELECT 
            c.code_code,
            c.code_system,
            c.patient_id
        FROM fact_fhir_conditions_view_v1 AS c 
        INNER JOIN (
            SELECT 
                patient_id,
                MAX(start_time) AS last_encounter_date
            FROM fact_fhir_encounters_view_v2
            GROUP BY patient_id
        ) AS e ON c.patient_id = e.patient_id
        WHERE (' || where_clause || ')
            AND e.last_encounter_date >= ''' || encounter_since::TEXT || '''
        GROUP BY c.code_code, c.code_system, c.patient_id
    )
    SELECT 
        code_code,
        code_system,
        patient_id,
        COUNT(*) OVER (PARTITION BY patient_id) as condition_count
    FROM target_patients
    ORDER BY patient_id, code_code';
    
    -- Open cursor with the query
    OPEN result_cursor FOR EXECUTE sql_query;
END;
$$ LANGUAGE plpgsql;

-- Example usage with cursor:
/*
BEGIN;

CALL sp_get_patients_by_diagnosis_v3(
    '[{"code":"C61","system":"ICD10"},{"code":"Z19.1","system":"ICD10"}]',
    '2024-01-01',
    'mycursor'
);

FETCH ALL FROM mycursor;

CLOSE mycursor;

COMMIT;
*/

-- Alternative: Create a simpler version that creates a temp table you can query
CREATE OR REPLACE PROCEDURE sp_get_patients_by_diagnosis_simple(
    code_system_json VARCHAR(MAX),
    encounter_since DATE
)
AS $$
DECLARE
    where_clause VARCHAR(MAX);
    i INTEGER;
    array_length INTEGER;
    json_element VARCHAR(500);
    code_val VARCHAR(100);
    system_val VARCHAR(100);
BEGIN
    -- Get array length
    SELECT JSON_ARRAY_LENGTH(code_system_json) INTO array_length;
    
    -- Build WHERE clause
    where_clause := '';
    i := 0;
    
    WHILE i < array_length LOOP
        json_element := JSON_EXTRACT_ARRAY_ELEMENT_TEXT(code_system_json, i);
        code_val := JSON_EXTRACT_PATH_TEXT(json_element, 'code');
        system_val := JSON_EXTRACT_PATH_TEXT(json_element, 'system');
        
        IF where_clause != '' THEN
            where_clause := where_clause || ' OR ';
        END IF;
        
        IF system_val = 'ICD10' THEN
            where_clause := where_clause || '(c.code_code = ''' || code_val || ''' AND c.code_system = ''http://hl7.org/fhir/sid/icd-10-cm'')';
        ELSIF system_val = 'SNOMED' THEN
            where_clause := where_clause || '(c.code_code = ''' || code_val || ''' AND c.code_system = ''http://snomed.info/sct'')';
        ELSE
            where_clause := where_clause || '(c.code_code = ''' || code_val || ''' AND c.code_system = ''' || system_val || ''')';
        END IF;
        
        i := i + 1;
    END LOOP;
    
    -- Create temp table with results
    EXECUTE 'DROP TABLE IF EXISTS diagnosis_results';
    EXECUTE '
    CREATE TEMP TABLE diagnosis_results AS
    WITH target_patients AS (
        SELECT 
            c.code_code,
            c.code_system,
            c.patient_id
        FROM fact_fhir_conditions_view_v1 AS c 
        INNER JOIN (
            SELECT 
                patient_id,
                MAX(start_time) AS last_encounter_date
            FROM fact_fhir_encounters_view_v2
            GROUP BY patient_id
        ) AS e ON c.patient_id = e.patient_id
        WHERE (' || where_clause || ')
            AND e.last_encounter_date >= ''' || encounter_since::TEXT || '''
        GROUP BY c.code_code, c.code_system, c.patient_id
    )
    SELECT 
        code_code,
        code_system,
        patient_id,
        COUNT(*) OVER (PARTITION BY patient_id) as condition_count
    FROM target_patients
    ORDER BY patient_id, code_code';
    
    -- Show summary
    RAISE INFO 'Results stored in temp table: diagnosis_results';
    RAISE INFO 'Run "SELECT * FROM diagnosis_results" to see results';
END;
$$ LANGUAGE plpgsql;

-- Usage of simple version:
/*
CALL sp_get_patients_by_diagnosis_simple(
    '[{"code":"C61","system":"ICD10"}]',
    '2024-01-01'
);

-- Then query the results
SELECT * FROM diagnosis_results;

-- Get summary
SELECT 
    COUNT(DISTINCT patient_id) as unique_patients,
    COUNT(*) as total_records
FROM diagnosis_results;
*/