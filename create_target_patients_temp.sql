-- Create temp table with target patients based on encounter and condition criteria

-- Drop temp table if it exists
DROP TABLE IF EXISTS temp_target_patients;

-- Create temp table with the patient IDs
CREATE TEMP TABLE temp_target_patients AS
WITH new_query_patients AS (
    -- Original query: patients with encounters after a specific date
    SELECT DISTINCT
        c.patient_id
    FROM fact_fhir_conditions_view_v1 AS c 
    INNER JOIN (
        SELECT 
            patient_id,
            MAX(start_time) AS last_encounter_date
        FROM fact_fhir_encounters_view_v2
        GROUP BY patient_id
    ) AS e ON c.patient_id = e.patient_id
    WHERE (
        (c.code_code IN ('C61', 'Z19.1', 'Z19.2', 'R97.21') 
         AND c.code_system = 'http://hl7.org/fhir/sid/icd-10-cm')
    )
        AND e.last_encounter_date >= '2024-01-01'
        -- AND e.last_encounter_date < '2025-01-01'
),
original_query_patients AS (
    -- New query: patients with conditions recorded after a specific date
    SELECT DISTINCT
        patient_id
    FROM fact_fhir_conditions_view_v1 AS c 
    WHERE c.code_code IN ('C61', 'Z19.1', 'Z19.2', 'R97.21')
        AND c.code_system = 'http://hl7.org/fhir/sid/icd-10-cm'
        AND c.recorded_date >= '2025-01-01'
        -- AND c.recorded_date < '2026-01-01'
)
-- Find patients that are in the new query but NOT in the original query
SELECT 
    patient_id,
    CURRENT_TIMESTAMP as created_at
FROM new_query_patients
WHERE patient_id NOT IN (SELECT patient_id FROM original_query_patients);

-- Check the results
SELECT COUNT(*) as patient_count FROM temp_target_patients;

-- View first 10 patients
SELECT * FROM temp_target_patients LIMIT 10;

-- Now you can use this temp table in other queries
-- Example: Get documents for these patients
/*
SELECT 
    tp.patient_id,
    COUNT(DISTINCT dr.document_reference_id) as document_count
FROM temp_target_patients tp
INNER JOIN public.fact_fhir_document_references_view_v1 dr
    ON tp.patient_id = dr.patient_id
WHERE EXTRACT(YEAR FROM dr.date) >= 2024 
    AND dr.content != 'NULL'
GROUP BY tp.patient_id
ORDER BY document_count DESC;
*/

-- Example: Get all documents with details
/*
SELECT 
    tp.patient_id,
    dr.document_reference_id,
    dr.date as document_date,
    dr.type_code,
    dr.type_display,
    dr.content,
    drc.attachment_content_type,
    drc.attachment_url
FROM temp_target_patients tp
INNER JOIN public.fact_fhir_document_references_view_v1 dr
    ON tp.patient_id = dr.patient_id
LEFT JOIN dev.public.document_reference_content drc
    ON dr.document_reference_id = drc.document_reference_id
WHERE EXTRACT(YEAR FROM dr.date) >= 2024 
    AND dr.content != 'NULL'
    AND drc.attachment_url NOT ILIKE 'ref:%'
ORDER BY tp.patient_id, dr.date DESC;
*/