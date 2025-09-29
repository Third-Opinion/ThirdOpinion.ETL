DROP TABLE IF EXISTS patient_medications;

CREATE TEMP TABLE patient_medications (
    id VARCHAR(255),
    resourceType VARCHAR(50)
);

INSERT INTO patient_medications
WITH target_patients AS (
    SELECT 
        code_code, 
        patient_id
    FROM fact_fhir_conditions_view_v1 AS c 
    WHERE c.code_code IN ('C61', 'Z19.1', 'Z19.2', 'R97.21') 
        AND c.recorded_date >= '2023-01-01' 
    GROUP BY code_code, patient_id
),
patient_medications_cte AS (
    SELECT 
        mr.medication_id
    FROM target_patients tp
    INNER JOIN fact_fhir_medication_requests_view_v1 mr 
        ON tp.patient_id = mr.patient_id
    WHERE mr.status IN ('active', 'completed', 'on-hold', 'stopped')
    -- TODO get the rest?
)
SELECT DISTINCT
    medication_id AS id,
    'Medication' AS resourceType
FROM patient_medications_cte
ORDER BY medication_id;