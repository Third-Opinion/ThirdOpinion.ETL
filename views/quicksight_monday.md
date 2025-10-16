fact_fhir_patients_view

Main Patient List 
rpt_fhir_hmu_patients -> Top of funnel 8k Patient
LEFT JOIN rpt_fhir_medication_requests_adt...  mpv.medication_id,
    mpv.medication_display, time?
LEFT JOIN rpt_fhir_observations_psa_total  tp.observation_text,
    tp.codes [ 0 ].code,
    tp.value_quantity_value,
    tp.value_quantity_unit, time?
LEFT JOIN test_LT_50  observation_text, 
    combined_value,
    effective_datetime,

New datasource names test_LT_50 for testosterne < 50
---- SQL View in Quicksight
WITH filtered_data AS (
    SELECT 
        o.patient_id, 
        o.observation_id,
        o.observation_text,
        COALESCE(o.value_string, CAST(o.value_quantity_value AS VARCHAR)) AS combined_value,
        CASE 
            WHEN o.value_string ~ '^[0-9]+\.?[0-9]*$' 
            THEN CAST(o.value_string AS DECIMAL(10,1))
            ELSE NULL 
        END AS numeric_value_string,
        o.effective_datetime,
        o.value_string, 
        o.value_quantity_value, 
        o.value_quantity_unit,
        o.value_codeable_concept_code, 
        o.value_codeable_concept_system, 
        o.value_codeable_concept_display, 
        o.value_codeable_concept_text
    FROM rpt_fhir_testosterone_total_hmu_view_v1 AS o
)
SELECT 
    patient_id, 
    observation_id,
    observation_text, 
    combined_value,
    effective_datetime,
    value_string, 
    value_quantity_value, 
    value_quantity_unit,
    value_codeable_concept_code, 
    value_codeable_concept_system, 
    value_codeable_concept_display, 
    value_codeable_concept_text
FROM filtered_data
WHERE 
    value_string ILIKE '%<%' 
    OR (numeric_value_string IS NOT NULL AND numeric_value_string <= 50)
    OR CAST(value_quantity_value AS VARCHAR) ILIKE '%<%'
    OR (value_quantity_value IS NOT NULL AND value_quantity_value <= 50)
ORDER BY patient_id, effective_datetime;


----