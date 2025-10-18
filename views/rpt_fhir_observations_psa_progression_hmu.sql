CREATE MATERIALIZED VIEW rpt_fhir_observations_psa_progression_hmu_v1
DISTKEY(patient_id)
SORTKEY(patient_id, effective_datetime)
AS
WITH target_patients AS (
    SELECT patient_id 
    FROM rpt_fhir_hmu_patients_v1 
    WHERE last_encounter_date >= '2025-06-01'
)
SELECT
    o.patient_id,
    o.observation_id,
    o.observation_text,
    o.effective_datetime,
     oc.code_code as observation_code,
    CASE 
        WHEN o.value_codeable_concept_code = '277022003' THEN 'TRUE'
        WHEN o.value_codeable_concept_code = '359746009' THEN 'FALSE'
        WHEN o.value_codeable_concept_code = '261665006' THEN 'UNKNOWN'
        ELSE NULL
    END AS psa_progression,
    o.value_codeable_concept_code,
    o.value_codeable_concept_system,
    o.value_codeable_concept_display,
    COALESCE(o.value_string, CAST(o.value_quantity_value AS VARCHAR)) AS combined_value,
    comp.component_code,
    comp.component_display AS psa_progression_display,
    comp.component_text,
    CASE 
        WHEN comp.component_code = 'mostRecentMeasurement_v1' 
        THEN CAST(LEFT(comp.component_value_string, 10) AS DATE)
        ELSE NULL
    END AS mostRecentMeasurement,
    comp.component_value_string,
    comp.component_value_quantity_value,
    obn.note_text
FROM Observations o
INNER JOIN target_patients tp 
    ON o.patient_id = tp.patient_id
INNER JOIN Observation_codes oc 
    ON o.observation_id = oc.observation_id
INNER JOIN observation_components comp
    ON o.observation_id = comp.observation_id
LEFT JOIN observation_notes obn
    ON o.observation_id = obn.observation_id
WHERE o.value_codeable_concept_code IN ('277022003', '359746009', '261665006')
    AND o.observation_id LIKE 'to.ai-inference-%'
    -- AND (o.observation_id = 'to.ai-inference-6bfe69f3-1ea5-48ec-9ed5-35a7fc8886e7' OR oc.observation_id = 'to.ai-inference-b6671abc-5e82-42c0-ad27-7b22110d0825')
    AND comp.component_code IN ('mostRecentMeasurement_v1', 'analysis-note')