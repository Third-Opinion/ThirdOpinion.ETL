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
    oc.code_code, 
    comp.component_code, 
    comp.component_display, 
    comp.component_text, 
    CAST(LEFT(comp.component_value_string, 10) AS DATE) AS treatmentStartDate,
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
WHERE oc.code_code = '413712001' 
    AND o.value_codeable_concept_code = '385654001' 
    AND comp.component_code = 'treatmentStartDate_v1';