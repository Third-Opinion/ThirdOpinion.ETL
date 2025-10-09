CREATE OR REPLACE VIEW public.rpt_fhir_testosterone_total_view_v1 AS
WITH target_patients AS (
    SELECT * FROM rpt_fhir_hmu_patients_v1 WHERE last_encounter_date >= '2025-07-01'
)
)
SELECT  
    -- fpv2.names,
    -- fpv2.birth_date,
    -- fpv2.gender,
    tp.patient_id,
    tp.observation_id,
    tp.encounter_id,
    tp.effective_datetime,
    tp.observation_text, 
    tp.codes[0].code,
    tp.value_quantity_value,
    tp.value_quantity_unit, 
    tp.value_codeable_concept_code,
    tp.value_quantity_system, 
    tp.value_string, 
    tp.codes,
    tp.categories, 
    tp.reference_ranges, 
    tp.interpretations, 
    tp.notes
FROM public.fact_fhir_observations_view_v2 tp
INNER JOIN target_patients tgt ON tp.patient_id = tgt.patient_id
INNER JOIN public.fact_fhir_patients_view_v2 fpv2 ON tp.patient_id = fpv2.patient_id
WHERE tp.observation_category = 'laboratory'
    AND tp.status = 'final'
    AND tp.observation_text NOT ILIKE '%ratio%'
    AND tp.observation_text NOT ILIKE '%free%'
    AND (
        tp.observation_text ILIKE '%testosterone%' 
        OR tp.observation_text ILIKE '%testost%' 
    )
    AND tp.effective_datetime >= '2024-01-01'
    AND tp.has_value = true;