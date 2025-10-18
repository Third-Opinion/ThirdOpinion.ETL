CREATE MATERIALIZED VIEW rpt_fhir_observations_testosterone_total_hmu_v1
DISTSTYLE KEY 
DISTKEY (patient_id)
SORTKEY (patient_id, effective_datetime)
AS
SELECT
    fpv2.names,
    fpv2.birth_date,
    fpv2.gender,
    tp.patient_id,
    tp.observation_id,
    tp.encounter_id,
    tp.effective_datetime,
    tp.observation_text,
    tp.codes[0].code,
COALESCE(tp.value_string, CAST(tp.value_quantity_value AS VARCHAR)) AS combined_value,
    tp.value_quantity_value,
    tp.value_quantity_unit,
    tp.value_codeable_concept_code,
    tp.value_quantity_system,
    tp.value_string,
    tp.codes,
    tp.categories,
    tp.reference_ranges,
    tp.interpretations,
    tp.notes,
    tp.components
FROM public.fact_fhir_observations_view_v1 tp
INNER JOIN public.fact_fhir_patients_view_v1 fpv2
    ON tp.patient_id = fpv2.patient_id
WHERE tp.observation_category = 'laboratory'
    AND tp.status = 'final'
    AND tp.observation_text NOT ILIKE '%ratio%'
    AND tp.observation_text NOT ILIKE '%free%'
    AND (
        tp.observation_text ILIKE '%testosterone%' 
        OR tp.observation_text ILIKE '%testost%' 
    )
    AND tp.has_value = true;