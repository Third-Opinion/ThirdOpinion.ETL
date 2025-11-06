CREATE MATERIALIZED VIEW rpt_fhir_observations_cd4_count_hmu_v1
DISTSTYLE KEY
DISTKEY (patient_id)
SORTKEY (patient_id, effective_datetime)
AS
WITH target_patients AS (
    SELECT * FROM rpt_fhir_hmu_patients_v1 WHERE last_encounter_date >= '2025-06-01'
)
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
INNER JOIN target_patients tgt ON tp.patient_id = tgt.patient_id
INNER JOIN public.fact_fhir_patients_view_v1 fpv2 ON tp.patient_id = fpv2.patient_id
WHERE tp.observation_category = 'laboratory'
    AND tp.status IN ('final', 'amended', 'corrected')
    AND (
       IN ('54218-3', '24467-3', '8123-2', '8122-4', '8124-0')
    )
    AND tp.has_value = true;
