CREATE MATERIALIZED VIEW rpt_fhir_conditions_cns_metastases_hmu_v1
DISTSTYLE KEY
DISTKEY (patient_id)
SORTKEY (patient_id, recorded_date)
AS
WITH target_patients AS (
    SELECT * FROM rpt_fhir_hmu_patients_v1 WHERE last_encounter_date >= '2025-06-01'
)
SELECT
    fpv2.names,
    fpv2.birth_date,
    fpv2.gender,
    tc.patient_id,
    tc.condition_id,
    tc.encounter_id,
    tc.clinical_status_code,
    tc.clinical_status_display,
    tc.verification_status_code,
    tc.condition_text,
    tc.severity_code,
    tc.severity_display,
    tc.onset_datetime,
    tc.abatement_datetime,
    tc.recorded_date,
    tc.code_system,
    tc.code_code,
    tc.code_display,
    tc.code_rank,
    tc.body_sites,
    tc.categories,
    tc.evidence,
    tc.notes,
    tc.stages,
    tc.is_active
FROM public.fact_fhir_conditions_view_v1 tc
INNER JOIN target_patients tgt ON tc.patient_id = tgt.patient_id
INNER JOIN public.fact_fhir_patients_view_v1 fpv2 ON tc.patient_id = fpv2.patient_id
WHERE tc.clinical_status_code IN ('active', 'recurrence', 'remission')
    AND (
        -- ICD-10 codes for CNS metastases (C79.3x, C79.4x, C79.5x)
        (tc.code_code LIKE 'C79.3%' OR tc.code_code LIKE 'C79.4%' OR tc.code_code LIKE 'C79.5%')
        -- String matching in condition text
        OR UPPER(tc.condition_text) LIKE '%CNS%METASTA%'
        OR UPPER(tc.condition_text) LIKE '%CENTRAL%NERVOUS%METASTA%'
        OR UPPER(tc.condition_text) LIKE '%BRAIN%METASTA%'
        OR UPPER(tc.condition_text) LIKE '%CEREBELLAR%METASTA%'
        OR UPPER(tc.condition_text) LIKE '%LEPTOMENINGEAL%'
        OR UPPER(tc.condition_text) LIKE '%SPINE%METASTA%'
        OR UPPER(tc.condition_text) LIKE '%SPINAL%CORD%METASTA%'
        OR UPPER(tc.condition_text) LIKE '%BRAIN%SECONDARY%'
        OR UPPER(tc.code_display) LIKE '%BRAIN%METASTA%'
        OR UPPER(tc.code_display) LIKE '%CNS%METASTA%'
    )
