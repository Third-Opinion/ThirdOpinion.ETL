CREATE MATERIALIZED VIEW rpt_fhir_conditions_additional_malignancy_hmu_v1
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
WHERE tc.clinical_status_code IN ('active', 'recurrence', 'remission', 'resolved')
    -- Exclude SNOMED coded conditions
    AND tc.code_system != 'http://snomed.info/sct'
    -- Exclude C61 (prostate cancer) - looking for additional malignancies only
    AND tc.code_code NOT LIKE 'C61%'
    AND (
        -- ICD-10 malignancy codes (C00-C97, D00-D09 for in situ)
        (tc.code_code LIKE 'C%' AND LOWER(tc.code_system) LIKE '%icd-10%')
        OR (tc.code_code LIKE 'D0%' AND LOWER(tc.code_system) LIKE '%icd-10%')
        -- String matching for cancer terms
        OR UPPER(tc.condition_text) LIKE '%CANCER%'
        OR UPPER(tc.condition_text) LIKE '%CARCINOMA%'
        OR UPPER(tc.condition_text) LIKE '%MELANOMA%'
        OR UPPER(tc.condition_text) LIKE '%LYMPHOMA%'
        OR UPPER(tc.condition_text) LIKE '%LEUKEMIA%'
        OR UPPER(tc.condition_text) LIKE '%SARCOMA%'
        OR UPPER(tc.condition_text) LIKE '%TUMOR%'
        OR UPPER(tc.condition_text) LIKE '%NEOPLASM%'
        OR UPPER(tc.condition_text) LIKE '%MALIGNANCY%'
    )
    -- Additional exclusion for prostate cancer text matches
    AND UPPER(tc.condition_text) NOT LIKE '%PROSTATE%'
