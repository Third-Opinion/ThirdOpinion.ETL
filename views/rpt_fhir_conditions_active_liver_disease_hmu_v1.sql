CREATE MATERIALIZED VIEW rpt_fhir_conditions_active_liver_disease_hmu_v1
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
WHERE tc.clinical_status_code IN ('active', 'recurrence')
    AND (
        -- ICD-10 codes for viral hepatitis (B15-B19.99)
        (tc.code_code LIKE 'B15%' OR tc.code_code LIKE 'B16%' OR tc.code_code LIKE 'B17%'
         OR tc.code_code LIKE 'B18%' OR tc.code_code LIKE 'B19%')
        -- ICD-10 codes for liver diseases (K70-K77)
        OR (tc.code_code LIKE 'K70%' OR tc.code_code LIKE 'K71%' OR tc.code_code LIKE 'K72%'
            OR tc.code_code LIKE 'K73%' OR tc.code_code LIKE 'K74%' OR tc.code_code LIKE 'K75%'
            OR tc.code_code LIKE 'K76%' OR tc.code_code LIKE 'K77%')
        -- String matching for liver disease terms
        OR UPPER(tc.condition_text) LIKE '%HEPATITIS%'
        OR UPPER(tc.condition_text) LIKE '%CIRRHOSIS%'
        OR UPPER(tc.condition_text) LIKE '%LIVER%DISEASE%'
        OR UPPER(tc.condition_text) LIKE '%LIVER%FAILURE%'
        OR UPPER(tc.condition_text) LIKE '%JAUNDICE%'
        OR UPPER(tc.condition_text) LIKE '%HEPATIC%'
        OR UPPER(tc.code_display) LIKE '%HEPATITIS%'
        OR UPPER(tc.code_display) LIKE '%CIRRHOSIS%'
        OR UPPER(tc.code_display) LIKE '%LIVER%'
    )
