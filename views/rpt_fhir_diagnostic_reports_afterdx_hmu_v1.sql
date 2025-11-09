CREATE MATERIALIZED VIEW rpt_fhir_diagnostic_reports_afterdx_hmu_v1
DISTSTYLE KEY
DISTKEY (patient_id)
SORTKEY (patient_id, effective_datetime)
AS WITH target_patients AS (SELECT patient_id FROM rpt_fhir_hmu_patients_v1 WHERE last_encounter_date >= '2025-06-01'),
     c61_diagnoses AS (SELECT condition_id,
                              cv.patient_id,
                              verification_status_code,
                              onset_datetime
                       FROM public.fact_fhir_conditions_view_v1 cv
                                INNER JOIN target_patients tgt ON cv.patient_id = tgt.patient_id
                       WHERE cv.code_code = 'C61'
                         AND cv.code_system = 'http://hl7.org/fhir/sid/icd-10-cm'
                         AND clinical_status_code = 'active')
SELECT dv.diagnostic_report_id,
       c61.patient_id,
       dv.status,
       dv.effective_datetime,
       dv.code_text,
       dv.code_primary_code,
       dv.code_primary_system,
       dv.code_primary_display,
       dv.encounter_id,
       dv.media,
       dv.presented_forms,
       c61.onset_datetime AS c61_onset_datetime
FROM public.fact_fhir_diagnostic_reports_view_v1 dv
         INNER JOIN c61_diagnoses c61 ON dv.patient_id = c61.patient_id
WHERE dv.status = 'final'
  AND code_primary_code IN (
                            '5372066',
                            '392068',
                            '392161',
                            '392176',
                            '392553',
                            '392678',
                            '394253',
                            '5327789',
                            '394716',
                            '36952.0',
                            '38268-9',
                            '394251',
                            '81551-4',
                            '8106798',
                            '2732048',
                            '3463527',
                            '8272112',
                            '8352724')
  AND dv.effective_datetime >= (c61.onset_datetime - INTERVAL '90 days')
  AND dv.effective_datetime < c61.onset_datetime;