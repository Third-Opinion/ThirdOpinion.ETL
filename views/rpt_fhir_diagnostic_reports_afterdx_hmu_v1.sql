CREATE MATERIALIZED VIEW rpt_fhir_diagnostic_reports_afterdx_hmu_v1
DISTSTYLE KEY
DISTKEY (patient_id)
SORTKEY (patient_id, effective_datetime)
AS WITH target_patients AS (
    SELECT patient_id
    FROM rpt_fhir_hmu_patients_v1
    WHERE last_encounter_date >= '2025-06-01'
),
c61_diagnoses_ranked AS (
    SELECT
           condition_id,
           cv.patient_id,
           verification_status_code,
           onset_datetime,
           ROW_NUMBER() OVER (PARTITION BY cv.patient_id ORDER BY onset_datetime ASC NULLS LAST) AS rn
    FROM public.fact_fhir_conditions_view_v1 cv
             INNER JOIN target_patients tgt ON cv.patient_id = tgt.patient_id
    WHERE cv.code_code = 'C61'
      AND cv.code_system = 'http://hl7.org/fhir/sid/icd-10-cm'
      AND clinical_status_code = 'active'
),
c61_diagnoses AS (
    SELECT
           condition_id,
           patient_id,
           verification_status_code,
           onset_datetime
    FROM c61_diagnoses_ranked
    WHERE rn = 1
)
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
'8352724',
'1223755',
'2856841',
'30673-8',
'30679-5',
'30693-6',
'30796-7',
'346697',
'36500-7',
'392100',
'392647',
'392649',
'392656',
'392675',
'392749',
'393267',
'394138',
'394368',
'4109882',
'25031-6',
'3468512',
'347864',
'392426',
'392527',
'394074',
'395765',
'39627-5',
'39818-0',
'39826-3',
'39883-4',
'425538',
'427749',
'48807-2',
'5302337',
'66108-2',
'66119-9',
'8269441',
'83012-5',
'83014-1',
'8317095',
'8317583',
'955722')
  AND dv.effective_datetime >= (c61.onset_datetime - INTERVAL '120 days')
