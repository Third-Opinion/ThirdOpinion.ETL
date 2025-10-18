CREATE MATERIALIZED VIEW rpt_fhir_conditions_hsms_hmu_v1
DISTKEY(patient_id)
SORTKEY(patient_id)
AS
WITH target_patients AS (
    SELECT * FROM rpt_fhir_hmu_patients_v1 WHERE last_encounter_date >= '2025-06-01'
),
ranked_conditions AS (
    SELECT 
        tgt.patient_id,	
        tgt.last_encounter_date,
        tc.code_code,	
        tc.code_system,	
        tc.condition_id,	
        tc.clinical_status_code,	
        tc.verification_status_code,			
        tc.condition_text,	
        tc.severity_code,
        tc.onset_datetime,
        tc.onset_period_start,
        tc.onset_period_end,
        tc.onset_text,
        tc.abatement_boolean,
        tc.recorded_date,
        tc.meta_last_updated,
        tc.etl_created_at,
        tc.code_display,
        tc.code_text,
        tc.code_rank,
        tc.notes,
        tc.condition_duration_days,
        tc.is_active,
        ROW_NUMBER() OVER (
            PARTITION BY tc.patient_id 
            ORDER BY tc.recorded_date DESC
        ) AS rn
    FROM fact_fhir_conditions_view_v1 tc
    INNER JOIN target_patients tgt ON tc.patient_id = tgt.patient_id
    WHERE tc.code_code ILIKE '%Z19%'
)
SELECT * 
FROM ranked_conditions
WHERE rn = 1;