CREATE MATERIALIZED VIEW rpt_fhir_conditions_union_hmu_v1
DISTSTYLE KEY
DISTKEY (patient_id)
SORTKEY (patient_id, recorded_date)
AS
SELECT
    names,
    birth_date,
    gender,
    patient_id,
    condition_id,
    encounter_id,
    clinical_status_code,
    clinical_status_display,
    verification_status_code,
    condition_text,
    severity_code,
    severity_display,
    onset_datetime,
    abatement_datetime,
    recorded_date,
    code_system,
    code_code,
    code_display,
    code_rank,
    body_sites,
    categories,
    evidence,
    notes,
    stages,
    is_active,
    'active_liver_disease' AS condition_type
FROM rpt_fhir_conditions_active_liver_disease_hmu_v1

UNION ALL

SELECT
    names,
    birth_date,
    gender,
    patient_id,
    condition_id,
    encounter_id,
    clinical_status_code,
    clinical_status_display,
    verification_status_code,
    condition_text,
    severity_code,
    severity_display,
    onset_datetime,
    abatement_datetime,
    recorded_date,
    code_system,
    code_code,
    code_display,
    code_rank,
    body_sites,
    categories,
    evidence,
    notes,
    stages,
    is_active,
    'additional_malignancy' AS condition_type
FROM rpt_fhir_conditions_additional_malignancy_hmu_v1

UNION ALL

SELECT
    names,
    birth_date,
    gender,
    patient_id,
    condition_id,
    encounter_id,
    clinical_status_code,
    clinical_status_display,
    verification_status_code,
    condition_text,
    severity_code,
    severity_display,
    onset_datetime,
    abatement_datetime,
    recorded_date,
    code_system,
    code_code,
    code_display,
    code_rank,
    body_sites,
    categories,
    evidence,
    notes,
    stages,
    is_active,
    'cns_metastases' AS condition_type
FROM rpt_fhir_conditions_cns_metastases_hmu_v1;
