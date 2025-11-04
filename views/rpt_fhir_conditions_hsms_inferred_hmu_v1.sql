CREATE MATERIALIZED VIEW rpt_fhir_conditions_hsms_inferred_hmu_v1
DISTKEY(patient_id)
SORTKEY(patient_id)
AS
WITH adt_patients AS (
    SELECT
        patient_id,
        treatmentStartDate AS adt_start_date,
        observation_id AS adt_obs_id
    FROM rpt_fhir_observations_adt_treatment_hmu_v1
    WHERE code_code = '413712001'
),
psa_progression_patients AS (
    SELECT
        patient_id,
        mostRecentMeasurement AS psa_progression_date,
        observation_id AS psa_obs_id,
        psa_progression
    FROM rpt_fhir_observations_psa_progression_hmu_v1
    WHERE value_codeable_concept_code = '277022003'
        AND component_code = 'mostRecentMeasurement_v1' 
),
testosterone_values AS (
    SELECT
        patient_id,
        effective_datetime AS testosterone_date,
        combined_value AS testosterone_value,
        observation_id AS testosterone_obs_id,
        ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY effective_datetime DESC) AS rn
    FROM rpt_fhir_observations_testosterone_total_hmu_v1
)
SELECT
    adt.patient_id,
    'mCRPC' as hsms,
    'Z19.2' as code_code,
    'http://hl7.org/fhir/sid/icd-10-cm' as code_system,
    'Hormone refractory prostate cancer' as condition_text,
    'inferred' as verification_status_code,
    'active' as  clinical_status_code 
    CONCAT('inferred-',psa.psa_obs_id) as condition_id,
    psa.psa_progression_date as onset_datetime
    adt.adt_start_date,
    adt.adt_obs_id,
    psa.psa_progression_date,
    psa.psa_progression,
    psa.psa_obs_id,
    tst.testosterone_date,
    tst.testosterone_value,
    tst.testosterone_obs_id,
    DATEDIFF(day, adt.adt_start_date, psa.psa_progression_date) AS days_to_progression
FROM adt_patients adt
INNER JOIN psa_progression_patients psa
    ON adt.patient_id = psa.patient_id
LEFT JOIN testosterone_values tst
    ON adt.patient_id = tst.patient_id
    AND tst.rn = 1;