-- Metastatic Castration-Resistant Prostate Cancer (mCRPC) Identification
-- Identifies patients who meet all mCRPC criteria:
-- 1. On active ADT treatment (code 413712001)
-- 2. Have PSA progression (code 277022003)
-- 4. PSA progression occurred AFTER ADT treatment start date

CREATE MATERIALIZED VIEW rpt_fhir_mcrpc_patients_hmu_v1
DISTKEY(patient_id)
SORTKEY(patient_id)
AS
WITH active_adt_patients AS (
    -- Patients currently on ADT treatment
    SELECT DISTINCT
        patient_id,
        treatmentStartDate,
        observation_id AS adt_observation_id,
        effective_datetime AS adt_effective_datetime,
        note_text AS adt_note
    FROM rpt_fhir_observations_adt_treatment_hmu_v1
    WHERE code_code = '413712001'
),
psa_progression_patients AS (
    -- Patients with documented PSA progression
    SELECT DISTINCT
        patient_id,
        mostRecentMeasurement AS psa_progression_date,
        observation_id AS progression_observation_id,
        effective_datetime AS progression_effective_datetime,
        psa_progression,
        note_text AS progression_note
    FROM rpt_fhir_observations_psa_progression_hmu_v1
    WHERE value_codeable_concept_code = '277022003'
        AND component_code = 'mostRecentMeasurement_v1'
),
low_testosterone_patients AS (
    -- Patients with testosterone < 50 ng/dL (castrate level)
    -- Get the most recent testosterone measurement per patient
    SELECT
        patient_id,
        observation_id AS testosterone_observation_id,
        effective_datetime AS testosterone_date,
        combined_value AS testosterone_value,
        value_quantity_value AS testosterone_numeric
    FROM (
        SELECT
            patient_id,
            observation_id,
            effective_datetime,
            combined_value,
            value_quantity_value,
            ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY effective_datetime DESC) AS rn
        FROM rpt_fhir_observations_testosterone_total_hmu_v1
        WHERE
            -- Values starting with '<' are assumed to be < 50 (castrate level)
            combined_value ILIKE '<%'
            -- Numeric value_quantity_value explicitly < 50
            OR (value_quantity_value IS NOT NULL AND value_quantity_value < 50)
    ) ranked
    WHERE rn = 1  -- Most recent measurement only
),
mcrpc_candidates AS (
    -- Join all criteria together
    SELECT
        adt.patient_id,
        adt.treatmentStartDate AS adt_treatment_start,
        adt.adt_observation_id,
        adt.adt_effective_datetime,
        adt.adt_note,
        prog.psa_progression_date,
        prog.psa_progression,
        prog.progression_observation_id,
        prog.progression_effective_datetime,
        prog.progression_note,
        test.testosterone_date,
        test.testosterone_value,
        test.testosterone_numeric,
        test.testosterone_observation_id
    FROM active_adt_patients adt
    INNER JOIN psa_progression_patients prog
        ON adt.patient_id = prog.patient_id
        -- PSA progression must occur AFTER ADT treatment start
        AND prog.psa_progression_date > adt.treatmentStartDate
    INNER JOIN low_testosterone_patients test
        ON adt.patient_id = test.patient_id
)
SELECT
    mc.patient_id,
    -- Patient demographics
    fp.names AS patient_name,
    fp.birth_date,
    fp.gender,
    -- ADT treatment details
    mc.adt_treatment_start,
    mc.adt_effective_datetime,
    mc.adt_observation_id,
    mc.adt_note,
    -- PSA progression details
    mc.psa_progression_date,
    mc.psa_progression,
    mc.progression_effective_datetime,
    mc.progression_observation_id,
    mc.progression_note,
    -- Days from ADT start to PSA progression
    DATEDIFF(day, mc.adt_treatment_start, mc.psa_progression_date) AS days_to_progression,
    -- Testosterone details
    mc.testosterone_date,
    mc.testosterone_value,
    mc.testosterone_numeric,
    mc.testosterone_observation_id,
    -- mCRPC flag
    TRUE AS is_mcrpc,
    CURRENT_TIMESTAMP AS identified_at
FROM mcrpc_candidates mc
INNER JOIN public.fact_fhir_patients_view_v1 fp
    ON mc.patient_id = fp.patient_id;
