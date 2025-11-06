-- ============================================================================
-- CARDIAC EXCLUSION CRITERIA VIEW WITH TARGET PATIENT FILTER
-- ============================================================================
-- Identifies patients with cardiac conditions that may exclude them from
-- clinical trials or specific treatments, including:
-- 1. NYHA Class III or IV Heart Failure
-- 2. Unstable Angina or Angina at Rest
-- 3. New-Onset Angina (within last 3 months)
-- 4. Recent Myocardial Infarction (within past 6 months)
-- 5. Clinically Significant Cardiac Disease
-- 6. History of Old MI
-- ============================================================================

CREATE MATERIALIZED VIEW rpt_fhir_conditions_cardiac_hmu_v1
DISTSTYLE KEY
DISTKEY (patient_id)
SORTKEY (patient_id, cardiac_exclusion_count)
AS
WITH

-- Target patient population (recent encounters)
target_patients AS (
    SELECT patient_id
    FROM rpt_fhir_hmu_patients_v1
    WHERE last_encounter_date >= '2025-06-01'
),

-- 1. NYHA Class III or IV Heart Failure (> Class II)
nyha_exclusions AS (
    SELECT DISTINCT
        c.patient_id,
        'CHF > NYHA Class II' AS exclusion_reason,
        c.code_code,
        c.code_display,
        c.onset_datetime,
        c.recorded_date
    FROM fact_fhir_conditions_view_v1 c
    INNER JOIN target_patients tp ON c.patient_id = tp.patient_id
    WHERE
        c.is_active = TRUE
      AND (
        -- Explicit NYHA codes
        c.code_code IN (
                        '420300004',     -- NYHA Class III
                        '421704003'      -- NYHA Class IV
            )
            -- Heart failure with severe classifications
            OR c.code_code IN (
                               '703272007',     -- Heart failure stage C
                               '23341000119109', -- End stage heart failure
                               '371807002'      -- Heart failure with reduced ejection fraction
            )
            -- CHF codes with severity indicators
            OR (
            c.code_code IN (
                            '42343007',      -- Congestive heart failure
                            '84114007',      -- Heart failure
                            '88805009',      -- Chronic congestive heart failure
                            '441530006',     -- Chronic heart failure
                            '441481004',     -- Chronic systolic heart failure
                            '417996009',     -- Systolic heart failure
                            'I50.9',         -- Heart failure, unspecified
                            'I50.32',        -- Chronic diastolic heart failure
                            'I50.23'         -- Acute on chronic systolic heart failure
                )
                AND (
                -- Check severity field for NYHA or severity indicators
                c.severity_display ILIKE '%NYHA%III%'
                    OR c.severity_display ILIKE '%NYHA%IV%'
                    OR c.severity_display ILIKE '%Class III%'
                    OR c.severity_display ILIKE '%Class IV%'
                    OR c.severity_display ILIKE '%severe%'
                    OR c.severity_display ILIKE '%advanced%'
                )
            )
        )
),

-- 2. Unstable Angina or Angina at Rest
unstable_angina_exclusions AS (
    SELECT DISTINCT
        c.patient_id,
        'Unstable Angina (at rest)' AS exclusion_reason,
        c.code_code,
        c.code_display,
        c.onset_datetime,
        c.recorded_date
    FROM fact_fhir_conditions_view_v1 c
    INNER JOIN target_patients tp ON c.patient_id = tp.patient_id
    WHERE
        c.is_active = TRUE
      AND c.code_code IN (
                          '4557003',       -- Unstable angina
                          '233840006',     -- Angina at rest
                          '59021001',      -- Angina decubitus
                          '194821006',     -- Angina at rest with documented spasm
                          '233843008',     -- Angina with documented spasm
                          'I20.1'          -- Angina pectoris with documented spasm
        )
),

-- 3. New-Onset Angina (within last 3 months)
new_onset_angina_exclusions AS (
    SELECT DISTINCT
        c.patient_id,
        'New-Onset Angina (< 3 months)' AS exclusion_reason,
        c.code_code,
        c.code_display,
        c.onset_datetime,
        c.recorded_date
    FROM fact_fhir_conditions_view_v1 c
    INNER JOIN target_patients tp ON c.patient_id = tp.patient_id
    WHERE
        c.code_code IN (
                        '194828000',     -- Angina pectoris
                        '85284003',      -- Angina pectoris
                        '233819005',     -- Stable angina
                        '300995000',     -- Exercise-induced angina
                        '161504004',     -- History of angina
                        'I20.9'          -- Angina pectoris, unspecified
            )
      AND (
        -- Check both onset and recorded dates
        c.onset_datetime >= CURRENT_DATE - INTERVAL '3 months'
            OR (c.onset_datetime IS NULL AND c.recorded_date >= CURRENT_DATE - INTERVAL '3 months')
        )
),

-- 4. Recent Myocardial Infarction (within past 6 months)
recent_mi_exclusions AS (
    SELECT DISTINCT
        c.patient_id,
        'Myocardial Infarction (< 6 months)' AS exclusion_reason,
        c.code_code,
        c.code_display,
        c.onset_datetime,
        c.recorded_date
    FROM fact_fhir_conditions_view_v1 c
    INNER JOIN target_patients tp ON c.patient_id = tp.patient_id
    WHERE
        c.code_code IN (
            -- Acute MI codes
                        '22298006',      -- Myocardial infarction
                        '57054005',      -- Acute myocardial infarction
                        '418304008',     -- Acute ST segment elevation myocardial infarction
                        '194779001',     -- Acute myocardial infarction
                        '10633002',      -- Acute myocardial infarction of anterior wall
                        '698296002',     -- Acute STEMI
                        '443344007',     -- Acute non-ST segment elevation myocardial infarction
                        '443253003',     -- Acute inferolateral STEMI
                        '426856002',     -- Acute ST segment elevation inferior MI
                        '443254009',     -- Acute inferoposterior STEMI
                        '401303003',     -- Acute STEMI of anterior wall
                        '401314000',     -- Acute non-Q wave myocardial infarction
                        '443343001',     -- Acute anterolateral STEMI
                        '698593009',     -- Acute coronary syndrome
                        '194767001',     -- Acute anteroseptal infarction
                        '194781004',     -- Acute subendocardial infarction
                        '194766005',     -- Acute anterolateral infarction
                        '90727007',      -- Posterolateral myocardial infarction
                        '395105005',     -- Acute inferoposterior myocardial infarction
                        '403457003',     -- Acute ischemic heart disease
                        '77737007',      -- Postoperative myocardial infarction
                        '196542004',     -- Lateral myocardial infarction
                        '36315003',      -- Subsequent myocardial infarction
                        '170593007',     -- Acute anteroapical infarction
                        '428752002',     -- Recent myocardial infarction
                        'I21.9',         -- Acute myocardial infarction, unspecified
                        'I21.3'          -- ST elevation MI of unspecified site
            )
      AND (
        c.onset_datetime >= CURRENT_DATE - INTERVAL '6 months'
            OR (c.onset_datetime IS NULL AND c.recorded_date >= CURRENT_DATE - INTERVAL '6 months')
        )
),

-- 5. Clinically Significant Cardiac Disease (active conditions)
significant_cardiac_disease_exclusions AS (
    SELECT DISTINCT
        c.patient_id,
        'Clinically Significant Cardiac Disease' AS exclusion_reason,
        c.code_code,
        c.code_display,
        c.onset_datetime,
        c.recorded_date
    FROM fact_fhir_conditions_view_v1 c
    INNER JOIN target_patients tp ON c.patient_id = tp.patient_id
    WHERE
        c.is_active = TRUE
      AND c.code_code IN (
                          '314207007',     -- Ischemic cardiomyopathy
                          '83105008',      -- Malignant hypertensive heart disease
                          '73795002',      -- Chronic cor pulmonale
                          '120901000119108', -- Right ventricular failure
                          '285721000119104', -- Biventricular heart failure
                          '85232009',      -- Left heart failure
                          '111000119104',  -- CHF due to valvular heart disease
                          '426263006',     -- CHF due to left ventricular systolic dysfunction
                          'I25.5'          -- Ischemic cardiomyopathy
        )
),

-- 6. History of Old MI (not time-restricted, but clinically significant)
historical_mi AS (
    SELECT DISTINCT
        c.patient_id,
        'History of MI' AS exclusion_reason,
        c.code_code,
        c.code_display,
        c.onset_datetime,
        c.recorded_date
    FROM fact_fhir_conditions_view_v1 c
    INNER JOIN target_patients tp ON c.patient_id = tp.patient_id
    WHERE
        c.is_active = TRUE
      AND c.code_code IN (
                          '399211009',     -- History of myocardial infarction
                          '1755008'        -- Old myocardial infarction
        )
),

-- Combine all exclusion criteria
all_cardiac_exclusions AS (
    SELECT patient_id, exclusion_reason, code_code, code_display, onset_datetime, recorded_date
    FROM nyha_exclusions

    UNION ALL

    SELECT patient_id, exclusion_reason, code_code, code_display, onset_datetime, recorded_date
    FROM unstable_angina_exclusions

    UNION ALL

    SELECT patient_id, exclusion_reason, code_code, code_display, onset_datetime, recorded_date
    FROM new_onset_angina_exclusions

    UNION ALL

    SELECT patient_id, exclusion_reason, code_code, code_display, onset_datetime, recorded_date
    FROM recent_mi_exclusions

    UNION ALL

    SELECT patient_id, exclusion_reason, code_code, code_display, onset_datetime, recorded_date
    FROM significant_cardiac_disease_exclusions

    UNION ALL

    SELECT patient_id, exclusion_reason, code_code, code_display, onset_datetime, recorded_date
    FROM historical_mi
)

-- ============================================================================
-- FINAL RESULTS: ALL TARGET PATIENTS WITH CARDIAC EXCLUSIONS
-- ============================================================================
SELECT
    tp.patient_id,
    COALESCE(ex.number_of_exclusion_criteria, 0) AS cardiac_exclusion_count,
    ex.earliest_cardiac_event,
    ex.most_recent_recording,
    COALESCE(ex.has_severe_chf, 0) AS has_severe_chf,
    COALESCE(ex.has_unstable_angina, 0) AS has_unstable_angina,
    COALESCE(ex.has_new_angina, 0) AS has_new_angina,
    COALESCE(ex.has_recent_mi, 0) AS has_recent_mi,
    COALESCE(ex.has_significant_cardiac, 0) AS has_significant_cardiac,
    COALESCE(ex.has_mi_history, 0) AS has_mi_history,
    CASE
        WHEN ex.number_of_exclusion_criteria > 0 THEN 'EXCLUDED'
        ELSE 'ELIGIBLE'
    END AS cardiac_eligibility_status
FROM target_patients tp
LEFT JOIN (
    SELECT
        patient_id,
        COUNT(DISTINCT exclusion_reason) AS number_of_exclusion_criteria,
        MIN(COALESCE(onset_datetime, recorded_date)) AS earliest_cardiac_event,
        MAX(recorded_date) AS most_recent_recording,
        MAX(CASE WHEN exclusion_reason = 'CHF > NYHA Class II' THEN 1 ELSE 0 END) AS has_severe_chf,
        MAX(CASE WHEN exclusion_reason = 'Unstable Angina (at rest)' THEN 1 ELSE 0 END) AS has_unstable_angina,
        MAX(CASE WHEN exclusion_reason = 'New-Onset Angina (< 3 months)' THEN 1 ELSE 0 END) AS has_new_angina,
        MAX(CASE WHEN exclusion_reason = 'Myocardial Infarction (< 6 months)' THEN 1 ELSE 0 END) AS has_recent_mi,
        MAX(CASE WHEN exclusion_reason = 'Clinically Significant Cardiac Disease' THEN 1 ELSE 0 END) AS has_significant_cardiac,
        MAX(CASE WHEN exclusion_reason = 'History of MI' THEN 1 ELSE 0 END) AS has_mi_history
    FROM all_cardiac_exclusions
    GROUP BY patient_id
) ex ON tp.patient_id = ex.patient_id
WHERE cardiac_eligibility_status = 'EXCLUDED';
