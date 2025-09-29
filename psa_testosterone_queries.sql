-- ===================================================================
-- PSA AND TESTOSTERONE ANALYSIS QUERIES
-- ===================================================================
-- 
-- OVERVIEW:
-- Standardized queries for extracting PSA and testosterone lab results
-- and testosterone replacement therapy medications using LOINC and RxNorm codes.
--
-- LOINC CODES USED:
-- PSA Tests:
--   2857-1  - Prostate specific Ag [Mass/volume] in Serum or Plasma
--   10886-0 - Prostate specific Ag Free [Mass/volume] in Serum or Plasma
--   35741-8 - Prostate specific Ag ultrasensitive (detection limit <= 0.01 ng/mL)
--   34611-4 - Prostate specific Ag [Mass/volume] in Urine
--
-- Testosterone Tests:
--   2986-8  - Testosterone [Mass/volume] in Serum or Plasma
--   2991-8  - Testosterone Free [Mass/volume] in Serum or Plasma
--   49041-7 - Testosterone with detection limit <= 1.0 ng/dL
--   6891-6  - Testosterone free+weakly bound/Testosterone total ratio
--
-- RXNORM CODES USED:
-- Testosterone Medications:
--   2047882 - Testosterone cypionate 200 MG in 1 ML Injection
--   835840  - Testosterone cypionate 200 MG/ML Injectable Solution
--   1597121 - AndroGel testosterone gel formulation
--
-- ===================================================================

-- ===================================================================
-- QUERY 1: PSA VALUES WITH LOINC CODES
-- ===================================================================
-- Extract all PSA test results using standardized LOINC codes
-- with fallback to text-based search for non-coded data

SELECT 
    patient_id, 
    effective_datetime, 
    value_quantity_value, 
    value_quantity_unit,
    observation_code,
    observation_display,
    CASE 
        WHEN observation_code = '2857-1' THEN 'PSA Total'
        WHEN observation_code = '10886-0' THEN 'PSA Free'
        WHEN observation_code = '35741-8' THEN 'PSA Ultrasensitive'
        WHEN observation_code = '34611-4' THEN 'PSA Urine'
        ELSE 'PSA (Text Match)'
    END AS test_type
FROM fact_fhir_observations_view_v2
WHERE observation_code IN (
    '2857-1',  -- PSA total [Mass/volume] in Serum or Plasma
    '10886-0', -- PSA free [Mass/volume] in Serum or Plasma  
    '35741-8', -- PSA ultrasensitive (detection limit <= 0.01 ng/mL)
    '34611-4'  -- PSA [Mass/volume] in Urine
)
   OR observation_display ILIKE '%PSA%'
   OR observation_display ILIKE '%prostate specific antigen%'
ORDER BY patient_id, effective_datetime DESC;

-- ===================================================================
-- QUERY 2: TESTOSTERONE VALUES WITH LOINC CODES
-- ===================================================================
-- Extract all testosterone test results using standardized LOINC codes
-- with fallback to text-based search for non-coded data

SELECT 
    patient_id, 
    effective_datetime, 
    value_quantity_value, 
    value_quantity_unit,
    observation_code,
    observation_display,
    CASE 
        WHEN observation_code = '2986-8' THEN 'Testosterone Total'
        WHEN observation_code = '2991-8' THEN 'Testosterone Free'
        WHEN observation_code = '49041-7' THEN 'Testosterone Ultrasensitive'
        WHEN observation_code = '6891-6' THEN 'Testosterone Free/Total Ratio'
        ELSE 'Testosterone (Text Match)'
    END AS test_type
FROM fact_fhir_observations_view_v2
WHERE observation_code IN (
    '2986-8',  -- Testosterone total [Mass/volume] in Serum or Plasma
    '2991-8',  -- Testosterone free [Mass/volume] in Serum or Plasma
    '49041-7', -- Testosterone with detection limit <= 1.0 ng/dL
    '6891-6'   -- Testosterone free+weakly bound/Testosterone total ratio
)
   OR observation_display ILIKE '%testosterone%'
ORDER BY patient_id, effective_datetime DESC;

-- ===================================================================
-- QUERY 3: TESTOSTERONE REPLACEMENT THERAPY MEDICATIONS
-- ===================================================================
-- Extract testosterone medications using RxNorm codes
-- with fallback to text-based search for non-coded data

SELECT 
    patient_id,
    authored_on,
    medication_code,
    medication_display,
    status,
    dosage_instruction_text,
    CASE 
        WHEN medication_code = '2047882' THEN 'Testosterone Cypionate Injection 200mg/mL'
        WHEN medication_code = '835840' THEN 'Testosterone Cypionate Injectable Solution'
        WHEN medication_code = '1597121' THEN 'AndroGel Testosterone Gel'
        WHEN medication_display ILIKE '%androgel%' THEN 'AndroGel (Text Match)'
        WHEN medication_display ILIKE '%testim%' THEN 'Testim (Text Match)'
        WHEN medication_display ILIKE '%cypionate%' THEN 'Testosterone Cypionate (Text Match)'
        WHEN medication_display ILIKE '%enanthate%' THEN 'Testosterone Enanthate (Text Match)'
        ELSE 'Testosterone Medication (Text Match)'
    END AS medication_type
FROM fact_fhir_medication_requests_view_v1  
WHERE medication_code IN (
    '2047882', -- Testosterone cypionate 200 MG in 1 ML Injection
    '835840',  -- Testosterone cypionate 200 MG/ML Injectable Solution
    '1597121'  -- AndroGel testosterone gel formulation
)
   OR medication_display ILIKE '%testosterone%'
   OR medication_display ILIKE '%androgel%'
   OR medication_display ILIKE '%testim%'
   OR medication_display ILIKE '%testosterone cypionate%'
   OR medication_display ILIKE '%testosterone enanthate%'
ORDER BY patient_id, authored_on DESC;

-- ===================================================================
-- QUERY 4: LATEST PSA AND TESTOSTERONE RESULTS PER PATIENT
-- ===================================================================
-- Get the most recent PSA and testosterone results for each patient

WITH latest_psa AS (
    SELECT 
        patient_id,
        value_quantity_value as psa_value,
        value_quantity_unit as psa_unit,
        effective_datetime as psa_date,
        observation_code as psa_code,
        observation_display as psa_display,
        ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY effective_datetime DESC) as rn
    FROM fact_fhir_observations_view_v2
    WHERE observation_code IN ('2857-1', '10886-0', '35741-8', '34611-4')
       OR observation_display ILIKE '%PSA%'
       OR observation_display ILIKE '%prostate specific antigen%'
),
latest_testosterone AS (
    SELECT 
        patient_id,
        value_quantity_value as testosterone_value,
        value_quantity_unit as testosterone_unit,
        effective_datetime as testosterone_date,
        observation_code as testosterone_code,
        observation_display as testosterone_display,
        ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY effective_datetime DESC) as rn
    FROM fact_fhir_observations_view_v2
    WHERE observation_code IN ('2986-8', '2991-8', '49041-7', '6891-6')
       OR observation_display ILIKE '%testosterone%'
)
SELECT 
    COALESCE(p.patient_id, t.patient_id) as patient_id,
    p.psa_value,
    p.psa_unit,
    p.psa_date,
    p.psa_code,
    p.psa_display,
    t.testosterone_value,
    t.testosterone_unit,
    t.testosterone_date,
    t.testosterone_code,
    t.testosterone_display
FROM latest_psa p
FULL OUTER JOIN latest_testosterone t 
    ON p.patient_id = t.patient_id 
    AND p.rn = 1 
    AND t.rn = 1
WHERE (p.rn = 1 OR p.rn IS NULL) 
  AND (t.rn = 1 OR t.rn IS NULL);

-- ===================================================================
-- QUERY 5: COMPREHENSIVE PATIENT ANALYSIS
-- ===================================================================
-- Combine PSA results, testosterone results, and TRT medications
-- for comprehensive patient analysis

WITH psa_results AS (
    SELECT 
        patient_id,
        MAX(CASE WHEN observation_code = '2857-1' THEN value_quantity_value END) as latest_psa_total,
        MAX(CASE WHEN observation_code = '10886-0' THEN value_quantity_value END) as latest_psa_free,
        MAX(effective_datetime) as latest_psa_date,
        COUNT(DISTINCT observation_id) as psa_test_count
    FROM fact_fhir_observations_view_v2
    WHERE observation_code IN ('2857-1', '10886-0', '35741-8', '34611-4')
       OR observation_display ILIKE '%PSA%'
       OR observation_display ILIKE '%prostate specific antigen%'
    GROUP BY patient_id
),
testosterone_results AS (
    SELECT 
        patient_id,
        MAX(CASE WHEN observation_code = '2986-8' THEN value_quantity_value END) as latest_testosterone_total,
        MAX(CASE WHEN observation_code = '2991-8' THEN value_quantity_value END) as latest_testosterone_free,
        MAX(effective_datetime) as latest_testosterone_date,
        COUNT(DISTINCT observation_id) as testosterone_test_count
    FROM fact_fhir_observations_view_v2
    WHERE observation_code IN ('2986-8', '2991-8', '49041-7', '6891-6')
       OR observation_display ILIKE '%testosterone%'
    GROUP BY patient_id
),
trt_medications AS (
    SELECT 
        patient_id,
        COUNT(DISTINCT medication_request_id) as trt_prescription_count,
        MAX(authored_on) as latest_trt_prescription,
        STRING_AGG(DISTINCT medication_display, '; ') as trt_medications_list
    FROM fact_fhir_medication_requests_view_v1
    WHERE medication_code IN ('2047882', '835840', '1597121')
       OR medication_display ILIKE '%testosterone%'
       OR medication_display ILIKE '%androgel%'
       OR medication_display ILIKE '%testim%'
    GROUP BY patient_id
)
SELECT 
    p.patient_id,
    p.birth_date,
    p.gender,
    -- PSA Results
    psa.latest_psa_total,
    psa.latest_psa_free,
    psa.latest_psa_date,
    psa.psa_test_count,
    -- Testosterone Results
    t.latest_testosterone_total,
    t.latest_testosterone_free,
    t.latest_testosterone_date,
    t.testosterone_test_count,
    -- TRT Medications
    trt.trt_prescription_count,
    trt.latest_trt_prescription,
    trt.trt_medications_list,
    -- Calculated Fields
    CASE 
        WHEN psa.latest_psa_total > 4.0 THEN 'Elevated'
        WHEN psa.latest_psa_total BETWEEN 2.5 AND 4.0 THEN 'Borderline'
        WHEN psa.latest_psa_total < 2.5 THEN 'Normal'
        ELSE 'Unknown'
    END AS psa_status,
    CASE 
        WHEN t.latest_testosterone_total < 300 THEN 'Low'
        WHEN t.latest_testosterone_total BETWEEN 300 AND 400 THEN 'Borderline Low'
        WHEN t.latest_testosterone_total > 400 THEN 'Normal'
        ELSE 'Unknown'
    END AS testosterone_status
FROM fact_fhir_patients_view_v2 p
LEFT JOIN psa_results psa ON p.patient_id = psa.patient_id
LEFT JOIN testosterone_results t ON p.patient_id = t.patient_id
LEFT JOIN trt_medications trt ON p.patient_id = trt.patient_id
WHERE psa.patient_id IS NOT NULL 
   OR t.patient_id IS NOT NULL 
   OR trt.patient_id IS NOT NULL
ORDER BY p.patient_id;

-- ===================================================================
-- QUERY 6: TRENDING ANALYSIS - PSA OVER TIME
-- ===================================================================
-- Track PSA values over time for trend analysis

SELECT 
    patient_id,
    effective_datetime,
    value_quantity_value as psa_value,
    value_quantity_unit as psa_unit,
    observation_code,
    observation_display,
    LAG(value_quantity_value) OVER (PARTITION BY patient_id ORDER BY effective_datetime) as previous_psa_value,
    LAG(effective_datetime) OVER (PARTITION BY patient_id ORDER BY effective_datetime) as previous_psa_date,
    value_quantity_value - LAG(value_quantity_value) OVER (PARTITION BY patient_id ORDER BY effective_datetime) as psa_change,
    DATEDIFF(day, 
        LAG(effective_datetime) OVER (PARTITION BY patient_id ORDER BY effective_datetime),
        effective_datetime
    ) as days_between_tests
FROM fact_fhir_observations_view_v2
WHERE observation_code = '2857-1'  -- PSA Total only for trending
   OR (observation_display ILIKE '%PSA%' AND observation_display NOT ILIKE '%free%')
ORDER BY patient_id, effective_datetime;

-- ===================================================================
-- QUERY 7: TESTOSTERONE REPLACEMENT THERAPY EFFECTIVENESS
-- ===================================================================
-- Analyze testosterone levels before and after TRT initiation

WITH trt_start AS (
    SELECT 
        patient_id,
        MIN(authored_on) as first_trt_prescription
    FROM fact_fhir_medication_requests_view_v1
    WHERE medication_code IN ('2047882', '835840', '1597121')
       OR medication_display ILIKE '%testosterone%'
    GROUP BY patient_id
),
testosterone_levels AS (
    SELECT 
        o.patient_id,
        o.effective_datetime,
        o.value_quantity_value as testosterone_level,
        o.observation_code,
        t.first_trt_prescription,
        CASE 
            WHEN o.effective_datetime < t.first_trt_prescription THEN 'Pre-TRT'
            WHEN o.effective_datetime >= t.first_trt_prescription THEN 'Post-TRT'
            ELSE 'No TRT'
        END AS trt_status
    FROM fact_fhir_observations_view_v2 o
    LEFT JOIN trt_start t ON o.patient_id = t.patient_id
    WHERE o.observation_code = '2986-8'  -- Testosterone Total
       OR o.observation_display ILIKE '%testosterone%total%'
)
SELECT 
    patient_id,
    trt_status,
    AVG(testosterone_level) as avg_testosterone,
    MIN(testosterone_level) as min_testosterone,
    MAX(testosterone_level) as max_testosterone,
    COUNT(*) as test_count
FROM testosterone_levels
WHERE first_trt_prescription IS NOT NULL
GROUP BY patient_id, trt_status
ORDER BY patient_id, trt_status;

-- ===================================================================
-- END OF PSA AND TESTOSTERONE ANALYSIS QUERIES
-- ===================================================================