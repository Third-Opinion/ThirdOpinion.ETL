-- Patient Data Summary Query
-- Provides record counts across all fact* and rpt* views for a specific patient
-- Usage: Replace 'PATIENT_ID_HERE' with actual patient ID

WITH patient_data AS (
    -- Fact Tables
    SELECT 'fact_fhir_patients' AS table_name, COUNT(*) AS record_count
    FROM fact_fhir_patients_view_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'fact_fhir_care_plans', COUNT(*)
    FROM fact_fhir_care_plans_view_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'fact_fhir_conditions', COUNT(*)
    FROM fact_fhir_conditions_view_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'fact_fhir_encounters', COUNT(*)
    FROM fact_fhir_encounters_view_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'fact_fhir_observations', COUNT(*)
    FROM fact_fhir_observations_view_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'fact_fhir_medication_requests', COUNT(*)
    FROM fact_fhir_medication_requests_view_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'fact_fhir_procedures', COUNT(*)
    FROM fact_fhir_procedures_view_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'fact_fhir_allergy_intolerance', COUNT(*)
    FROM fact_fhir_allergy_intolerance_view_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'fact_fhir_diagnostic_reports', COUNT(*)
    FROM fact_fhir_diagnostic_reports_view_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'fact_fhir_document_references', COUNT(*)
    FROM fact_fhir_document_references_view_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    -- Reporting Views - Cohorts
    UNION ALL

    SELECT 'rpt_fhir_hmu_patients', COUNT(*)
    FROM rpt_fhir_hmu_patients_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_mcrpc_patients', COUNT(*)
    FROM rpt_fhir_mcrpc_patients_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    -- Reporting Views - Conditions
    UNION ALL

    SELECT 'rpt_fhir_conditions_active_liver_disease', COUNT(*)
    FROM rpt_fhir_conditions_active_liver_disease_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_conditions_cns_metastases', COUNT(*)
    FROM rpt_fhir_conditions_cns_metastases_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_conditions_additional_malignancy', COUNT(*)
    FROM rpt_fhir_conditions_additional_malignancy_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_conditions_hsms', COUNT(*)
    FROM rpt_fhir_conditions_hsms_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_conditions_hsms_inferred', COUNT(*)
    FROM rpt_fhir_conditions_hsms_inferred_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    -- Reporting Views - Medications
    UNION ALL

    SELECT 'rpt_fhir_medication_requests_adt_meds', COUNT(*)
    FROM rpt_fhir_medication_requests_adt_meds_hmu_view_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    -- Reporting Views - Observations (Labs/Vitals)
    UNION ALL

    SELECT 'rpt_fhir_observations_absolute_neutrophil_count', COUNT(*)
    FROM rpt_fhir_observations_absolute_neutrophil_count_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_observations_adt_treatment', COUNT(*)
    FROM rpt_fhir_observations_adt_treatment_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_observations_alt', COUNT(*)
    FROM rpt_fhir_observations_alt_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_observations_ast', COUNT(*)
    FROM rpt_fhir_observations_ast_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_observations_bmi', COUNT(*)
    FROM rpt_fhir_observations_bmi_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_observations_cd4_count', COUNT(*)
    FROM rpt_fhir_observations_cd4_count_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_observations_creatinine', COUNT(*)
    FROM rpt_fhir_observations_creatinine_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_observations_egfr', COUNT(*)
    FROM rpt_fhir_observations_egfr_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_observations_hba1c', COUNT(*)
    FROM rpt_fhir_observations_hba1c_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_observations_hemoglobin', COUNT(*)
    FROM rpt_fhir_observations_hemoglobin_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_observations_hiv_viral_load', COUNT(*)
    FROM rpt_fhir_observations_hiv_viral_load_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_observations_platelet_count', COUNT(*)
    FROM rpt_fhir_observations_platelet_count_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_observations_psa_progression', COUNT(*)
    FROM rpt_fhir_observations_psa_progression_hmu
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_observations_psa_total', COUNT(*)
    FROM rpt_fhir_observations_psa_total_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_observations_serum_albumin', COUNT(*)
    FROM rpt_fhir_observations_serum_albumin_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_observations_serum_potassium', COUNT(*)
    FROM rpt_fhir_observations_serum_potassium_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_observations_testosterone_total', COUNT(*)
    FROM rpt_fhir_observations_testosterone_total_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'

    UNION ALL

    SELECT 'rpt_fhir_observations_total_bilirubin', COUNT(*)
    FROM rpt_fhir_observations_total_bilirubin_hmu_v1
    WHERE patient_id = 'PATIENT_ID_HERE'
)
SELECT
    table_name,
    record_count,
    CASE
        WHEN table_name LIKE 'fact_%' THEN 'Fact Table'
        WHEN table_name LIKE 'rpt_fhir_hmu_%' OR table_name LIKE 'rpt_fhir_mcrpc_%' THEN 'Cohort View'
        WHEN table_name LIKE 'rpt_fhir_conditions_%' THEN 'Condition View'
        WHEN table_name LIKE 'rpt_fhir_medication_%' THEN 'Medication View'
        WHEN table_name LIKE 'rpt_fhir_observations_%' THEN 'Observation View'
        ELSE 'Other'
    END AS category
FROM patient_data
WHERE record_count > 0  -- Only show tables with data
ORDER BY
    CASE category
        WHEN 'Fact Table' THEN 1
        WHEN 'Cohort View' THEN 2
        WHEN 'Condition View' THEN 3
        WHEN 'Medication View' THEN 4
        WHEN 'Observation View' THEN 5
        ELSE 6
    END,
    table_name;
