-- Comprehensive Patient Data Query
-- Queries all fact* and rpt* views for a specific patient_id
--
-- USAGE: Find and replace 'PATIENT_ID_HERE' with the actual patient ID throughout this file
-- Example: Replace 'PATIENT_ID_HERE' with 'patient-12345'
--
-- This query provides a complete view of all FHIR data for a single patient across:
-- - Fact tables (core FHIR resources)
-- - Reporting views (clinical metrics and derived data)

-- ==============================================================================
-- SECTION 1: FACT TABLES (Core FHIR Resources)
-- ==============================================================================

-- Patient Demographics
SELECT 'fact_fhir_patients' AS source_table, * FROM fact_fhir_patients_view_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- Care Plans
SELECT 'fact_fhir_care_plans' AS source_table, * FROM fact_fhir_care_plans_view_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- Conditions (Diagnoses)
SELECT 'fact_fhir_conditions' AS source_table, * FROM fact_fhir_conditions_view_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- Encounters (Visits)
SELECT 'fact_fhir_encounters' AS source_table, * FROM fact_fhir_encounters_view_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- Observations (Labs, Vitals, etc.)
SELECT 'fact_fhir_observations' AS source_table, * FROM fact_fhir_observations_view_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- Medication Requests (Prescriptions)
SELECT 'fact_fhir_medication_requests' AS source_table, * FROM fact_fhir_medication_requests_view_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- Procedures
SELECT 'fact_fhir_procedures' AS source_table, * FROM fact_fhir_procedures_view_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- Allergy Intolerances
SELECT 'fact_fhir_allergy_intolerance' AS source_table, * FROM fact_fhir_allergy_intolerance_view_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- Diagnostic Reports
SELECT 'fact_fhir_diagnostic_reports' AS source_table, * FROM fact_fhir_diagnostic_reports_view_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- Document References
SELECT 'fact_fhir_document_references' AS source_table, * FROM fact_fhir_document_references_view_v1
WHERE patient_id = 'PATIENT_ID_HERE'

-- ==============================================================================
-- SECTION 2: REPORTING VIEWS - COHORTS
-- ==============================================================================

UNION ALL

-- HMU Patients (Cohort Identification)
SELECT 'rpt_fhir_hmu_patients' AS source_table, * FROM rpt_fhir_hmu_patients_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- mCRPC Patients (Metastatic Castration-Resistant Prostate Cancer)
SELECT 'rpt_fhir_mcrpc_patients' AS source_table, * FROM rpt_fhir_mcrpc_patients_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

-- ==============================================================================
-- SECTION 3: CONDITION-SPECIFIC REPORTING VIEWS
-- ==============================================================================

UNION ALL

-- Active Liver Disease
SELECT 'rpt_fhir_conditions_active_liver_disease' AS source_table, * FROM rpt_fhir_conditions_active_liver_disease_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- CNS Metastases
SELECT 'rpt_fhir_conditions_cns_metastases' AS source_table, * FROM rpt_fhir_conditions_cns_metastases_view_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- Additional Malignancy
SELECT 'rpt_fhir_conditions_additional_malignancy' AS source_table, * FROM rpt_fhir_conditions_additional_malignancy_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- HSMS Conditions
SELECT 'rpt_fhir_conditions_hsms' AS source_table, * FROM rpt_fhir_conditions_hsms_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- HSMS Conditions (Inferred)
SELECT 'rpt_fhir_conditions_hsms_inferred' AS source_table, * FROM rpt_fhir_conditions_hsms_inferred_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

-- ==============================================================================
-- SECTION 4: MEDICATION REPORTING VIEWS
-- ==============================================================================

UNION ALL

-- ADT Medications
SELECT 'rpt_fhir_medication_requests_adt_meds' AS source_table, * FROM rpt_fhir_medication_requests_adt_meds_hmu_view_v1
WHERE patient_id = 'PATIENT_ID_HERE'

-- ==============================================================================
-- SECTION 5: OBSERVATION-SPECIFIC REPORTING VIEWS (Labs & Vitals)
-- ==============================================================================

UNION ALL

-- Absolute Neutrophil Count
SELECT 'rpt_fhir_observations_absolute_neutrophil_count' AS source_table, * FROM rpt_fhir_observations_absolute_neutrophil_count_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- ALT (Liver Function)
SELECT 'rpt_fhir_observations_alt' AS source_table, * FROM rpt_fhir_observations_alt_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- AST (Liver Function)
SELECT 'rpt_fhir_observations_ast' AS source_table, * FROM rpt_fhir_observations_ast_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- BMI
SELECT 'rpt_fhir_observations_bmi' AS source_table, * FROM rpt_fhir_observations_bmi_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- CD4 Count
SELECT 'rpt_fhir_observations_cd4_count' AS source_table, * FROM rpt_fhir_observations_cd4_count_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- Creatinine
SELECT 'rpt_fhir_observations_creatinine' AS source_table, * FROM rpt_fhir_observations_creatinine_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- eGFR (Kidney Function)
SELECT 'rpt_fhir_observations_egfr' AS source_table, * FROM rpt_fhir_observations_egfr_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- HbA1c (Diabetes)
SELECT 'rpt_fhir_observations_hba1c' AS source_table, * FROM rpt_fhir_observations_hba1c_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- Hemoglobin
SELECT 'rpt_fhir_observations_hemoglobin' AS source_table, * FROM rpt_fhir_observations_hemoglobin_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- HIV Viral Load
SELECT 'rpt_fhir_observations_hiv_viral_load' AS source_table, * FROM rpt_fhir_observations_hiv_viral_load_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- Platelet Count
SELECT 'rpt_fhir_observations_platelet_count' AS source_table, * FROM rpt_fhir_observations_platelet_count_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- PSA Total
SELECT 'rpt_fhir_observations_psa_total' AS source_table, * FROM rpt_fhir_observations_psa_total_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- PSA Progression
SELECT 'rpt_fhir_observations_psa_progression' AS source_table, * FROM rpt_fhir_observations_psa_progression_hmu
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- Serum Albumin
SELECT 'rpt_fhir_observations_serum_albumin' AS source_table, * FROM rpt_fhir_observations_serum_albumin_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- Serum Potassium
SELECT 'rpt_fhir_observations_serum_potassium' AS source_table, * FROM rpt_fhir_observations_serum_potassium_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- Testosterone Total
SELECT 'rpt_fhir_observations_testosterone_total' AS source_table, * FROM rpt_fhir_observations_testosterone_total_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- Total Bilirubin
SELECT 'rpt_fhir_observations_total_bilirubin' AS source_table, * FROM rpt_fhir_observations_total_bilirubin_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

UNION ALL

-- ADT Treatment Observations
SELECT 'rpt_fhir_observations_adt_treatment' AS source_table, * FROM rpt_fhir_observations_adt_treatment_hmu_v1
WHERE patient_id = 'PATIENT_ID_HERE'

-- ==============================================================================
-- ORDER BY: Sort all results by source table
-- ==============================================================================
ORDER BY source_table;

-- ==============================================================================
-- NOTES
-- ==============================================================================
-- This query uses SELECT * for each view to avoid column name mismatches
-- The source_table column identifies which view each row comes from
-- Results are sorted by source table name for easy browsing
--
-- To add date-based sorting, you would need to know the specific date column
-- names for each view, which may vary (meta_last_updated, effective_datetime, etc.)
--
-- For a summary of record counts per table, use query_patient_summary.sql instead
