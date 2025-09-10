-- =====================================================================
-- REDSHIFT TABLE RECORD COUNTS QUERY
-- =====================================================================
-- Run this query in AWS Redshift Query Editor to get record counts
-- for all FHIR tables, excluding tables with zero records
-- 
-- Instructions:
-- 1. Open AWS Console -> Amazon Redshift -> Query Editor v2
-- 2. Connect to: prod-redshift-main-ue2 cluster, dev database
-- 3. Copy and paste this entire query
-- 4. Execute to get results
-- =====================================================================

WITH table_counts AS (
  -- Core FHIR Entity Tables
  SELECT 'patients' as table_name, COUNT(*) as record_count FROM public.patients
  UNION ALL SELECT 'encounters', COUNT(*) FROM public.encounters
  UNION ALL SELECT 'care_plans', COUNT(*) FROM public.care_plans
  UNION ALL SELECT 'conditions', COUNT(*) FROM public.conditions
  UNION ALL SELECT 'diagnostic_reports', COUNT(*) FROM public.diagnostic_reports
  UNION ALL SELECT 'document_references', COUNT(*) FROM public.document_references
  UNION ALL SELECT 'medication_requests', COUNT(*) FROM public.medication_requests
  UNION ALL SELECT 'observations', COUNT(*) FROM public.observations
  UNION ALL SELECT 'practitioners', COUNT(*) FROM public.practitioners
  UNION ALL SELECT 'procedures', COUNT(*) FROM public.procedures
  UNION ALL SELECT 'medications', COUNT(*) FROM public.medications
  UNION ALL SELECT 'medication_dispenses', COUNT(*) FROM public.medication_dispenses
  
  -- Patient Related Tables
  UNION ALL SELECT 'patient_names', COUNT(*) FROM public.patient_names
  UNION ALL SELECT 'patient_addresses', COUNT(*) FROM public.patient_addresses
  UNION ALL SELECT 'patient_contacts', COUNT(*) FROM public.patient_contacts
  UNION ALL SELECT 'patient_communications', COUNT(*) FROM public.patient_communications
  UNION ALL SELECT 'patient_links', COUNT(*) FROM public.patient_links
  UNION ALL SELECT 'patient_practitioners', COUNT(*) FROM public.patient_practitioners
  UNION ALL SELECT 'patient_telecoms', COUNT(*) FROM public.patient_telecoms
  
  -- Encounter Related Tables
  UNION ALL SELECT 'encounter_hospitalization', COUNT(*) FROM public.encounter_hospitalization
  UNION ALL SELECT 'encounter_identifiers', COUNT(*) FROM public.encounter_identifiers
  UNION ALL SELECT 'encounter_locations', COUNT(*) FROM public.encounter_locations
  UNION ALL SELECT 'encounter_participants', COUNT(*) FROM public.encounter_participants
  UNION ALL SELECT 'encounter_reasons', COUNT(*) FROM public.encounter_reasons
  UNION ALL SELECT 'encounter_types', COUNT(*) FROM public.encounter_types
  
  -- Care Plan Related Tables
  UNION ALL SELECT 'care_plan_care_teams', COUNT(*) FROM public.care_plan_care_teams
  UNION ALL SELECT 'care_plan_categories', COUNT(*) FROM public.care_plan_categories
  UNION ALL SELECT 'care_plan_goals', COUNT(*) FROM public.care_plan_goals
  UNION ALL SELECT 'care_plan_identifiers', COUNT(*) FROM public.care_plan_identifiers
  
  -- Condition Related Tables
  UNION ALL SELECT 'condition_body_sites', COUNT(*) FROM public.condition_body_sites
  UNION ALL SELECT 'condition_categories', COUNT(*) FROM public.condition_categories
  UNION ALL SELECT 'condition_codes', COUNT(*) FROM public.condition_codes
  UNION ALL SELECT 'condition_evidence', COUNT(*) FROM public.condition_evidence
  UNION ALL SELECT 'condition_extensions', COUNT(*) FROM public.condition_extensions
  UNION ALL SELECT 'condition_notes', COUNT(*) FROM public.condition_notes
  UNION ALL SELECT 'condition_stages', COUNT(*) FROM public.condition_stages
  
  -- Diagnostic Report Related Tables
  UNION ALL SELECT 'diagnostic_report_based_on', COUNT(*) FROM public.diagnostic_report_based_on
  UNION ALL SELECT 'diagnostic_report_categories', COUNT(*) FROM public.diagnostic_report_categories
  UNION ALL SELECT 'diagnostic_report_media', COUNT(*) FROM public.diagnostic_report_media
  UNION ALL SELECT 'diagnostic_report_performers', COUNT(*) FROM public.diagnostic_report_performers
  UNION ALL SELECT 'diagnostic_report_presented_forms', COUNT(*) FROM public.diagnostic_report_presented_forms
  UNION ALL SELECT 'diagnostic_report_results', COUNT(*) FROM public.diagnostic_report_results
  
  -- Document Reference Related Tables
  UNION ALL SELECT 'document_reference_authors', COUNT(*) FROM public.document_reference_authors
  UNION ALL SELECT 'document_reference_categories', COUNT(*) FROM public.document_reference_categories
  UNION ALL SELECT 'document_reference_content', COUNT(*) FROM public.document_reference_content
  UNION ALL SELECT 'document_reference_identifiers', COUNT(*) FROM public.document_reference_identifiers
  
  -- Medication Request Related Tables
  UNION ALL SELECT 'medication_request_categories', COUNT(*) FROM public.medication_request_categories
  UNION ALL SELECT 'medication_request_dosage_instructions', COUNT(*) FROM public.medication_request_dosage_instructions
  UNION ALL SELECT 'medication_request_identifiers', COUNT(*) FROM public.medication_request_identifiers
  UNION ALL SELECT 'medication_request_notes', COUNT(*) FROM public.medication_request_notes
  
  -- Medication Dispense Related Tables
  UNION ALL SELECT 'medication_dispense_auth_prescriptions', COUNT(*) FROM public.medication_dispense_auth_prescriptions
  UNION ALL SELECT 'medication_dispense_dosage_instructions', COUNT(*) FROM public.medication_dispense_dosage_instructions
  UNION ALL SELECT 'medication_dispense_identifiers', COUNT(*) FROM public.medication_dispense_identifiers
  UNION ALL SELECT 'medication_dispense_performers', COUNT(*) FROM public.medication_dispense_performers
  
  -- Medication Related Tables
  UNION ALL SELECT 'medication_identifiers', COUNT(*) FROM public.medication_identifiers
  
  -- Observation Related Tables
  UNION ALL SELECT 'observation_categories', COUNT(*) FROM public.observation_categories
  UNION ALL SELECT 'observation_components', COUNT(*) FROM public.observation_components
  UNION ALL SELECT 'observation_derived_from', COUNT(*) FROM public.observation_derived_from
  UNION ALL SELECT 'observation_interpretations', COUNT(*) FROM public.observation_interpretations
  UNION ALL SELECT 'observation_members', COUNT(*) FROM public.observation_members
  UNION ALL SELECT 'observation_notes', COUNT(*) FROM public.observation_notes
  UNION ALL SELECT 'observation_performers', COUNT(*) FROM public.observation_performers
  UNION ALL SELECT 'observation_reference_ranges', COUNT(*) FROM public.observation_reference_ranges
  
  -- Practitioner Related Tables
  UNION ALL SELECT 'practitioner_addresses', COUNT(*) FROM public.practitioner_addresses
  UNION ALL SELECT 'practitioner_names', COUNT(*) FROM public.practitioner_names
  UNION ALL SELECT 'practitioner_telecoms', COUNT(*) FROM public.practitioner_telecoms
  
  -- Procedure Related Tables
  UNION ALL SELECT 'procedure_code_codings', COUNT(*) FROM public.procedure_code_codings
  UNION ALL SELECT 'procedure_identifiers', COUNT(*) FROM public.procedure_identifiers
  
  -- Materialized Views
  UNION ALL SELECT 'mv_tbl__fact_fhir_encounters_view_v1__0', COUNT(*) FROM public.mv_tbl__fact_fhir_encounters_view_v1__0
  UNION ALL SELECT 'mv_tbl__fact_fhir_patients_view_v1__0', COUNT(*) FROM public.mv_tbl__fact_fhir_patients_view_v1__0
)
SELECT 
    table_name,
    record_count,
    -- Add categorization for easier analysis
    CASE 
        WHEN table_name IN ('patients', 'encounters', 'care_plans', 'conditions', 'diagnostic_reports', 
                           'document_references', 'medication_requests', 'observations', 'practitioners', 
                           'procedures', 'medications', 'medication_dispenses') 
        THEN 'Main FHIR Entity'
        WHEN table_name LIKE 'mv_tbl__%' THEN 'Materialized View'
        ELSE 'Related Table'
    END as table_category
FROM table_counts 
WHERE record_count > 0  -- Only show tables with data
ORDER BY 
    table_category,
    record_count DESC,
    table_name;

-- =====================================================================
-- SUMMARY QUERY (Run this separately to get totals by category)
-- =====================================================================
/*
WITH table_counts AS (
  -- [Same CTE as above - copy the full CTE if running separately]
)
SELECT 
    CASE 
        WHEN table_name IN ('patients', 'encounters', 'care_plans', 'conditions', 'diagnostic_reports', 
                           'document_references', 'medication_requests', 'observations', 'practitioners', 
                           'procedures', 'medications', 'medication_dispenses') 
        THEN 'Main FHIR Entity'
        WHEN table_name LIKE 'mv_tbl__%' THEN 'Materialized View'
        ELSE 'Related Table'
    END as table_category,
    COUNT(*) as tables_with_data,
    SUM(record_count) as total_records
FROM table_counts 
WHERE record_count > 0
GROUP BY table_category
ORDER BY total_records DESC;
*/