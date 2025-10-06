-- ===================================================================
-- RPT FHIR HMU PATIENTS VIEW V1
-- ===================================================================
--
-- OVERVIEW:
-- Comprehensive materialized view for HMU target patients with prostate cancer conditions
-- and recent encounters. This view combines patient demographics with clinical data
-- for analytics and reporting purposes.
--
-- PRIMARY KEY: patient_id
--
-- PATIENT CRITERIA:
-- - Patients with prostate cancer related conditions (ICD-10-CM codes: C61, Z19.1, Z19.2, R97.21)
-- - Must have encounters since 2024-01-01
--
-- SOURCE TABLES:
-- - fact_fhir_conditions_view_v1: Condition data for filtering
-- - fact_fhir_encounters_view_v1: Encounter data for date filtering
-- - fact_fhir_patients_view_v1: Patient demographics and details
--
-- REFRESH STRATEGY:
-- - AUTO REFRESH NO: Manual refresh required via scheduled jobs
-- - BACKUP NO: No backup required for this materialized view
--
-- ===================================================================

CREATE VIEW rpt_fhir_hmu_patients_v1
AS WITH target_patients AS (
    SELECT
        DISTINCT c.patient_id,
        e.last_encounter_date,
        c.code_code,
        c.code_system
    FROM
        fact_fhir_conditions_view_v1 AS c
        INNER JOIN (
            SELECT
                patient_id,
                MAX(start_time) AS last_encounter_date
            FROM
                fact_fhir_encounters_view_v1
            GROUP BY
                patient_id
        ) AS e ON c.patient_id = e.patient_id
    WHERE
        (
            (
                c.code_code IN ('C61', 'Z19.1', 'Z19.2', 'R97.21')
                AND c.code_system = 'http://hl7.org/fhir/sid/icd-10-cm'
            )
        )
        AND e.last_encounter_date >= '2024-01-01'
)
SELECT
    -- PATIENT IDENTIFICATION
    p.patient_id,
    tp.last_encounter_date,
    -- CONDITION CODES
    tp.code_code,
    tp.code_system,
    -- DEMOGRAPHICS
    p.birth_date,
    p.gender,
    -- NAME INFORMATION (DISCRETE FIELDS FROM PATIENT VIEW)
    p.name_text,
    p.family_name,
    p.given_names,
    p.prefix AS name_prefix,
    p.suffix AS name_suffix,
    p.name_use,
    -- NAME INFORMATION (JSON - LEGACY COMPATIBILITY)
    p.names,
    -- CONTACT INFORMATION
    p.all_addresses,
    -- STATUS AND METADATA
    p.active,
    p.deceased,
    p.deceased_date,
    p.managing_organization_id,
    -- CLINICAL METRICS
    p.total_encounter_count,
    p.encounters_last_year,
    p.total_condition_count,
    p.active_condition_count,
    -- METADATA
    p.meta_last_updated,
    p.meta_source,
    -- DERIVED FIELDS (from patients view)
    p.current_age,
    -- TIME SINCE LAST ENCOUNTER
    DATEDIFF(day, tp.last_encounter_date, CURRENT_DATE) AS days_since_last_encounter
FROM
    target_patients tp
    INNER JOIN fact_fhir_patients_view_v1 p ON tp.patient_id = p.patient_id;

-- ===================================================================
-- USAGE NOTES
-- ===================================================================
-- This materialized view provides a comprehensive dataset for HMU target patients including:
-- 1. Patient demographics and contact information
-- 2. Clinical status indicators
-- 3. Time-based analytics (age, days since last encounter)
-- 4. Pre-filtered for prostate cancer patients with recent activity
--
-- Use this view for:
-- - Patient cohort analysis
-- - Demographics reporting
-- - Care gap identification
-- - Population health management
--
-- Refresh frequency: Daily or as needed based on data update patterns
-- ===================================================================