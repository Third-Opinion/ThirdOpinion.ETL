-- ===================================================================
-- FACT FHIR ALLERGY INTOLERANCE VIEW V1
--
-- OVERVIEW:
-- Materialized view that creates a fact table from FHIR AllergyIntolerance resources
-- Provides denormalized access to allergy/intolerance data for analytics
--
-- PRIMARY KEY: allergy_intolerance_id
--
-- SOURCE TABLES:
-- - public.allergy_intolerance: Main allergy intolerance data
--
-- REFRESH STRATEGY:
-- - AUTO REFRESH NO: Manual refresh required via scheduled jobs
--
-- USAGE NOTES:
-- - View contains one row per allergy intolerance
-- - Code information is stored directly in main columns
-- - Extensions stored as JSON text
-- - Calculated fields for common filters (is_active, is_confirmed, etc.)
--
-- ===================================================================

CREATE MATERIALIZED VIEW fact_fhir_allergy_intolerance_view_v1
AUTO REFRESH NO
AS
SELECT
    -- CORE ALLERGY INTOLERANCE DATA
    -- ============================================
    ai.allergy_intolerance_id,           -- Unique allergy intolerance identifier (Primary Key)
    ai.resourcetype,                     -- Resource type (AllergyIntolerance)
    ai.patient_id,                       -- Patient reference
    ai.encounter_id,                     -- Encounter reference

    -- Clinical Status Fields
    ai.clinical_status_code,             -- Status code
    ai.clinical_status_display,          -- Status display
    ai.clinical_status_system,           -- Status system

    -- Verification Status Fields
    ai.verification_status_code,         -- Verification status code
    ai.verification_status_display,      -- Verification display
    ai.verification_status_system,       -- Verification system

    -- Allergy Classification
    ai.type,                             -- allergy | intolerance
    ai.category,                         -- food | medication | environment | biologic
    ai.criticality,                      -- low | high | unable-to-assess

    -- Allergy Code Information
    ai.code,                             -- Primary code value
    ai.code_display,                     -- Human-readable code display
    ai.code_system,                      -- Code system (SNOMED, RxNorm, etc.)
    ai.code_text,                        -- Free text description

    -- Onset Information
    ai.onset_datetime,                   -- When allergy started
    ai.onset_age_value,                  -- Age at onset (value)
    ai.onset_age_unit,                   -- Age at onset (unit)
    ai.onset_period_start,               -- Onset period start
    ai.onset_period_end,                 -- Onset period end

    -- Recording Information
    ai.recorded_date,                    -- Date when allergy was recorded
    ai.recorder_practitioner_id,         -- Practitioner who recorded
    ai.asserter_practitioner_id,         -- Practitioner who asserted
    ai.last_occurrence,                  -- Last occurrence timestamp

    -- Notes and Reactions
    ai.note,                             -- Clinical notes
    ai.reactions,                        -- Reaction details (JSON)

    -- FHIR Metadata
    ai.meta_last_updated,                -- FHIR version ID
    ai.meta_source,                      -- Data source
    ai.meta_security,                    -- Security tags
    ai.meta_tag,                         -- Tags

    -- Extensions
    ai.extensions,                       -- FHIR extensions (JSON)

    -- ETL Audit Fields
    ai.created_at AS etl_created_at,     -- Record creation time
    ai.updated_at AS etl_updated_at,     -- Record update time

    -- CALCULATED FIELDS
    -- Determine if allergy is currently active
    CASE
        WHEN ai.clinical_status_code = 'active'
        THEN TRUE
        ELSE FALSE
    END AS is_active,

    -- Determine if allergy is confirmed
    CASE
        WHEN ai.verification_status_code = 'confirmed'
        THEN TRUE
        ELSE FALSE
    END AS is_confirmed,

    -- Determine if high criticality
    CASE
        WHEN ai.criticality = 'high'
        THEN TRUE
        ELSE FALSE
    END AS is_high_criticality,

    -- Create combined allergy description
    COALESCE(
        ai.code_text,
        ai.code_display,
        'Unknown Allergy'
    ) AS combined_description

FROM public.allergy_intolerance ai;
