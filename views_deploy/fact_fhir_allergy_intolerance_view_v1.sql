CREATE VIEW fact_fhir_allergy_intolerance_view_v1
AS
WITH aggregated_extensions AS (
    SELECT
        ae.allergy_intolerance_id,
        JSON_PARSE(
            '[' || LISTAGG(DISTINCT
                '{' ||
                '"url":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(ae.extension_url, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"value_string":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(ae.value_string, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"value_reference":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(ae.value_reference, '[\r\n\t]', ''), '"', ''), '') || '"' ||
                '}',
                ','
            ) WITHIN GROUP (ORDER BY ae.extension_url) || ']'
        ) AS extensions
    FROM public.allergy_intolerance_extensions ae
    WHERE ae.extension_url IS NOT NULL
    GROUP BY ae.allergy_intolerance_id
)
SELECT
    -- CORE ALLERGY INTOLERANCE DATA
    -- ============================================
    ai.allergy_intolerance_id,           -- Unique allergy intolerance identifier (Primary Key)
    ai.resource_type,                    -- Resource type (AllergyIntolerance)
    ai.patient_id,                       -- Patient reference

    -- Clinical Status Fields
    ai.clinical_status,                  -- active | inactive | resolved
    ai.clinical_status_code,             -- Status code

    -- Verification Status Fields
    ai.verification_status,              -- confirmed | unconfirmed | presumed | refuted
    ai.verification_status_code,         -- Verification status code

    -- Allergy Classification
    ai.category,                         -- food | medication | environment | biologic
    ai.criticality,                      -- low | high | unable-to-assess

    -- Allergy Description
    ai.code_text,                        -- Human-readable allergy description

    -- Recording Information
    ai.recorded_date,                    -- Date when allergy was recorded

    -- FHIR Metadata
    ai.meta_last_updated,                  -- FHIR version ID

    -- ETL Audit Fields
    ai.created_at,                       -- Record creation time
    ai.updated_at,                       -- Record update time

    -- ALLERGY CODES (DENORMALIZED COLUMNS)
    -- Individual code columns from allergy_intolerance_codings table
    -- This will create one row per allergy-code combination
    aic.system AS coding_system,
    aic.code AS coding_code,
    aic.display AS coding_display,

    -- Code ranking to identify primary codes (1 = primary, 2+ = secondary)
    ROW_NUMBER() OVER (PARTITION BY ai.allergy_intolerance_id ORDER BY
        CASE aic.system
            WHEN 'http://snomed.info/sct' THEN 1
            WHEN 'http://www.nlm.nih.gov/research/umls/rxnorm' THEN 2
            WHEN 'http://hl7.org/fhir/sid/ndc' THEN 3
            WHEN 'http://unitsofmeasure.org' THEN 4
            ELSE 5
        END,
        aic.code
    ) AS code_rank,
    -- EXTENSIONS (AGGREGATED AS JSON)
    -- ============================================
    ae.extensions,
    -- CALCULATED FIELDS
    -- Determine if allergy is currently active
    CASE
        WHEN ai.clinical_status = 'active' OR ai.clinical_status_code = 'active'
        THEN TRUE
        ELSE FALSE
    END AS is_active,

    -- Determine if allergy is confirmed
    CASE
        WHEN ai.verification_status = 'confirmed' OR ai.verification_status_code = 'confirmed'
        THEN TRUE
        ELSE FALSE
    END AS is_confirmed,


    -- Create combined allergy description
    CASE
        WHEN ai.code_text IS NOT NULL AND aic.display IS NOT NULL
        THEN ai.code_text || ' (' || aic.display || ')'
        WHEN ai.code_text IS NOT NULL
        THEN ai.code_text
        WHEN aic.display IS NOT NULL
        THEN aic.display
        ELSE 'Unknown Allergy'
    END AS combined_description

FROM public.allergy_intolerances ai
    LEFT JOIN public.allergy_intolerance_codings aic ON ai.allergy_intolerance_id = aic.allergy_intolerance_id
    LEFT JOIN aggregated_extensions ae ON ai.allergy_intolerance_id = ae.allergy_intolerance_id;
-- REFRESH CONFIGURATION
-- This materialized view is configured with AUTO REFRESH NO
-- Manual refresh will be scheduled via AWS Lambda or Airflow
-- Refresh frequency should align with source data update patterns
--
-- To manually refresh:
-- REFRESH MATERIALIZED VIEW fact_fhir_allergy_intolerance_view_v1;
-- ===================================================================
-- INDEXES AND OPTIMIZATION
-- Redshift automatically creates and maintains sort keys and distribution keys
-- based on query patterns. Monitor query performance and adjust if needed:
-- - Consider DISTKEY on patient_id for patient-centric queries
-- - Consider SORTKEY on (patient_id, coding_system, coding_code) for code-based analysis
-- - Consider SORTKEY on (allergy_intolerance_id, code_rank) for primary code queries
-- - Consider SORTKEY on (category, criticality) for allergy type analysis
-- ===================================================================
-- DATA QUALITY NOTES
-- 1. DENORMALIZED STRUCTURE: Each allergy may produce multiple rows (one per code)
-- 2. Allergies without codes will still appear with NULL code columns (LEFT JOIN preserved)
-- 3. Some allergies may not have recorded dates
-- 4. Extension data is aggregated as JSON for complex queries
-- 5. All text fields are sanitized to ensure valid JSON format
-- 6. Code ranking prioritizes: SNOMED (1) > RxNorm (2) > NDC (3) > UCUM (4) > Others (5)
-- 7. Use code_rank = 1 to filter for primary diagnostic codes only
-- 8. Clinical and verification statuses are preserved in both text and code formats
-- ===================================================================
-- DENORMALIZATION IMPLEMENTATION NOTES
-- This view uses denormalization strategy for allergy codes:
-- - Original allergy_intolerances table: Variable records
-- - Denormalized view: Multiple records per allergy (one per code combination)
-- - Benefits: Direct access to code columns without JSON parsing
-- - Trade-offs: Larger result set, repeated allergy data across code rows
-- - Use code_rank column to identify primary vs secondary codes
-- - Query patterns: Filter by coding_system/coding_code for prevalence analysis
-- ===================================================================
-- SAMPLE QUERIES
--
-- 1. Get all active allergies with primary codes only:
-- SELECT * FROM fact_fhir_allergy_intolerance_view_v1
-- WHERE is_active = TRUE AND code_rank = 1;
--
-- 2. Find high criticality food allergies:
-- SELECT patient_id, combined_description, criticality
-- FROM fact_fhir_allergy_intolerance_view_v1
-- WHERE category = 'food' AND is_high_criticality = TRUE;
--
-- 3. Count allergies by coding system:
-- SELECT coding_system, COUNT(*) as allergy_count
-- FROM fact_fhir_allergy_intolerance_view_v1
-- WHERE coding_system IS NOT NULL
-- GROUP BY coding_system;
--
-- 4. Recent allergies recorded in last 30 days:
-- SELECT * FROM fact_fhir_allergy_intolerance_view_v1
-- WHERE days_since_recorded <= 30;
--
-- ===================================================================