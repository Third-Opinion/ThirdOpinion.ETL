-- ===================================================================
-- FACT FHIR CONDITIONS VIEW V1
-- ===================================================================
-- OVERVIEW:
-- Comprehensive materialized view that creates a fact table from FHIR Condition resources
-- by joining the main conditions table with all related condition tables for analytics and reporting.
-- DENORMALIZED DESIGN: Creates one row per condition-code combination for easier analysis.
--
-- PRIMARY KEY: condition_id + code_system + code_code (composite key due to denormalization)
--
-- SOURCE TABLES:
-- - public.conditions: Core condition data including clinical status and dates
-- - public.condition_codes: Condition diagnostic codes (ICD-10, SNOMED, etc.)
-- - public.condition_body_sites: Anatomical locations affected by condition
-- - public.condition_categories: Condition category classifications
-- - public.condition_evidence: Supporting evidence for the condition
-- - public.condition_extensions: Extended FHIR data elements
-- - public.condition_notes: Clinical notes and annotations
-- - public.condition_stages: Staging information for conditions
--
-- REFRESH STRATEGY:
-- - AUTO REFRESH NO: Manual refresh required via scheduled jobs
-- - BACKUP NO: No backup required for this materialized view
--
-- DATA PROCESSING:
-- - DENORMALIZES condition codes: Creates one row per condition-code combination
-- - Aggregates body sites, categories, evidence, notes, and stages as JSON arrays/objects
-- - Constructs JSON structures for complex nested data (excluding codes)
-- - Sanitizes text data using REGEXP_REPLACE to ensure valid JSON
-- - Handles null/empty cases gracefully across all aggregations
-- - Adds code_rank column to identify primary (1) vs secondary (2+) codes
--
-- FILTERING:
-- - Includes all conditions regardless of status
-- - Uses LEFT JOINs to preserve conditions without related data
--
-- OUTPUT COLUMNS:
-- - All core condition fields from conditions table
-- - code_system, code_code, code_display, code_text: Individual denormalized code columns
-- - code_rank: Ranking of codes (1=primary, 2+=secondary) based on coding system priority
-- - body_sites: JSON array of affected body sites
-- - categories: JSON array of condition categories
-- - evidence: JSON array of supporting evidence
-- - notes: JSON array of clinical notes
-- - stages: JSON object with staging information
--
-- PERFORMANCE CONSIDERATIONS:
-- - DENORMALIZED STRUCTURE: ~5.9M rows (one per condition-code combination) vs 3.2M conditions
-- - Uses LISTAGG for JSON aggregation (body sites, categories, evidence, notes, stages)
-- - Groups by all condition fields AND code fields due to denormalization
-- - Materialized view provides fast query performance for code-specific analysis
-- - JSON_PARSE converts strings to SUPER type for efficient querying
-- - ROW_NUMBER() window function for code ranking adds minimal overhead
--
-- USAGE:
-- This view is designed for:
-- - Condition prevalence analysis by specific diagnostic codes
-- - Clinical reporting and dashboards with code-level detail
-- - Quality measure calculations requiring specific ICD-10/SNOMED codes
-- - Population health analytics with code granularity
-- - ETL downstream processing for code-specific workflows
-- - Primary vs secondary code analysis using code_rank column
--
-- ===================================================================
CREATE MATERIALIZED VIEW fact_fhir_conditions_view_v1
AUTO REFRESH NO
AS WITH aggregated_body_sites AS (
    SELECT
        cb.condition_id,
        JSON_PARSE(
            '[' || LISTAGG(
                DISTINCT '{' || '"system":"' || COALESCE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(cb.body_site_system, '[\r\n\t]', ''),
                        '"',
                        ''
                    ),
                    ''
                ) || '",' || '"code":"' || COALESCE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(cb.body_site_code, '[\r\n\t]', ''),
                        '"',
                        ''
                    ),
                    ''
                ) || '",' || '"display":"' || COALESCE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(cb.body_site_display, '[\r\n\t]', ''),
                        '"',
                        ''
                    ),
                    ''
                ) || '",' || '"text":"' || COALESCE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(cb.body_site_text, '[\r\n\t]', ''),
                        '"',
                        ''
                    ),
                    ''
                ) || '"' || '}',
                ','
            ) WITHIN GROUP (
                ORDER BY
                    cb.body_site_system,
                    cb.body_site_code
            ) || ']'
        ) AS body_sites
    FROM
        public.condition_body_sites cb
    WHERE
        cb.body_site_code IS NOT NULL
    GROUP BY
        cb.condition_id
),
aggregated_categories AS (
    SELECT
        cat.condition_id,
        JSON_PARSE(
            '[' || LISTAGG(
                DISTINCT '{' || '"system":"' || COALESCE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(cat.category_system, '[\r\n\t]', ''),
                        '"',
                        ''
                    ),
                    ''
                ) || '",' || '"code":"' || COALESCE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(cat.category_code, '[\r\n\t]', ''),
                        '"',
                        ''
                    ),
                    ''
                ) || '",' || '"display":"' || COALESCE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(cat.category_display, '[\r\n\t]', ''),
                        '"',
                        ''
                    ),
                    ''
                ) || '",' || '"text":"' || COALESCE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(cat.category_text, '[\r\n\t]', ''),
                        '"',
                        ''
                    ),
                    ''
                ) || '"' || '}',
                ','
            ) WITHIN GROUP (
                ORDER BY
                    cat.category_system,
                    cat.category_code
            ) || ']'
        ) AS categories
    FROM
        public.condition_categories cat
    WHERE
        cat.category_code IS NOT NULL
    GROUP BY
        cat.condition_id
),
aggregated_evidence AS (
    SELECT
        ce.condition_id,
        JSON_PARSE(
            '[' || LISTAGG(
                DISTINCT '{' || '"system":"' || COALESCE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(ce.evidence_system, '[\r\n\t]', ''),
                        '"',
                        ''
                    ),
                    ''
                ) || '",' || '"code":"' || COALESCE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(ce.evidence_code, '[\r\n\t]', ''),
                        '"',
                        ''
                    ),
                    ''
                ) || '",' || '"display":"' || COALESCE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(ce.evidence_display, '[\r\n\t]', ''),
                        '"',
                        ''
                    ),
                    ''
                ) || '",' || '"detail_reference":"' || COALESCE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(ce.evidence_detail_reference, '[\r\n\t]', ''),
                        '"',
                        ''
                    ),
                    ''
                ) || '"' || '}',
                ','
            ) WITHIN GROUP (
                ORDER BY
                    ce.evidence_system,
                    ce.evidence_code
            ) || ']'
        ) AS evidence
    FROM
        public.condition_evidence ce
    WHERE
        ce.evidence_code IS NOT NULL
    GROUP BY
        ce.condition_id
),
aggregated_notes AS (
    SELECT
        cn.condition_id,
        JSON_PARSE(
            '[' || LISTAGG(
                '{' || '"text":"' || COALESCE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(cn.note_text, '[\r\n\t]', ''),
                            '"',
                            ''
                        ),
                        '[\\\\]',
                        '\\\\\\\\'
                    ),
                    ''
                ) || '",' || '"author_reference":"' || COALESCE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(cn.note_author_reference, '[\r\n\t]', ''),
                        '"',
                        ''
                    ),
                    ''
                ) || '",' || '"time":"' || COALESCE(cn.note_time:: VARCHAR, '') || '"' || '}',
                ','
            ) WITHIN GROUP (
                ORDER BY
                    cn.note_time
            ) || ']'
        ) AS notes
    FROM
        public.condition_notes cn
    WHERE
        cn.note_text IS NOT NULL
    GROUP BY
        cn.condition_id
),
aggregated_stages AS (
    SELECT
        cs.condition_id,
        JSON_PARSE(
            '{' || '"summary":{' || '"system":"' || COALESCE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(MAX(cs.stage_summary_system), '[\r\n\t]', ''),
                    '"',
                    ''
                ),
                ''
            ) || '",' || '"code":"' || COALESCE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(MAX(cs.stage_summary_code), '[\r\n\t]', ''),
                    '"',
                    ''
                ),
                ''
            ) || '",' || '"display":"' || COALESCE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(MAX(cs.stage_summary_display), '[\r\n\t]', ''),
                    '"',
                    ''
                ),
                ''
            ) || '"' || '},' || '"assessment":{' || '"system":"' || COALESCE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(MAX(cs.stage_assessment_system), '[\r\n\t]', ''),
                    '"',
                    ''
                ),
                ''
            ) || '",' || '"code":"' || COALESCE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(MAX(cs.stage_assessment_code), '[\r\n\t]', ''),
                    '"',
                    ''
                ),
                ''
            ) || '",' || '"display":"' || COALESCE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(MAX(cs.stage_assessment_display), '[\r\n\t]', ''),
                    '"',
                    ''
                ),
                ''
            ) || '"' || '},' || '"type":{' || '"system":"' || COALESCE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(MAX(cs.stage_type_system), '[\r\n\t]', ''),
                    '"',
                    ''
                ),
                ''
            ) || '",' || '"code":"' || COALESCE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(MAX(cs.stage_type_code), '[\r\n\t]', ''),
                    '"',
                    ''
                ),
                ''
            ) || '",' || '"display":"' || COALESCE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(MAX(cs.stage_type_display), '[\r\n\t]', ''),
                    '"',
                    ''
                ),
                ''
            ) || '"' || '}' || '}'
        ) AS stages
    FROM
        public.condition_stages cs
    GROUP BY
        cs.condition_id
)
SELECT
    -- CORE CONDITION DATA
    -- ============================================
    c.condition_id,
    -- Unique condition identifier (Primary Key)
    c.patient_id,
    -- Patient reference
    c.encounter_id,
    -- Associated encounter (if any)
    -- Clinical Status Fields
    c.clinical_status_code,
    -- active | inactive | resolved
    c.clinical_status_display,
    c.clinical_status_system,
    -- Verification Status Fields
    c.verification_status_code,
    -- confirmed | provisional | differential | refuted
    c.verification_status_display,
    c.verification_status_system,
    -- Condition Description
    c.condition_text,
    -- Human-readable condition description
    -- Severity Information
    c.severity_code,
    c.severity_display,
    c.severity_system,
    -- Onset Information (multiple data types supported)
    c.onset_datetime,
    c.onset_age_value,
    c.onset_age_unit,
    c.onset_period_start,
    c.onset_period_end,
    c.onset_text,
    -- Abatement Information (resolution/remission)
    c.abatement_datetime,
    c.abatement_age_value,
    c.abatement_age_unit,
    c.abatement_period_start,
    c.abatement_period_end,
    c.abatement_text,
    c.abatement_boolean,
    -- Recording Information
    c.recorded_date,
    c.recorder_type,
    c.recorder_id,
    c.asserter_type,
    c.asserter_id,
    -- FHIR Metadata
    c.meta_last_updated,
    c.meta_source,
    c.meta_profile,
    c.meta_security,
    c.meta_tag,
    -- ETL Audit Fields
    c.created_at AS etl_created_at,
    c.updated_at AS etl_updated_at,
    -- CONDITION CODES (DENORMALIZED COLUMNS)
    -- Individual code columns from condition_codes table
    -- This will create one row per condition-code combination
    cc.code_system,
    cc.code_code,
    cc.code_display,
    cc.code_text,
    -- Code ranking to identify primary codes (1 = primary, 2+ = secondary)
    ROW_NUMBER() OVER (
        PARTITION BY c.condition_id
        ORDER BY
            CASE cc.code_system
            WHEN 'http://hl7.org/fhir/sid/icd-10-cm' THEN 1
            WHEN 'http://hl7.org/fhir/sid/icd-10' THEN 2
            WHEN 'http://snomed.info/sct' THEN 3
            ELSE 4 END,
            cc.code_code
    ) AS code_rank,
    -- BODY SITES (AGGREGATED AS JSON)
    -- ============================================
    abs.body_sites,
    -- CATEGORIES (AGGREGATED AS JSON)
    -- ============================================
    acat.categories,
    -- EVIDENCE (AGGREGATED AS JSON)
    -- ============================================
    ae.evidence,
    -- NOTES (AGGREGATED AS JSON)
    -- ============================================
    an.notes,
    -- STAGES (AGGREGATED AS JSON)
    -- ============================================
    ast.stages,
    -- CALCULATED FIELDS
    -- Calculate condition duration if both onset and abatement dates exist
    CASE
    WHEN c.onset_datetime IS NOT NULL
    AND c.abatement_datetime IS NOT NULL THEN DATEDIFF(day, c.onset_datetime, c.abatement_datetime)
    WHEN c.onset_period_start IS NOT NULL
    AND c.abatement_period_start IS NOT NULL THEN DATEDIFF(
        day,
        c.onset_period_start,
        c.abatement_period_start
    )
    ELSE NULL END AS condition_duration_days,
    -- Determine if condition is currently active
    CASE
    WHEN c.clinical_status_code = 'active'
    AND (
        c.abatement_datetime IS NULL
        OR c.abatement_datetime > CURRENT_DATE
    ) THEN TRUE
    ELSE FALSE END AS is_active
FROM
    public.conditions c
    LEFT JOIN public.condition_codes cc ON c.condition_id = cc.condition_id
    LEFT JOIN aggregated_body_sites abs ON c.condition_id = abs.condition_id
    LEFT JOIN aggregated_categories acat ON c.condition_id = acat.condition_id
    LEFT JOIN aggregated_evidence ae ON c.condition_id = ae.condition_id
    LEFT JOIN aggregated_notes an ON c.condition_id = an.condition_id
    LEFT JOIN aggregated_stages ast ON c.condition_id = ast.condition_id;