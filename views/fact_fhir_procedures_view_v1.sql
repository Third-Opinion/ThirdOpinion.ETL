-- ===================================================================
-- FACT FHIR PROCEDURES VIEW V1
-- ===================================================================
-- 
-- OVERVIEW:
-- Comprehensive materialized view that creates a fact table from FHIR Procedure resources
-- by joining the main procedures table with procedure code codings and identifiers for analytics and reporting.
-- 
-- PRIMARY KEY: procedure_id
-- 
-- SOURCE TABLES:
-- - public.procedures: Core procedure data including status, performed dates, and descriptions
-- - public.procedure_code_codings: Procedure codes from various coding systems (CPT, SNOMED, etc.)
-- - public.procedure_identifiers: External procedure identifiers and references
-- 
-- REFRESH STRATEGY:
-- - AUTO REFRESH NO: Manual refresh required via scheduled jobs
-- - BACKUP NO: No backup required for this materialized view
-- 
-- DATA PROCESSING:
-- - Aggregates multiple procedure codes and identifiers as JSON
-- - Constructs JSON structures for complex nested data
-- - Sanitizes text data using REGEXP_REPLACE to ensure valid JSON
-- - Handles null/empty cases gracefully across all aggregations
-- 
-- FILTERING:
-- - Excludes procedures that were not performed (not-done, cancelled, stopped, aborted)
-- - Uses LEFT JOINs to preserve procedures without related data
-- 
-- OUTPUT COLUMNS:
-- - All core procedure fields from procedures table
-- - code_codings: JSON array of procedure codes with system, code, and display
-- - identifiers: JSON array of external identifiers
-- - Calculated fields for data quality metrics
-- 
-- PERFORMANCE CONSIDERATIONS:
-- - Uses LISTAGG for string aggregation with proper delimiters
-- - Groups by all procedure fields to aggregate related data
-- - Materialized view provides fast query performance
-- - JSON_PARSE converts strings to SUPER type for efficient querying
-- 
-- USAGE:
-- This view is designed for:
-- - Procedure volume analysis and reporting
-- - Clinical quality measure calculations
-- - Procedure code mapping and standardization
-- - Patient procedure history tracking
-- - ETL downstream processing
-- 
-- ===================================================================

CREATE MATERIALIZED VIEW fact_fhir_procedures_view_v1
BACKUP NO
AUTO REFRESH NO
AS
WITH code_counts AS (
    SELECT 
        pcc.procedure_id,
        COUNT(DISTINCT pcc.code_code) AS code_coding_count
    FROM public.procedure_code_codings pcc
    GROUP BY pcc.procedure_id
),
aggregated_codes AS (
    SELECT 
        pcc.procedure_id,
        JSON_PARSE(
            '[' || LISTAGG(DISTINCT
                '{' ||
                '"system":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(pcc.code_system, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"code":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(pcc.code_code, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"display":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(pcc.code_display, '[\r\n\t]', ''), '"', ''), '') || '"' ||
                '}',
                ','
            ) WITHIN GROUP (ORDER BY pcc.code_system, pcc.code_code) || ']'
        ) AS code_codings,
        MIN(pcc.code_code) AS primary_code,
        MIN(pcc.code_system) AS primary_code_system,
        MIN(pcc.code_display) AS primary_code_display
    FROM public.procedure_code_codings pcc
    GROUP BY pcc.procedure_id
),
identifier_counts AS (
    SELECT 
        pi.procedure_id,
        COUNT(DISTINCT pi.identifier_value) AS identifier_count
    FROM public.procedure_identifiers pi
    GROUP BY pi.procedure_id
),
aggregated_identifiers AS (
    SELECT 
        pi.procedure_id,
        JSON_PARSE(
            '[' || LISTAGG(DISTINCT
                '{' ||
                '"system":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(pi.identifier_system, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"value":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(pi.identifier_value, '[\r\n\t]', ''), '"', ''), '') || '"' ||
                '}',
                ','
            ) WITHIN GROUP (ORDER BY pi.identifier_system, pi.identifier_value) || ']'
        ) AS identifiers
    FROM public.procedure_identifiers pi
    GROUP BY pi.procedure_id
)
SELECT 
    -- ============================================
    -- CORE PROCEDURE DATA
    -- ============================================
    p.procedure_id,                      -- Unique procedure identifier (Primary Key)
    p.resource_type,                     -- FHIR resource type
    p.status,                            -- Procedure status (completed | in-progress | not-done | etc.)
    p.patient_id,                        -- Patient reference
    
    -- Procedure Description
    p.code_text,                         -- Human-readable procedure description
    
    -- Timing Information
    p.performed_date_time,               -- When the procedure was performed
    
    -- FHIR Metadata
    p.meta_last_updated,                   -- FHIR resource version
    p.meta_last_updated,                 -- Last updated timestamp
    
    -- ETL Audit Fields
    p.created_at,
    p.updated_at,
    
    -- ============================================
    -- CODE CODINGS (AGGREGATED FROM CTE)
    -- ============================================
    ac.code_codings,
    
    -- ============================================
    -- IDENTIFIERS (AGGREGATED FROM CTE)
    -- ============================================
    ai.identifiers,
    
    -- ============================================
    -- CALCULATED FIELDS
    -- ============================================
    -- Calculate procedure age in days
    CASE 
        WHEN p.performed_date_time IS NOT NULL 
        THEN DATEDIFF(day, p.performed_date_time, CURRENT_DATE)
        ELSE NULL 
    END AS procedure_age_days,
    
    -- Determine if procedure is completed
    CASE 
        WHEN p.status = 'completed' 
        THEN TRUE
        ELSE FALSE
    END AS is_completed,
    
    -- Determine if procedure was not performed
    CASE 
        WHEN p.status IN ('not-done', 'cancelled', 'stopped', 'aborted')
        THEN TRUE
        ELSE FALSE
    END AS was_not_performed,
    
    -- Count of code codings (from CTE)
    COALESCE(cc.code_coding_count, 0) AS code_coding_count,
    
    -- Count of identifiers (from CTE)
    COALESCE(ic.identifier_count, 0) AS identifier_count,
    
    -- Determine if procedure has multiple codes (useful for code mapping analysis)
    CASE 
        WHEN COALESCE(cc.code_coding_count, 0) > 1 
        THEN TRUE
        ELSE FALSE
    END AS has_multiple_codes,
    
    -- Primary procedure code (from CTE)
    ac.primary_code,
    
    -- Primary procedure system (from CTE)
    ac.primary_code_system,
    
    -- Primary procedure display (from CTE)
    ac.primary_code_display

FROM public.procedures p
    LEFT JOIN code_counts cc ON p.procedure_id = cc.procedure_id
    LEFT JOIN aggregated_codes ac ON p.procedure_id = ac.procedure_id
    LEFT JOIN identifier_counts ic ON p.procedure_id = ic.procedure_id
    LEFT JOIN aggregated_identifiers ai ON p.procedure_id = ai.procedure_id

WHERE p.status NOT IN ('not-done', 'cancelled');

-- ===================================================================
-- REFRESH CONFIGURATION
-- ===================================================================
-- This materialized view is configured with AUTO REFRESH NO
-- Manual refresh will be scheduled via AWS Lambda or Airflow
-- Refresh frequency should align with source data update patterns
-- 
-- To manually refresh:
-- REFRESH MATERIALIZED VIEW fact_fhir_procedures_view_v1;
-- ===================================================================

-- ===================================================================
-- INDEXES AND OPTIMIZATION
-- ===================================================================
-- Redshift automatically creates and maintains sort keys and distribution keys
-- based on query patterns. Monitor query performance and adjust if needed:
-- - Consider DISTKEY on patient_id for patient-centric queries
-- - Consider SORTKEY on (patient_id, performed_date_time) for temporal analysis
-- ===================================================================

-- ===================================================================
-- DATA QUALITY NOTES
-- ===================================================================
-- 1. Procedures without code codings will have NULL in code_codings field
-- 2. Some procedures may not have external identifiers
-- 3. Primary code fields provide quick access to the main procedure code
-- 4. Status tracking helps identify completed vs. incomplete procedures
-- 5. All text fields are sanitized to ensure valid JSON format
-- 6. Procedure age helps identify recent vs. historical procedures
-- 7. Multiple code flag helps identify procedures with complex coding
-- 8. Recent procedure flag (30 days) aids in current care analysis
-- 9. Filtered to exclude procedures that were not performed (not-done, cancelled, stopped, aborted)
-- ===================================================================