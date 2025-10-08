-- ===================================================================
-- FACT FHIR DIAGNOSTIC REPORTS VIEW V1
-- 
-- OVERVIEW:
-- Comprehensive materialized view that creates a fact table from FHIR DiagnosticReport resources
-- by joining the main diagnostic_reports table with all related tables for analytics and reporting.
-- 
-- PRIMARY KEY: diagnostic_report_id
-- 
-- SOURCE TABLES:
-- - public.diagnostic_reports: Core diagnostic report data
-- - public.diagnostic_report_results: Links to observation results
-- - public.diagnostic_report_performers: Performing practitioners/organizations
-- - public.diagnostic_report_categories: Report categorization
-- - public.diagnostic_report_media: Associated media/images
-- - public.diagnostic_report_presented_forms: Report documents/attachments
-- - public.diagnostic_report_based_on: Source service requests
-- 
-- REFRESH STRATEGY:
-- - AUTO REFRESH NO: Manual refresh required via scheduled jobs
-- - BACKUP NO: No backup required for this materialized view
-- 
-- DATA PROCESSING:
-- - Aggregates multiple results, performers, and categories per report
-- - Constructs JSON structures for complex nested data
-- - Sanitizes text data using REGEXP_REPLACE to ensure valid JSON
-- - Handles null/empty cases gracefully across all aggregations
-- 
-- FILTERING:
-- - Includes all diagnostic reports regardless of status
-- - Uses LEFT JOINs to preserve reports without related data
-- 
-- OUTPUT COLUMNS:
-- - All core diagnostic report fields from diagnostic_reports table
-- - results: JSON array of observation IDs linked to this report
-- - performers: JSON array of performing practitioners/organizations
-- - categories: JSON array of report categories
-- - media: JSON array of associated media references
-- - presented_forms: JSON array of report documents
-- - based_on: JSON array of source service requests
-- 
-- PERFORMANCE CONSIDERATIONS:
-- - Uses LISTAGG for string aggregation with proper delimiters
-- - Groups by all diagnostic report fields to aggregate related data
-- - Materialized view provides fast query performance
-- - JSON_PARSE converts strings to SUPER type for efficient querying
-- 
-- USAGE:
-- This view is designed for:
-- - Lab result analysis and reporting
-- - Radiology report analytics
-- - Clinical decision support
-- - Quality measure calculations
-- - ETL downstream processing
-- 
-- ===================================================================

CREATE MATERIALIZED VIEW fact_fhir_diagnostic_reports_view_v1
AUTO REFRESH NO
AS
WITH result_counts AS (
    SELECT 
        drr.diagnostic_report_id,
        COUNT(DISTINCT drr.observation_id) AS result_count
    FROM public.diagnostic_report_results drr
    GROUP BY drr.diagnostic_report_id
),
aggregated_results AS (
    SELECT 
        drr.diagnostic_report_id,
        JSON_PARSE(
            '[' || LISTAGG(DISTINCT
                '"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(drr.observation_id, '[\r\n\t]', ''), '"', ''), '') || '"',
                ','
            ) WITHIN GROUP (ORDER BY drr.observation_id) || ']'
        ) AS results
    FROM public.diagnostic_report_results drr
    GROUP BY drr.diagnostic_report_id
),
aggregated_performers AS (
    SELECT 
        drp.diagnostic_report_id,
        JSON_PARSE(
            '[' || LISTAGG(DISTINCT
                '{' ||
                '"type":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(drp.performer_type, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"id":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(drp.performer_id, '[\r\n\t]', ''), '"', ''), '') || '"' ||
                '}',
                ','
            ) WITHIN GROUP (ORDER BY drp.performer_id) || ']'
        ) AS performers
    FROM public.diagnostic_report_performers drp
    GROUP BY drp.diagnostic_report_id
),
aggregated_categories AS (
    SELECT 
        drc.diagnostic_report_id,
        JSON_PARSE(
            '[' || LISTAGG(DISTINCT
                '{' ||
                '"system":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(drc.category_system, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"code":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(drc.category_code, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"display":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(drc.category_display, '[\r\n\t]', ''), '"', ''), '') || '"' ||
                '}',
                ','
            ) WITHIN GROUP (ORDER BY drc.category_code) || ']'
        ) AS categories
    FROM public.diagnostic_report_categories drc
    GROUP BY drc.diagnostic_report_id
),
aggregated_media AS (
    SELECT 
        drm.diagnostic_report_id,
        JSON_PARSE(
            '[' || LISTAGG(DISTINCT
                '"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(drm.media_id, '[\r\n\t]', ''), '"', ''), '') || '"',
                ','
            ) WITHIN GROUP (ORDER BY drm.media_id) || ']'
        ) AS media
    FROM public.diagnostic_report_media drm
    GROUP BY drm.diagnostic_report_id
),
aggregated_forms AS (
    SELECT 
        drpf.diagnostic_report_id,
        JSON_PARSE(
            '[' || LISTAGG(
                '{' ||
                '"content_type":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(drpf.content_type, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"title":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(drpf.title, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"data":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(drpf.data, '[\r\n\t]', ''), '"', ''), '') || '"' ||
                '}',
                ','
            ) WITHIN GROUP (ORDER BY drpf.title) || ']'
        ) AS presented_forms
    FROM public.diagnostic_report_presented_forms drpf
    GROUP BY drpf.diagnostic_report_id
),
aggregated_based_on AS (
    SELECT 
        drb.diagnostic_report_id,
        JSON_PARSE(
            '[' || LISTAGG(DISTINCT
                '"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(drb.service_request_id, '[\r\n\t]', ''), '"', ''), '') || '"',
                ','
            ) WITHIN GROUP (ORDER BY drb.service_request_id) || ']'
        ) AS based_on
    FROM public.diagnostic_report_based_on drb
    GROUP BY drb.diagnostic_report_id
)
SELECT 
    -- CORE DIAGNOSTIC REPORT DATA
    -- ============================================
    dr.diagnostic_report_id,              -- Unique report identifier (Primary Key)
    dr.resource_type,                     -- Resource type (always 'DiagnosticReport')
    dr.status,                            -- registered | partial | preliminary | final | amended | corrected | appended | cancelled | entered-in-error
    
    -- Report Dates
    dr.effective_datetime,                -- When the observation was made
    dr.issued_datetime,                   -- When the report was issued
    
    -- Report Code Information
    dr.code_text,                         -- Human-readable report name
    dr.code_primary_code,                 -- Primary diagnostic code (e.g., LOINC)
    dr.code_primary_system,               -- Code system (e.g., http://loinc.org)
    dr.code_primary_display,              -- Display name for the code
    
    -- References
    dr.patient_id,                        -- Patient reference
    dr.encounter_id,                      -- Associated encounter (if any)
    
    -- Metadata
    dr.meta_last_updated,                   -- Version identifier
    dr.extensions,                        -- FHIR extensions as JSON string
    
    -- ETL Audit Fields
    dr.created_at AS etl_created_at,
    dr.updated_at AS etl_updated_at,
    -- AGGREGATED DATA FROM CTEs
    -- ============================================
    ar.results,
    COALESCE(rc.result_count, 0) AS result_count,
    ap.performers,
    ac.categories,
    am.media,
    af.presented_forms,
    ab.based_on,
    -- CALCULATED FIELDS
    -- Calculate report age in days
    CASE
        WHEN dr.issued_datetime IS NOT NULL
        THEN DATEDIFF(day, dr.issued_datetime, CURRENT_DATE)
        ELSE NULL
    END AS report_age_days,
    
    -- Determine if report is final
    CASE 
        WHEN dr.status IN ('final', 'amended', 'corrected', 'appended')
        THEN TRUE
        ELSE FALSE
    END AS is_final,
    
    -- Determine if report has results
    CASE 
        WHEN COALESCE(rc.result_count, 0) > 0
        THEN TRUE
        ELSE FALSE
    END AS has_results

FROM public.diagnostic_reports dr
    LEFT JOIN result_counts rc ON dr.diagnostic_report_id = rc.diagnostic_report_id
    LEFT JOIN aggregated_results ar ON dr.diagnostic_report_id = ar.diagnostic_report_id
    LEFT JOIN aggregated_performers ap ON dr.diagnostic_report_id = ap.diagnostic_report_id
    LEFT JOIN aggregated_categories ac ON dr.diagnostic_report_id = ac.diagnostic_report_id
    LEFT JOIN aggregated_media am ON dr.diagnostic_report_id = am.diagnostic_report_id
    LEFT JOIN aggregated_forms af ON dr.diagnostic_report_id = af.diagnostic_report_id
    LEFT JOIN aggregated_based_on ab ON dr.diagnostic_report_id = ab.diagnostic_report_id;
-- REFRESH CONFIGURATION
-- This materialized view is configured with AUTO REFRESH NO
-- Manual refresh will be scheduled via AWS Lambda or Airflow
-- Refresh frequency should align with source data update patterns
-- 
-- To manually refresh:
-- REFRESH MATERIALIZED VIEW fact_fhir_diagnostic_reports_view_v1;
-- ===================================================================
-- INDEXES AND OPTIMIZATION
-- Redshift automatically creates and maintains sort keys and distribution keys
-- based on query patterns. Monitor query performance and adjust if needed:
-- - Consider DISTKEY on patient_id for patient-centric queries
-- - Consider SORTKEY on (patient_id, issued_datetime) for temporal analysis
-- ===================================================================
-- DATA QUALITY NOTES
-- 1. Reports without results will have NULL in results field but has_results = FALSE
-- 2. Some reports may not have associated encounters (outpatient labs)
-- 3. Performer information may reference practitioners or organizations
-- 4. Media references require separate lookup in media table for actual content
-- 5. Presented forms may contain base64 encoded data in the data field
-- 6. Status values follow FHIR DiagnosticReport status value set
-- ===================================================================