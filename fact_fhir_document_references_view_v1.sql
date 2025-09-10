-- ===================================================================
-- FACT FHIR DOCUMENT REFERENCES VIEW V1
-- ===================================================================
-- 
-- OVERVIEW:
-- Comprehensive materialized view that creates a fact table from FHIR DocumentReference resources
-- by joining the main document_references table with all related document reference tables for analytics and reporting.
-- 
-- PRIMARY KEY: document_reference_id
-- 
-- SOURCE TABLES:
-- - public.document_references: Core document reference data including status, type, and dates
-- - public.document_reference_content: Document content metadata with attachment info
-- - public.document_reference_authors: Document authors and contributors
-- - public.document_reference_categories: Document category classifications
-- - public.document_reference_identifiers: Document identifiers and references
-- 
-- REFRESH STRATEGY:
-- - AUTO REFRESH NO: Manual refresh required via scheduled jobs
-- - BACKUP NO: No backup required for this materialized view
-- 
-- DATA PROCESSING:
-- - Aggregates multiple authors, categories, and identifiers per document reference
-- - Constructs JSON structures for complex nested data
-- - Sanitizes text data using REGEXP_REPLACE to ensure valid JSON
-- - Handles null/empty cases gracefully across all aggregations
-- 
-- FILTERING:
-- - Excludes documents with status 'entered-in-error' or 'superseded'
-- - Uses LEFT JOINs to preserve document references without related data
-- 
-- OUTPUT COLUMNS:
-- - All core document reference fields from document_references table
-- - content: JSON array of document content attachments
-- - authors: JSON array of document authors with IDs
-- - categories: JSON array of document categories with codes and display values
-- - identifiers: JSON array of document identifiers with system and value
-- 
-- PERFORMANCE CONSIDERATIONS:
-- - Uses CTEs to separate LISTAGG operations from COUNT DISTINCT operations (Redshift requirement)
-- - Groups by all document reference fields to aggregate related data
-- - Materialized view provides fast query performance
-- - JSON_PARSE converts strings to SUPER type for efficient querying
-- 
-- USAGE:
-- This view is designed for:
-- - Document management and tracking analysis
-- - Clinical documentation reporting and dashboards
-- - Document workflow and status monitoring
-- - Content management analytics
-- - ETL downstream processing
-- 
-- ===================================================================

CREATE MATERIALIZED VIEW fact_fhir_document_references_view_v1
BACKUP NO
AUTO REFRESH NO
AS
WITH aggregated_content AS (
    SELECT 
        drc.document_reference_id,
        JSON_PARSE(
            '[' || LISTAGG(DISTINCT
                '{' ||
                '"contentType":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(drc.attachment_content_type, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"url":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(drc.attachment_url, '[\r\n\t]', ''), '"', ''), '') || '"' ||
                '}',
                ','
            ) WITHIN GROUP (ORDER BY drc.attachment_content_type) || ']'
        ) AS content,
        COUNT(DISTINCT drc.attachment_content_type) AS content_count
    FROM public.document_reference_content drc
    GROUP BY drc.document_reference_id
),
aggregated_authors AS (
    SELECT 
        dra.document_reference_id,
        JSON_PARSE(
            '[' || LISTAGG(DISTINCT
                '{' ||
                '"authorId":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(dra.author_id, '[\r\n\t]', ''), '"', ''), '') || '"' ||
                '}',
                ','
            ) WITHIN GROUP (ORDER BY dra.author_id) || ']'
        ) AS authors,
        COUNT(DISTINCT dra.author_id) AS author_count
    FROM public.document_reference_authors dra
    GROUP BY dra.document_reference_id
),
aggregated_categories AS (
    SELECT 
        drcat.document_reference_id,
        JSON_PARSE(
            '[' || LISTAGG(DISTINCT
                '{' ||
                '"system":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(drcat.category_system, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"code":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(drcat.category_code, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"display":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(drcat.category_display, '[\r\n\t]', ''), '"', ''), '') || '"' ||
                '}',
                ','
            ) WITHIN GROUP (ORDER BY drcat.category_system, drcat.category_code) || ']'
        ) AS categories,
        COUNT(DISTINCT drcat.category_code) AS category_count
    FROM public.document_reference_categories drcat
    GROUP BY drcat.document_reference_id
),
aggregated_identifiers AS (
    SELECT 
        dri.document_reference_id,
        JSON_PARSE(
            '[' || LISTAGG(DISTINCT
                '{' ||
                '"system":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(dri.identifier_system, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"value":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(dri.identifier_value, '[\r\n\t]', ''), '"', ''), '') || '"' ||
                '}',
                ','
            ) WITHIN GROUP (ORDER BY dri.identifier_system, dri.identifier_value) || ']'
        ) AS identifiers,
        COUNT(DISTINCT dri.identifier_value) AS identifier_count
    FROM public.document_reference_identifiers dri
    GROUP BY dri.document_reference_id
)
SELECT 
    -- ============================================
    -- CORE DOCUMENT REFERENCE DATA
    -- ============================================
    dr.document_reference_id,            -- Unique document reference identifier (Primary Key)
    dr.patient_id,                       -- Patient reference
    
    -- Document Status and Type
    dr.status,                           -- Document status (current | superseded | entered-in-error)
    dr.type_code,                        -- Document type code
    dr.type_system,                      -- Document type system (e.g., LOINC)
    dr.type_display,                     -- Document type display name
    
    -- Document Metadata
    dr.date,                             -- Document creation/last modified date
    dr.custodian_id,                     -- Organization that maintains the document
    dr.description,                      -- Human-readable description of document
    
    -- Context Period
    dr.context_period_start,             -- Start of the service period documented
    dr.context_period_end,               -- End of the service period documented
    
    -- FHIR Metadata
    dr.meta_version_id,                  -- FHIR resource version
    dr.meta_last_updated,                -- Last updated timestamp
    
    -- ETL Audit Fields
    dr.created_at,
    dr.updated_at,
    
    -- ============================================
    -- CONTENT ATTACHMENTS (FROM CTE)
    -- ============================================
    ac.content,
    
    -- ============================================
    -- AUTHORS (FROM CTE)
    -- ============================================
    aa.authors,
    
    -- ============================================
    -- CATEGORIES (FROM CTE)
    -- ============================================
    acat.categories,
    
    -- ============================================
    -- IDENTIFIERS (FROM CTE)
    -- ============================================
    ai.identifiers,
    
    -- ============================================
    -- CALCULATED FIELDS
    -- ============================================
    -- Calculate document age in days
    CASE 
        WHEN dr.date IS NOT NULL 
        THEN DATEDIFF(day, dr.date, CURRENT_DATE)
        ELSE NULL 
    END AS document_age_days,
    
    -- Count of authors (from CTE)
    COALESCE(aa.author_count, 0) AS author_count,
    
    -- Count of categories (from CTE)
    COALESCE(acat.category_count, 0) AS category_count,
    
    -- Determine if document has content attachments (from CTE)
    CASE 
        WHEN COALESCE(ac.content_count, 0) > 0 
        THEN TRUE
        ELSE FALSE
    END AS has_content,
    
    -- Count of identifiers (from CTE)
    COALESCE(ai.identifier_count, 0) AS identifier_count

FROM public.document_references dr
    LEFT JOIN aggregated_content ac ON dr.document_reference_id = ac.document_reference_id
    LEFT JOIN aggregated_authors aa ON dr.document_reference_id = aa.document_reference_id
    LEFT JOIN aggregated_categories acat ON dr.document_reference_id = acat.document_reference_id
    LEFT JOIN aggregated_identifiers ai ON dr.document_reference_id = ai.document_reference_id

WHERE dr.status NOT IN ('entered-in-error', 'superseded');

-- ===================================================================
-- REFRESH CONFIGURATION
-- ===================================================================
-- This materialized view is configured with AUTO REFRESH NO
-- Manual refresh will be scheduled via AWS Lambda or Airflow
-- Refresh frequency should align with source data update patterns
-- 
-- To manually refresh:
-- REFRESH MATERIALIZED VIEW fact_fhir_document_references_view_v1;
-- ===================================================================

-- ===================================================================
-- INDEXES AND OPTIMIZATION
-- ===================================================================
-- Redshift automatically creates and maintains sort keys and distribution keys
-- based on query patterns. Monitor query performance and adjust if needed:
-- - Consider DISTKEY on patient_id for patient-centric queries
-- - Consider SORTKEY on (patient_id, date) for temporal analysis
-- ===================================================================

-- ===================================================================
-- DATA QUALITY NOTES
-- ===================================================================
-- 1. Documents without content attachments will have NULL in content field
-- 2. Some documents may not have authors identified
-- 3. Category information may be incomplete for older documents
-- 4. Identifier systems vary by document source
-- 5. All text fields are sanitized to ensure valid JSON format
-- 6. Filtered to exclude entered-in-error and superseded documents
-- ===================================================================