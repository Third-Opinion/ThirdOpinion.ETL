CREATE VIEW fact_fhir_document_references_view_v1
AS
WITH content_counts AS (
    SELECT 
        drc.document_reference_id,
        COUNT(DISTINCT drc.attachment_content_type) AS content_count
    FROM public.document_reference_content drc
    GROUP BY drc.document_reference_id
),
aggregated_content AS (
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
        ) AS content
    FROM public.document_reference_content drc
    GROUP BY drc.document_reference_id
),
author_counts AS (
    SELECT 
        dra.document_reference_id,
        COUNT(DISTINCT dra.author_id) AS author_count
    FROM public.document_reference_authors dra
    GROUP BY dra.document_reference_id
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
        ) AS authors
    FROM public.document_reference_authors dra
    GROUP BY dra.document_reference_id
),
category_counts AS (
    SELECT 
        drcat.document_reference_id,
        COUNT(DISTINCT drcat.category_code) AS category_count
    FROM public.document_reference_categories drcat
    GROUP BY drcat.document_reference_id
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
        ) AS categories
    FROM public.document_reference_categories drcat
    GROUP BY drcat.document_reference_id
),
identifier_counts AS (
    SELECT 
        dri.document_reference_id,
        COUNT(DISTINCT dri.identifier_value) AS identifier_count
    FROM public.document_reference_identifiers dri
    GROUP BY dri.document_reference_id
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
        ) AS identifiers
    FROM public.document_reference_identifiers dri
    GROUP BY dri.document_reference_id
)
SELECT 
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
    dr.meta_last_updated,                  -- FHIR resource version
    
    -- ETL Audit Fields
    dr.created_at AS etl_created_at,
    dr.updated_at AS etl_updated_at,
    -- CONTENT ATTACHMENTS (FROM CTE)
    -- ============================================
    ac.content,
    -- AUTHORS (FROM CTE)
    -- ============================================
    aa.authors,
    -- CATEGORIES (FROM CTE)
    -- ============================================
    acat.categories,
    -- IDENTIFIERS (FROM CTE)
    -- ============================================
    ai.identifiers,
    -- CALCULATED FIELDS
    -- Calculate document age in days
    CASE 
        WHEN dr.date IS NOT NULL 
        THEN DATEDIFF(day, dr.date, CURRENT_DATE)
        ELSE NULL 
    END AS document_age_days,
    
    -- Count of categories (from CTE)
    COALESCE(catc.category_count, 0) AS category_count,
    
    -- Determine if document has content attachments (from CTE)
    CASE 
        WHEN COALESCE(cc.content_count, 0) > 0 
        THEN TRUE
        ELSE FALSE
    END AS has_content,
    
    -- Count of identifiers (from CTE)
    COALESCE(ic.identifier_count, 0) AS identifier_count

FROM public.document_references dr
    LEFT JOIN content_counts cc ON dr.document_reference_id = cc.document_reference_id
    LEFT JOIN aggregated_content ac ON dr.document_reference_id = ac.document_reference_id
    LEFT JOIN author_counts authc ON dr.document_reference_id = authc.document_reference_id
    LEFT JOIN aggregated_authors aa ON dr.document_reference_id = aa.document_reference_id
    LEFT JOIN category_counts catc ON dr.document_reference_id = catc.document_reference_id
    LEFT JOIN aggregated_categories acat ON dr.document_reference_id = acat.document_reference_id
    LEFT JOIN identifier_counts ic ON dr.document_reference_id = ic.document_reference_id
    LEFT JOIN aggregated_identifiers ai ON dr.document_reference_id = ai.document_reference_id

WHERE dr.status NOT IN ('entered-in-error', 'superseded');
-- REFRESH CONFIGURATION
-- This materialized view is configured with AUTO REFRESH NO
-- Manual refresh will be scheduled via AWS Lambda or Airflow
-- Refresh frequency should align with source data update patterns
-- 
-- To manually refresh:
-- REFRESH MATERIALIZED VIEW fact_fhir_document_references_view_v1;
-- ===================================================================
-- INDEXES AND OPTIMIZATION
-- Redshift automatically creates and maintains sort keys and distribution keys
-- based on query patterns. Monitor query performance and adjust if needed:
-- - Consider DISTKEY on patient_id for patient-centric queries
-- - Consider SORTKEY on (patient_id, date) for temporal analysis
-- ===================================================================
-- DATA QUALITY NOTES
-- 1. Documents without content attachments will have NULL in content field
-- 2. Some documents may not have authors identified
-- 3. Category information may be incomplete for older documents
-- 4. Identifier systems vary by document source
-- 5. All text fields are sanitized to ensure valid JSON format
-- 6. Filtered to exclude entered-in-error and superseded documents
-- ===================================================================