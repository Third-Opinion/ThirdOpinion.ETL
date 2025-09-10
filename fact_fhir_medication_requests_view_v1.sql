-- ===================================================================
-- FACT FHIR MEDICATION REQUESTS VIEW V1
-- ===================================================================
-- 
-- OVERVIEW:
-- Comprehensive materialized view that creates a fact table from FHIR MedicationRequest resources
-- by joining the main medication_requests table with all related tables for analytics and reporting.
-- 
-- PRIMARY KEY: medication_request_id
-- 
-- SOURCE TABLES:
-- - public.medication_requests: Core medication request data including status, intent, and dates
-- - public.medication_request_dosage_instructions: Detailed dosage instructions with timing and routes
-- - public.medication_request_categories: Medication request category classifications
-- - public.medication_request_notes: Clinical notes and annotations
-- - public.medications: Medication details and descriptions
-- 
-- REFRESH STRATEGY:
-- - AUTO REFRESH NO: Manual refresh required via scheduled jobs
-- - BACKUP NO: No backup required for this materialized view
-- 
-- DATA PROCESSING:
-- - Aggregates complex dosage instructions with timing patterns as JSON
-- - Constructs JSON structures for categories and notes
-- - Sanitizes text data using REGEXP_REPLACE to ensure valid JSON
-- - Handles null/empty cases gracefully across all aggregations
-- - Joins with medications table for medication details
-- 
-- FILTERING:
-- - Excludes medication requests with status 'entered-in-error' or 'cancelled'
-- - Uses LEFT JOINs to preserve medication requests without related data
-- 
-- OUTPUT COLUMNS:
-- - All core medication request fields from medication_requests table
-- - Medication details from medications table
-- - dosage_instructions: JSON array of dosage instructions with timing and routes
-- - categories: JSON array of medication request categories
-- - notes: JSON array of clinical notes
-- 
-- PERFORMANCE CONSIDERATIONS:
-- - Uses CTEs to separate LISTAGG operations from COUNT DISTINCT operations (Redshift requirement)
-- - Groups by all medication request fields to aggregate related data
-- - Materialized view provides fast query performance
-- - JSON_PARSE converts strings to SUPER type for efficient querying
-- 
-- USAGE:
-- This view is designed for:
-- - Medication management and prescribing analytics
-- - Drug utilization reviews and reporting
-- - Clinical decision support for medication therapy
-- - Compliance and adherence tracking
-- - ETL downstream processing
-- 
-- ===================================================================

CREATE MATERIALIZED VIEW fact_fhir_medication_requests_view_v1
BACKUP NO
AUTO REFRESH NO
AS
WITH dosage_counts AS (
    SELECT 
        medication_request_id,
        COUNT(DISTINCT dosage_text) AS dosage_instruction_count
    FROM public.medication_request_dosage_instructions
    GROUP BY medication_request_id
),
aggregated_dosage AS (
    SELECT 
        mrdi.medication_request_id,
        JSON_PARSE(
            '[' || LISTAGG(DISTINCT
                '{' ||
                '"text":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(mrdi.dosage_text, '[\r\n\t]', ''), '"', ''), '[\\\\]', '\\\\\\\\'), '') || '",' ||
                '"timing":{' ||
                    '"frequency":' || COALESCE(mrdi.dosage_timing_frequency::VARCHAR, 'null') || ',' ||
                    '"period":' || COALESCE(mrdi.dosage_timing_period::VARCHAR, 'null') || ',' ||
                    '"periodUnit":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(mrdi.dosage_timing_period_unit, '[\r\n\t]', ''), '"', ''), '') || '"' ||
                '},' ||
                '"route":{' ||
                    '"system":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(mrdi.dosage_route_system, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                    '"code":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(mrdi.dosage_route_code, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                    '"display":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(mrdi.dosage_route_display, '[\r\n\t]', ''), '"', ''), '') || '"' ||
                '},' ||
                '"dose":{' ||
                    '"value":' || COALESCE(mrdi.dosage_dose_value::VARCHAR, 'null') || ',' ||
                    '"unit":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(mrdi.dosage_dose_unit, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                    '"system":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(mrdi.dosage_dose_system, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                    '"code":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(mrdi.dosage_dose_code, '[\r\n\t]', ''), '"', ''), '') || '"' ||
                '},' ||
                '"asNeeded":' || CASE WHEN mrdi.dosage_as_needed_boolean IS TRUE THEN 'true' 
                                     WHEN mrdi.dosage_as_needed_boolean IS FALSE THEN 'false' 
                                     ELSE 'null' END ||
                '}',
                ','
            ) WITHIN GROUP (ORDER BY mrdi.dosage_text) || ']'
        ) AS dosage_instructions
    FROM public.medication_request_dosage_instructions mrdi
    GROUP BY mrdi.medication_request_id
),
category_counts AS (
    SELECT 
        medication_request_id,
        COUNT(DISTINCT category_code) AS category_count
    FROM public.medication_request_categories
    GROUP BY medication_request_id
),
aggregated_categories AS (
    SELECT 
        mrc.medication_request_id,
        JSON_PARSE(
            '[' || LISTAGG(DISTINCT
                '{' ||
                '"system":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(mrc.category_system, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"code":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(mrc.category_code, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"display":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(mrc.category_display, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"text":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(mrc.category_text, '[\r\n\t]', ''), '"', ''), '') || '"' ||
                '}',
                ','
            ) WITHIN GROUP (ORDER BY mrc.category_system, mrc.category_code) || ']'
        ) AS categories
    FROM public.medication_request_categories mrc
    GROUP BY mrc.medication_request_id
),
note_counts AS (
    SELECT 
        medication_request_id,
        COUNT(note_text) AS note_count
    FROM public.medication_request_notes
    GROUP BY medication_request_id
),
aggregated_notes AS (
    SELECT 
        mrn.medication_request_id,
        JSON_PARSE(
            '[' || LISTAGG(
                '{' ||
                '"text":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(mrn.note_text, '[\r\n\t]', ''), '"', ''), '[\\\\]', '\\\\\\\\'), '') || '"' ||
                '}',
                ','
            ) WITHIN GROUP (ORDER BY mrn.note_text) || ']'
        ) AS notes
    FROM public.medication_request_notes mrn
    GROUP BY mrn.medication_request_id
)
SELECT 
    -- ============================================
    -- CORE MEDICATION REQUEST DATA
    -- ============================================
    mr.medication_request_id,            -- Unique medication request identifier (Primary Key)
    mr.patient_id,                       -- Patient reference
    mr.encounter_id,                     -- Associated encounter (if any)
    mr.medication_id,                    -- Medication reference
    mr.medication_display,               -- Medication display name
    
    -- Request Status and Intent
    mr.status,                           -- active | on-hold | cancelled | completed | entered-in-error | stopped | unknown
    mr.intent,                           -- proposal | plan | order | instance-order | option
    mr.reported_boolean,                 -- Whether request was reported by someone other than prescriber
    
    -- Dates
    mr.authored_on,                       -- When request was initially authored
    
    -- Metadata
    mr.meta_version_id,                  -- FHIR resource version
    mr.meta_last_updated,                -- Last updated timestamp
    
    -- ETL Audit Fields
    mr.created_at,
    mr.updated_at,
    
    -- ============================================
    -- MEDICATION DETAILS
    -- ============================================
    m.code_text AS medication_name,      -- Medication name from medications table
    m.status AS medication_status,       -- Medication status
    
    -- ============================================
    -- DOSAGE INSTRUCTIONS (FROM CTE)
    -- ============================================
    ad.dosage_instructions,
    
    -- ============================================
    -- CATEGORIES (FROM CTE)
    -- ============================================
    ac.categories,
    
    -- ============================================
    -- NOTES (FROM CTE)
    -- ============================================
    an.notes,
    
    -- ============================================
    -- CALCULATED FIELDS
    -- ============================================
    -- Calculate request age in days
    CASE 
        WHEN mr.authored_on IS NOT NULL 
        THEN DATEDIFF(day, mr.authored_on, CURRENT_DATE)
        ELSE NULL 
    END AS request_age_days,
    
    -- Count of dosage instructions (from CTE)
    COALESCE(dc.dosage_instruction_count, 0) AS dosage_instruction_count,
    
    -- Count of categories (from CTE)
    COALESCE(cc.category_count, 0) AS category_count,
    
    -- Count of notes (from CTE)
    COALESCE(nc.note_count, 0) AS note_count,
    
    -- Determine if request is active
    CASE 
        WHEN mr.status IN ('active', 'on-hold', 'unknown') 
        THEN TRUE
        ELSE FALSE
    END AS is_active_request,
    
    -- Determine if medication is PRN (as needed)
    CASE 
        WHEN ad.dosage_instructions::VARCHAR LIKE '%"asNeeded":true%'
        THEN TRUE
        ELSE FALSE
    END AS is_prn_medication

FROM public.medication_requests mr
    LEFT JOIN public.medications m ON mr.medication_id = m.medication_id
    LEFT JOIN dosage_counts dc ON mr.medication_request_id = dc.medication_request_id
    LEFT JOIN aggregated_dosage ad ON mr.medication_request_id = ad.medication_request_id
    LEFT JOIN category_counts cc ON mr.medication_request_id = cc.medication_request_id
    LEFT JOIN aggregated_categories ac ON mr.medication_request_id = ac.medication_request_id
    LEFT JOIN note_counts nc ON mr.medication_request_id = nc.medication_request_id
    LEFT JOIN aggregated_notes an ON mr.medication_request_id = an.medication_request_id

WHERE mr.status NOT IN ('entered-in-error', 'cancelled');

-- ===================================================================
-- REFRESH CONFIGURATION
-- ===================================================================
-- This materialized view is configured with AUTO REFRESH NO
-- Manual refresh will be scheduled via AWS Lambda or Airflow
-- Refresh frequency should align with source data update patterns
-- 
-- To manually refresh:
-- REFRESH MATERIALIZED VIEW fact_fhir_medication_requests_view_v1;
-- ===================================================================

-- ===================================================================
-- INDEXES AND OPTIMIZATION
-- ===================================================================
-- Redshift automatically creates and maintains sort keys and distribution keys
-- based on query patterns. Monitor query performance and adjust if needed:
-- - Consider DISTKEY on patient_id for patient-centric queries
-- - Consider SORTKEY on (patient_id, authored_on) for temporal analysis
-- ===================================================================

-- ===================================================================
-- DATA QUALITY NOTES
-- ===================================================================
-- 1. Medication requests without dosage instructions will have NULL in dosage_instructions field
-- 2. Some requests may not have explicit categories assigned
-- 3. Notes provide additional clinical context for prescribing decisions
-- 4. All text fields are sanitized to ensure valid JSON format
-- 5. Timing patterns in dosage instructions support complex medication schedules
-- 6. PRN (as needed) medications are identified via dosage_as_needed_boolean flag
-- 7. Request age helps identify old or stale medication requests
-- 8. Filtered to exclude 'entered-in-error' and 'cancelled' status requests
-- ===================================================================