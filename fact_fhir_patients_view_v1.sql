-- ===================================================================
-- FACT FHIR PATIENTS VIEW V1
-- ===================================================================
-- 
-- OVERVIEW:
-- Comprehensive materialized view that creates a fact table from FHIR Patient resources
-- by joining the main patients table with patient names for analytics and reporting.
-- 
-- PRIMARY KEY: patient_id
-- 
-- SOURCE TABLES:
-- - public.patients: Core patient demographic and metadata
-- - public.patient_names: Patient name information with use types
-- 
-- REFRESH STRATEGY:
-- - AUTO REFRESH NO: Manual refresh required via scheduled jobs
-- - BACKUP NO: No backup required for this materialized view
-- 
-- DATA PROCESSING:
-- - Selects the primary name per patient using ranking logic
-- - Sanitizes name data using REGEXP_REPLACE to remove special characters
-- - Constructs JSON structure for name data to enable nested querying
-- - Handles null/empty name cases gracefully
-- 
-- NAME SELECTION LOGIC:
-- Names are ranked by preference:
-- 1. 'official' use type (rank 1)
-- 2. 'usual' use type (rank 2) 
-- 3. All other types (rank 3)
-- Within each rank, ordered by family_name, given_names
-- 
-- FILTERING:
-- - Only includes patients with valid patient_id
-- - Only includes names with non-empty family_name and given_names
-- - Uses LEFT JOIN to preserve patients without valid names
-- 
-- OUTPUT COLUMNS:
-- - All core patient fields from patients table
-- - names: JSON object containing primary name information
-- - name_count: Debug field showing number of valid names found
-- 
-- PERFORMANCE CONSIDERATIONS:
-- - Uses ROW_NUMBER() window function for name ranking
-- - Groups by all patient fields to aggregate name data
-- - Materialized view provides fast query performance
-- 
-- USAGE:
-- This view is designed for:
-- - Patient demographic analysis
-- - Clinical reporting dashboards
-- - Data quality monitoring
-- - ETL downstream processing
-- 
-- ===================================================================

CREATE MATERIALIZED VIEW fact_fhir_patients_view_v1
BACKUP NO
AUTO REFRESH NO
AS
SELECT 
    -- ============================================
    -- CORE PATIENT DEMOGRAPHICS AND METADATA
    -- ============================================
    p.patient_id,                    -- Unique patient identifier (Primary Key)
    p.active,                        -- Boolean: Is patient record active?
    p.gender,                        -- Patient gender (male/female/other/unknown)
    p.birth_date,                    -- Patient date of birth
    p.deceased,                      -- Boolean: Is patient deceased?
    p.deceased_date,                 -- Date of death (if applicable)
    p.managing_organization_id,      -- Organization managing this patient
    
    -- FHIR Metadata fields for tracking and versioning
    p.meta_version_id,               -- FHIR resource version
    p.meta_last_updated,             -- Last update timestamp
    p.meta_source,                   -- Source system identifier
    p.meta_security,                 -- Security labels
    p.meta_tag,                      -- Resource tags
    
    -- ETL Audit fields
    p.created_at,                    -- Record creation timestamp
    p.updated_at,                    -- Record last update timestamp
    
    -- ============================================
    -- PATIENT NAME PROCESSING
    -- ============================================
    -- Constructs a JSON object containing the patient's primary name
    -- Uses extensive string cleaning to handle malformed data
    CASE 
        WHEN COUNT(pn.patient_id) > 0 THEN
            -- Build JSON string manually and parse it
            JSON_PARSE(
                '{"primary_name":{"use":"' || 
                -- Clean name_use field: remove whitespace and special chars
                COALESCE(REGEXP_REPLACE(
                    REGEXP_REPLACE(MAX(pn.name_use), '[\r\n\t]', ''),
                    '[^a-zA-Z0-9 .-]', ''
                ), '') || 
                '","family":"' || 
                -- Clean family_name: remove quotes, backslashes, whitespace
                COALESCE(REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(MAX(pn.family_name), '"', ''),
                        '\\\\', ''
                    ),
                    '[\r\n\t]', ''
                ), '') || 
                '","given":"' || 
                -- Clean given_names: remove quotes, backslashes, whitespace  
                COALESCE(REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(MAX(pn.given_names), '"', ''),
                        '\\\\', ''
                    ),
                    '[\r\n\t]', ''
                ), '') || 
                '"}}'
            )
        ELSE
            -- Return null structure for patients without valid names
            JSON_PARSE('{"primary_name":null}')
    END AS names,
    
    -- ============================================
    -- DEBUG AND QUALITY METRICS
    -- ============================================
    COUNT(pn.patient_id) as name_count  -- Number of valid names found per patient

-- ============================================
-- TABLE JOINS AND NAME RANKING LOGIC
-- ============================================
FROM public.patients p
    LEFT JOIN (
        -- Subquery to rank and select the best name per patient
        SELECT patient_id, name_use, name_text, family_name, given_names, 
               prefix, suffix, period_start, period_end,
               -- Rank names by preference: official > usual > other types
               ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY 
                   CASE WHEN name_use = 'official' THEN 1    -- Prefer official names
                        WHEN name_use = 'usual' THEN 2       -- Then usual names
                        ELSE 3 END,                          -- Then any other type
                   family_name, given_names) as name_rank   -- Tie-break by name alphabetically
        FROM public.patient_names
        -- Only include complete names (both family and given names present)
        WHERE family_name IS NOT NULL 
          AND family_name != ''
          AND given_names IS NOT NULL
          AND given_names != ''
    ) pn ON p.patient_id = pn.patient_id AND pn.name_rank = 1  -- Only join rank 1 (best) name

-- ============================================
-- FILTERING AND GROUPING
-- ============================================
WHERE p.patient_id IS NOT NULL  -- Exclude any malformed patient records

-- Group by all patient fields to enable aggregation of name data
GROUP BY 
    p.patient_id,
    p.active,
    p.gender,
    p.birth_date,
    p.deceased,
    p.deceased_date,
    p.managing_organization_id,
    p.meta_version_id,
    p.meta_last_updated,
    p.meta_source,
    p.meta_security,
    p.meta_tag,
    p.created_at,
    p.updated_at;
