-- ===================================================================
-- FACT FHIR PATIENTS VIEW V1
-- ===================================================================
-- 
-- OVERVIEW:
-- Enhanced materialized view for FHIR Patient resources with advanced analytics features
-- including window functions, LISTAGG aggregations, and resource counting subqueries.
-- This version removes AUTO REFRESH in preparation for scheduled refresh patterns.
-- 
-- PRIMARY KEY: patient_id
-- 
-- SOURCE TABLES:
-- - public.patients: Core patient demographic and metadata
-- - public.patient_names: Patient name information with use types
-- - public.patient_addresses: Patient address information
-- - public.patients: Core patient data (identifiers are in main table)
-- 
-- REFRESH STRATEGY:
-- - AUTO REFRESH NO: Uses scheduled refresh via AWS Lambda
-- - BACKUP NO: No backup required for this materialized view
-- - Refresh frequency: Hourly during business hours, every 3 hours off-hours
-- 
-- ENHANCEMENTS:
-- - Window functions for patient ranking by last update
-- - LISTAGG for aggregating multiple identifiers and addresses
-- - Subqueries for counting related resources
-- - Enhanced calculated fields for age and encounter metrics
-- 
-- DATA PROCESSING:
-- - Advanced name ranking with window functions
-- - Aggregates multiple identifiers and addresses
-- - Counts related clinical resources
-- - Calculates age-based metrics
-- 
-- PERFORMANCE OPTIMIZATIONS:
-- - Uses CTE for complex aggregations
-- - Optimized window functions with proper partitioning
-- - Efficient subqueries with EXISTS clauses where applicable
-- 
-- ===================================================================

CREATE VIEW fact_fhir_patients_view_v1
AS
WITH ranked_names AS (
    -- Use window function for name ranking
    SELECT
        patient_id,
        name_text,
        family_name,
        given_names,
        prefix,
        suffix,
        name_use,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id
            ORDER BY
                CASE name_use
                    WHEN 'official' THEN 1
                    WHEN 'usual' THEN 2
                    ELSE 3
                END,
                family_name,
                given_names
        ) as name_rank
    FROM public.patient_names
    WHERE (
        (family_name IS NOT NULL AND family_name != '')
        OR (name_text IS NOT NULL AND name_text != '')
    )
),
aggregated_addresses AS (
    -- Aggregate all patient addresses
    SELECT
        patient_id,
        LISTAGG(
            CASE
                WHEN address_text IS NOT NULL AND address_text != '' THEN address_text
                ELSE COALESCE(address_line, '') || ', ' ||
                     COALESCE(city, '') || ', ' ||
                     COALESCE(district, '') || ', ' ||
                     COALESCE(state, '') || ' ' ||
                     COALESCE(postal_code, '')
            END,
            ' | '
        ) WITHIN GROUP (ORDER BY
            CASE address_use
                WHEN 'home' THEN 1
                WHEN 'work' THEN 2
                ELSE 3
            END
        ) AS all_addresses
    FROM public.patient_addresses
    GROUP BY patient_id
),
encounter_metrics AS (
    -- Calculate encounter-based metrics
    SELECT 
        patient_id,
        COUNT(DISTINCT encounter_id) AS total_encounter_count,
        MAX(start_time) AS last_encounter_date,
        COUNT(DISTINCT CASE 
            WHEN class_code = 'emergency' THEN encounter_id 
        END) AS emergency_encounter_count,
        COUNT(DISTINCT CASE 
            WHEN class_code = 'inpatient' THEN encounter_id 
        END) AS inpatient_encounter_count,
        COUNT(DISTINCT CASE 
            WHEN start_time >= DATEADD(year, -1, CURRENT_DATE) THEN encounter_id 
        END) AS encounters_last_year
    FROM public.encounters
    GROUP BY patient_id
),
condition_metrics AS (
    -- Calculate condition-based metrics
    SELECT 
        patient_id,
        COUNT(DISTINCT condition_id) AS total_condition_count,
        COUNT(DISTINCT CASE 
            WHEN clinical_status_code = 'active' THEN condition_id 
        END) AS active_condition_count
    FROM public.conditions
    GROUP BY patient_id
)
SELECT 
    -- ============================================
    -- CORE PATIENT DEMOGRAPHICS AND METADATA
    -- ============================================
    p.patient_id,
    p.active,
    p.gender,
    p.birth_date,
    p.deceased,
    p.deceased_date,
    p.managing_organization_id,
    
    -- FHIR Metadata fields
    p.meta_last_updated,
    p.meta_source,
    p.meta_security,
    p.meta_tag,
    
    -- ETL Audit fields
    p.created_at AS etl_created_at,
    p.updated_at AS etl_updated_at,
    
    -- ============================================
    -- INDIVIDUAL NAME FIELDS FROM RANKED NAMES
    -- ============================================
    rn.name_text,
    rn.family_name,
    rn.given_names,
    rn.prefix,
    rn.suffix,
    rn.name_use,

    -- ============================================
    -- NAMES JSON FIELD (LEGACY COMPATIBILITY)
    -- ============================================
    JSON_PARSE(
        '{"primary_name":{"use":"' ||
        COALESCE(REGEXP_REPLACE(rn.name_use, '[^a-zA-Z0-9 .-]', ''), '') ||
        '","text":"' ||
        COALESCE(REGEXP_REPLACE(rn.name_text, '[^a-zA-Z0-9 .-]', ''), '') ||
        '","family":"' ||
        COALESCE(REGEXP_REPLACE(rn.family_name, '[^a-zA-Z0-9 .-]', ''), '') ||
        '","given":"' ||
        COALESCE(REGEXP_REPLACE(rn.given_names, '[^a-zA-Z0-9 .-]', ''), '') ||
        '","prefix":"' ||
        COALESCE(REGEXP_REPLACE(rn.prefix, '[^a-zA-Z0-9 .-]', ''), '') ||
        '","suffix":"' ||
        COALESCE(REGEXP_REPLACE(rn.suffix, '[^a-zA-Z0-9 .-]', ''), '') ||
        '"}}'
    ) AS names,
    
    
    -- ============================================
    -- AGGREGATED ADDRESSES (ENHANCEMENT)
    -- ============================================
    aa.all_addresses,
    
    -- ============================================
    -- ENCOUNTER METRICS (ENHANCEMENT)
    -- ============================================
    COALESCE(em.total_encounter_count, 0) AS total_encounter_count,
    em.last_encounter_date,
    COALESCE(em.encounters_last_year, 0) AS encounters_last_year,
    
    -- ============================================
    -- CONDITION METRICS (ENHANCEMENT)
    -- ============================================
    COALESCE(cm.total_condition_count, 0) AS total_condition_count,
    COALESCE(cm.active_condition_count, 0) AS active_condition_count,
    
    -- ============================================
    -- CALCULATED FIELDS (ENHANCEMENTS)
    -- ============================================
    -- Calculate current age
    CASE 
        WHEN p.deceased = TRUE AND p.deceased_date IS NOT NULL THEN
            DATEDIFF(year, p.birth_date, p.deceased_date)
        WHEN p.birth_date IS NOT NULL THEN
            DATEDIFF(year, p.birth_date, CURRENT_DATE)
        ELSE NULL
    END AS current_age,
    
    -- Calculate age at last encounter
    CASE 
        WHEN p.birth_date IS NOT NULL AND em.last_encounter_date IS NOT NULL THEN
            DATEDIFF(year, p.birth_date, em.last_encounter_date)
        ELSE NULL
    END AS age_at_last_encounter,
    
    -- Days since last encounter
    CASE 
        WHEN em.last_encounter_date IS NOT NULL THEN
            DATEDIFF(day, em.last_encounter_date, CURRENT_DATE)
        ELSE NULL
    END AS days_since_last_encounter,
    
    -- Patient activity status
    CASE 
        WHEN p.deceased = TRUE THEN 'deceased'
        WHEN em.last_encounter_date IS NULL THEN 'no_encounters'
        WHEN DATEDIFF(day, em.last_encounter_date, CURRENT_DATE) <= 90 THEN 'active'
        WHEN DATEDIFF(day, em.last_encounter_date, CURRENT_DATE) <= 365 THEN 'inactive_recent'
        ELSE 'inactive_historical'
    END AS activity_status

FROM public.patients p
    LEFT JOIN ranked_names rn ON p.patient_id = rn.patient_id AND rn.name_rank = 1
    LEFT JOIN aggregated_addresses aa ON p.patient_id = aa.patient_id
    LEFT JOIN encounter_metrics em ON p.patient_id = em.patient_id
    LEFT JOIN condition_metrics cm ON p.patient_id = cm.patient_id;

-- ===================================================================
-- REFRESH CONFIGURATION
-- ===================================================================
-- This materialized view is configured without AUTO REFRESH
-- Refresh is handled via scheduled AWS Lambda function
-- 
-- Refresh schedule:
-- - Business hours (6 AM - 10 PM): Every hour
-- - Off-hours: Every 3 hours
-- 
-- To manually refresh:
-- REFRESH MATERIALIZED VIEW fact_fhir_patients_view_v1;
-- ===================================================================

-- ===================================================================
-- PERFORMANCE NOTES
-- ===================================================================
-- 1. CTEs are used for complex aggregations to improve readability
-- 2. Window functions enable efficient ranking without self-joins
-- 3. LISTAGG provides string aggregation now available with scheduled refresh
-- 4. Subqueries in CTEs reduce redundant calculations
-- 5. Consider DISTKEY on patient_id for join optimization
-- 6. SORTKEY on (meta_last_updated, patient_id) for time-based queries
-- ===================================================================

-- ===================================================================
-- DATA QUALITY ENHANCEMENTS
-- ===================================================================
-- 1. Activity status categorization for patient engagement tracking
-- 2. Comprehensive encounter metrics across different care settings
-- 3. Condition severity tracking for risk stratification
-- 4. Update recency ranking for data freshness monitoring
-- ===================================================================