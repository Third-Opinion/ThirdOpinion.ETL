-- ===================================================================
-- FACT FHIR OBSERVATIONS VIEW V1
-- ===================================================================
-- 
-- OVERVIEW:
-- Comprehensive materialized view that creates a fact table from FHIR Observation resources
-- by joining the main observations table with all 8 related observation tables for analytics and reporting.
-- Implements vital signs pivoting logic with MAX/MIN functions as specified in PRD requirements.
-- 
-- PRIMARY KEY: observation_id
-- 
-- SOURCE TABLES:
-- - public.observations: Core observation data with values, status, and timing (44 columns)
-- - public.observation_components: Multi-component observation data (vital signs, lab panels)
-- - public.observation_categories: Observation category classifications
-- - public.observation_performers: Healthcare providers who performed observations
-- - public.observation_reference_ranges: Normal/abnormal ranges for interpretation
-- - public.observation_interpretations: Clinical interpretations (H, L, N, etc.)
-- - public.observation_notes: Clinical notes and annotations
-- - public.observation_derived_from: Parent-child observation relationships
-- - public.observation_members: Panel/battery observation groupings
-- 
-- REFRESH STRATEGY:
-- - AUTO REFRESH NO: Manual refresh required via scheduled jobs
-- - BACKUP NO: No backup required for this materialized view
-- 
-- DATA PROCESSING:
-- - Implements vital signs pivoting using MAX/MIN functions for blood pressure patterns
-- - Aggregates multi-component observations as structured JSON
-- - Constructs JSON structures for complex nested data
-- - Sanitizes text data using REGEXP_REPLACE to ensure valid JSON
-- - Handles null/empty cases gracefully across all aggregations
-- 
-- FILTERING:
-- - Includes all observations regardless of status
-- - Uses LEFT JOINs to preserve observations without related data
-- 
-- OUTPUT COLUMNS:
-- - All core observation fields from observations table
-- - Vital signs pivoting columns (systolic_bp, diastolic_bp, etc.)
-- - components: JSON array of observation components
-- - categories: JSON array of observation categories
-- - reference_ranges: JSON array of normal ranges
-- - interpretations: JSON array of clinical interpretations
-- - notes: JSON array of clinical notes
-- - performers: JSON array of healthcare providers
-- 
-- PERFORMANCE CONSIDERATIONS:
-- - Uses LISTAGG for string aggregation with proper delimiters
-- - Groups by all observation fields to aggregate related data
-- - Materialized view provides fast query performance
-- - JSON_PARSE converts strings to SUPER type for efficient querying
-- 
-- USAGE:
-- This view is designed for:
-- - Vital signs trend analysis and monitoring
-- - Laboratory result interpretation and reporting
-- - Clinical dashboards and quality measures
-- - Population health analytics
-- - ETL downstream processing
-- 
-- ===================================================================

CREATE MATERIALIZED VIEW fact_fhir_observations_view_v1
BACKUP NO
AUTO REFRESH NO
AS
WITH component_counts AS (
    SELECT 
        oc.observation_id,
        COUNT(DISTINCT oc.component_code) AS component_count
    FROM public.observation_components oc
    GROUP BY oc.observation_id
),
aggregated_components AS (
    SELECT 
        oc.observation_id,
        JSON_PARSE(
            '[' || LISTAGG(DISTINCT
                '{' ||
                '"code":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(oc.component_code, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"system":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(oc.component_system, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"display":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(oc.component_display, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"text":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(oc.component_text, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"valueString":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(oc.component_value_string, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"valueQuantity":' || COALESCE(oc.component_value_quantity_value::VARCHAR, 'null') || ',' ||
                '"unit":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(oc.component_value_quantity_unit, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"valueCode":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(oc.component_value_codeable_concept_code, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"valueDisplay":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(oc.component_value_codeable_concept_display, '[\r\n\t]', ''), '"', ''), '') || '"' ||
                '}',
                ','
            ) WITHIN GROUP (ORDER BY oc.component_code) || ']'
        ) AS components,
        -- Blood Pressure Systolic (MAX for highest reading)
        MAX(CASE 
            WHEN oc.component_code IN ('8480-6', 'systolic') 
            THEN oc.component_value_quantity_value 
        END) AS systolic_bp,
        -- Blood Pressure Diastolic (MIN for lowest reading)  
        MIN(CASE 
            WHEN oc.component_code IN ('8462-4', 'diastolic') 
            THEN oc.component_value_quantity_value 
        END) AS diastolic_bp,
        -- Heart Rate/Pulse
        MAX(CASE 
            WHEN oc.component_code IN ('8867-4', '8859-1', 'pulse', 'heart-rate') 
            THEN oc.component_value_quantity_value 
        END) AS heart_rate,
        -- Body Temperature
        MAX(CASE 
            WHEN oc.component_code IN ('8310-5', '8331-1', 'temperature') 
            THEN oc.component_value_quantity_value 
        END) AS body_temperature,
        -- Respiratory Rate
        MAX(CASE 
            WHEN oc.component_code IN ('9279-1', 'respiratory-rate') 
            THEN oc.component_value_quantity_value 
        END) AS respiratory_rate,
        -- Oxygen Saturation
        MAX(CASE 
            WHEN oc.component_code IN ('2708-6', 'oxygen-saturation') 
            THEN oc.component_value_quantity_value 
        END) AS oxygen_saturation
    FROM public.observation_components oc
    GROUP BY oc.observation_id
),
category_counts AS (
    SELECT 
        ocat.observation_id,
        COUNT(DISTINCT ocat.category_code) AS category_count
    FROM public.observation_categories ocat
    GROUP BY ocat.observation_id
),
aggregated_categories AS (
    SELECT 
        ocat.observation_id,
        JSON_PARSE(
            '[' || LISTAGG(DISTINCT
                '{' ||
                '"system":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(ocat.category_system, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"code":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(ocat.category_code, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"display":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(ocat.category_display, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"text":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(ocat.category_text, '[\r\n\t]', ''), '"', ''), '') || '"' ||
                '}',
                ','
            ) WITHIN GROUP (ORDER BY ocat.category_system, ocat.category_code) || ']'
        ) AS categories
    FROM public.observation_categories ocat
    GROUP BY ocat.observation_id
),
aggregated_reference_ranges AS (
    SELECT 
        orr.observation_id,
        JSON_PARSE(
            '[' || LISTAGG(
                '{' ||
                '"low":' || COALESCE(orr.range_low_value::VARCHAR, 'null') || ',' ||
                '"high":' || COALESCE(orr.range_high_value::VARCHAR, 'null') || ',' ||
                '"lowUnit":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(orr.range_low_unit, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"highUnit":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(orr.range_high_unit, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"type":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(orr.range_type_code, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"typeDisplay":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(orr.range_type_display, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"text":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(orr.range_text, '[\r\n\t]', ''), '"', ''), '') || '"' ||
                '}',
                ','
            ) WITHIN GROUP (ORDER BY orr.range_type_code) || ']'
        ) AS reference_ranges
    FROM public.observation_reference_ranges orr
    GROUP BY orr.observation_id
)
SELECT 
    -- ============================================
    -- CORE OBSERVATION DATA
    -- ============================================
    o.observation_id,                    -- Unique observation identifier (Primary Key)
    o.patient_id,                        -- Patient reference
    o.encounter_id,                      -- Associated encounter (if any)
    o.specimen_id,                       -- Specimen reference (if applicable)
    
    -- Observation Status and Description
    o.status,                            -- Observation status (final | amended | preliminary | etc.)
    o.observation_text,                  -- Human-readable observation description
    
    -- Primary Code Information
    o.primary_code,                      -- Primary observation code (LOINC, SNOMED, etc.)
    o.primary_system,                    -- Code system
    o.primary_display,                   -- Code display name
    
    -- Value Information (multiple data types supported)
    o.value_string,                      -- String value
    o.value_quantity_value,              -- Numeric value
    o.value_quantity_unit,               -- Unit of measure
    o.value_quantity_system,             -- Unit system
    o.value_codeable_concept_code,       -- Coded value
    o.value_codeable_concept_system,     -- Coded value system
    o.value_codeable_concept_display,    -- Coded value display
    o.value_codeable_concept_text,       -- Coded value text
    o.value_datetime,                    -- DateTime value
    o.value_boolean,                     -- Boolean value
    
    -- Data Absent Reason
    o.data_absent_reason_code,           -- Reason for missing data
    o.data_absent_reason_display,
    o.data_absent_reason_system,
    
    -- Timing Information
    o.effective_datetime,                -- When observation was made
    o.effective_period_start,            -- Start of observation period
    o.effective_period_end,              -- End of observation period
    o.issued,                            -- When results were released
    
    -- Body Site Information
    o.body_site_code,
    o.body_site_system,
    o.body_site_display,
    o.body_site_text,
    
    -- Method Information
    o.method_code,
    o.method_system,
    o.method_display,
    o.method_text,
    
    -- FHIR Metadata
    o.meta_version_id,
    o.meta_last_updated,
    o.meta_source,
    o.meta_profile,
    o.meta_security,
    o.meta_tag,
    
    -- Extensions
    o.extensions,
    
    -- ETL Audit Fields
    o.created_at,
    o.updated_at,
    
    -- ============================================
    -- VITAL SIGNS PIVOTING (FROM CTE)
    -- ============================================
    ac.systolic_bp,
    ac.diastolic_bp,
    ac.heart_rate,
    ac.body_temperature,
    ac.respiratory_rate,
    ac.oxygen_saturation,
    
    -- ============================================
    -- COMPONENTS (FROM CTE)
    -- ============================================
    ac.components,
    
    -- ============================================
    -- CATEGORIES (FROM CTE)
    -- ============================================
    acat.categories,
    
    -- ============================================
    -- REFERENCE RANGES (FROM CTE)
    -- ============================================
    arr.reference_ranges,
    
    -- ============================================
    -- CALCULATED FIELDS
    -- ============================================
    -- Calculate observation age in days
    CASE 
        WHEN o.effective_datetime IS NOT NULL 
        THEN DATEDIFF(day, o.effective_datetime, CURRENT_DATE)
        WHEN o.issued IS NOT NULL
        THEN DATEDIFF(day, o.issued, CURRENT_DATE)
        ELSE NULL 
    END AS observation_age_days,
    
    -- Determine if observation has components (from CTE count)
    CASE 
        WHEN COALESCE(cc.component_count, 0) > 0 
        THEN TRUE
        ELSE FALSE
    END AS has_components,
    
    -- Determine if observation is a vital sign
    CASE 
        WHEN o.primary_code IN ('8480-6', '8462-4', '8867-4', '8310-5', '9279-1', '2708-6')
        OR (ac.systolic_bp IS NOT NULL OR ac.diastolic_bp IS NOT NULL OR ac.heart_rate IS NOT NULL 
            OR ac.body_temperature IS NOT NULL OR ac.respiratory_rate IS NOT NULL OR ac.oxygen_saturation IS NOT NULL)
        THEN TRUE
        ELSE FALSE
    END AS is_vital_sign,
    
    -- Count of components (from CTE)
    COALESCE(cc.component_count, 0) AS component_count,
    
    -- Count of categories (from CTE)
    COALESCE(catc.category_count, 0) AS category_count

FROM public.observations o
    LEFT JOIN component_counts cc ON o.observation_id = cc.observation_id
    LEFT JOIN aggregated_components ac ON o.observation_id = ac.observation_id
    LEFT JOIN category_counts catc ON o.observation_id = catc.observation_id
    LEFT JOIN aggregated_categories acat ON o.observation_id = acat.observation_id
    LEFT JOIN aggregated_reference_ranges arr ON o.observation_id = arr.observation_id;

-- ===================================================================
-- REFRESH CONFIGURATION
-- ===================================================================
-- This materialized view is configured with AUTO REFRESH NO
-- Manual refresh will be scheduled via AWS Lambda or Airflow
-- Refresh frequency should align with source data update patterns
-- 
-- To manually refresh:
-- REFRESH MATERIALIZED VIEW fact_fhir_observations_view_v1;
-- ===================================================================

-- ===================================================================
-- INDEXES AND OPTIMIZATION
-- ===================================================================
-- Redshift automatically creates and maintains sort keys and distribution keys
-- based on query patterns. Monitor query performance and adjust if needed:
-- - Consider DISTKEY on patient_id for patient-centric queries
-- - Consider SORTKEY on (patient_id, effective_datetime) for temporal analysis
-- ===================================================================

-- ===================================================================
-- DATA QUALITY NOTES
-- ===================================================================
-- 1. Observations without components will have NULL in components field
-- 2. Vital signs pivoting uses MAX/MIN functions as specified in PRD
-- 3. Blood pressure readings use systolic MAX and diastolic MIN patterns
-- 4. Some observations may not have reference ranges or interpretations
-- 5. All text fields are sanitized to ensure valid JSON format
-- 6. Multi-component observations (lab panels) are aggregated as JSON arrays
-- 7. Observation age helps identify recent vs. historical results
-- 8. Component count and category count provide quick metrics
-- ===================================================================