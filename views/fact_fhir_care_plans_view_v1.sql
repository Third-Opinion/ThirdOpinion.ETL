-- ===================================================================
-- FACT FHIR CARE PLANS VIEW V1
-- ===================================================================
-- 
-- OVERVIEW:
-- Comprehensive materialized view that creates a fact table from FHIR CarePlan resources
-- by joining the main care_plans table with related tables for analytics and reporting.
-- 
-- PRIMARY KEY: care_plan_id
-- 
-- SOURCE TABLES:
-- - public.care_plans: Core care plan data (171 records)
-- - public.care_plan_categories: Plan categorization codes (171 records)
-- - public.care_plan_goals: Associated goals (181 records) 
-- - public.care_plan_care_teams: Team assignments (161 records)
-- - public.care_plan_identifiers: External identifiers (171 records)
-- 
-- REFRESH STRATEGY:
-- - AUTO REFRESH NO: Uses scheduled refresh pattern for consistency across all views
-- - BACKUP NO: No backup required for this materialized view
-- - SCHEDULED REFRESH: Will be refreshed via external scheduling system (Airflow/cron)
-- 
-- DATA PROCESSING:
-- - Aggregates categories, goals, care teams, and identifiers as JSON arrays
-- - Sanitizes data using REGEXP_REPLACE to remove special characters
-- - Constructs JSON structures using manual construction with JSON_PARSE
-- - Handles null/empty cases gracefully with COALESCE
-- 
-- FILTERING:
-- - Only includes care plans with valid care_plan_id
-- - Filters out care plans with 'entered-in-error' status (standard FHIR pattern)
-- - Uses LEFT JOINs to preserve care plans without related data
-- 
-- OUTPUT COLUMNS:
-- - All core care plan fields from care_plans table
-- - categories: JSON array containing categorization information
-- - goals: JSON array containing associated goal IDs
-- - care_teams: JSON array containing care team assignments
-- - identifiers: JSON array containing external identifier information
-- 
-- PERFORMANCE CONSIDERATIONS:
-- - Low volume data (171 records) allows for fast scheduled refresh
-- - Simple aggregation patterns due to straightforward related tables
-- - Materialized view provides fast query performance for care plan analytics
-- - Scheduled refresh ensures consistent refresh timing across all FHIR views
-- 
-- USAGE:
-- This view is designed for:
-- - Care plan management and tracking
-- - Care coordination reporting
-- - Population health analytics focused on care plans
-- - Quality measure reporting
-- - Care team assignment analysis
-- 
-- ===================================================================

CREATE MATERIALIZED VIEW fact_fhir_care_plans_view_v1
BACKUP NO
AUTO REFRESH NO
AS
SELECT 
    -- ============================================
    -- CORE CARE PLAN DEMOGRAPHICS AND METADATA
    -- ============================================
    cp.care_plan_id,                 -- Unique care plan identifier (Primary Key)
    cp.patient_id,                   -- Patient this care plan belongs to
    cp.status,                       -- Care plan status (draft, active, suspended, etc.)
    cp.intent,                       -- Care plan intent (proposal, plan, order)
    cp.title,                        -- Human-readable care plan title
    
    -- FHIR Metadata fields for tracking and versioning
    cp.meta_last_updated,              -- Last update timestamp

    -- ETL Audit fields
    cp.created_at,                   -- Record creation timestamp
    cp.updated_at,                   -- Record last update timestamp
    
    -- ============================================
    -- CARE PLAN CATEGORIES PROCESSING
    -- ============================================
    -- Constructs a JSON array containing care plan category information
    -- Uses extensive string cleaning to handle malformed data
    CASE 
        WHEN COUNT(cpc.care_plan_id) > 0 THEN
            -- Build JSON array manually and parse it
            JSON_PARSE(
                '[' || LISTAGG(
                    '{' ||
                    '"code":"' || 
                    -- Clean category_code: remove quotes and special chars
                    COALESCE(REGEXP_REPLACE(
                        REGEXP_REPLACE(cpc.category_code, '"', ''),
                        '[\r\n\t]', ''
                    ), '') || '",' ||
                    '"system":"' || 
                    -- Clean category_system: remove quotes and special chars
                    COALESCE(REGEXP_REPLACE(
                        REGEXP_REPLACE(cpc.category_system, '"', ''),
                        '[\r\n\t]', ''
                    ), '') || '",' ||
                    '"display":"' || 
                    -- Clean category_display: remove quotes and special chars
                    COALESCE(REGEXP_REPLACE(
                        REGEXP_REPLACE(cpc.category_display, '"', ''),
                        '[\r\n\t]', ''
                    ), '') || '",' ||
                    '"text":"' || 
                    -- Clean category_text: remove quotes and special chars
                    COALESCE(REGEXP_REPLACE(
                        REGEXP_REPLACE(cpc.category_text, '"', ''),
                        '[\r\n\t]', ''
                    ), '') || '"' ||
                    '}',
                    ','
                ) || ']'
            )
        ELSE
            -- Return empty array for care plans without categories
            JSON_PARSE('[]')
    END AS categories,
    
    -- ============================================
    -- CARE PLAN GOALS PROCESSING
    -- ============================================
    -- Constructs a JSON array containing associated goal IDs
    CASE 
        WHEN COUNT(cpg.care_plan_id) > 0 THEN
            JSON_PARSE(
                '[' || LISTAGG(
                    '{' ||
                    '"goal_id":"' || 
                    -- Clean goal_id: remove quotes and special chars
                    COALESCE(REGEXP_REPLACE(
                        REGEXP_REPLACE(cpg.goal_id, '"', ''),
                        '[\r\n\t]', ''
                    ), '') || '"' ||
                    '}',
                    ','
                ) || ']'
            )
        ELSE
            -- Return empty array for care plans without goals
            JSON_PARSE('[]')
    END AS goals,
    
    -- ============================================
    -- CARE TEAM ASSIGNMENTS PROCESSING
    -- ============================================
    -- Constructs a JSON array containing care team assignments
    CASE 
        WHEN COUNT(cpct.care_plan_id) > 0 THEN
            JSON_PARSE(
                '[' || LISTAGG(
                    '{' ||
                    '"care_team_id":"' || 
                    -- Clean care_team_id: remove quotes and special chars
                    COALESCE(REGEXP_REPLACE(
                        REGEXP_REPLACE(cpct.care_team_id, '"', ''),
                        '[\r\n\t]', ''
                    ), '') || '"' ||
                    '}',
                    ','
                ) || ']'
            )
        ELSE
            -- Return empty array for care plans without care teams
            JSON_PARSE('[]')
    END AS care_teams,
    
    -- ============================================
    -- EXTERNAL IDENTIFIERS PROCESSING
    -- ============================================
    -- Constructs a JSON array containing external identifier information
    CASE 
        WHEN COUNT(cpi.care_plan_id) > 0 THEN
            JSON_PARSE(
                '[' || LISTAGG(
                    '{' ||
                    '"system":"' || 
                    -- Clean identifier_system: remove quotes and special chars
                    COALESCE(REGEXP_REPLACE(
                        REGEXP_REPLACE(cpi.identifier_system, '"', ''),
                        '[\r\n\t]', ''
                    ), '') || '",' ||
                    '"value":"' || 
                    -- Clean identifier_value: remove quotes and special chars
                    COALESCE(REGEXP_REPLACE(
                        REGEXP_REPLACE(cpi.identifier_value, '"', ''),
                        '[\r\n\t]', ''
                    ), '') || '"' ||
                    '}',
                    ','
                ) || ']'
            )
        ELSE
            -- Return empty array for care plans without identifiers
            JSON_PARSE('[]')
    END AS identifiers,
    
    -- ============================================
    -- DEBUG AND QUALITY METRICS
    -- ============================================
    COUNT(cpc.care_plan_id) as categories_count,      -- Number of categories per care plan
    COUNT(cpg.care_plan_id) as goals_count,           -- Number of goals per care plan
    COUNT(cpct.care_plan_id) as care_teams_count,     -- Number of care teams per care plan
    COUNT(cpi.care_plan_id) as identifiers_count      -- Number of identifiers per care plan

-- ============================================
-- TABLE JOINS AND RELATIONSHIPS
-- ============================================
FROM public.care_plans cp
    LEFT JOIN public.care_plan_categories cpc ON cp.care_plan_id = cpc.care_plan_id
    LEFT JOIN public.care_plan_goals cpg ON cp.care_plan_id = cpg.care_plan_id
    LEFT JOIN public.care_plan_care_teams cpct ON cp.care_plan_id = cpct.care_plan_id
    LEFT JOIN public.care_plan_identifiers cpi ON cp.care_plan_id = cpi.care_plan_id

-- ============================================
-- FILTERING AND GROUPING
-- ============================================
WHERE cp.care_plan_id IS NOT NULL         -- Exclude malformed care plan records
  AND cp.status != 'entered-in-error'     -- Exclude erroneous entries (standard FHIR pattern)

-- Group by all care plan fields to enable aggregation of related data
GROUP BY 
    cp.care_plan_id,
    cp.patient_id,
    cp.status,
    cp.intent,
    cp.title,
    cp.meta_last_updated,
    cp.created_at,
    cp.updated_at;

-- ===================================================================
-- SCHEDULED REFRESH CONFIGURATION
-- ===================================================================
-- This materialized view is configured with AUTO REFRESH NO
-- View will be refreshed via external scheduling system (e.g., Airflow, cron)
-- Scheduled refresh ensures consistent timing across all FHIR materialized views
-- 
-- REFRESH STRATEGY:
-- - Low volume data (171 records) allows for frequent scheduled refresh
-- - Full refresh recommended due to small data size
-- - Consider hourly or daily refresh based on business requirements
-- ===================================================================

-- ===================================================================
-- MATERIALIZED VIEW METADATA
-- ===================================================================
-- Purpose: Materialized fact table for FHIR CarePlan analytics
-- Source: FHIR CarePlan resources from AWS HealthLake
-- Primary Key: care_plan_id
-- Grain: One row per care plan
-- Type: Materialized View (pre-computed for performance)
-- 
-- Key Features:
-- - Combines all care plan related normalized tables
-- - Uses JSON_PARSE to convert JSON strings to SUPER data type for efficient querying
-- - Aggregates complex fields as SUPER type JSON arrays (categories, goals, care_teams, identifiers)
-- - Includes debug count fields for data quality monitoring
-- - Maintains referential integrity with patient_id
-- - Filters out entered-in-error care plans (status != 'entered-in-error')
-- - Preserves care plan status as string for flexible querying
-- - Pre-computed for fast query performance
-- - Uses scheduled refresh for consistent timing across all FHIR views
-- - Backup disabled for cost optimization
-- - Escapes quotes in JSON strings to ensure valid JSON format
-- 
-- Performance Benefits:
-- - Faster query execution (pre-aggregated data)
-- - Reduced load on source tables
-- - Optimized for analytics workloads
-- - Consistent performance regardless of query complexity
-- - Scheduled refresh ensures data freshness and consistency
-- - Cost optimized (backup disabled)
-- 
-- COLUMN MAPPING:
-- - Columns 1-9: Core care plan fields (care_plan_id, patient_id, status, intent, title, 
--                 meta_last_updated, meta_last_updated, created_at, updated_at)
-- - Column 10: Categories as SUPER type JSON array with structure:
--   [{"code":"string","system":"string","display":"string","text":"string"},...]
-- - Column 11: Goals as SUPER type JSON array with structure:
--   [{"goal_id":"string"},...]
-- - Column 12: Care teams as SUPER type JSON array with structure:
--   [{"care_team_id":"string"},...]
-- - Column 13: Identifiers as SUPER type JSON array with structure:
--   [{"system":"string","value":"string"},...]
-- - Columns 14-17: Debug count fields for data quality monitoring
--
-- Usage Examples:
-- - Care plan analytics and reporting
-- - Care coordination tracking
-- - Population health management
-- - Quality measure reporting
-- - Care team assignment analysis
-- - Dashboard and BI tool queries
--
-- JSON Query Examples (using SUPER data type with JSON_PARSE):
-- -- Count categories per care plan:
-- SELECT care_plan_id, ARRAY_SIZE(categories) as category_count FROM fact_fhir_care_plans_view_v1;
--
-- -- Extract first category code:
-- SELECT care_plan_id, categories[0].code FROM fact_fhir_care_plans_view_v1;
--
-- -- Find care plans with specific category code:
-- SELECT care_plan_id FROM fact_fhir_care_plans_view_v1 WHERE categories @> '[{"code":"assess-plan"}]';
--
-- -- Get all goal IDs for a care plan:
-- SELECT care_plan_id, g.goal_id 
-- FROM fact_fhir_care_plans_view_v1, goals g 
-- WHERE care_plan_id = 'your-care-plan-id';
--
-- -- Find care plans with multiple care teams:
-- SELECT care_plan_id FROM fact_fhir_care_plans_view_v1 WHERE ARRAY_SIZE(care_teams) > 1;
--
-- -- Serialize back to JSON string if needed:
-- SELECT care_plan_id, JSON_SERIALIZE(categories) as categories_json FROM fact_fhir_care_plans_view_v1;
-- ===================================================================