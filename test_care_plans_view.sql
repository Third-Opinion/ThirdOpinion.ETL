-- ===================================================================
-- TEST QUERIES FOR FACT_FHIR_CARE_PLANS_VIEW_V1
-- ===================================================================
-- Validation queries to test the materialized view after creation
-- Run these queries to verify the view works correctly
-- ===================================================================

-- Test 1: Basic record count and structure validation
-- Expected: Should return ~171 records based on production data
SELECT 
    COUNT(*) as total_care_plans,
    COUNT(DISTINCT care_plan_id) as unique_care_plan_ids,
    COUNT(DISTINCT patient_id) as unique_patients
FROM fact_fhir_care_plans_view_v1;

-- Test 2: Status distribution analysis
SELECT 
    status,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM fact_fhir_care_plans_view_v1
GROUP BY status
ORDER BY count DESC;

-- Test 3: JSON array structure validation
SELECT 
    care_plan_id,
    patient_id,
    status,
    ARRAY_SIZE(categories) as category_count,
    ARRAY_SIZE(goals) as goals_count, 
    ARRAY_SIZE(care_teams) as care_teams_count,
    ARRAY_SIZE(identifiers) as identifiers_count
FROM fact_fhir_care_plans_view_v1
LIMIT 10;

-- Test 4: JSON content extraction examples
SELECT 
    care_plan_id,
    title,
    -- Extract first category if exists
    CASE WHEN ARRAY_SIZE(categories) > 0 
         THEN categories[0].code 
         ELSE NULL END as first_category_code,
    -- Extract first goal if exists
    CASE WHEN ARRAY_SIZE(goals) > 0 
         THEN goals[0].goal_id 
         ELSE NULL END as first_goal_id,
    -- Extract first identifier if exists
    CASE WHEN ARRAY_SIZE(identifiers) > 0 
         THEN identifiers[0].system 
         ELSE NULL END as first_identifier_system
FROM fact_fhir_care_plans_view_v1
WHERE ARRAY_SIZE(categories) > 0
   OR ARRAY_SIZE(goals) > 0 
   OR ARRAY_SIZE(identifiers) > 0
LIMIT 5;

-- Test 5: Data quality check - compare against source tables
SELECT 
    'source_care_plans' as source_table,
    COUNT(*) as record_count
FROM public.care_plans
WHERE care_plan_id IS NOT NULL 
  AND status != 'entered-in-error'

UNION ALL

SELECT 
    'materialized_view' as source_table,
    COUNT(*) as record_count  
FROM fact_fhir_care_plans_view_v1;

-- Test 6: Aggregation accuracy validation
-- Compare debug counts with actual related table counts
WITH source_counts AS (
    SELECT 
        cp.care_plan_id,
        COUNT(DISTINCT cpc.care_plan_id) as actual_categories_count,
        COUNT(DISTINCT cpg.care_plan_id) as actual_goals_count,
        COUNT(DISTINCT cpct.care_plan_id) as actual_care_teams_count,
        COUNT(DISTINCT cpi.care_plan_id) as actual_identifiers_count
    FROM public.care_plans cp
        LEFT JOIN public.care_plan_categories cpc ON cp.care_plan_id = cpc.care_plan_id
        LEFT JOIN public.care_plan_goals cpg ON cp.care_plan_id = cpg.care_plan_id  
        LEFT JOIN public.care_plan_care_teams cpct ON cp.care_plan_id = cpct.care_plan_id
        LEFT JOIN public.care_plan_identifiers cpi ON cp.care_plan_id = cpi.care_plan_id
    WHERE cp.care_plan_id IS NOT NULL 
      AND cp.status != 'entered-in-error'
    GROUP BY cp.care_plan_id
)
SELECT 
    mv.care_plan_id,
    -- Compare debug counts with actual counts
    mv.categories_count,
    sc.actual_categories_count,
    CASE WHEN mv.categories_count = sc.actual_categories_count THEN 'MATCH' ELSE 'MISMATCH' END as categories_check,
    
    mv.goals_count,
    sc.actual_goals_count, 
    CASE WHEN mv.goals_count = sc.actual_goals_count THEN 'MATCH' ELSE 'MISMATCH' END as goals_check,
    
    mv.care_teams_count,
    sc.actual_care_teams_count,
    CASE WHEN mv.care_teams_count = sc.actual_care_teams_count THEN 'MATCH' ELSE 'MISMATCH' END as care_teams_check,
    
    mv.identifiers_count,
    sc.actual_identifiers_count,
    CASE WHEN mv.identifiers_count = sc.actual_identifiers_count THEN 'MATCH' ELSE 'MISMATCH' END as identifiers_check
FROM fact_fhir_care_plans_view_v1 mv
JOIN source_counts sc ON mv.care_plan_id = sc.care_plan_id
WHERE mv.categories_count != sc.actual_categories_count
   OR mv.goals_count != sc.actual_goals_count
   OR mv.care_teams_count != sc.actual_care_teams_count  
   OR mv.identifiers_count != sc.actual_identifiers_count
LIMIT 10;

-- Test 7: Performance validation
-- Simple query performance test
SELECT 
    status,
    intent,
    COUNT(*) as count,
    AVG(categories_count) as avg_categories_per_plan,
    AVG(goals_count) as avg_goals_per_plan
FROM fact_fhir_care_plans_view_v1
GROUP BY status, intent
ORDER BY count DESC;

-- ===================================================================
-- EXPECTED RESULTS BASED ON PRODUCTION DATA:
-- ===================================================================
-- Test 1: ~171 total care plans, ~171 unique IDs
-- Test 2: Status distribution showing active, draft, etc.
-- Test 3: JSON arrays with various sizes, most should have identifiers
-- Test 4: Sample care plan data with extracted JSON elements
-- Test 5: Source and view counts should match (~171)
-- Test 6: No mismatches if aggregation logic is correct
-- Test 7: Performance should be very fast due to low volume
-- ===================================================================