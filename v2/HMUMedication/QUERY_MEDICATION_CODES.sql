-- ============================================================================
-- Medication Code System Queries
-- ============================================================================
-- This file contains practical SQL queries for querying medication codes
-- in your clinical trials system
-- ============================================================================

-- ============================================================================
-- 1. ENRICHMENT STATUS & COVERAGE
-- ============================================================================

-- Summary: Overall enrichment coverage with RxNorm codes
SELECT 
    COUNT(*) AS total_medications,
    COUNT(CASE WHEN primary_code IS NOT NULL 
          AND primary_system = 'http://www.nlm.nih.gov/research/umls/rxnorm' 
          THEN 1 END) AS has_rxnorm,
    COUNT(CASE WHEN primary_code IS NULL OR 
               primary_system IS NULL OR
               primary_system != 'http://www.nlm.nih.gov/research/umls/rxnorm' 
          THEN 1 END) AS missing_rxnorm,
    ROUND(100.0 * COUNT(CASE WHEN primary_code IS NOT NULL 
                            AND primary_system = 'http://www.nlm.nih.gov/research/umls/rxnorm' 
                            THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS enrichment_percentage
FROM public.medications;

-- Medications missing RxNorm codes (candidates for enrichment)
SELECT 
    medication_id,
    primary_text AS medication_name,
    primary_code AS existing_code,
    primary_system AS existing_system,
    status,
    meta_last_updated
FROM public.medications
WHERE primary_code IS NULL 
    OR primary_system IS NULL
    OR primary_system != 'http://www.nlm.nih.gov/research/umls/rxnorm'
ORDER BY meta_last_updated DESC NULLS LAST
LIMIT 100;

-- ============================================================================
-- 2. ALL CODE SYSTEMS PRESENT IN DATA
-- ============================================================================

-- Find all unique code systems in the code JSON column
-- This shows what code systems are already present in your FHIR data
WITH code_systems AS (
    SELECT DISTINCT
        JSON_EXTRACT_PATH_TEXT(codes.code_entry, 'system') AS code_system,
        JSON_EXTRACT_PATH_TEXT(codes.code_entry, 'code') AS code_value,
        JSON_EXTRACT_PATH_TEXT(codes.code_entry, 'display') AS code_display
    FROM (
        SELECT 
            medication_id,
            JSON_PARSE(code) AS codes_array
        FROM public.medications
        WHERE code IS NOT NULL
            AND code != 'null'
            AND code != ''
    ) AS meds,
    LATERAL FLATTEN(INPUT => meds.codes_array) AS codes(code_entry)
)
SELECT 
    code_system,
    COUNT(DISTINCT code_value) AS unique_codes,
    COUNT(*) AS total_occurrences
FROM code_systems
WHERE code_system IS NOT NULL
GROUP BY code_system
ORDER BY total_occurrences DESC;

-- Medications with multiple code systems (e.g., RxNorm + SNOMED)
SELECT 
    m.medication_id,
    m.primary_text AS medication_name,
    m.primary_code AS primary_code,
    m.primary_system AS primary_system,
    JSON_ARRAY_LENGTH(JSON_PARSE(m.code)) AS total_code_entries,
    m.code AS all_codes_json
FROM public.medications m
WHERE m.code IS NOT NULL
    AND m.code != 'null'
    AND m.code != ''
    AND JSON_ARRAY_LENGTH(JSON_PARSE(m.code)) > 1
ORDER BY total_code_entries DESC, m.primary_text
LIMIT 50;

-- Extract all codes for a specific medication (shows all code systems)
-- Replace 'YOUR_MEDICATION_ID' with actual medication_id
SELECT 
    m.medication_id,
    m.primary_text AS medication_name,
    codes.code_entry AS individual_code_entry,
    JSON_EXTRACT_PATH_TEXT(codes.code_entry, 'system') AS code_system,
    JSON_EXTRACT_PATH_TEXT(codes.code_entry, 'code') AS code_value,
    JSON_EXTRACT_PATH_TEXT(codes.code_entry, 'display') AS code_display
FROM public.medications m,
LATERAL FLATTEN(INPUT => JSON_PARSE(m.code)) AS codes(code_entry)
WHERE m.code IS NOT NULL
    AND m.code != 'null'
    AND m.code != ''
    -- AND m.medication_id = 'YOUR_MEDICATION_ID'  -- Uncomment and specify
ORDER BY m.primary_text, code_system;

-- ============================================================================
-- 3. RxNorm CODE QUERIES
-- ============================================================================

-- All medications with RxNorm codes
SELECT 
    medication_id,
    primary_text AS medication_name,
    primary_code AS rxnorm_code,
    primary_system AS code_system,
    status,
    meta_last_updated,
    created_at
FROM public.medications
WHERE primary_system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
    AND primary_code IS NOT NULL
ORDER BY primary_text;

-- Medications with RxNorm codes from enrichment (check lookup table)
SELECT 
    m.medication_id,
    m.primary_text AS medication_name,
    m.primary_code AS rxnorm_code,
    lk.enrichment_source,
    lk.confidence_score,
    lk.updated_at AS enrichment_date
FROM public.medications m
JOIN public.medication_code_lookup lk 
    ON UPPER(TRIM(COALESCE(m.primary_text, ''))) = lk.normalized_name
WHERE m.primary_system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
    AND m.primary_code = lk.rxnorm_code
ORDER BY lk.updated_at DESC;

-- Search medications by name pattern (useful for finding specific drugs)
SELECT 
    medication_id,
    primary_text AS medication_name,
    primary_code AS rxnorm_code,
    primary_system AS code_system
FROM public.medications
WHERE primary_system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
    AND (
        primary_text ILIKE '%lisinopril%'
        OR primary_text ILIKE '%metformin%'
        OR primary_text ILIKE '%atorvastatin%'
        -- Add more patterns as needed
    )
ORDER BY primary_text;

-- ============================================================================
-- 4. SNOMED CT CODE QUERIES (if present in data)
-- ============================================================================

-- Medications with SNOMED CT codes
SELECT 
    m.medication_id,
    m.primary_text AS medication_name,
    JSON_EXTRACT_PATH_TEXT(codes.code_entry, 'code') AS snomed_code,
    JSON_EXTRACT_PATH_TEXT(codes.code_entry, 'display') AS snomed_display
FROM public.medications m,
LATERAL FLATTEN(INPUT => JSON_PARSE(m.code)) AS codes(code_entry)
WHERE JSON_EXTRACT_PATH_TEXT(codes.code_entry, 'system') = 'http://snomed.info/sct'
    OR JSON_EXTRACT_PATH_TEXT(codes.code_entry, 'system') LIKE '%snomed%'
ORDER BY m.primary_text;

-- Count medications by code system (including SNOMED)
SELECT 
    CASE 
        WHEN JSON_EXTRACT_PATH_TEXT(codes.code_entry, 'system') = 'http://snomed.info/sct' 
            OR JSON_EXTRACT_PATH_TEXT(codes.code_entry, 'system') LIKE '%snomed%'
        THEN 'SNOMED CT'
        WHEN JSON_EXTRACT_PATH_TEXT(codes.code_entry, 'system') = 'http://www.nlm.nih.gov/research/umls/rxnorm'
            OR JSON_EXTRACT_PATH_TEXT(codes.code_entry, 'system') LIKE '%rxnorm%'
        THEN 'RxNorm'
        WHEN JSON_EXTRACT_PATH_TEXT(codes.code_entry, 'system') LIKE '%ndc%'
        THEN 'NDC'
        WHEN JSON_EXTRACT_PATH_TEXT(codes.code_entry, 'system') LIKE '%unii%'
        THEN 'UNII'
        ELSE JSON_EXTRACT_PATH_TEXT(codes.code_entry, 'system')
    END AS code_system_category,
    COUNT(DISTINCT m.medication_id) AS medication_count
FROM public.medications m,
LATERAL FLATTEN(INPUT => JSON_PARSE(m.code)) AS codes(code_entry)
WHERE m.code IS NOT NULL
    AND m.code != 'null'
    AND m.code != ''
GROUP BY code_system_category
ORDER BY medication_count DESC;

-- ============================================================================
-- 5. NDC CODE QUERIES (from identifiers table)
-- ============================================================================

-- Medications with NDC codes (from medication_identifiers table)
SELECT 
    m.medication_id,
    m.primary_text AS medication_name,
    m.primary_code AS rxnorm_code,
    mi.identifier_system,
    mi.identifier_value AS ndc_code
FROM public.medications m
JOIN public.medication_identifiers mi 
    ON m.medication_id = mi.medication_id
WHERE mi.identifier_system LIKE '%ndc%'
    OR mi.identifier_system = 'http://hl7.org/fhir/sid/ndc'
    OR LOWER(mi.identifier_system) LIKE '%national%drug%code%'
ORDER BY m.primary_text;

-- All identifier systems present (shows what code systems are in identifiers)
SELECT 
    identifier_system,
    COUNT(DISTINCT medication_id) AS medication_count,
    COUNT(*) AS total_identifiers
FROM public.medication_identifiers
GROUP BY identifier_system
ORDER BY medication_count DESC;

-- ============================================================================
-- 6. ENRICHMENT LOOKUP TABLE QUERIES
-- ============================================================================

-- View all entries in enrichment lookup table
SELECT 
    normalized_name,
    medication_name,
    rxnorm_code,
    rxnorm_system,
    confidence_score,
    enrichment_source,
    created_at,
    updated_at
FROM public.medication_code_lookup
WHERE rxnorm_code IS NOT NULL
ORDER BY updated_at DESC
LIMIT 100;

-- Statistics on enrichment sources
SELECT 
    enrichment_source,
    COUNT(*) AS enrichment_count,
    ROUND(AVG(confidence_score), 3) AS avg_confidence,
    ROUND(MIN(confidence_score), 3) AS min_confidence,
    ROUND(MAX(confidence_score), 3) AS max_confidence,
    MIN(created_at) AS first_enriched,
    MAX(updated_at) AS last_updated
FROM public.medication_code_lookup
WHERE enrichment_source IS NOT NULL
GROUP BY enrichment_source
ORDER BY enrichment_count DESC;

-- High-confidence enrichments (confidence >= 0.9)
SELECT 
    normalized_name,
    medication_name,
    rxnorm_code,
    confidence_score,
    enrichment_source
FROM public.medication_code_lookup
WHERE confidence_score >= 0.9
    AND rxnorm_code IS NOT NULL
ORDER BY confidence_score DESC, medication_name;

-- Medications that need enrichment (not in lookup table)
SELECT 
    m.medication_id,
    m.primary_text AS medication_name,
    m.primary_code,
    m.primary_system
FROM public.medications m
WHERE (m.primary_code IS NULL 
    OR m.primary_system != 'http://www.nlm.nih.gov/research/umls/rxnorm')
    AND NOT EXISTS (
        SELECT 1 
        FROM public.medication_code_lookup lk
        WHERE UPPER(TRIM(COALESCE(m.primary_text, ''))) = lk.normalized_name
    )
LIMIT 100;

-- ============================================================================
-- 7. CLINICAL TRIALS SPECIFIC QUERIES
-- ============================================================================

-- Find medications by ingredient name (for ingredient-level analysis)
-- Note: This is a name-based search. For true ingredient-level, 
-- you'd need to use RxNorm API to get ingredient relationships
SELECT 
    medication_id,
    primary_text AS medication_name,
    primary_code AS rxnorm_code,
    'Search by name pattern - verify with RxNorm API' AS note
FROM public.medications
WHERE primary_system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
    AND (
        -- Example: Find ACE inhibitors
        primary_text ILIKE '%lisinopril%'
        OR primary_text ILIKE '%enalapril%'
        OR primary_text ILIKE '%captopril%'
        OR primary_text ILIKE '%ramipril%'
        -- Add more ingredients as needed
    )
ORDER BY primary_text;

-- Medications with both RxNorm and NDC codes (complete identification)
SELECT 
    m.medication_id,
    m.primary_text AS medication_name,
    m.primary_code AS rxnorm_code,
    STRING_AGG(DISTINCT mi.identifier_value, ', ') AS ndc_codes,
    COUNT(DISTINCT mi.identifier_value) AS ndc_count
FROM public.medications m
JOIN public.medication_identifiers mi 
    ON m.medication_id = mi.medication_id
WHERE m.primary_system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
    AND m.primary_code IS NOT NULL
    AND (mi.identifier_system LIKE '%ndc%'
        OR mi.identifier_system = 'http://hl7.org/fhir/sid/ndc')
GROUP BY m.medication_id, m.primary_text, m.primary_code
HAVING COUNT(DISTINCT mi.identifier_value) > 0
ORDER BY m.primary_text;

-- Recent medication updates (for monitoring data quality)
SELECT 
    medication_id,
    primary_text AS medication_name,
    primary_code,
    primary_system,
    status,
    meta_last_updated,
    updated_at AS record_updated_at
FROM public.medications
WHERE meta_last_updated >= CURRENT_DATE - INTERVAL '30 days'
    OR updated_at >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY meta_last_updated DESC NULLS LAST, updated_at DESC
LIMIT 100;

-- ============================================================================
-- 8. DATA QUALITY & VALIDATION QUERIES
-- ============================================================================

-- Medications with missing codes
SELECT 
    COUNT(*) AS missing_codes_count,
    COUNT(DISTINCT medication_id) AS unique_medications
FROM public.medications
WHERE primary_code IS NULL 
    AND primary_text IS NOT NULL  -- Has name but no code
    AND primary_text != '';

-- Code system consistency check
SELECT 
    primary_system,
    COUNT(*) AS count,
    COUNT(DISTINCT medication_id) AS unique_medications
FROM public.medications
WHERE primary_system IS NOT NULL
GROUP BY primary_system
ORDER BY count DESC;

-- Duplicate RxNorm codes (same code, different medication_id - may indicate duplicates)
SELECT 
    primary_code AS rxnorm_code,
    primary_text AS medication_name,
    COUNT(DISTINCT medication_id) AS medication_count,
    STRING_AGG(DISTINCT medication_id, ', ') AS medication_ids
FROM public.medications
WHERE primary_system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
    AND primary_code IS NOT NULL
GROUP BY primary_code, primary_text
HAVING COUNT(DISTINCT medication_id) > 1
ORDER BY medication_count DESC
LIMIT 50;

-- ============================================================================
-- NOTES
-- ============================================================================
-- 1. Replace 'YOUR_MEDICATION_ID' with actual medication_id values when using specific queries
-- 2. The code SUPER column contains JSON array of all code.coding entries from FHIR
-- 3. Use JSON_PARSE() and LATERAL FLATTEN() to extract individual codes
-- 4. medication_identifiers table may contain NDC, UNII, or other identifier codes
-- 5. For ingredient-level queries, consider using RxNorm API to get ingredient relationships
-- 6. Enrichment lookup table caches enrichment results to reduce API calls
-- ============================================================================

