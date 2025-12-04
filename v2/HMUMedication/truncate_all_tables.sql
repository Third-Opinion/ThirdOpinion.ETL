-- Truncate all HMUMedication ETL related tables to reset bookmarks and force full re-import
-- 
-- This script will:
-- 1. Truncate the main medications table (resets bookmark)
-- 2. Truncate the medication_identifiers child table
-- 3. Optionally truncate medication_code_lookup (enrichment cache)
--
-- After running this, the next ETL run will process ALL records from Iceberg (full load)

-- Truncate main medications table (this resets the bookmark)
TRUNCATE TABLE public.medications;

-- Truncate child table
TRUNCATE TABLE public.medication_identifiers;

-- Optionally truncate enrichment lookup table (if you want to clear cached enrichment results)
-- Uncomment the next line if you want to clear enrichment cache as well:
-- TRUNCATE TABLE public.medication_code_lookup;

-- Verify tables are empty
SELECT 
    'medications' as table_name,
    COUNT(*) as record_count
FROM public.medications
UNION ALL
SELECT 
    'medication_identifiers' as table_name,
    COUNT(*) as record_count
FROM public.medication_identifiers
UNION ALL
SELECT 
    'medication_code_lookup' as table_name,
    COUNT(*) as record_count
FROM public.medication_code_lookup;




