-- Reset bookmark for HMUMedication ETL by truncating the main table
-- This will force the next ETL run to process ALL records (full load)
--
-- The bookmark is derived from MAX(meta_last_updated) in the medications table
-- Truncating this table resets the bookmark to NULL, causing full load

TRUNCATE TABLE public.medications;

-- Optionally truncate child tables as well
TRUNCATE TABLE public.medication_identifiers;

-- Verify the table is empty (bookmark will be NULL)
SELECT 
    'medications' as table_name,
    COUNT(*) as record_count,
    MAX(meta_last_updated) as max_timestamp
FROM public.medications;

-- If you see 0 records and NULL timestamp, the bookmark is reset successfully




