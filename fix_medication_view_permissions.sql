-- ===================================================================
-- QUICK FIX FOR MEDICATION REQUEST VIEW PERMISSIONS
-- ===================================================================
-- Run this script as a superuser or table owner

-- Option 1: Grant permissions to the current user (simplest fix)
DO $$
DECLARE
    current_username TEXT;
BEGIN
    -- Get current user
    SELECT CURRENT_USER INTO current_username;
    
    -- Grant permissions dynamically
    EXECUTE 'GRANT SELECT ON public.medication_requests TO ' || current_username;
    EXECUTE 'GRANT SELECT ON public.medication_request_dosage_instructions TO ' || current_username;
    EXECUTE 'GRANT SELECT ON public.medication_request_dispense_requests TO ' || current_username;
    EXECUTE 'GRANT SELECT ON public.medication_request_substitutions TO ' || current_username;
    
    RAISE NOTICE 'Granted SELECT permissions on medication request tables to user: %', current_username;
END $$;

-- Option 2: If you need to grant to PUBLIC (less secure but works for all users)
-- Uncomment the following if Option 1 doesn't work:

-- GRANT SELECT ON public.medication_requests TO PUBLIC;
-- GRANT SELECT ON public.medication_request_dosage_instructions TO PUBLIC;
-- GRANT SELECT ON public.medication_request_dispense_requests TO PUBLIC;
-- GRANT SELECT ON public.medication_request_substitutions TO PUBLIC;

-- Option 3: Create the view with owner permissions
-- If you can't grant permissions, try creating the view as the table owner
-- This requires knowing who owns the tables

-- First check who owns the tables:
SELECT DISTINCT tableowner 
FROM pg_tables 
WHERE schemaname = 'public' 
    AND tablename LIKE 'medication_request%';

-- Then either:
-- 1. Switch to that user and create the view
-- 2. Or have that user run the CREATE MATERIALIZED VIEW statement