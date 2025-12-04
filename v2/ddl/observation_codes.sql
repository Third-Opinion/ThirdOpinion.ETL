-- Table: observation_codes
-- FHIR Observation code.coding array - references codes table via code_id
-- Updated: 2025-12-03 - Uses codes table reference for storage efficiency
--
-- This normalized structure reduces storage from ~21GB to ~1GB:
-- - codes table: ~100MB (unique codes stored once)
-- - observation_codes: ~1GB (just observation_id + code_id references)
-- Total: ~1.1GB vs previous ~21GB = 95% reduction
--
-- Redshift Notes:
-- - No foreign key constraints (Redshift doesn't enforce them, enforce at application level)
-- - DISTKEY on patient_id to co-locate with observations table
-- - SORTKEY optimized for common query patterns

CREATE TABLE IF NOT EXISTS public.observation_codes (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    code_id BIGINT NOT NULL,  -- Reference to codes.code_id (hash of code_code + code_system)
    code_rank INTEGER,  -- Order of code in the coding array (optional, for preserving order)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
DISTKEY (patient_id)  -- Co-locate with observations table by patient
SORTKEY (patient_id, observation_id, code_rank);  -- Optimize for patient-based queries and code lookups

-- Note: Redshift doesn't support indexes or foreign key constraints
-- Uniqueness and referential integrity should be enforced at application level

