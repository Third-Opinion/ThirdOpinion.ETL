-- Table: condition_codes
-- FHIR Condition code.coding array - references codes table via code_id
-- Updated: 2025-01-XX - Uses codes table reference for storage efficiency
--
-- This normalized structure reduces storage significantly:
-- - codes table: ~100MB (unique codes stored once, shared with observations)
-- - condition_codes: ~500MB (just condition_id + code_id references)
-- Total: ~600MB vs previous ~5GB+ = 90%+ reduction
--
-- Redshift Notes:
-- - No foreign key constraints (Redshift doesn't enforce them, enforce at application level)
-- - DISTKEY on patient_id to co-locate with conditions table
-- - SORTKEY optimized for common query patterns

CREATE TABLE IF NOT EXISTS public.condition_codes (
    condition_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    code_id BIGINT NOT NULL,  -- Reference to codes.code_id (hash of code_code + code_system)
    code_rank INTEGER,  -- Order of code in the coding array (optional, for preserving order)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
DISTKEY (patient_id)  -- Co-locate with conditions table by patient
SORTKEY (patient_id, condition_id, code_rank);  -- Optimize for patient-based queries and code lookups

-- Note: Redshift doesn't support indexes or foreign key constraints
-- Uniqueness and referential integrity should be enforced at application level

