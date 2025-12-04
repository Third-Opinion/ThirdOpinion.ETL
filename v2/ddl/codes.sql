-- Table: codes
-- Normalized codes table - stores unique codes to avoid duplication
-- Updated: 2025-12-03 - Created normalized codes table for storage efficiency
--
-- This table stores each unique code (code_code + code_system combination) once,
-- reducing storage from ~21GB to ~100MB for codes data.
-- The observation_codes table references codes via code_id (hash of code_code + code_system).
--
-- Redshift Notes:
-- - Uses hash-based code_id (MD5 hash of code_code + code_system) for deterministic IDs
-- - No foreign key constraints (Redshift doesn't enforce them)
-- - DISTKEY on code_system for even distribution
-- - SORTKEY on (code_system, code_code) for efficient lookups

CREATE TABLE IF NOT EXISTS public.codes (
    code_id BIGINT NOT NULL,  -- Hash of code_code + code_system (deterministic)
    code_code VARCHAR(255) NOT NULL,
    code_system VARCHAR(255) NOT NULL,
    code_display VARCHAR(2000),  -- Increased from 500 to handle long display values
    code_text VARCHAR(1000),  -- Truncated to 1000 chars (was 65535) - full text in observations table
    normalized_code_text VARCHAR(500),  -- Reduced from 65535
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
DISTKEY (code_system)  -- Distribute by system (LOINC, SNOMED, etc.)
SORTKEY (code_system, code_code);  -- Optimize for lookups by system+code

-- Primary key constraint (Redshift supports this)
ALTER TABLE public.codes ADD PRIMARY KEY (code_id);

