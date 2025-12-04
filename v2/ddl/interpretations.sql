-- Table: interpretations
-- Normalized interpretations table - stores unique observation interpretations to avoid duplication
-- Updated: 2025-12-03 - Created normalized interpretations table for storage efficiency
--
-- This table stores each unique interpretation (interpretation_code + interpretation_system combination) once,
-- reducing storage from ~45MB to ~150 bytes for interpretations data.
-- The observation_interpretations table references interpretations via interpretation_id (hash of interpretation_code + interpretation_system).
--
-- Redshift Notes:
-- - Uses hash-based interpretation_id (MD5 hash of interpretation_code + interpretation_system) for deterministic IDs
-- - No foreign key constraints (Redshift doesn't enforce them)
-- - DISTKEY on interpretation_system for even distribution
-- - SORTKEY on (interpretation_system, interpretation_code) for efficient lookups

CREATE TABLE IF NOT EXISTS public.interpretations (
    interpretation_id BIGINT NOT NULL,  -- Hash of interpretation_code + interpretation_system (deterministic)
    interpretation_code VARCHAR(50) NOT NULL,
    interpretation_system VARCHAR(255) NOT NULL,
    interpretation_display VARCHAR(255),
    interpretation_text VARCHAR(100),  -- Truncated to 100 chars (was 65535) - usually short text
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
DISTKEY (interpretation_system)  -- Distribute by system
SORTKEY (interpretation_system, interpretation_code);  -- Optimize for lookups by system+code

