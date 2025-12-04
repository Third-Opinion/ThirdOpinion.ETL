-- Table: condition_stage_summaries
-- FHIR Condition stage.summary - references codes table via code_id
-- Updated: 2025-01-XX - Normalized to use codes table reference for storage efficiency
--
-- This normalized structure reduces storage significantly:
-- - codes table: ~100MB (unique codes stored once, shared with observations and conditions)
-- - condition_stage_summaries: ~XXMB (just condition_id + code_id references)
-- Total: ~XXMB vs previous ~XXMB+ = significant reduction
--
-- Redshift Notes:
-- - No foreign key constraints (Redshift doesn't enforce them, enforce at application level)
-- - DISTKEY on patient_id to co-locate with conditions table
-- - SORTKEY optimized for common query patterns

CREATE TABLE IF NOT EXISTS public.condition_stage_summaries (
    condition_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    code_id BIGINT NOT NULL,  -- Reference to codes.code_id (hash of code_code + code_system)
    stage_rank INTEGER,  -- Order of stage in the stage array (optional, for preserving order)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
DISTKEY (patient_id)  -- Co-locate with conditions table by patient
SORTKEY (patient_id, condition_id, stage_rank);  -- Optimize for patient-based queries and stage lookups

-- Note: Redshift doesn't support indexes or foreign key constraints
-- Uniqueness and referential integrity should be enforced at application level

