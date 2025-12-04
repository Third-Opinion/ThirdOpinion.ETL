-- Table: observation_interpretations
-- FHIR Observation interpretation array - references interpretations table via interpretation_id
-- Updated: 2025-12-03 - Normalized to use interpretations table reference for storage efficiency
--
-- This normalized structure reduces storage from ~45MB to ~50KB:
-- - interpretations table: ~150 bytes (unique interpretations stored once)
-- - observation_interpretations: ~50KB (just observation_id + interpretation_id references)
-- Total: ~50KB vs previous ~45MB = 99.99% reduction
--
-- Redshift Notes:
-- - No foreign key constraints (Redshift doesn't enforce them, enforce at application level)
-- - DISTKEY on patient_id to co-locate with observations table
-- - SORTKEY optimized for common query patterns

CREATE TABLE IF NOT EXISTS public.observation_interpretations (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    interpretation_id BIGINT NOT NULL,  -- Reference to interpretations.interpretation_id (hash of interpretation_code + interpretation_system)
    interpretation_rank INTEGER,  -- Order of interpretation in the interpretation array (optional, for preserving order)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
DISTKEY (patient_id)  -- Co-locate with observations table by patient
SORTKEY (patient_id, observation_id, interpretation_rank);  -- Optimize for patient-based queries and interpretation lookups

-- Note: Redshift doesn't support indexes or foreign key constraints
-- Uniqueness and referential integrity should be enforced at application level
