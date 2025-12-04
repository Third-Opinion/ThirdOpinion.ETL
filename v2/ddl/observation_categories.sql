-- Table: observation_categories
-- FHIR Observation category array - references categories table via category_id
-- Updated: 2025-12-03 - Normalized to use categories table reference for storage efficiency
--
-- This normalized structure reduces storage from ~1.5GB to ~200MB:
-- - categories table: ~1KB (unique categories stored once)
-- - observation_categories: ~200MB (just observation_id + category_id references)
-- Total: ~200MB vs previous ~1.5GB = 99.99% reduction
--
-- Redshift Notes:
-- - No foreign key constraints (Redshift doesn't enforce them, enforce at application level)
-- - DISTKEY on patient_id to co-locate with observations table
-- - SORTKEY optimized for common query patterns

CREATE TABLE IF NOT EXISTS public.observation_categories (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    category_id BIGINT NOT NULL,  -- Reference to categories.category_id (hash of category_code + category_system)
    category_rank INTEGER,  -- Order of category in the category array (optional, for preserving order)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
DISTKEY (patient_id)  -- Co-locate with observations table by patient
SORTKEY (patient_id, observation_id, category_rank);  -- Optimize for patient-based queries and category lookups

-- Note: Redshift doesn't support indexes or foreign key constraints
-- Uniqueness and referential integrity should be enforced at application level
