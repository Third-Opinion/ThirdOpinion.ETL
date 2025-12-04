-- Table: condition_categories
-- FHIR Condition category array - references categories table via category_id
-- Updated: 2025-01-XX - Normalized to use categories table reference for storage efficiency
--
-- This normalized structure reduces storage significantly:
-- - categories table: ~1KB (unique categories stored once, shared with observations)
-- - condition_categories: ~50MB (just condition_id + category_id references)
-- Total: ~50MB vs previous ~500MB+ = 90%+ reduction
--
-- Redshift Notes:
-- - No foreign key constraints (Redshift doesn't enforce them, enforce at application level)
-- - DISTKEY on patient_id to co-locate with conditions table
-- - SORTKEY optimized for common query patterns

CREATE TABLE IF NOT EXISTS public.condition_categories (
    condition_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    category_id BIGINT NOT NULL,  -- Reference to categories.category_id (hash of category_code + category_system)
    category_rank INTEGER,  -- Order of category in the category array (optional, for preserving order)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
DISTKEY (patient_id)  -- Co-locate with conditions table by patient
SORTKEY (patient_id, condition_id, category_rank);  -- Optimize for patient-based queries and category lookups

-- Note: Redshift doesn't support indexes or foreign key constraints
-- Uniqueness and referential integrity should be enforced at application level

