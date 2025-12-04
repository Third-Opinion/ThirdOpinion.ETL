-- Table: condition_body_sites
-- FHIR Condition bodySite array - references body_sites table via body_site_id
-- Updated: 2025-01-XX - Normalized to use body_sites table reference for storage efficiency
--
-- This normalized structure reduces storage significantly:
-- - body_sites table: ~1.5KB (only 3 unique body sites stored once)
-- - condition_body_sites: ~170KB (just condition_id + body_site_id references)
-- Total: ~170KB vs previous ~1.65MB = 88% reduction
--
-- Current Data Analysis:
-- - 3,471 rows total
-- - Only 3 unique body sites (SNOMED codes: 419161000, 419465000, 51440002)
-- - Average: ~1,157 repetitions per body site
--
-- Redshift Notes:
-- - No foreign key constraints (Redshift doesn't enforce them, enforce at application level)
-- - DISTKEY on patient_id to co-locate with conditions table
-- - SORTKEY optimized for common query patterns

CREATE TABLE IF NOT EXISTS public.condition_body_sites (
    condition_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    body_site_id BIGINT NOT NULL,  -- Reference to body_sites.body_site_id (hash of body_site_code + body_site_system)
    body_site_rank INTEGER,  -- Order of body site in the bodySite array (optional, for preserving order)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
DISTKEY (patient_id)  -- Co-locate with conditions table by patient
SORTKEY (patient_id, condition_id, body_site_rank);  -- Optimize for patient-based queries and body site lookups

-- Note: Redshift doesn't support indexes or foreign key constraints
-- Uniqueness and referential integrity should be enforced at application level

