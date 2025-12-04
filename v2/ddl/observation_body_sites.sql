-- Table: observation_body_sites
-- FHIR Observation bodySite array - references body_sites table via body_site_id
-- Updated: 2025-01-XX - Normalized to use body_sites table reference for storage efficiency
--
-- This normalized structure reduces storage and ensures consistency with conditions:
-- - body_sites table: Shared with conditions (only unique body sites stored once)
-- - observation_body_sites: Just observation_id + body_site_id references
-- Total: Significantly reduced storage vs denormalized body site columns
--
-- Redshift Notes:
-- - No foreign key constraints (Redshift doesn't enforce them, enforce at application level)
-- - DISTKEY on patient_id to co-locate with observations table
-- - SORTKEY optimized for common query patterns

CREATE TABLE IF NOT EXISTS public.observation_body_sites (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    body_site_id BIGINT NOT NULL,  -- Reference to body_sites.body_site_id (hash of body_site_code + body_site_system)
    body_site_rank INTEGER,  -- Order of body site in the bodySite array (optional, for preserving order)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
DISTKEY (patient_id)  -- Co-locate with observations table by patient
SORTKEY (patient_id, observation_id, body_site_rank);  -- Optimize for patient-based queries and body site lookups

-- Note: Redshift doesn't support indexes or foreign key constraints
-- Uniqueness and referential integrity should be enforced at application level


