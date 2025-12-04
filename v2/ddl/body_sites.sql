-- Table: body_sites
-- Normalized body_sites table - stores unique body sites to avoid duplication
-- Updated: 2025-01-XX - Created normalized body_sites table for storage efficiency
--
-- This table stores each unique body site (body_site_code + body_site_system combination) once,
-- reducing storage significantly for body sites data.
-- The condition_body_sites table references body_sites via body_site_id (hash of body_site_code + body_site_system).
--
-- Current Data Analysis:
-- - Only 3 unique body sites across 3,471 condition_body_sites rows
-- - Average: ~1,157 repetitions per body site
-- - Storage savings: ~88% reduction (1.65MB → 0.17MB)
--
-- Redshift Notes:
-- - Uses hash-based body_site_id (MD5 hash of body_site_code + body_site_system) for deterministic IDs
-- - No foreign key constraints (Redshift doesn't enforce them)
-- - DISTKEY on body_site_system for even distribution
-- - SORTKEY on (body_site_system, body_site_code) for efficient lookups

CREATE TABLE IF NOT EXISTS public.body_sites (
    body_site_id BIGINT NOT NULL,  -- Hash of body_site_code + body_site_system (deterministic)
    body_site_code VARCHAR(50) NOT NULL,
    body_site_system VARCHAR(255) NOT NULL,
    body_site_display VARCHAR(255),
    body_site_text VARCHAR(500),  -- Truncated to 500 chars - descriptive text
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
DISTKEY (body_site_system)  -- Distribute by system (SNOMED, etc.)
SORTKEY (body_site_system, body_site_code);  -- Optimize for lookups by system+code

-- Primary key constraint (Redshift supports this)
ALTER TABLE public.body_sites ADD PRIMARY KEY (body_site_id);


