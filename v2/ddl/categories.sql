-- Table: categories
-- Normalized categories table - stores unique observation categories to avoid duplication
-- Updated: 2025-12-03 - Created normalized categories table for storage efficiency
--
-- This table stores each unique category (category_code + category_system combination) once,
-- reducing storage from ~1.5GB to ~1KB for categories data.
-- The observation_categories table references categories via category_id (hash of category_code + category_system).
--
-- Redshift Notes:
-- - Uses hash-based category_id (MD5 hash of category_code + category_system) for deterministic IDs
-- - No foreign key constraints (Redshift doesn't enforce them)
-- - DISTKEY on category_system for even distribution
-- - SORTKEY on (category_system, category_code) for efficient lookups

CREATE TABLE IF NOT EXISTS public.categories (
    category_id BIGINT NOT NULL,  -- Hash of category_code + category_system (deterministic)
    category_code VARCHAR(50) NOT NULL,
    category_system VARCHAR(255) NOT NULL,
    category_display VARCHAR(255),
    category_text VARCHAR(500),  -- Truncated to 500 chars (was 65535) - descriptive text
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
DISTKEY (category_system)  -- Distribute by system
SORTKEY (category_system, category_code);  -- Optimize for lookups by system+code

