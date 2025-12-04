-- Migration: Create shared reference tables
-- Date: 2024-01-01 00:01:00
-- Description: Creates normalized reference tables used by both observations and conditions
-- Tables: codes, categories, body_sites, interpretations

-- Up Migration
-- ============================================
-- Forward migration: applies the changes
-- ============================================

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

-- Primary key constraint
ALTER TABLE public.categories ADD PRIMARY KEY (category_id);

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

-- Primary key constraint
ALTER TABLE public.interpretations ADD PRIMARY KEY (interpretation_id);

-- Down Migration
-- ============================================
-- Rollback migration: reverses the changes
-- ============================================

-- Drop tables in reverse dependency order
DROP TABLE IF EXISTS public.interpretations CASCADE;
DROP TABLE IF EXISTS public.body_sites CASCADE;
DROP TABLE IF EXISTS public.categories CASCADE;
DROP TABLE IF EXISTS public.codes CASCADE;
