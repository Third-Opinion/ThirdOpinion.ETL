-- Migration: Create condition tables
-- Date: 2024-01-01 00:03:00
-- Description: Creates all condition-related tables
-- Tables: conditions + all condition_* junction and detail tables

-- Up Migration
-- ============================================
-- Forward migration: applies the changes
-- ============================================

-- Table: conditions
-- Main FHIR Condition table with co-located distribution
-- Updated: 2025-01-XX - v2 schema with effective_datetime, status, and diagnosis_name fields
-- Versioning: Uses meta_last_updated for version comparison (no separate version_id column)

CREATE TABLE IF NOT EXISTS public.conditions (
    condition_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    encounter_id VARCHAR(255),
    clinical_status_code VARCHAR(50),
    clinical_status_display VARCHAR(255),
    clinical_status_system VARCHAR(255),
    verification_status_code VARCHAR(50),
    verification_status_display VARCHAR(255),
    verification_status_system VARCHAR(255),
    condition_text VARCHAR(500),
    diagnosis_name VARCHAR(500),
    severity_code VARCHAR(50),
    severity_display VARCHAR(255),
    severity_system VARCHAR(255),
    onset_datetime TIMESTAMP,
    onset_age_value DECIMAL(10,2),
    onset_age_unit VARCHAR(20),
    onset_period_start TIMESTAMP,
    onset_period_end TIMESTAMP,
    onset_text VARCHAR(500),
    abatement_datetime TIMESTAMP,
    abatement_age_value DECIMAL(10,2),
    abatement_age_unit VARCHAR(20),
    abatement_period_start TIMESTAMP,
    abatement_period_end TIMESTAMP,
    abatement_text VARCHAR(500),
    abatement_boolean BOOLEAN,
    recorded_date TIMESTAMP,
    effective_datetime TIMESTAMP,
    status VARCHAR(50),
    recorder_type VARCHAR(50),
    recorder_id VARCHAR(255),
    asserter_type VARCHAR(50),
    asserter_id VARCHAR(255),
    meta_last_updated TIMESTAMP,
    meta_source VARCHAR(255),
    meta_profile VARCHAR(MAX),
    meta_security VARCHAR(MAX),
    meta_tag VARCHAR(MAX),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
DISTKEY (patient_id) 
SORTKEY (patient_id, effective_datetime);

-- Table: condition_codes
-- FHIR Condition code.coding array - references codes table via code_id
-- Updated: 2025-01-XX - Uses codes table reference for storage efficiency
--
-- This normalized structure reduces storage significantly:
-- - codes table: ~100MB (unique codes stored once, shared with observations)
-- - condition_codes: ~500MB (just condition_id + code_id references)
-- Total: ~600MB vs previous ~5GB+ = 90%+ reduction
--
-- Redshift Notes:
-- - No foreign key constraints (Redshift doesn't enforce them, enforce at application level)
-- - DISTKEY on patient_id to co-locate with conditions table
-- - SORTKEY optimized for common query patterns

CREATE TABLE IF NOT EXISTS public.condition_codes (
    condition_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    code_id BIGINT NOT NULL,  -- Reference to codes.code_id (hash of code_code + code_system)
    code_rank INTEGER,  -- Order of code in the coding array (optional, for preserving order)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
DISTKEY (patient_id)  -- Co-locate with conditions table by patient
SORTKEY (patient_id, condition_id, code_rank);  -- Optimize for patient-based queries and code lookups

-- Note: Redshift doesn't support indexes or foreign key constraints
-- Uniqueness and referential integrity should be enforced at application level

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

-- Table: condition_stages
-- FHIR Condition stage.assessment - denormalized (can be Reference or CodeableConcept)
-- Updated: 2025-01-XX - v2 schema with normalized summary and type
--
-- Note: stage.summary and stage.type are now normalized to codes table via:
-- - condition_stage_summaries (references codes.code_id)
-- - condition_stage_types (references codes.code_id)
--
-- stage.assessment remains denormalized because it can be either:
-- - Reference (Observation/Procedure) - stored in stage_assessment_code as reference string
-- - CodeableConcept - stored with code/system/display
--
-- This hybrid approach provides storage savings for summary/type while handling
-- the mixed nature of assessment field.

CREATE TABLE IF NOT EXISTS public.condition_stages (
    condition_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    meta_last_updated TIMESTAMP,
    stage_assessment_code VARCHAR(255),  -- Can be Reference string or code
    stage_assessment_system VARCHAR(255),  -- Only populated if CodeableConcept
    stage_assessment_display VARCHAR(255),  -- Only populated if CodeableConcept
    stage_rank INTEGER,  -- Order of stage in the stage array (optional, for preserving order)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
DISTKEY (patient_id)  -- Co-locate with conditions table by patient
SORTKEY (patient_id, condition_id, stage_rank);  -- Optimize for patient-based queries

-- Note: Redshift doesn't support indexes or foreign key constraints
-- Uniqueness and referential integrity should be enforced at application level

-- Table: condition_stage_types
-- FHIR Condition stage.type - references codes table via code_id
-- Updated: 2025-01-XX - Normalized to use codes table reference for storage efficiency
--
-- This normalized structure reduces storage significantly:
-- - codes table: ~100MB (unique codes stored once, shared with observations and conditions)
-- - condition_stage_types: ~XXMB (just condition_id + code_id references)
-- Total: ~XXMB vs previous ~XXMB+ = significant reduction
--
-- Redshift Notes:
-- - No foreign key constraints (Redshift doesn't enforce them, enforce at application level)
-- - DISTKEY on patient_id to co-locate with conditions table
-- - SORTKEY optimized for common query patterns

CREATE TABLE IF NOT EXISTS public.condition_stage_types (
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

-- Table: condition_evidence
-- Normalized table for condition evidence
-- Each condition can have multiple evidence items (one row per evidence)
-- Updated: 2025-01-XX - v2 schema

CREATE TABLE IF NOT EXISTS public.condition_evidence (
    condition_id VARCHAR(255),
    meta_last_updated TIMESTAMP,
    evidence_code VARCHAR(50),
    evidence_system VARCHAR(255),
    evidence_display VARCHAR(255),
    evidence_detail_reference VARCHAR(255)
) 
SORTKEY (condition_id, evidence_code);

-- Table: condition_extensions
-- Normalized table for condition extensions
-- Each condition can have multiple extensions (one row per extension)
-- Supports nested extensions and various value types
-- Updated: 2025-01-XX - v2 schema

CREATE TABLE IF NOT EXISTS public.condition_extensions (
    condition_id VARCHAR(255),
    extension_url VARCHAR(500),
    extension_type VARCHAR(50),
    value_type VARCHAR(50),
    value_string VARCHAR(MAX),
    value_datetime TIMESTAMP,
    value_reference VARCHAR(255),
    value_code VARCHAR(100),
    value_boolean BOOLEAN,
    value_decimal DECIMAL(18,6),
    value_integer INTEGER,
    parent_extension_url VARCHAR(500),
    extension_order INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
SORTKEY (condition_id, extension_url, extension_order);

-- Table: condition_notes
-- Normalized table for condition notes
-- Each condition can have multiple notes (one row per note)
-- Updated: 2025-01-XX - v2 schema

CREATE TABLE IF NOT EXISTS public.condition_notes (
    condition_id VARCHAR(255),
    meta_last_updated TIMESTAMP,
    note_text VARCHAR(MAX),
    note_author_reference VARCHAR(255),
    note_time TIMESTAMP
) 
SORTKEY (condition_id, note_time);

-- Down Migration
-- ============================================
-- Rollback migration: reverses the changes
-- ============================================

-- Drop tables in reverse dependency order (detail tables first, then main table)
DROP TABLE IF EXISTS public.condition_notes CASCADE;
DROP TABLE IF EXISTS public.condition_extensions CASCADE;
DROP TABLE IF EXISTS public.condition_evidence CASCADE;
DROP TABLE IF EXISTS public.condition_stage_summaries CASCADE;
DROP TABLE IF EXISTS public.condition_stage_types CASCADE;
DROP TABLE IF EXISTS public.condition_stages CASCADE;
DROP TABLE IF EXISTS public.condition_body_sites CASCADE;
DROP TABLE IF EXISTS public.condition_categories CASCADE;
DROP TABLE IF EXISTS public.condition_codes CASCADE;
DROP TABLE IF EXISTS public.conditions CASCADE;

