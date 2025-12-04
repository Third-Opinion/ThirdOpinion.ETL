-- Migration: Create observation tables
-- Date: 2024-01-01 00:02:00
-- Description: Creates all observation-related tables
-- Tables: observations + all observation_* junction and detail tables

-- Up Migration
-- ============================================
-- Forward migration: applies the changes
-- ============================================

-- Table: observations
-- Main FHIR Observation table with co-located distribution
-- Updated: 2025-01-XX - Normalized code-based fields to use codes table references
--
-- Normalization Changes:
-- - Body sites: Moved to observation_body_sites junction table (references body_sites table)
-- - Method codes: Normalized via codes table (method_code_id)
-- - Data absent reason codes: Normalized via codes table (data_absent_reason_code_id)
-- - Value codeable concept codes: Normalized via codes table (value_code_id)
-- - Keep denormalized: value_codeable_concept_text (unique text per observation)

CREATE TABLE IF NOT EXISTS public.observations (
    observation_id VARCHAR(255),
    patient_id VARCHAR(255) NOT NULL,
    encounter_id VARCHAR(255),
    specimen_id VARCHAR(255),
    status VARCHAR(50),
    observation_text VARCHAR(65535),
    normalized_observation_text VARCHAR(65535),
    value_string VARCHAR(65535),
    value_quantity_value DECIMAL(15,4),
    value_quantity_unit VARCHAR(50),
    value_quantity_system VARCHAR(255),
    value_code_id BIGINT,  -- Reference to codes.code_id (for valueCodeableConcept.code)
    value_codeable_concept_text VARCHAR(65535),  -- Keep denormalized (unique text per observation)
    value_datetime TIMESTAMP,
    value_boolean BOOLEAN,
    data_absent_reason_code_id BIGINT,  -- Reference to codes.code_id (for dataAbsentReason.code)
    effective_datetime TIMESTAMP,
    effective_period_start TIMESTAMP,
    effective_period_end TIMESTAMP,
    issued TIMESTAMP,
    method_code_id BIGINT,  -- Reference to codes.code_id (for method.code)
    method_text VARCHAR(65535),  -- Keep denormalized (unique text per observation)
    meta_last_updated TIMESTAMP,
    meta_source VARCHAR(255),
    meta_profile VARCHAR(65535),
    meta_security VARCHAR(65535),
    meta_tag VARCHAR(65535),
    extensions VARCHAR(65535),
    derived_from SUPER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
DISTKEY (patient_id) 
SORTKEY (patient_id, effective_datetime);

-- Table: observation_codes
-- FHIR Observation code.coding array - references codes table via code_id
-- Updated: 2025-12-03 - Uses codes table reference for storage efficiency
--
-- This normalized structure reduces storage from ~21GB to ~1GB:
-- - codes table: ~100MB (unique codes stored once)
-- - observation_codes: ~1GB (just observation_id + code_id references)
-- Total: ~1.1GB vs previous ~21GB = 95% reduction
--
-- Redshift Notes:
-- - No foreign key constraints (Redshift doesn't enforce them, enforce at application level)
-- - DISTKEY on patient_id to co-locate with observations table
-- - SORTKEY optimized for common query patterns

CREATE TABLE IF NOT EXISTS public.observation_codes (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    code_id BIGINT NOT NULL,  -- Reference to codes.code_id (hash of code_code + code_system)
    code_rank INTEGER,  -- Order of code in the coding array (optional, for preserving order)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
DISTKEY (patient_id)  -- Co-locate with observations table by patient
SORTKEY (patient_id, observation_id, code_rank);  -- Optimize for patient-based queries and code lookups

-- Note: Redshift doesn't support indexes or foreign key constraints
-- Uniqueness and referential integrity should be enforced at application level

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

-- Table: observation_components
-- FHIR Observation component array - sub-observations (e.g., blood pressure systolic/diastolic)
-- Updated: 2025-01-XX - Normalized code-based fields to use codes table references
--
-- Normalization Changes:
-- - Component codes: Normalized via codes table (component_code_id)
-- - Component value codeable concept codes: Normalized via codes table (component_value_code_id)
-- - Component data absent reason codes: Normalized via codes table (component_data_absent_reason_code_id)
-- - Keep denormalized: component_text, component values (unique per component)

CREATE TABLE IF NOT EXISTS public.observation_components (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    component_code_id BIGINT,  -- Reference to codes.code_id (for component.code)
    component_text VARCHAR(65535),  -- Keep denormalized (unique text per component)
    component_value_string VARCHAR(65535),
    component_value_quantity_value DECIMAL(15,4),
    component_value_quantity_unit VARCHAR(50),
    component_value_code_id BIGINT,  -- Reference to codes.code_id (for component.valueCodeableConcept.code)
    component_data_absent_reason_code_id BIGINT  -- Reference to codes.code_id (for component.dataAbsentReason.code)
) 
DISTKEY (patient_id)
SORTKEY (patient_id, component_code_id);

-- Table: observation_reference_ranges
-- FHIR Observation referenceRange array - normal/expected value ranges
-- Updated: 2025-01-XX - Normalized range type codes to use codes table reference
--
-- Normalization Changes:
-- - Range type codes: Normalized via codes table (range_type_code_id)
-- - Keep denormalized: range values, units, text (unique per reference range)

CREATE TABLE IF NOT EXISTS public.observation_reference_ranges (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    range_low_value DECIMAL(15,4),
    range_low_unit VARCHAR(50),
    range_high_value DECIMAL(15,4),
    range_high_unit VARCHAR(50),
    range_type_code_id BIGINT,  -- Reference to codes.code_id (for referenceRange.type.code)
    range_text VARCHAR(65535)
) 
DISTKEY (patient_id)
SORTKEY (patient_id, range_type_code_id);

-- Table: observation_performers
-- FHIR Observation performer array - who performed/validated the observation
-- Updated: 2025-10-09 - Added patient_id for co-location

CREATE TABLE IF NOT EXISTS public.observation_performers (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    performer_type VARCHAR(50),
    performer_id VARCHAR(255)
) 
DISTKEY (patient_id)
SORTKEY (patient_id, performer_type);

-- Table: observation_derived_from
-- Source: HMUObservation.py
-- FHIR - References to supporting evidence/source observations
--
-- This table stores references from observations to their source documents or observations.
-- Each observation can have multiple derivedFrom references (one-to-many relationship).
--
-- Common use cases:
-- 1. AI-generated observations referencing source clinical documents
-- 2. Interpreted observations referencing raw measurement observations
-- 3. Summary observations referencing component observations
--
-- Example FHIR structure:
-- "derivedFrom": [
--   {
--     "reference": "DocumentReference/ct-2024-12-20",
--     "display": "Supporting fact: imaging",
--     "type": "DocumentReference"
--   }
-- ]

CREATE TABLE IF NOT EXISTS public.observation_derived_from (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    reference VARCHAR(500),
    reference_type VARCHAR(100),
    reference_id VARCHAR(255),
    display VARCHAR(65535),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
DISTKEY (patient_id)
SORTKEY (observation_id, reference);

-- Table: observation_members
-- FHIR Observation hasMember array - grouped observation relationships
-- Updated: 2025-10-09 - Added patient_id for co-location

CREATE TABLE IF NOT EXISTS public.observation_members (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    member_observation_id VARCHAR(255) NOT NULL
) 
DISTKEY (patient_id)
SORTKEY (patient_id, member_observation_id);

-- Table: observation_notes
-- FHIR Observation note array - clinical notes and annotations
-- Updated: 2025-10-09 - Added patient_id for co-location, changed TEXT to VARCHAR(65535)

CREATE TABLE IF NOT EXISTS public.observation_notes (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    note_text VARCHAR(65535),
    note_author_reference VARCHAR(255),
    note_time TIMESTAMP
) 
DISTKEY (patient_id)
SORTKEY (patient_id, note_time);

-- Down Migration
-- ============================================
-- Rollback migration: reverses the changes
-- ============================================

-- Drop tables in reverse dependency order (detail tables first, then main table)
DROP TABLE IF EXISTS public.observation_notes CASCADE;
DROP TABLE IF EXISTS public.observation_members CASCADE;
DROP TABLE IF EXISTS public.observation_derived_from CASCADE;
DROP TABLE IF EXISTS public.observation_performers CASCADE;
DROP TABLE IF EXISTS public.observation_reference_ranges CASCADE;
DROP TABLE IF EXISTS public.observation_components CASCADE;
DROP TABLE IF EXISTS public.observation_interpretations CASCADE;
DROP TABLE IF EXISTS public.observation_body_sites CASCADE;
DROP TABLE IF EXISTS public.observation_categories CASCADE;
DROP TABLE IF EXISTS public.observation_codes CASCADE;
DROP TABLE IF EXISTS public.observations CASCADE;

