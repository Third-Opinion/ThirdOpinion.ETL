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
