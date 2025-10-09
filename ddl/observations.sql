-- Table: observations
-- Main FHIR Observation table with co-located distribution
-- Updated: 2025-10-09 - Removed primary_* columns, increased text field sizes

CREATE TABLE IF NOT EXISTS public.observations (
    observation_id VARCHAR(255),
    patient_id VARCHAR(255) NOT NULL,
    encounter_id VARCHAR(255),
    specimen_id VARCHAR(255),
    status VARCHAR(50),
    observation_text VARCHAR(65535),
    value_string VARCHAR(65535),
    value_quantity_value DECIMAL(15,4),
    value_quantity_unit VARCHAR(50),
    value_quantity_system VARCHAR(255),
    value_codeable_concept_code VARCHAR(50),
    value_codeable_concept_system VARCHAR(255),
    value_codeable_concept_display VARCHAR(255),
    value_codeable_concept_text VARCHAR(65535),
    value_datetime TIMESTAMP,
    value_boolean BOOLEAN,
    data_absent_reason_code VARCHAR(50),
    data_absent_reason_display VARCHAR(255),
    data_absent_reason_system VARCHAR(255),
    effective_datetime TIMESTAMP,
    effective_period_start TIMESTAMP,
    effective_period_end TIMESTAMP,
    issued TIMESTAMP,
    body_site_code VARCHAR(50),
    body_site_system VARCHAR(255),
    body_site_display VARCHAR(255),
    body_site_text VARCHAR(65535),
    method_code VARCHAR(50),
    method_system VARCHAR(255),
    method_display VARCHAR(255),
    method_text VARCHAR(65535),
    meta_last_updated TIMESTAMP,
    meta_source VARCHAR(255),
    meta_profile VARCHAR(65535),
    meta_security VARCHAR(65535),
    meta_tag VARCHAR(65535),
    extensions VARCHAR(65535),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
DISTKEY (patient_id) 
SORTKEY (patient_id, effective_datetime);
