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

