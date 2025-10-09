-- Table: observation_reference_ranges
-- FHIR Observation referenceRange array - normal/expected value ranges
-- Updated: 2025-10-09 - Added patient_id for co-location, increased text sizes

CREATE TABLE IF NOT EXISTS public.observation_reference_ranges (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    range_low_value DECIMAL(15,4),
    range_low_unit VARCHAR(50),
    range_high_value DECIMAL(15,4),
    range_high_unit VARCHAR(50),
    range_type_code VARCHAR(50),
    range_type_system VARCHAR(255),
    range_type_display VARCHAR(255),
    range_text VARCHAR(65535)
) 
DISTKEY (patient_id)
SORTKEY (patient_id, range_type_code);
