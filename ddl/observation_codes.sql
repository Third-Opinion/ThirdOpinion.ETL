-- Table: observation_codes
-- FHIR Observation code.coding array - multiple codes per observation
-- Updated: 2025-10-09 - Added patient_id for co-location, increased text sizes

CREATE TABLE IF NOT EXISTS public.observation_codes (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    code_code VARCHAR(50),
    code_system VARCHAR(255),
    code_display VARCHAR(255),
    code_text VARCHAR(65535)
) 
DISTKEY (patient_id)
SORTKEY (patient_id, code_code);
