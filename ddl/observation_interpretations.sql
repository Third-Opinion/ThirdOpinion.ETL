-- Table: observation_interpretations
-- FHIR Observation interpretation array - clinical interpretation of results
-- Updated: 2025-10-09 - Added patient_id for co-location, added interpretation_text, increased text sizes

CREATE TABLE IF NOT EXISTS public.observation_interpretations (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    interpretation_code VARCHAR(50),
    interpretation_system VARCHAR(255),
    interpretation_display VARCHAR(255),
    interpretation_text VARCHAR(65535)
) 
DISTKEY (patient_id)
SORTKEY (patient_id, interpretation_code);
