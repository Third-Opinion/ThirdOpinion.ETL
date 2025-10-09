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
