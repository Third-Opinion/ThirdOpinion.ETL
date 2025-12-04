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
