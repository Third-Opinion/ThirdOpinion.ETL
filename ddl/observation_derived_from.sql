-- Table: observation_derived_from
-- FHIR Observation derivedFrom array - source observations/documents
-- Updated: 2025-10-09 - Added patient_id for co-location

CREATE TABLE IF NOT EXISTS public.observation_derived_from (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    derived_from_reference VARCHAR(255) NOT NULL
) 
DISTKEY (patient_id)
SORTKEY (patient_id, derived_from_reference);
