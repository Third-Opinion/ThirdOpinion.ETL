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
