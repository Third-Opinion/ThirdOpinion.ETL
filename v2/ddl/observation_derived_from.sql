-- Table: observation_derived_from
-- Source: HMUObservation.py
-- FHIR - References to supporting evidence/source observations
--
-- This table stores references from observations to their source documents or observations.
-- Each observation can have multiple derivedFrom references (one-to-many relationship).
--
-- Common use cases:
-- 1. AI-generated observations referencing source clinical documents
-- 2. Interpreted observations referencing raw measurement observations
-- 3. Summary observations referencing component observations
--
-- Example FHIR structure:
-- "derivedFrom": [
--   {
--     "reference": "DocumentReference/ct-2024-12-20",
--     "display": "Supporting fact: imaging",
--     "type": "DocumentReference"
--   }
-- ]

CREATE TABLE IF NOT EXISTS public.observation_derived_from (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    reference VARCHAR(500),
    reference_type VARCHAR(100),
    reference_id VARCHAR(255),
    display VARCHAR(65535),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
DISTKEY (patient_id)
SORTKEY (observation_id, reference);
