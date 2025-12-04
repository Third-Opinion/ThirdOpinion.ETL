-- Table: condition_stages
-- FHIR Condition stage.assessment - denormalized (can be Reference or CodeableConcept)
-- Updated: 2025-01-XX - v2 schema with normalized summary and type
--
-- Note: stage.summary and stage.type are now normalized to codes table via:
-- - condition_stage_summaries (references codes.code_id)
-- - condition_stage_types (references codes.code_id)
--
-- stage.assessment remains denormalized because it can be either:
-- - Reference (Observation/Procedure) - stored in stage_assessment_code as reference string
-- - CodeableConcept - stored with code/system/display
--
-- This hybrid approach provides storage savings for summary/type while handling
-- the mixed nature of assessment field.

CREATE TABLE IF NOT EXISTS public.condition_stages (
    condition_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    meta_last_updated TIMESTAMP,
    stage_assessment_code VARCHAR(255),  -- Can be Reference string or code
    stage_assessment_system VARCHAR(255),  -- Only populated if CodeableConcept
    stage_assessment_display VARCHAR(255),  -- Only populated if CodeableConcept
    stage_rank INTEGER,  -- Order of stage in the stage array (optional, for preserving order)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
DISTKEY (patient_id)  -- Co-locate with conditions table by patient
SORTKEY (patient_id, condition_id, stage_rank);  -- Optimize for patient-based queries

-- Note: Redshift doesn't support indexes or foreign key constraints
-- Uniqueness and referential integrity should be enforced at application level


