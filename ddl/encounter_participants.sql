-- Table: encounter_participants
-- Source: HMUEncounter.py (create_encounter_participants_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.encounter_participants (
        encounter_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        participant_type VARCHAR(50),
        participant_id VARCHAR(255),
        participant_display VARCHAR(255),
        period_start TIMESTAMP,
        period_end TIMESTAMP
    ) SORTKEY (encounter_id, participant_type)
