-- Table: observation_notes
-- Source: HMUObservation.py (create_observation_notes_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.observation_notes (
        observation_id VARCHAR(255),
        note_text TEXT,
        note_author_reference VARCHAR(255),
        note_time TIMESTAMP
    ) SORTKEY (observation_id, note_time)
