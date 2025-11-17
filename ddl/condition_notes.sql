-- Table: condition_notes
-- Source: HMUMyJob.py (create_condition_notes_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.condition_notes (
        condition_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        note_text VARCHAR(MAX),
        note_author_reference VARCHAR(255),
        note_time TIMESTAMP
    ) SORTKEY (condition_id, note_time)
