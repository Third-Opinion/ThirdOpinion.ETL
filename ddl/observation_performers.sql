-- Table: observation_performers
-- Source: HMUObservation.py (create_observation_performers_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.observation_performers (
        observation_id VARCHAR(255),
        performer_type VARCHAR(50),
        performer_id VARCHAR(255)
    ) SORTKEY (observation_id, performer_type)
