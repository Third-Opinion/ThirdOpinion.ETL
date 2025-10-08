-- Table: observation_interpretations
-- Source: HMUObservation.py (create_observation_interpretations_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.observation_interpretations (
        observation_id VARCHAR(255),
        interpretation_code VARCHAR(50),
        interpretation_system VARCHAR(255),
        interpretation_display VARCHAR(255)
    ) SORTKEY (observation_id, interpretation_code)
