-- Table: observation_derived_from
-- Source: HMUObservation.py (create_observation_derived_from_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.observation_derived_from (
        observation_id VARCHAR(255),
        derived_from_reference VARCHAR(255)
    ) SORTKEY (observation_id, derived_from_reference)
