-- Table: observation_members
-- Source: HMUObservation.py (create_observation_members_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.observation_members (
        observation_id VARCHAR(255),
        member_observation_id VARCHAR(255)
    ) SORTKEY (observation_id, member_observation_id)
