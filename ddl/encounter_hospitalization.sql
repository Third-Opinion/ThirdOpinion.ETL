-- Table: encounter_hospitalization
-- Source: HMUEncounter.py (create_encounter_hospitalization_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.encounter_hospitalization (
        encounter_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        discharge_disposition_text VARCHAR(500),
        discharge_code VARCHAR(50),
        discharge_system VARCHAR(500)
    ) SORTKEY (encounter_id)
