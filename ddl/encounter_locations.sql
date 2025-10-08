-- Table: encounter_locations
-- Source: HMUEncounter.py (create_encounter_locations_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.encounter_locations (
        encounter_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        location_id VARCHAR(255)
    ) SORTKEY (encounter_id)
