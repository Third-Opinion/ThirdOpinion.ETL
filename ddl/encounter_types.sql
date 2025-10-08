-- Table: encounter_types
-- Source: HMUEncounter.py (create_encounter_types_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.encounter_types (
        encounter_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        type_code VARCHAR(50),
        type_system VARCHAR(255),
        type_display VARCHAR(255),
        type_text VARCHAR(500)
    ) SORTKEY (encounter_id, type_code)
