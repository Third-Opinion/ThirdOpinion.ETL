-- Table: encounter_reasons
-- Source: HMUEncounter.py (create_encounter_reasons_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.encounter_reasons (
        encounter_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        reason_code VARCHAR(50),
        reason_system VARCHAR(255),
        reason_display VARCHAR(255),
        reason_text VARCHAR(500)
    ) SORTKEY (encounter_id, reason_code)
