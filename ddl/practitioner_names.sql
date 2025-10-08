-- Table: practitioner_names
-- Source: HMUPractitioner.py (create_practitioner_names_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.practitioner_names (
        practitioner_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        text VARCHAR(500),
        family VARCHAR(255),
        given VARCHAR(255)
    ) SORTKEY (practitioner_id, family);
