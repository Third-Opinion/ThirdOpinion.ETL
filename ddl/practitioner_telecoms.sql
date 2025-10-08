-- Table: practitioner_telecoms
-- Source: HMUPractitioner.py (create_practitioner_telecoms_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.practitioner_telecoms (
        practitioner_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        "system" VARCHAR(50),
        value VARCHAR(255)
    ) SORTKEY (practitioner_id, "system");
