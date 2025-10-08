-- Table: practitioner_addresses
-- Source: HMUPractitioner.py (create_practitioner_addresses_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.practitioner_addresses (
        practitioner_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        line VARCHAR(500),
        city VARCHAR(100),
        state VARCHAR(50),
        postal_code VARCHAR(20)
    ) SORTKEY (practitioner_id, state);
