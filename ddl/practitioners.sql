-- Table: practitioners
-- Source: HMUPractitioner.py (create_redshift_tables_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.practitioners (
        practitioner_id VARCHAR(255) NOT NULL DISTKEY,
        resource_type VARCHAR(50),
        active BOOLEAN,
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTSTYLE KEY SORTKEY (practitioner_id);
