-- Table: medications
-- Source: HMUMedication.py (create_redshift_tables_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

-- Main medications table
    CREATE TABLE IF NOT EXISTS public.medications (
        medication_id VARCHAR(255) NOT NULL,
        resource_type VARCHAR(50),
        code_text VARCHAR(500),
        code SUPER,
        primary_code VARCHAR(100),
        primary_system VARCHAR(255),
        primary_text VARCHAR(500),
        status VARCHAR(50),
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) SORTKEY (medication_id);
