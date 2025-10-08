-- Table: procedures
-- Source: HMUProcedure.py (create_redshift_tables_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.procedures (
        procedure_id VARCHAR(255) NOT NULL,
        resource_type VARCHAR(50),
        status VARCHAR(50),
        patient_id VARCHAR(255) NOT NULL,
        code_text VARCHAR(500),
        performed_date_time TIMESTAMP,
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, performed_date_time);
