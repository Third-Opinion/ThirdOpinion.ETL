-- Table: patients
-- Source: HMUPatient.py (create_redshift_tables_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

-- Main patients table
    CREATE TABLE IF NOT EXISTS public.patients (
        patient_id VARCHAR(255) NOT NULL,
        active BOOLEAN,
        gender VARCHAR(10),
        birth_date DATE,
        deceased BOOLEAN,
        deceased_date TIMESTAMP,
        resourcetype VARCHAR(50),
        marital_status_code VARCHAR(50),
        marital_status_display VARCHAR(255),
        marital_status_system VARCHAR(255),
        multiple_birth BOOLEAN,
        birth_order INTEGER,
        managing_organization_id VARCHAR(255),
        photos TEXT,
        meta_last_updated TIMESTAMP,
        meta_source VARCHAR(255),
        meta_security TEXT,
        meta_tag TEXT,
        extensions TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, birth_date)
