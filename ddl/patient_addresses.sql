-- Table: patient_addresses
-- Source: HMUPatient.py (create_patient_addresses_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.patient_addresses (
        patient_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        address_use VARCHAR(50),
        address_type VARCHAR(50),
        address_text VARCHAR(500),
        address_line TEXT,
        city VARCHAR(100),
        district VARCHAR(100),
        state VARCHAR(100),
        postal_code VARCHAR(20),
        country VARCHAR(100),
        period_start DATE,
        period_end DATE
    ) SORTKEY (patient_id, address_use)
