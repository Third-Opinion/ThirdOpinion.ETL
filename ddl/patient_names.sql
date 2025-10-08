-- Table: patient_names
-- Source: HMUPatient.py (create_patient_names_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.patient_names (
        patient_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        name_use VARCHAR(50),
        name_text VARCHAR(255),
        family_name VARCHAR(255),
        given_names TEXT,
        prefix VARCHAR(50),
        suffix VARCHAR(50),
        period_start DATE,
        period_end DATE
    ) SORTKEY (patient_id, name_use)
