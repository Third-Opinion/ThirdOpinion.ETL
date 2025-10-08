-- Table: patient_communications
-- Source: HMUPatient.py (create_patient_communications_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.patient_communications (
        patient_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        language_code VARCHAR(10),
        language_system VARCHAR(255),
        language_display VARCHAR(255),
        preferred BOOLEAN,
        extensions TEXT
    ) SORTKEY (patient_id, language_code)
