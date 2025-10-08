-- Table: patient_telecoms
-- Source: HMUPatient.py (create_patient_telecoms_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.patient_telecoms (
        patient_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        telecom_system VARCHAR(50),
        telecom_value VARCHAR(255),
        telecom_use VARCHAR(50),
        telecom_rank INTEGER,
        period_start DATE,
        period_end DATE
    ) SORTKEY (patient_id, telecom_system)
