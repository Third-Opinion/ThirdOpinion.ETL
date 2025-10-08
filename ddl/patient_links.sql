-- Table: patient_links
-- Source: HMUPatient.py (create_patient_links_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.patient_links (
        patient_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        other_patient_id VARCHAR(255),
        link_type_code VARCHAR(50),
        link_type_system VARCHAR(255),
        link_type_display VARCHAR(255)
    ) SORTKEY (patient_id, other_patient_id)
