-- Table: patient_practitioners
-- Source: HMUPatient.py (create_patient_practitioners_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.patient_practitioners (
        patient_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        practitioner_id VARCHAR(255),
        practitioner_role_id VARCHAR(255),
        organization_id VARCHAR(255),
        reference_type VARCHAR(50)
    ) SORTKEY (patient_id, reference_type)
