-- Table: patient_contacts
-- Source: HMUPatient.py (create_patient_contacts_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.patient_contacts (
        patient_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        contact_relationship_code VARCHAR(50),
        contact_relationship_system VARCHAR(255),
        contact_relationship_display VARCHAR(255),
        contact_name_text VARCHAR(255),
        contact_name_family VARCHAR(255),
        contact_name_given TEXT,
        contact_telecom_system VARCHAR(50),
        contact_telecom_value VARCHAR(255),
        contact_telecom_use VARCHAR(50),
        contact_gender VARCHAR(10),
        contact_organization_id VARCHAR(255),
        period_start DATE,
        period_end DATE
    ) SORTKEY (patient_id, contact_relationship_code)
