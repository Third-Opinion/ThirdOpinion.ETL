-- Table: medication_requests
-- Source: HMUMedicationRequest.py (create_redshift_tables_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

-- Main medication requests table
    CREATE TABLE IF NOT EXISTS public.medication_requests (
        medication_request_id VARCHAR(255) NOT NULL,
        patient_id VARCHAR(255) NOT NULL,
        encounter_id VARCHAR(255),
        medication_id VARCHAR(255),
        medication_display VARCHAR(500),
        status VARCHAR(50),
        intent VARCHAR(50),
        reported_boolean BOOLEAN,
        authored_on TIMESTAMP,
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, authored_on);
