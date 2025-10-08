-- Table: encounters
-- Source: HMUEncounter.py (create_redshift_tables_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

-- Main encounters table
    CREATE TABLE IF NOT EXISTS public.encounters (
        encounter_id VARCHAR(255) NOT NULL,
        patient_id VARCHAR(255),
        status VARCHAR(50),
        resourcetype VARCHAR(50),
        class_code VARCHAR(10),
        class_display VARCHAR(255),
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        service_provider_id VARCHAR(255),
        appointment_id VARCHAR(255),
        parent_encounter_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, start_time)
