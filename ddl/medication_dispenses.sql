-- Table: medication_dispenses
-- Source: HMUMedicationDispense.py (create_redshift_tables_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

-- Main medication dispenses table
    CREATE TABLE IF NOT EXISTS public.medication_dispenses (
        medication_dispense_id VARCHAR(255) NOT NULL,
        resource_type VARCHAR(50),
        status VARCHAR(50),
        patient_id VARCHAR(255) NOT NULL,
        medication_id VARCHAR(255),
        medication_display VARCHAR(500),
        type_system VARCHAR(255),
        type_code VARCHAR(50),
        type_display VARCHAR(255),
        quantity_value DECIMAL(10,2),
        when_handed_over TIMESTAMP,
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, when_handed_over);
