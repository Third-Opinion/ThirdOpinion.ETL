-- Table: medication_dispense_auth_prescriptions
-- Source: HMUMedicationDispense.py (create_medication_dispense_auth_prescriptions_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.medication_dispense_auth_prescriptions (
        medication_dispense_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        authorizing_prescription_id VARCHAR(255)
    ) SORTKEY (medication_dispense_id)
