-- Table: medication_dispense_identifiers
-- Source: HMUMedicationDispense.py (create_medication_dispense_identifiers_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.medication_dispense_identifiers (
        medication_dispense_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        identifier_system VARCHAR(255),
        identifier_value VARCHAR(255)
    ) SORTKEY (medication_dispense_id, identifier_system)
