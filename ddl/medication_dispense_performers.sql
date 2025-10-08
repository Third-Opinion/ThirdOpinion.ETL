-- Table: medication_dispense_performers
-- Source: HMUMedicationDispense.py (create_medication_dispense_performers_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.medication_dispense_performers (
        medication_dispense_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        performer_actor_reference VARCHAR(255)
    ) SORTKEY (medication_dispense_id, performer_actor_reference)
