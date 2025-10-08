-- Table: medication_dispense_dosage_instructions
-- Source: HMUMedicationDispense.py (create_medication_dispense_dosage_instructions_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.medication_dispense_dosage_instructions (
        medication_dispense_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        dosage_text VARCHAR(MAX),
        dosage_timing_frequency INTEGER,
        dosage_timing_period INTEGER,
        dosage_timing_period_unit VARCHAR(20),
        dosage_route_code VARCHAR(50),
        dosage_route_system VARCHAR(255),
        dosage_route_display VARCHAR(255),
        dosage_dose_value DECIMAL(10,2),
        dosage_dose_unit VARCHAR(100),
        dosage_dose_system VARCHAR(255),
        dosage_dose_code VARCHAR(50)
    ) SORTKEY (medication_dispense_id)
