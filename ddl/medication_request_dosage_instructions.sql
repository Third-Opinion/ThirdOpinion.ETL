-- Table: medication_request_dosage_instructions
-- Source: HMUMedicationRequest.py (create_medication_request_dosage_instructions_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.medication_request_dosage_instructions (
        medication_request_id VARCHAR(255),
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
        dosage_dose_code VARCHAR(50),
        dosage_as_needed_boolean BOOLEAN
    ) SORTKEY (medication_request_id, dosage_timing_frequency)
