-- Table: medication_request_identifiers
-- Source: HMUMedicationRequest.py (create_medication_request_identifiers_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.medication_request_identifiers (
        medication_request_id VARCHAR(255),
        identifier_system VARCHAR(255),
        identifier_value VARCHAR(255)
    ) SORTKEY (medication_request_id, identifier_system)
