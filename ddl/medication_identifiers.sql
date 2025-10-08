-- Table: medication_identifiers
-- Source: HMUMedication.py (create_medication_identifiers_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.medication_identifiers (
        medication_id VARCHAR(255),
        identifier_system VARCHAR(255),
        identifier_value VARCHAR(255)
    ) SORTKEY (medication_id, identifier_system)
