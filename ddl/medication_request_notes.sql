-- Table: medication_request_notes
-- Source: HMUMedicationRequest.py (create_medication_request_notes_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.medication_request_notes (
        medication_request_id VARCHAR(255),
        note_text VARCHAR(MAX)
    ) SORTKEY (medication_request_id)
