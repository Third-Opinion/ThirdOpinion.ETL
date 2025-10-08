-- Table: medication_request_categories
-- Source: HMUMedicationRequest.py (create_medication_request_categories_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.medication_request_categories (
        medication_request_id VARCHAR(255),
        category_code VARCHAR(50),
        category_system VARCHAR(255),
        category_display VARCHAR(255),
        category_text VARCHAR(500)
    ) SORTKEY (medication_request_id, category_code)
