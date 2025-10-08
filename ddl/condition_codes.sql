-- Table: condition_codes
-- Source: HMUMyJob.py (create_condition_codes_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.condition_codes (
        condition_id VARCHAR(255),
        code_code VARCHAR(50),
        code_system VARCHAR(255),
        code_display VARCHAR(255),
        code_text VARCHAR(500)
    ) SORTKEY (condition_id, code_system, code_code)
