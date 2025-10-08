-- Table: procedure_code_codings
-- Source: HMUProcedure.py (create_procedure_code_codings_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.procedure_code_codings (
        procedure_id VARCHAR(255),
        code_system VARCHAR(255),
        code_code VARCHAR(100),
        code_display VARCHAR(500)
    ) SORTKEY (procedure_id, code_system);
