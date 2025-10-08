-- Table: procedure_identifiers
-- Source: HMUProcedure.py (create_procedure_identifiers_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.procedure_identifiers (
        procedure_id VARCHAR(255),
        identifier_system VARCHAR(255),
        identifier_value VARCHAR(255)
    ) SORTKEY (procedure_id, identifier_system);
