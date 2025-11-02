-- Table: diagnostic_report_presented_forms
-- Source: HMUDiagnosticReport.py (create_diagnostic_report_presented_forms_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.diagnostic_report_presented_forms (
        diagnostic_report_id VARCHAR(255),
        content_type VARCHAR(100),
        data VARCHAR(MAX),
        title VARCHAR(255)
    ) SORTKEY (diagnostic_report_id)
