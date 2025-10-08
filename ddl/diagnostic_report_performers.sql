-- Table: diagnostic_report_performers
-- Source: HMUDiagnosticReport.py (create_diagnostic_report_performers_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.diagnostic_report_performers (
        diagnostic_report_id VARCHAR(255),
        performer_type VARCHAR(50),
        performer_id VARCHAR(255)
    ) SORTKEY (diagnostic_report_id, performer_type)
