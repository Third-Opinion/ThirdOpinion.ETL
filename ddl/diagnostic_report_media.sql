-- Table: diagnostic_report_media
-- Source: HMUDiagnosticReport.py (create_diagnostic_report_media_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.diagnostic_report_media (
        diagnostic_report_id VARCHAR(255),
        media_id VARCHAR(255)
    ) SORTKEY (diagnostic_report_id)
