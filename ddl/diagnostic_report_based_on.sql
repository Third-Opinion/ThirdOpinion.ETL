-- Table: diagnostic_report_based_on
-- Source: HMUDiagnosticReport.py (create_diagnostic_report_based_on_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.diagnostic_report_based_on (
        diagnostic_report_id VARCHAR(255),
        service_request_id VARCHAR(255)
    ) SORTKEY (diagnostic_report_id)
