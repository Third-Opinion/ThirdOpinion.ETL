-- Table: diagnostic_report_results
-- Source: HMUDiagnosticReport.py (create_diagnostic_report_results_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.diagnostic_report_results (
        diagnostic_report_id VARCHAR(255),
        observation_id VARCHAR(255)
    ) SORTKEY (diagnostic_report_id)
