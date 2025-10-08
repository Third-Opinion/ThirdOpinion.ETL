-- Table: diagnostic_report_categories
-- Source: HMUDiagnosticReport.py (create_diagnostic_report_categories_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.diagnostic_report_categories (
        diagnostic_report_id VARCHAR(255),
        category_code VARCHAR(50),
        category_system VARCHAR(255),
        category_display VARCHAR(255)
    ) SORTKEY (diagnostic_report_id, category_code)
