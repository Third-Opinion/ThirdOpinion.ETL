-- Table: diagnostic_reports
-- Source: HMUDiagnosticReport.py (create_redshift_tables_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

-- Main diagnostic reports table
    CREATE TABLE IF NOT EXISTS public.diagnostic_reports (
        diagnostic_report_id VARCHAR(255) NOT NULL,
        resource_type VARCHAR(50),
        status VARCHAR(50),
        effective_datetime TIMESTAMP,
        issued_datetime TIMESTAMP,
        code_text VARCHAR(500),
        code_primary_code VARCHAR(50),
        code_primary_system VARCHAR(255),
        code_primary_display VARCHAR(255),
        patient_id VARCHAR(255),
        encounter_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        extensions VARCHAR(MAX),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, effective_datetime)
