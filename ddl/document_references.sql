-- Table: document_references
-- Source: HMUDocumentReference.py (create_redshift_tables_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.document_references (
        document_reference_id VARCHAR(255) NOT NULL,
        patient_id VARCHAR(255) NOT NULL,
        status VARCHAR(50),
        type_code VARCHAR(50),
        type_system VARCHAR(255),
        type_display VARCHAR(500),
        meta_type VARCHAR(50),
        date TIMESTAMP,
        custodian_id VARCHAR(255),
        description VARCHAR(MAX),
        context_period_start TIMESTAMP,
        context_period_end TIMESTAMP,
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, date);
