-- Table: document_reference_content
-- Source: HMUDocumentReference.py (create_document_reference_content_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.document_reference_content (
        document_reference_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        attachment_content_type VARCHAR(100),
        attachment_url VARCHAR(MAX)
    ) SORTKEY (document_reference_id);
