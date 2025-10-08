-- Table: document_reference_authors
-- Source: HMUDocumentReference.py (create_document_reference_authors_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.document_reference_authors (
        document_reference_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        author_id VARCHAR(255)
    ) SORTKEY (document_reference_id);
