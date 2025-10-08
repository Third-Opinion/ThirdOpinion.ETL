-- Table: document_reference_identifiers
-- Source: HMUDocumentReference.py (create_document_reference_identifiers_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.document_reference_identifiers (
        document_reference_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        identifier_system VARCHAR(255),
        identifier_value VARCHAR(255)
    ) SORTKEY (document_reference_id, identifier_system);
