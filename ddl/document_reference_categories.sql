-- Table: document_reference_categories
-- Source: HMUDocumentReference.py (create_document_reference_categories_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.document_reference_categories (
        document_reference_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        category_code VARCHAR(50),
        category_system VARCHAR(255),
        category_display VARCHAR(255)
    ) SORTKEY (document_reference_id, category_code);
