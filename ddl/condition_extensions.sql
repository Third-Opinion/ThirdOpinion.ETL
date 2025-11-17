-- Table: condition_extensions
-- Source: HMUMyJob.py (create_condition_extensions_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.condition_extensions (
        condition_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        extension_url VARCHAR(500),
        extension_type VARCHAR(50),
        value_type VARCHAR(50),
        value_string VARCHAR(MAX),
        value_datetime TIMESTAMP,
        value_reference VARCHAR(255),
        value_code VARCHAR(100),
        value_boolean BOOLEAN,
        value_decimal DECIMAL(18,6),
        value_integer INTEGER,
        parent_extension_url VARCHAR(500),
        extension_order INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) SORTKEY (condition_id, extension_url, extension_order)
