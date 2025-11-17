-- Table: condition_categories
-- Source: HMUMyJob.py (create_condition_categories_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.condition_categories (
        condition_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        category_code VARCHAR(50),
        category_system VARCHAR(255),
        category_display VARCHAR(255),
        category_text VARCHAR(500)
    ) SORTKEY (condition_id, category_code)
