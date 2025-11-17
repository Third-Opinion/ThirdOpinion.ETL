-- Table: condition_body_sites
-- Source: HMUMyJob.py (create_condition_body_sites_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.condition_body_sites (
        condition_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        body_site_code VARCHAR(50),
        body_site_system VARCHAR(255),
        body_site_display VARCHAR(255),
        body_site_text VARCHAR(500)
    ) SORTKEY (condition_id, body_site_code)
