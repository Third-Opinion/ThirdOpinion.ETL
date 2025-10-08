-- Table: care_plan_categories
-- Source: HMUCarePlan.py (create_care_plan_categories_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.care_plan_categories (
        care_plan_id VARCHAR(255),
        category_code VARCHAR(50),
        category_system VARCHAR(255),
        category_display VARCHAR(255),
        category_text VARCHAR(500)
    ) SORTKEY (care_plan_id, category_code);
