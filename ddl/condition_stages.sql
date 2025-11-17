-- Table: condition_stages
-- Source: HMUMyJob.py (create_condition_stages_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.condition_stages (
        condition_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        stage_summary_code VARCHAR(50),
        stage_summary_system VARCHAR(255),
        stage_summary_display VARCHAR(255),
        stage_assessment_code VARCHAR(50),
        stage_assessment_system VARCHAR(255),
        stage_assessment_display VARCHAR(255),
        stage_type_code VARCHAR(50),
        stage_type_system VARCHAR(255),
        stage_type_display VARCHAR(255)
    ) SORTKEY (condition_id, stage_summary_code)
