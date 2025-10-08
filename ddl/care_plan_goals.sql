-- Table: care_plan_goals
-- Source: HMUCarePlan.py (create_care_plan_goals_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.care_plan_goals (
        care_plan_id VARCHAR(255),
        goal_id VARCHAR(255)
    ) SORTKEY (care_plan_id);
