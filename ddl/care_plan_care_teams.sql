-- Table: care_plan_care_teams
-- Source: HMUCarePlan.py (create_care_plan_care_teams_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.care_plan_care_teams (
        care_plan_id VARCHAR(255),
        care_team_id VARCHAR(255)
    ) SORTKEY (care_plan_id);
