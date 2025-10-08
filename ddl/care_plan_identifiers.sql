-- Table: care_plan_identifiers
-- Source: HMUCarePlan.py (create_care_plan_identifiers_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.care_plan_identifiers (
        care_plan_id VARCHAR(255),
        identifier_system VARCHAR(255),
        identifier_value VARCHAR(255)
    ) SORTKEY (care_plan_id, identifier_system);
