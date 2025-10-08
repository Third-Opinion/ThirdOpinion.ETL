-- Table: care_plans
-- Source: HMUCarePlan.py (create_care_plans_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.care_plans (
        care_plan_id VARCHAR(255) NOT NULL,
        patient_id VARCHAR(255) NOT NULL,
        status VARCHAR(50),
        intent VARCHAR(50),
        title VARCHAR(500),
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id);
