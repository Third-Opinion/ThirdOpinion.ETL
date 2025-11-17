-- Table: condition_evidence
-- Source: HMUMyJob.py (create_condition_evidence_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.condition_evidence (
        condition_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        evidence_code VARCHAR(50),
        evidence_system VARCHAR(255),
        evidence_display VARCHAR(255),
        evidence_detail_reference VARCHAR(255)
    ) SORTKEY (condition_id, evidence_code)
