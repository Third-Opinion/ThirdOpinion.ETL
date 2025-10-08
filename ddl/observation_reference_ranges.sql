-- Table: observation_reference_ranges
-- Source: HMUObservation.py (create_observation_reference_ranges_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.observation_reference_ranges (
        observation_id VARCHAR(255),
        range_low_value DECIMAL(15,4),
        range_low_unit VARCHAR(50),
        range_high_value DECIMAL(15,4),
        range_high_unit VARCHAR(50),
        range_type_code VARCHAR(50),
        range_type_system VARCHAR(255),
        range_type_display VARCHAR(255),
        range_text VARCHAR(500)
    ) SORTKEY (observation_id, range_type_code)
