-- Table: observation_components
-- Source: HMUObservation.py (create_observation_components_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.observation_components (
        observation_id VARCHAR(255),
        component_code VARCHAR(50),
        component_system VARCHAR(255),
        component_display VARCHAR(255),
        component_text VARCHAR(500),
        component_value_string VARCHAR(500),
        component_value_quantity_value DECIMAL(15,4),
        component_value_quantity_unit VARCHAR(50),
        component_value_codeable_concept_code VARCHAR(50),
        component_value_codeable_concept_system VARCHAR(255),
        component_value_codeable_concept_display VARCHAR(255),
        component_data_absent_reason_code VARCHAR(50),
        component_data_absent_reason_display VARCHAR(255)
    ) SORTKEY (observation_id, component_code)
