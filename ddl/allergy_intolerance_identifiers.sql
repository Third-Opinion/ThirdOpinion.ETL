-- Table: allergy_intolerance_identifiers
-- Source: HMUAllergyIntolerance.py (create_allergy_identifiers_table_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.allergy_intolerance_identifiers (
        allergy_intolerance_id VARCHAR(255),
        identifier_use VARCHAR(50),
        identifier_type_code VARCHAR(50),
        identifier_type_display VARCHAR(255),
        identifier_system VARCHAR(255),
        identifier_value VARCHAR(255),
        identifier_period_start DATE,
        identifier_period_end DATE
    ) SORTKEY (allergy_intolerance_id, identifier_system)
