-- Table: allergy_intolerance
-- Source: HMUAllergyIntolerance.py (create_redshift_tables_sql)
-- Extracted: extract_ddl_from_glue_jobs.py

-- Main allergy intolerance table
    CREATE TABLE IF NOT EXISTS public.allergy_intolerance (
        allergy_intolerance_id VARCHAR(255) NOT NULL,
        resourcetype VARCHAR(50),
        clinical_status_code VARCHAR(50),
        clinical_status_display VARCHAR(255),
        clinical_status_system VARCHAR(255),
        verification_status_code VARCHAR(50),
        verification_status_display VARCHAR(255),
        verification_status_system VARCHAR(255),
        type VARCHAR(50),
        category TEXT,
        criticality VARCHAR(50),
        code VARCHAR(255),
        code_display VARCHAR(500),
        code_system VARCHAR(255),
        code_text VARCHAR(500),
        patient_id VARCHAR(255),
        encounter_id VARCHAR(255),
        onset_datetime TIMESTAMP,
        onset_age_value DECIMAL(10,2),
        onset_age_unit VARCHAR(20),
        onset_period_start DATE,
        onset_period_end DATE,
        recorded_date TIMESTAMP,
        recorder_practitioner_id VARCHAR(255),
        asserter_practitioner_id VARCHAR(255),
        last_occurrence TIMESTAMP,
        note TEXT,
        reactions TEXT,
        meta_last_updated TIMESTAMP,
        meta_source VARCHAR(255),
        meta_security TEXT,
        meta_tag TEXT,
        extensions TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (allergy_intolerance_id) SORTKEY (allergy_intolerance_id, recorded_date)
