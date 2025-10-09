-- Table: observation_categories
-- FHIR Observation category array - classification of observations
-- Updated: 2025-10-09 - Added patient_id for co-location, increased text sizes

CREATE TABLE IF NOT EXISTS public.observation_categories (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    category_code VARCHAR(50),
    category_system VARCHAR(255),
    category_display VARCHAR(255),
    category_text VARCHAR(65535)
) 
DISTKEY (patient_id)
SORTKEY (patient_id, category_code);
