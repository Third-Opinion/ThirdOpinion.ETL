-- Table: observation_components
-- FHIR Observation component array - sub-observations (e.g., blood pressure systolic/diastolic)
-- Updated: 2025-10-09 - Added patient_id for co-location, increased text sizes

CREATE TABLE IF NOT EXISTS public.observation_components (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    component_code VARCHAR(50),
    component_system VARCHAR(255),
    component_display VARCHAR(255),
    component_text VARCHAR(65535),
    component_value_string VARCHAR(65535),
    component_value_quantity_value DECIMAL(15,4),
    component_value_quantity_unit VARCHAR(50),
    component_value_codeable_concept_code VARCHAR(50),
    component_value_codeable_concept_system VARCHAR(255),
    component_value_codeable_concept_display VARCHAR(255),
    component_data_absent_reason_code VARCHAR(50),
    component_data_absent_reason_display VARCHAR(255)
) 
DISTKEY (patient_id)
SORTKEY (patient_id, component_code);
