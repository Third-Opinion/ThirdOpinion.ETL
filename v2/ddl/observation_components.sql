-- Table: observation_components
-- FHIR Observation component array - sub-observations (e.g., blood pressure systolic/diastolic)
-- Updated: 2025-01-XX - Normalized code-based fields to use codes table references
--
-- Normalization Changes:
-- - Component codes: Normalized via codes table (component_code_id)
-- - Component value codeable concept codes: Normalized via codes table (component_value_code_id)
-- - Component data absent reason codes: Normalized via codes table (component_data_absent_reason_code_id)
-- - Keep denormalized: component_text, component values (unique per component)

CREATE TABLE IF NOT EXISTS public.observation_components (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    component_code_id BIGINT,  -- Reference to codes.code_id (for component.code)
    component_text VARCHAR(65535),  -- Keep denormalized (unique text per component)
    component_value_string VARCHAR(65535),
    component_value_quantity_value DECIMAL(15,4),
    component_value_quantity_unit VARCHAR(50),
    component_value_code_id BIGINT,  -- Reference to codes.code_id (for component.valueCodeableConcept.code)
    component_data_absent_reason_code_id BIGINT  -- Reference to codes.code_id (for component.dataAbsentReason.code)
) 
DISTKEY (patient_id)
SORTKEY (patient_id, component_code_id);
