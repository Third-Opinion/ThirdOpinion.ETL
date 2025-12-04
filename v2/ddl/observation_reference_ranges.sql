-- Table: observation_reference_ranges
-- FHIR Observation referenceRange array - normal/expected value ranges
-- Updated: 2025-01-XX - Normalized range type codes to use codes table reference
--
-- Normalization Changes:
-- - Range type codes: Normalized via codes table (range_type_code_id)
-- - Keep denormalized: range values, units, text (unique per reference range)

CREATE TABLE IF NOT EXISTS public.observation_reference_ranges (
    observation_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    range_low_value DECIMAL(15,4),
    range_low_unit VARCHAR(50),
    range_high_value DECIMAL(15,4),
    range_high_unit VARCHAR(50),
    range_type_code_id BIGINT,  -- Reference to codes.code_id (for referenceRange.type.code)
    range_text VARCHAR(65535)
) 
DISTKEY (patient_id)
SORTKEY (patient_id, range_type_code_id);
