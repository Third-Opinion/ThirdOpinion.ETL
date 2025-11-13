CREATE MATERIALIZED VIEW rpt_fhir_observations_labs_union_hmu_v1
DISTSTYLE KEY
DISTKEY (patient_id)
SORTKEY (patient_id, effective_datetime)
AS
SELECT
    names,
    birth_date,
    gender,
    patient_id,
    observation_id,
    encounter_id,
    effective_datetime,
    observation_text,
    code,
    combined_value,
    value_quantity_value,
    value_quantity_unit,
    value_codeable_concept_code,
    value_quantity_system,
    value_string,
    codes,
    categories,
    reference_ranges,
    interpretations,
    notes,
    components,
    ai_evidence,
    'alt' AS observation_type
FROM rpt_fhir_observations_alt_hmu_v1

UNION ALL

SELECT
    names,
    birth_date,
    gender,
    patient_id,
    observation_id,
    encounter_id,
    effective_datetime,
    observation_text,
    code,
    combined_value,
    value_quantity_value,
    value_quantity_unit,
    value_codeable_concept_code,
    value_quantity_system,
    value_string,
    codes,
    categories,
    reference_ranges,
    interpretations,
    notes,
    components,
    ai_evidence,
    'ast' AS observation_type
FROM rpt_fhir_observations_ast_hmu_v1

UNION ALL

SELECT
    names,
    birth_date,
    gender,
    patient_id,
    observation_id,
    encounter_id,
    effective_datetime,
    observation_text,
    code,
    combined_value,
    value_quantity_value,
    value_quantity_unit,
    value_codeable_concept_code,
    value_quantity_system,
    value_string,
    codes,
    categories,
    reference_ranges,
    interpretations,
    notes,
    components,
    ai_evidence,
    'bmi' AS observation_type
FROM rpt_fhir_observations_bmi_hmu_v1

UNION ALL

SELECT
    names,
    birth_date,
    gender,
    patient_id,
    observation_id,
    encounter_id,
    effective_datetime,
    observation_text,
    code,
    combined_value,
    value_quantity_value,
    value_quantity_unit,
    value_codeable_concept_code,
    value_quantity_system,
    value_string,
    codes,
    categories,
    reference_ranges,
    interpretations,
    notes,
    components,
    ai_evidence,
    'cd4_count' AS observation_type
FROM rpt_fhir_observations_cd4_count_hmu_v1

UNION ALL

SELECT
    names,
    birth_date,
    gender,
    patient_id,
    observation_id,
    encounter_id,
    effective_datetime,
    observation_text,
    code,
    combined_value,
    value_quantity_value,
    value_quantity_unit,
    value_codeable_concept_code,
    value_quantity_system,
    value_string,
    codes,
    categories,
    reference_ranges,
    interpretations,
    notes,
    components,
    ai_evidence,
    'creatinine' AS observation_type
FROM rpt_fhir_observations_creatinine_hmu_v1

UNION ALL

SELECT
    names,
    birth_date,
    gender,
    patient_id,
    observation_id,
    encounter_id,
    effective_datetime,
    observation_text,
    code,
    combined_value,
    value_quantity_value,
    value_quantity_unit,
    value_codeable_concept_code,
    value_quantity_system,
    value_string,
    codes,
    categories,
    reference_ranges,
    interpretations,
    notes,
    components,
    ai_evidence,
    'egfr' AS observation_type
FROM rpt_fhir_observations_egfr_hmu_v1

UNION ALL

SELECT
    names,
    birth_date,
    gender,
    patient_id,
    observation_id,
    encounter_id,
    effective_datetime,
    observation_text,
    code,
    combined_value,
    value_quantity_value,
    value_quantity_unit,
    value_codeable_concept_code,
    value_quantity_system,
    value_string,
    codes,
    categories,
    reference_ranges,
    interpretations,
    notes,
    components,
    ai_evidence,
    'hba1c' AS observation_type
FROM rpt_fhir_observations_hba1c_hmu_v1

UNION ALL

SELECT
    names,
    birth_date,
    gender,
    patient_id,
    observation_id,
    encounter_id,
    effective_datetime,
    observation_text,
    code,
    combined_value,
    value_quantity_value,
    value_quantity_unit,
    value_codeable_concept_code,
    value_quantity_system,
    value_string,
    codes,
    categories,
    reference_ranges,
    interpretations,
    notes,
    components,
    ai_evidence,
    'hemoglobin' AS observation_type
FROM rpt_fhir_observations_hemoglobin_hmu_v1

UNION ALL

SELECT
    names,
    birth_date,
    gender,
    patient_id,
    observation_id,
    encounter_id,
    effective_datetime,
    observation_text,
    code,
    combined_value,
    value_quantity_value,
    value_quantity_unit,
    value_codeable_concept_code,
    value_quantity_system,
    value_string,
    codes,
    categories,
    reference_ranges,
    interpretations,
    notes,
    components,
    ai_evidence,
    'hiv_viral_load' AS observation_type
FROM rpt_fhir_observations_hiv_viral_load_hmu_v1

UNION ALL

SELECT
    names,
    birth_date,
    gender,
    patient_id,
    observation_id,
    encounter_id,
    effective_datetime,
    observation_text,
    code,
    combined_value,
    value_quantity_value,
    value_quantity_unit,
    value_codeable_concept_code,
    value_quantity_system,
    value_string,
    codes,
    categories,
    reference_ranges,
    interpretations,
    notes,
    components,
    ai_evidence,
    'platelet_count' AS observation_type
FROM rpt_fhir_observations_platelet_count_hmu_v1

UNION ALL

SELECT
    names,
    birth_date,
    gender,
    patient_id,
    observation_id,
    encounter_id,
    effective_datetime,
    observation_text,
    code,
    combined_value,
    value_quantity_value,
    value_quantity_unit,
    value_codeable_concept_code,
    value_quantity_system,
    value_string,
    codes,
    categories,
    reference_ranges,
    interpretations,
    notes,
    components,
    ai_evidence,
    'serum_albumin' AS observation_type
FROM rpt_fhir_observations_serum_albumin_hmu_v1

UNION ALL

SELECT
    names,
    birth_date,
    gender,
    patient_id,
    observation_id,
    encounter_id,
    effective_datetime,
    observation_text,
    code,
    combined_value,
    value_quantity_value,
    value_quantity_unit,
    value_codeable_concept_code,
    value_quantity_system,
    value_string,
    codes,
    categories,
    reference_ranges,
    interpretations,
    notes,
    components,
    ai_evidence,
    'serum_potassium' AS observation_type
FROM rpt_fhir_observations_serum_potassium_hmu_v1

UNION ALL

SELECT
    names,
    birth_date,
    gender,
    patient_id,
    observation_id,
    encounter_id,
    effective_datetime,
    observation_text,
    code,
    combined_value,
    value_quantity_value,
    value_quantity_unit,
    value_codeable_concept_code,
    value_quantity_system,
    value_string,
    codes,
    categories,
    reference_ranges,
    interpretations,
    notes,
    components,
    ai_evidence,
    'total_bilirubin' AS observation_type
FROM rpt_fhir_observations_total_bilirubin_hmu_v1

UNION ALL

SELECT
    names,
    birth_date,
    gender,
    patient_id,
    observation_id,
    encounter_id,
    effective_datetime,
    observation_text,
    code,
    combined_value,
    value_quantity_value,
    value_quantity_unit,
    value_codeable_concept_code,
    value_quantity_system,
    value_string,
    codes,
    categories,
    reference_ranges,
    interpretations,
    notes,
    components,
    ai_evidence,
    'absolute_neutrophil_count' AS observation_type
FROM rpt_fhir_observations_absolute_neutrophil_count_hmu_v1

UNION ALL

SELECT
    names,
    birth_date,
    gender,
    patient_id,
    observation_id,
    encounter_id,
    effective_datetime,
    observation_text,
    code,
    combined_value,
    value_quantity_value,
    value_quantity_unit,
    value_codeable_concept_code,
    value_quantity_system,
    value_string,
    codes,
    categories,
    reference_ranges,
    interpretations,
    notes,
    components,
    ai_evidence,
    'testosterone_total' AS observation_type
FROM rpt_fhir_observations_testosterone_total_hmu_v1

UNION ALL

SELECT
    names,
    birth_date,
    gender,
    patient_id,
    observation_id,
    encounter_id,
    effective_datetime,
    observation_text,
    code,
    combined_value,
    value_quantity_value,
    value_quantity_unit,
    value_codeable_concept_code,
    value_quantity_system,
    value_string,
    codes,
    categories,
    reference_ranges,
    interpretations,
    notes,
    components,
    ai_evidence,
    'psa_total' AS observation_type
FROM rpt_fhir_observations_psa_total_hmu_v1;
