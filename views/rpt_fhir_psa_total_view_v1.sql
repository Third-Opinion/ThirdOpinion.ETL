CREATE OR REPLACE VIEW public.rpt_fhir_psa_patients_v1 AS
WITH target_patients AS (
    SELECT * FROM rpt_fhir_hmu_patients_v1 WHERE last_encounter_date >= '2025-06-01'
)
SELECT  
    fpv2.names,
    fpv2.birth_date,
    fpv2.gender,
    tp.patient_id,
    tp.observation_id,
    tp.encounter_id,
    tp.effective_datetime,
    tp.observation_text, 
    tp.codes[0].code,
    tp.value_quantity_value,
    tp.value_quantity_unit, 
    tp.value_codeable_concept_code,
    tp.value_quantity_system, 
    tp.value_string, 
    tp.codes,
    tp.categories, 
    tp.reference_ranges, 
    tp.interpretations, 
    tp.notes
FROM public.fact_fhir_observations_view_v1 tp
INNER JOIN target_patients tgt ON tp.patient_id = tgt.patient_id
INNER JOIN public.fact_fhir_patients_view_v1 fpv2 ON tp.patient_id = fpv2.patient_id
WHERE tp.observation_category = 'laboratory'
    AND tp.status = 'final'
    AND tp.observation_text NOT ILIKE '%ratio%'
    AND tp.observation_text NOT ILIKE '%free%'
    AND (
        tp.codes[0].code IN (
            '59221-2',  -- PSA Free/Total ratio
            '19195-7',  -- PSA.free [Mass/volume] in Serum or Plasma
            '100716-0', -- PSA variant
            '59230-3',  -- PSA variant
            '47738-0',  -- PSA variant
            '2857-1',   -- PSA [Mass/volume] in Serum or Plasma
            '19197-3',  -- PSA.free/PSA.total in Serum or Plasma
            '83112-3',  -- PSA variant
            '72576-2',  -- PSA variant
            '35741-8',  -- PSA [Mass/volume] in Serum or Plasma by Detection limit
            '53764-7',  -- PSA variant
            '59232-9',  -- PSA variant
            '19201-3',  -- PSA.total [Mass/volume] in Serum or Plasma
            '59239-4',  -- PSA variant
            '59231-1',  -- PSA variant
            '10886-0',  -- PSA [Presence] in Serum or Plasma
            '19203-9',  -- PSA.total future [Mass/volume] in Serum or Plasma
            '33667-7',  -- PSA [Mass/volume] in Serum or Plasma --post XXX challenge
            '83113-1',  -- PSA variant
            '97149-9'   -- PSA variant
        )
        OR tp.observation_text ILIKE '%PSA%' 
        OR tp.observation_text ILIKE '%prostate specific a%'
    )
    AND tp.effective_datetime >= '2024-01-01'
    AND tp.has_value = true;