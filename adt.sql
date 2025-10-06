-- adt
WITH target_patients AS (
     SELECT * FROM rpt_fhir_hmu_patients_v1
  )
  SELECT
        tp.patient_id,
       --  tp.observation_id,
       --  tp.encounter_id,
       --  tp.effective_datetime,
       --  tp.observation_text, 
       --  tp.codes[0].code,
       --  tp.value_quantity_value,
       --  tp.value_quantity_unit, 
       --  tp.value_codeable_concept_code,
       --  tp.value_quantity_system, 
       --  tp.value_string, 
       --  tp.codes,
       --  tp.categories, 
       --  tp.reference_ranges, 
       --  tp.interpretations, 
       --  tp.notes
      tp.last_encounter_date,
      LISTAGG(DISTINCT
  mpv.medication_display, ', ')
   WITHIN GROUP (ORDER BY
  mpv.medication_display) as
  medications_list,
  COUNT(mpv.medication_display)
--    as total_medication_requests
  FROM target_patients tp
  INNER JOIN public.fact_fhir_medication_requests_view_v1 tp
      ON tp.patient_id = mpv.patient_id
  WHERE
      (
          medication_display
  ILIKE '%casodex%'
          OR medication_display
   ILIKE '%eligard%'
          OR medication_display
   ILIKE '%xtandi%'
          OR medication_display
   ILIKE '%abiraterone%'
          OR medication_display
   ILIKE '%lupron%'
          OR medication_display
   ILIKE '%bicalutamide%'
          OR medication_display
   ILIKE '%trelstar%'
          OR medication_display
   ILIKE '%enzalutamide%'
          OR medication_display
   ILIKE '%zytiga%'
          OR medication_display
   ILIKE '%leuprolide%'
          OR medication_display
   ILIKE '%degarelix%'
          OR medication_display
   ILIKE '%camcevi%'
          OR medication_display
   ILIKE '%eulexin%'
          OR medication_display
   ILIKE '%orgovyx%'
          OR medication_display
   ILIKE '%yonsa%'
          OR medication_display
   ILIKE '%firmagon%'
          OR medication_display
   ILIKE '%flutamide%'
          OR medication_display
   ILIKE '%zoladex%'
      )
      AND mpv.authored_on >=
  '2023-01-01'
  GROUP BY
      tp.patient_id,
  ORDER BY
      tp.patient_id;


-----

WITH target_patients AS (
   SELECT * FROM rpt_fhir_hmu_patients_v1 WHERE  last_encounter_date >= '2025-07-15'
)

    SELECT  
        -- fpv2.names,
        -- fpv2.birth_date,
        -- fpv2.gender,
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
    FROM public.fact_fhir_observations_view_v2 tp
    INNER JOIN target_patients tgt ON tp.patient_id = tgt.patient_id
    INNER JOIN public.fact_fhir_patients_view_v2 fpv2 ON tp.patient_id = fpv2.patient_id
    WHERE observation_category = 'laboratory'
        AND tp.status = 'final'

        AND tp.observation_text NOT ILIKE '%ratio%'
        AND tp.observation_text NOT ILIKE '%free%'
        AND (
            tp.codes[0].code IN (
                '59221-2', '19195-7', '100716-0', '59230-3', '47738-0', '2857-1', 
                '19197-3', '83112-3', '72576-2', '35741-8', '53764-7', '59232-9', 
                '19201-3', '59239-4', '59231-1', '10886-0', '19203-9', '33667-7', 
                '83113-1', '97149-9'
            )
            OR tp.observation_text ILIKE '%PSA%' 
            OR tp.observation_text ILIKE '%prostate specific a%'
        )

        AND tp.effective_datetime >= '2024-01-01'
       AND tp.has_value = true;

-----


-- 2986-8  - Testosterone [Mass/volume] in Serum or Plasma
--   2991-8  - Testosterone Free [Mass/volume] in Serum or Plasma
--   49041-7 - Testosterone with detection limit <= 1.0 ng/dL
--   6891-6  - Testosterone free+weakly bound/Testosterone total ratio


--  AND tp.observation_text NOT ILIKE '%ratio%'
--         AND tp.observation_text NOT ILIKE '%free%'
        AND (
            tp.codes[0].code IN (
                '2986-8 ', '2991-8', '49041-7', '6891-6',
            )
            OR tp.observation_text ILIKE '%test%' 
        )