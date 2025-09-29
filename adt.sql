WITH target_patients AS (
     SELECT * FROM rpt_fhir_hmu_patients_v1
  )
  SELECT
      tp.patient_id,
      tp.birth_date,
      tp.names::JSON.primary_name.given::VARCHAR as
  first_name,
      tp.names::JSON.primary_name.family::VARCHAR as
  family_name,
      tp.last_encounter_date,
      LISTAGG(DISTINCT
  mpv.medication_display, ', ')
   WITHIN GROUP (ORDER BY
  mpv.medication_display) as
  medications_list,

  COUNT(mpv.medication_display)
   as total_medication_requests
  FROM target_patients tp
  INNER JOIN public.fact_fhir_medication_requests_view_v1 mpv
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
  '2024-01-01'
  GROUP BY
      tp.patient_id,
      tp.birth_date,
      tp.names,
      tp.last_encounter_date
  ORDER BY
      tp.patient_id;