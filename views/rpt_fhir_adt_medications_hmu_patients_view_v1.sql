CREATE MATERIALIZED VIEW rpt_fhir_adt_medications_hmu_patients_view_v1 
DISTSTYLE KEY 
DISTKEY (patient_id)
SORTKEY (patient_id, last_encounter_date)
AS
WITH target_patients AS (
     SELECT * FROM rpt_fhir_hmu_patients_v1 WHERE last_encounter_date >= '2025-06-01'
  )
  SELECT
      tp.patient_id,
      tp.last_encounter_date,
      mpv.medication_request_id,
      mpv.medication_id,
      mpv.medication_display,
      mpv.authored_on,
      mpv.medication_name,
      LISTAGG(DISTINCT mpv.medication_display, ', ') WITHIN GROUP (ORDER BY mpv.medication_display) as medications_list,
      mpv.medication_status,
      mpv.medication_code,
      mpv.medication_primary_code,
      mpv.medication_primary_system,
      mpv.medication_primary_text,
      mpv.dosage_instructions,
      mpv.categories,
      mpv.notes,
      COUNT(mpv.medication_display) as total_medication_requests
  FROM target_patients tp
  INNER JOIN public.fact_fhir_medication_requests_view_v1 mpv
      ON tp.patient_id = mpv.patient_id
  WHERE
      (
          medication_display ILIKE '%casodex%'
          OR medication_display ILIKE '%eligard%'
          OR medication_display ILIKE '%xtandi%'
          OR medication_display ILIKE '%abiraterone%'
          OR medication_display ILIKE '%lupron%'
          OR medication_display ILIKE '%bicalutamide%'
          OR medication_display ILIKE '%trelstar%'
          OR medication_display ILIKE '%enzalutamide%'
          OR medication_display ILIKE '%zytiga%'
          OR medication_display ILIKE '%leuprolide%'
          OR medication_display ILIKE '%degarelix%'
          OR medication_display ILIKE '%camcevi%'
          OR medication_display ILIKE '%eulexin%'
          OR medication_display ILIKE '%orgovyx%'
          OR medication_display ILIKE '%yonsa%'
          OR medication_display ILIKE '%firmagon%'
          OR medication_display ILIKE '%flutamide%'
          OR medication_display ILIKE '%zoladex%'
      )
      AND mpv.authored_on >= '2024-01-01'
  GROUP BY
      tp.patient_id,
      tp.last_encounter_date,
      mpv.medication_request_id,
      mpv.medication_id,
      mpv.medication_display,
      mpv.authored_on,
      mpv.medication_name,
      mpv.medication_status,
      mpv.medication_code,
      mpv.medication_primary_code,
      mpv.medication_primary_system,
      mpv.medication_primary_text,
      mpv.dosage_instructions,
      mpv.categories,
      mpv.notes;