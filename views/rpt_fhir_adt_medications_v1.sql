CREATE OR REPLACE VIEW public.rpt_fhir_adt_medications_v1 AS
WITH target_patients AS (
    SELECT * FROM rpt_fhir_hmu_patients_v1 WHERE last_encounter_date >= '2025-07-01'
)
SELECT
    tp.patient_id,
    tp.last_encounter_date,
    LISTAGG(DISTINCT mpv.medication_display, ', ') WITHIN GROUP (ORDER BY mpv.medication_display) AS medications_list,
    COUNT(mpv.medication_display) AS total_medication_requests
FROM target_patients tp
INNER JOIN public.fact_fhir_medication_requests_view_v1 mpv
    ON tp.patient_id = mpv.patient_id
WHERE
    (
        mpv.medication_display ILIKE '%casodex%'
        OR mpv.medication_display ILIKE '%eligard%'
        OR mpv.medication_display ILIKE '%xtandi%'
        OR mpv.medication_display ILIKE '%abiraterone%'
        OR mpv.medication_display ILIKE '%lupron%'
        OR mpv.medication_display ILIKE '%bicalutamide%'
        OR mpv.medication_display ILIKE '%trelstar%'
        OR mpv.medication_display ILIKE '%enzalutamide%'
        OR mpv.medication_display ILIKE '%zytiga%'
        OR mpv.medication_display ILIKE '%leuprolide%'
        OR mpv.medication_display ILIKE '%degarelix%'
        OR mpv.medication_display ILIKE '%camcevi%'
        OR mpv.medication_display ILIKE '%eulexin%'
        OR mpv.medication_display ILIKE '%orgovyx%'
        OR mpv.medication_display ILIKE '%yonsa%'
        OR mpv.medication_display ILIKE '%firmagon%'
        OR mpv.medication_display ILIKE '%flutamide%'
        OR mpv.medication_display ILIKE '%zoladex%'
    )
    AND mpv.authored_on >= '2024-01-01'
    -- AND tp.patient_id IN('a-15454.E-464973', 'a-15454.E-337187','a-15454.E-527614','a-15454.E-490596')
GROUP BY
    tp.patient_id,
    tp.last_encounter_date
ORDER BY
    tp.patient_id;