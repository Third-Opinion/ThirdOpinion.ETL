WITH target_patients AS (
     SELECT 
        code_code, 
       patient_id
    FROM fact_fhir_conditions_view_v1 AS c 
    WHERE c.code_code IN ('C61', 'Z19.1', 'Z19.2', 'R97.21') 
        AND EXTRACT(YEAR FROM c.recorded_date) = 2025 
    GROUP BY code_code, patient_id
)

SELECT 
    tp.patient_id,
    medication_id,
    tp.code_code as condition_code,
  "medication_display",
  authored_on,
  "status"
  FROM target_patients tp
INNER JOIN public.fact_fhir_medication_requests_view_v1 mpv 
    ON tp.patient_id = mpv.patient_id
WHERE
 mpv.medication_display ILIKE '%Leuprolide%' OR 
mpv.medication_display ILIKE '%Leuprorelin%' OR 
mpv.medication_display ILIKE '%Leuproreline%' OR 
mpv.medication_display ILIKE '%Lupron%' OR 
mpv.medication_display ILIKE '%Eligard%' OR 
mpv.medication_display ILIKE '%Camcevi%' OR 
mpv.medication_display ILIKE '%Fensolvi%' OR 
mpv.medication_display ILIKE '%Viadur%' OR 
mpv.medication_display ILIKE '%TAP-144%' OR 
mpv.medication_display ILIKE '%A-43818%' OR 
mpv.medication_display ILIKE '%Abbott-43818%' OR 

-- Goserelin
mpv.medication_display ILIKE '%Goserelin%' OR 
mpv.medication_display ILIKE '%Zoladex%' OR 
mpv.medication_display ILIKE '%ICI-118630%' OR 

-- Triptorelin (including international variations)
mpv.medication_display ILIKE '%Triptorelin%' OR 
mpv.medication_display ILIKE '%Triptoréline%' OR 
mpv.medication_display ILIKE '%Triptorelinum%' OR 
mpv.medication_display ILIKE '%Trelstar%' OR 
mpv.medication_display ILIKE '%Triptodur%' OR 
mpv.medication_display ILIKE '%Decapeptyl%' OR 
mpv.medication_display ILIKE '%CL 118532%' OR 

-- LHRH/GnRH Antagonists
-- Degarelix
mpv.medication_display ILIKE '%Degarelix%' OR 
mpv.medication_display ILIKE '%Firmagon%' OR 
mpv.medication_display ILIKE '%FE 200486%' OR 
mpv.medication_display ILIKE '%ASP-3550%' OR 

-- Relugolix
mpv.medication_display ILIKE '%Relugolix%' OR 
mpv.medication_display ILIKE '%Orgovyx%' OR 
mpv.medication_display ILIKE '%Relumina%' OR 
mpv.medication_display ILIKE '%Myfembree%' OR 
mpv.medication_display ILIKE '%Ryeqo%' OR 
mpv.medication_display ILIKE '%TAK-385%' OR 
mpv.medication_display ILIKE '%RVT-601%' OR 

-- Anti-androgens
-- Bicalutamide
mpv.medication_display ILIKE '%Bicalutamide%' OR 
mpv.medication_display ILIKE '%Casodex%' OR 
mpv.medication_display ILIKE '%Cosudex%' OR 
mpv.medication_display ILIKE '%Calutide%' OR 
mpv.medication_display ILIKE '%ICI 176334%' OR 
mpv.medication_display ILIKE '%ICI-176334%' OR 

-- Enzalutamide (CRITICAL: Include MDV3100 variations)
mpv.medication_display ILIKE '%Enzalutamide%' OR 
mpv.medication_display ILIKE '%Xtandi%' OR 
mpv.medication_display ILIKE '%MDV3100%' OR 
mpv.medication_display ILIKE '%MDV 3100%' OR 
mpv.medication_display ILIKE '%MDV-3100%' OR 

-- Abiraterone (including development codes)
mpv.medication_display ILIKE '%Abiraterone%' OR 
mpv.medication_display ILIKE '%Zytiga%' OR 
mpv.medication_display ILIKE '%Yonsa%' OR 
mpv.medication_display ILIKE '%CB-7630%' OR 
mpv.medication_display ILIKE '%CB7630%' OR 
mpv.medication_display ILIKE '%CB 7630%' OR 
mpv.medication_display ILIKE '%JNJ-212082%' OR 
mpv.medication_display ILIKE '%JNJ212082%' OR 
mpv.medication_display ILIKE '%JNJ 212082%' OR 
mpv.medication_display ILIKE '%CB-7598%' OR 

-- Additional first-generation anti-androgens
-- Flutamide
mpv.medication_display ILIKE '%Flutamide%' OR 
mpv.medication_display ILIKE '%Eulexin%' OR 
mpv.medication_display ILIKE '%SCH-13521%' OR 

-- Nilutamide
mpv.medication_display ILIKE '%Nilutamide%' OR 
mpv.medication_display ILIKE '%Nilandron%' OR 
mpv.medication_display ILIKE '%Anandron%' OR 

-- Common ADT abbreviations that might appear
mpv.medication_display ILIKE '%LHRH agonist%' OR 
mpv.medication_display ILIKE '%GnRH agonist%' OR 
mpv.medication_display ILIKE '%LHRH analog%' OR 
mpv.medication_display ILIKE '%GnRH analog%' OR 
mpv.medication_display ILIKE '%androgen deprivation%' OR 
mpv.medication_display ILIKE '%antiandrogen%' OR 
mpv.medication_display ILIKE '%anti-androgen%'
ORDER BY 
    mpv.authored_on;