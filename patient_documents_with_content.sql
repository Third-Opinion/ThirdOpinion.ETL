-- Query to get document references with content for selected patients
-- Includes the document_reference_content table for URLs and content types

WITH target_patients AS (
    -- Patients with specific conditions who had encounters after 2024-01-01
    SELECT DISTINCT
        c.patient_id,
        c.code_code,
        c.code_system
    FROM fact_fhir_conditions_view_v1 AS c 
    INNER JOIN (
        SELECT 
            patient_id,
            MAX(start_time) AS last_encounter_date
        FROM fact_fhir_encounters_view_v2
        GROUP BY patient_id
    ) AS e ON c.patient_id = e.patient_id
    WHERE (
        (c.code_code IN ('C61', 'Z19.1', 'Z19.2', 'R97.21') 
         AND c.code_system = 'http://hl7.org/fhir/sid/icd-10-cm')
    )
        AND e.last_encounter_date >= '2024-01-01'
    GROUP BY c.patient_id, c.code_code, c.code_system
)
-- Get documents with content details
SELECT 
    tp.patient_id,
    tp.code_code as condition_code,
    mpv.document_reference_id,
    mpv.date as document_date,
    mpv.type_code,
    mpv.type_display,
    mpv.content,
    drc.attachment_content_type,
    drc.attachment_url,
    CASE 
        WHEN drc.attachment_url ILIKE 'ref:%' THEN 'Reference'
        WHEN drc.attachment_url ILIKE 's3:%' THEN 'S3'
        WHEN drc.attachment_url ILIKE 'http%' THEN 'HTTP'
        ELSE 'Other'
    END as url_type
FROM target_patients tp
INNER JOIN public.fact_fhir_document_references_view_v1 mpv 
    ON tp.patient_id = mpv.patient_id
LEFT JOIN dev.public.document_reference_content drc
    ON mpv.document_reference_id = drc.document_reference_id
WHERE EXTRACT(YEAR FROM mpv.date) >= 2024 
    AND mpv.content != 'NULL'
    AND drc.attachment_url NOT ILIKE 'ref:%'  -- Exclude reference URLs
ORDER BY tp.patient_id, mpv.date DESC;

-- Summary with content type breakdown
WITH target_patients AS (
    SELECT DISTINCT
        c.patient_id
    FROM fact_fhir_conditions_view_v1 AS c 
    INNER JOIN (
        SELECT 
            patient_id,
            MAX(start_time) AS last_encounter_date
        FROM fact_fhir_encounters_view_v2
        GROUP BY patient_id
    ) AS e ON c.patient_id = e.patient_id
    WHERE (
        (c.code_code IN ('C61', 'Z19.1', 'Z19.2', 'R97.21') 
         AND c.code_system = 'http://hl7.org/fhir/sid/icd-10-cm')
    )
        AND e.last_encounter_date >= '2024-01-01'
)
SELECT 
    drc.attachment_content_type,
    COUNT(DISTINCT mpv.document_reference_id) as document_count,
    COUNT(DISTINCT tp.patient_id) as patient_count
FROM target_patients tp
INNER JOIN public.fact_fhir_document_references_view_v1 mpv 
    ON tp.patient_id = mpv.patient_id
LEFT JOIN dev.public.document_reference_content drc
    ON mpv.document_reference_id = drc.document_reference_id
WHERE EXTRACT(YEAR FROM mpv.date) >= 2024 
    AND mpv.content != 'NULL'
    AND drc.attachment_url NOT ILIKE 'ref:%'
GROUP BY drc.attachment_content_type
ORDER BY document_count DESC;