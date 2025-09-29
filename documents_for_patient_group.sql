WITH target_patients AS (
    SELECT 
        c.code_code,
        c.code_system,
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
        -- ICD-10-CM codes
        (c.code_code IN ('C61', 'Z19.1', 'Z19.2', 'R97.21') 
         AND c.code_system = 'http://hl7.org/fhir/sid/icd-10-cm')
        -- Add SNOMED CT codes here as needed, e.g.:
        -- OR (c.code_code IN ('399068003', '161891005') 
        --     AND c.code_system = 'http://snomed.info/sct')
        -- Add other code systems as needed:
        -- OR (c.code_code IN ('other_codes_here') 
        --     AND c.code_system = 'other_system_uri_here')
    )
        AND EXTRACT(YEAR FROM e.last_encounter_date) = 2025 
    GROUP BY c.code_code, c.code_system, c.patient_id
)

SELECT 
--    type_display,
--    COUNT((mpv.type_display))

    tp.patient_id,
    mpv.date,
    mpv.type_code,
    mpv.type_display,
    drc.attachment_content_type,
    drc.attachment_url
FROM target_patients tp
INNER JOIN public.fact_fhir_document_references_view_v1 mpv 
    ON tp.patient_id = mpv.patient_id
INNER JOIN dev.public.document_reference_content drc
    ON mpv.document_reference_id = drc.document_reference_id
WHERE EXTRACT(YEAR FROM mpv.date) >= 2024 
    AND attachment_url NOT ILIKE 'ref:%'
    AND mpv.content != 'NULL';
-- GROUP BY mpv.type_display;
