-- ===================================================================
-- FACT FHIR OBSERVATIONS VIEW V1 (NO VITAL SIGNS)
--
-- OVERVIEW:
-- Modified version of the observations view that:
-- 1. Excludes vital signs observations
-- 2. ONE ROW PER OBSERVATION with primary code and all codes in JSON array
-- 3. Includes JOIN with observation_notes table
-- 4. Includes JOIN with observation_codes table with rank-based filtering
-- 5. Fixed observation_reference_ranges field names
--
-- PRIMARY KEY: observation_id
-- 
-- SOURCE TABLES:
-- - public.observations: Core observation data with values, status, and timing
-- - public.observation_codes: Observation codes from different coding systems (NEW)
-- - public.observation_components: Multi-component observation data (lab panels)
-- - public.observation_categories: Observation category classifications
-- - public.observation_performers: Healthcare providers who performed observations
-- - public.observation_reference_ranges: Normal/abnormal ranges for interpretation (FIXED)
-- - public.observation_interpretations: Clinical interpretations (H, L, N, etc.)
-- - public.observation_notes: Clinical notes and annotations
-- - public.observation_derived_from: Parent-child observation relationships
-- - public.observation_members: Panel/battery observation groupings
-- 
-- REFRESH STRATEGY:
-- - AUTO REFRESH NO: Manual refresh required via scheduled jobs
-- - BACKUP NO: No backup required for this materialized view
-- 
-- FILTERING:
-- - EXCLUDES vital signs (category = 'vital-signs')
-- - Excludes observations with status 'entered-in-error'
-- - Uses LEFT JOINs to preserve observations without related data
-- - Returns one row per observation (not per code)
--
-- OUTPUT COLUMNS:
-- - All core observation fields
-- - primary_system, primary_code, primary_display: Primary code (rank 1)
-- - codes: JSON array of all codes with system, code, display, text, and rank
-- - All other aggregated JSON fields (components, categories, etc.)
--
-- ===================================================================

CREATE MATERIALIZED VIEW fact_fhir_observations_view_v1
AS
WITH aggregated_codes AS (
    SELECT
        oc.observation_id,
        CASE
            WHEN COUNT(*) > 0 THEN
                JSON_PARSE(
                    '[' || LISTAGG(
                        '{' ||
                        '"system":"' || COALESCE(REPLACE(REPLACE(REPLACE(oc.code_system, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                        '"code":"' || COALESCE(REPLACE(REPLACE(REPLACE(oc.code_code, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                        '"display":"' || COALESCE(REPLACE(REPLACE(REPLACE(oc.code_display, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                        '"text":"' || COALESCE(REPLACE(REPLACE(REPLACE(oc.code_text, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                        '"rank":' || code_rank ||
                        '}',
                        ','
                    ) WITHIN GROUP (ORDER BY code_rank) || ']'
                )
            ELSE NULL
        END AS codes,
        COUNT(*) AS code_count
    FROM (
        SELECT
            observation_id,
            code_system,
            code_code,
            code_display,
            code_text,
            ROW_NUMBER() OVER (
                PARTITION BY observation_id
                ORDER BY
                    CASE
                        WHEN code_system = 'http://loinc.org' THEN 1
                        WHEN code_system LIKE '%snomed%' THEN 2
                        ELSE 3
                    END,
                    code_code
            ) AS code_rank
        FROM public.observation_codes
        WHERE code_code IS NOT NULL
    ) oc
    GROUP BY oc.observation_id
),
aggregated_components AS (
    SELECT
        oc.observation_id,
        CASE
            WHEN COUNT(*) > 0 THEN
                JSON_PARSE(
                    '[' || LISTAGG(
                        '{' ||
                        -- Code object with nested coding array (FHIR structure)
                        '"code":{' ||
                            '"coding":[{' ||
                                '"system":"' || COALESCE(REPLACE(REPLACE(REPLACE(oc.component_system, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                                '"code":"' || COALESCE(REPLACE(REPLACE(REPLACE(oc.component_code, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                                '"display":"' || COALESCE(REPLACE(REPLACE(REPLACE(oc.component_display, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '"' ||
                            '}],' ||
                            '"text":"' || COALESCE(REPLACE(REPLACE(REPLACE(oc.component_text, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '"' ||
                        '}' ||
                        -- Value fields (conditional based on what's populated) - only one value type per component
                        CASE
                            -- valueQuantity with nested structure
                            WHEN oc.component_value_quantity_value IS NOT NULL THEN
                                ',"valueQuantity":{' ||
                                    '"value":' || oc.component_value_quantity_value || ',' ||
                                    '"unit":"' || COALESCE(REPLACE(REPLACE(REPLACE(oc.component_value_quantity_unit, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '"' ||
                                '}'
                            -- valueCodeableConcept with nested coding array
                            WHEN oc.component_value_codeable_concept_code IS NOT NULL THEN
                                ',"valueCodeableConcept":{' ||
                                    '"coding":[{' ||
                                        '"system":"' || COALESCE(REPLACE(REPLACE(REPLACE(oc.component_value_codeable_concept_system, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                                        '"code":"' || COALESCE(REPLACE(REPLACE(REPLACE(oc.component_value_codeable_concept_code, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                                        '"display":"' || COALESCE(REPLACE(REPLACE(REPLACE(oc.component_value_codeable_concept_display, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '"' ||
                                    '}]' ||
                                '}'
                            -- valueString (simple string value)
                            WHEN oc.component_value_string IS NOT NULL AND oc.component_value_string != '' THEN
                                ',"valueString":"' || REPLACE(REPLACE(REPLACE(oc.component_value_string, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' ') || '"'
                            ELSE ''
                        END ||
                        -- Data absent reason (if present)
                        CASE
                            WHEN oc.component_data_absent_reason_code IS NOT NULL THEN
                                ',"dataAbsentReason":{' ||
                                    '"coding":[{' ||
                                        '"code":"' || COALESCE(REPLACE(REPLACE(REPLACE(oc.component_data_absent_reason_code, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                                        '"display":"' || COALESCE(REPLACE(REPLACE(REPLACE(oc.component_data_absent_reason_display, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '"' ||
                                    '}]' ||
                                '}'
                            ELSE ''
                        END ||
                        '}',
                        ','
                    ) || ']'
                )
            ELSE NULL
        END AS components,
        COUNT(*) AS component_count
    FROM public.observation_components oc
    GROUP BY oc.observation_id
),
aggregated_categories AS (
    SELECT
        ocat.observation_id,
        CASE
            WHEN COUNT(*) > 0 THEN
                JSON_PARSE(
                    '[' || LISTAGG(
                        '{' ||
                        '"system":"' || COALESCE(REPLACE(REPLACE(REPLACE(ocat.category_system, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                        '"code":"' || COALESCE(REPLACE(REPLACE(REPLACE(ocat.category_code, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                        '"display":"' || COALESCE(REPLACE(REPLACE(REPLACE(ocat.category_display, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                        '"text":"' || COALESCE(REPLACE(REPLACE(REPLACE(ocat.category_text, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '"' ||
                        '}',
                        ','
                    ) || ']'
                )
            ELSE NULL
        END AS categories,
        COUNT(*) AS category_count
    FROM public.observation_categories ocat
    GROUP BY ocat.observation_id
),
aggregated_reference_ranges AS (
    SELECT 
        orr.observation_id,
        CASE 
            WHEN COUNT(*) > 0 THEN
                JSON_PARSE(
                    '[' || LISTAGG(
                        '{' ||
                        '"low":' || COALESCE(orr.range_low_value::VARCHAR, 'null') || ',' ||
                        '"high":' || COALESCE(orr.range_high_value::VARCHAR, 'null') || ',' ||
                        '"lowUnit":"' || COALESCE(REPLACE(REPLACE(REPLACE(orr.range_low_unit, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                        '"highUnit":"' || COALESCE(REPLACE(REPLACE(REPLACE(orr.range_high_unit, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                        '"type":"' || COALESCE(REPLACE(REPLACE(REPLACE(orr.range_type_code, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                        '"typeSystem":"' || COALESCE(REPLACE(REPLACE(REPLACE(orr.range_type_system, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                        '"typeDisplay":"' || COALESCE(REPLACE(REPLACE(REPLACE(orr.range_type_display, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                        '"text":"' || COALESCE(REPLACE(REPLACE(REPLACE(orr.range_text, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '"' ||
                        '}',
                        ','
                    ) || ']'
                )
            ELSE NULL
        END AS reference_ranges
    FROM public.observation_reference_ranges orr
    GROUP BY orr.observation_id
),
aggregated_interpretations AS (
    SELECT
        oi.observation_id,
        CASE
            WHEN COUNT(*) > 0 THEN
                JSON_PARSE(
                    '[' || LISTAGG(
                        '{' ||
                        '"code":"' || COALESCE(REPLACE(REPLACE(REPLACE(oi.interpretation_code, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                        '"system":"' || COALESCE(REPLACE(REPLACE(REPLACE(oi.interpretation_system, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                        '"display":"' || COALESCE(REPLACE(REPLACE(REPLACE(oi.interpretation_display, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '"' ||
                        '}',
                        ','
                    ) || ']'
                )
            ELSE NULL
        END AS interpretations,
        COUNT(*) AS interpretation_count
    FROM public.observation_interpretations oi
    GROUP BY oi.observation_id
),
aggregated_notes AS (
    SELECT
        obs_notes.observation_id,
        CASE
            WHEN COUNT(*) > 0 THEN
                JSON_PARSE(
                    '[' || LISTAGG(
                        '{' ||
                        '"text":"' || COALESCE(REPLACE(REPLACE(REPLACE(obs_notes.note_text, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                        '"time":"' || COALESCE(obs_notes.note_time::VARCHAR, '') || '",' ||
                        '"authorReference":"' || COALESCE(REPLACE(REPLACE(REPLACE(obs_notes.note_author_reference, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '"' ||
                        '}',
                        ','
                    ) || ']'
                )
            ELSE NULL
        END AS notes,
        COUNT(*) AS note_count
    FROM public.observation_notes obs_notes
    WHERE obs_notes.note_text IS NOT NULL AND obs_notes.note_text != ''
    GROUP BY obs_notes.observation_id
),
aggregated_performers AS (
    SELECT
        op.observation_id,
        CASE
            WHEN COUNT(*) > 0 THEN
                JSON_PARSE(
                    '[' || LISTAGG(
                        '{' ||
                        '"performerId":"' || COALESCE(REPLACE(REPLACE(REPLACE(op.performer_id, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '",' ||
                        '"performerType":"' || COALESCE(REPLACE(REPLACE(REPLACE(op.performer_type, '\\', ''), '"', ''), CHR(10)||CHR(13)||CHR(9), ' '), '') || '"' ||
                        '}',
                        ','
                    ) || ']'
                )
            ELSE NULL
        END AS performers,
        COUNT(*) AS performer_count
    FROM public.observation_performers op
    GROUP BY op.observation_id
),
primary_observation_category AS (
    SELECT DISTINCT
        oc.observation_id,
        FIRST_VALUE(oc.category_code) OVER (
            PARTITION BY oc.observation_id
            ORDER BY
                CASE
                    WHEN oc.category_system = 'http://terminology.hl7.org/CodeSystem/observation-category' THEN 1
                    ELSE 2
                END,
                oc.category_code
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS observation_category
    FROM public.observation_categories oc
    WHERE oc.category_code IS NOT NULL
),
primary_observation_code AS (
    SELECT DISTINCT
        oc.observation_id,
        FIRST_VALUE(oc.code_system) OVER (
            PARTITION BY oc.observation_id
            ORDER BY
                CASE
                    WHEN oc.code_system = 'http://loinc.org' THEN 1
                    WHEN oc.code_system LIKE '%snomed%' THEN 2
                    ELSE 3
                END,
                oc.code_code
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS primary_system,
        FIRST_VALUE(oc.code_code) OVER (
            PARTITION BY oc.observation_id
            ORDER BY
                CASE
                    WHEN oc.code_system = 'http://loinc.org' THEN 1
                    WHEN oc.code_system LIKE '%snomed%' THEN 2
                    ELSE 3
                END,
                oc.code_code
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS primary_code,
        FIRST_VALUE(oc.code_display) OVER (
            PARTITION BY oc.observation_id
            ORDER BY
                CASE
                    WHEN oc.code_system = 'http://loinc.org' THEN 1
                    WHEN oc.code_system LIKE '%snomed%' THEN 2
                    ELSE 3
                END,
                oc.code_code
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS primary_display
    FROM public.observation_codes oc
    WHERE oc.code_code IS NOT NULL
)
SELECT
    -- CORE OBSERVATION FIELDS FROM observations TABLE
    o.observation_id,
    o.patient_id,
    o.encounter_id,
    o.specimen_id,
    o.status,
    o.observation_text,
    o.effective_datetime,
    o.effective_period_start,
    o.effective_period_end,
    o.issued,
    poc2.primary_system,
    poc2.primary_code,
    poc2.primary_display,
    o.value_quantity_value,
    o.value_quantity_unit,
    o.value_quantity_system,
    o.value_codeable_concept_code,
    o.value_codeable_concept_system,
    o.value_codeable_concept_display,
    o.value_codeable_concept_text,
    o.value_string,
    o.value_boolean,
    o.value_datetime,
    o.data_absent_reason_system,
    o.data_absent_reason_code,
    o.data_absent_reason_display,
    o.body_site_system,
    o.body_site_code,
    o.body_site_display,
    o.body_site_text,
    o.method_system,
    o.method_code,
    o.method_display,
    o.method_text,
    o.meta_last_updated,
    o.derived_from,

    -- ETL Audit Fields
    o.created_at AS etl_created_at,
    o.updated_at AS etl_updated_at,

    -- COMPUTED FIELDS
    CASE
        WHEN o.value_string IS NOT NULL
          OR o.value_quantity_value IS NOT NULL
          OR o.value_datetime IS NOT NULL
          OR o.value_boolean IS NOT NULL
        THEN true
        ELSE false
    END AS has_value,

    -- CATEGORY FIELD
    poc.observation_category,

    -- AGGREGATED JSON FIELDS FROM CTEs
    aco.codes,
    ac.components,
    acat.categories,
    arr.reference_ranges,
    ai.interpretations,
    an.notes,
    ap.performers,
    
    -- COUNT FIELDS
    COALESCE(aco.code_count, 0) AS code_count,
    COALESCE(ac.component_count, 0) AS component_count,
    COALESCE(acat.category_count, 0) AS category_count,
    COALESCE(ai.interpretation_count, 0) AS interpretation_count,
    COALESCE(an.note_count, 0) AS note_count,
    COALESCE(ap.performer_count, 0) AS performer_count

FROM public.observations o
    LEFT JOIN aggregated_codes aco ON o.observation_id = aco.observation_id
    LEFT JOIN aggregated_components ac ON o.observation_id = ac.observation_id
    LEFT JOIN aggregated_categories acat ON o.observation_id = acat.observation_id
    LEFT JOIN aggregated_reference_ranges arr ON o.observation_id = arr.observation_id
    LEFT JOIN aggregated_interpretations ai ON o.observation_id = ai.observation_id
    LEFT JOIN aggregated_notes an ON o.observation_id = an.observation_id
    LEFT JOIN aggregated_performers ap ON o.observation_id = ap.observation_id
    LEFT JOIN primary_observation_category poc ON o.observation_id = poc.observation_id
    LEFT JOIN primary_observation_code poc2 ON o.observation_id = poc2.observation_id

WHERE o.status != 'entered-in-error'
    -- EXCLUDE VITAL SIGNS
    AND o.observation_id NOT IN (
        SELECT DISTINCT observation_id 
        FROM public.observation_categories 
        WHERE category_code = 'vital-signs'
            OR category_display ILIKE '%vital%'
    );
-- USAGE NOTES
-- This view provides a comprehensive non-vital sign observations dataset with:
-- 1. All observation codes from different coding systems (LOINC, SNOMED, etc.)
-- 2. Complete reference ranges with low/high values and text
-- 3. Clinical notes and annotations
-- 4. Performer information
-- 5. Components for multi-part observations
-- 6. Categories and interpretations
-- 
-- The view excludes vital signs to focus on lab results and diagnostic tests.
-- All multi-valued fields are aggregated as JSON arrays for easy querying.
-- ===================================================================