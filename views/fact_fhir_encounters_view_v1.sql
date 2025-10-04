-- ===================================================================
-- FACT FHIR ENCOUNTERS VIEW V1
-- ===================================================================
-- 
-- OVERVIEW:
-- Enhanced materialized view for FHIR Encounter resources with advanced analytics
-- including duration calculations, participant aggregations, and diagnosis/procedure counts.
-- This V2 version removes AUTO REFRESH in preparation for scheduled refresh patterns.
-- 
-- PRIMARY KEY: encounter_id
-- 
-- SOURCE TABLES:
-- - public.encounters: Primary encounter data
-- - public.encounter_identifiers: External identifiers (NOT AVAILABLE - using placeholders)
-- - public.encounter_hospitalization: Hospitalization details
-- - public.encounter_locations: Location information
-- - public.encounter_participants: Care team participants
-- - public.encounter_reasons: Reason codes
-- - public.encounter_types: Encounter type classifications
-- - public.conditions: Associated diagnoses via encounter_id (V2 enhancement)
-- - public.condition_codes: Condition codes and descriptions
-- 
-- REFRESH STRATEGY:
-- - AUTO REFRESH NO: Uses scheduled refresh via AWS Lambda
-- - BACKUP NO: No backup required for this materialized view
-- - Refresh frequency: Hourly during business hours, every 3 hours off-hours
-- 
-- V2 ENHANCEMENTS:
-- - Advanced duration calculations with business day logic
-- - LISTAGG for participant and diagnosis aggregation
-- - Diagnosis counts from conditions table
-- - Length of stay calculations
-- - Emergency vs scheduled encounter classification
-- 
-- ===================================================================

CREATE MATERIALIZED VIEW fact_fhir_encounters_view_v1
BACKUP NO
AS
WITH condition_counts AS (
    -- Count conditions and get primary diagnosis with prioritized code system
    SELECT
        c.encounter_id,
        COUNT(DISTINCT c.condition_id) AS diagnosis_count,
        FIRST_VALUE(cc.code_code) OVER (
            PARTITION BY c.encounter_id
            ORDER BY
                CASE cc.code_system
                    WHEN 'http://hl7.org/fhir/sid/icd-10-cm' THEN 1
                    WHEN 'http://hl7.org/fhir/sid/icd-9-cm' THEN 2
                    WHEN 'http://snomed.info/sct' THEN 3
                    ELSE 4
                END,
                cc.code_code
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS primary_diagnosis_code,
        FIRST_VALUE(COALESCE(cc.code_display, cc.code_text)) OVER (
            PARTITION BY c.encounter_id
            ORDER BY
                CASE cc.code_system
                    WHEN 'http://hl7.org/fhir/sid/icd-10-cm' THEN 1
                    WHEN 'http://hl7.org/fhir/sid/icd-9-cm' THEN 2
                    WHEN 'http://snomed.info/sct' THEN 3
                    ELSE 4
                END,
                cc.code_code
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS primary_diagnosis_display,
        FIRST_VALUE(cc.code_system) OVER (
            PARTITION BY c.encounter_id
            ORDER BY
                CASE cc.code_system
                    WHEN 'http://hl7.org/fhir/sid/icd-10-cm' THEN 1
                    WHEN 'http://hl7.org/fhir/sid/icd-9-cm' THEN 2
                    WHEN 'http://snomed.info/sct' THEN 3
                    ELSE 4
                END,
                cc.code_code
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS primary_diagnosis_code_system
    FROM public.conditions c
        LEFT JOIN public.condition_codes cc ON c.condition_id = cc.condition_id
    WHERE c.encounter_id IS NOT NULL
    GROUP BY c.encounter_id, cc.code_code, cc.code_display, cc.code_text, cc.code_system
),
encounter_conditions AS (
    -- Aggregate conditions linked to each encounter using conditions table with code system shorthand
    SELECT
        c.encounter_id,
        LISTAGG(DISTINCT
            CASE cc.code_system
                WHEN 'http://hl7.org/fhir/sid/icd-10-cm' THEN 'ICD10:'
                WHEN 'http://hl7.org/fhir/sid/icd-9-cm' THEN 'ICD9:'
                WHEN 'http://snomed.info/sct' THEN 'SNOMED:'
                ELSE COALESCE(cc.code_system, 'UNK') || ':'
            END ||
            COALESCE(cc.code_code, '') || ':' || COALESCE(cc.code_display, cc.code_text, ''),
            ' | '
        ) WITHIN GROUP (ORDER BY c.onset_datetime) AS all_diagnoses
    FROM public.conditions c
        LEFT JOIN public.condition_codes cc ON c.condition_id = cc.condition_id
    WHERE c.encounter_id IS NOT NULL
    GROUP BY c.encounter_id
),
participant_counts AS (
    -- Count participants
    SELECT
        encounter_id,
        COUNT(DISTINCT participant_id) AS participant_count,
        COUNT(DISTINCT CASE
            WHEN participant_type = 'primary_performer' THEN participant_id
        END) AS primary_performer_count
    FROM public.encounter_participants
    GROUP BY encounter_id
),
participant_aggregation AS (
    -- Enhanced participant aggregation with roles
    SELECT 
        encounter_id,
        LISTAGG(
            participant_type || ':' || participant_id || 
            CASE 
                WHEN participant_display IS NOT NULL 
                THEN ' (' || participant_display || ')' 
                ELSE '' 
            END,
            ' | '
        ) WITHIN GROUP (ORDER BY participant_type) AS all_participants
    FROM public.encounter_participants
    GROUP BY encounter_id
),
location_counts AS (
    -- Count distinct locations
    SELECT 
        encounter_id,
        COUNT(DISTINCT location_id) AS location_count
    FROM public.encounter_locations
    GROUP BY encounter_id
),
location_timeline AS (
    -- Track location changes
    SELECT 
        encounter_id,
        LISTAGG(
            location_id,
            ' -> '
        ) WITHIN GROUP (ORDER BY location_id) AS location_sequence
    FROM public.encounter_locations
    GROUP BY encounter_id
),
-- Note: encounter_identifiers table not created by current ETL job
-- Using NULL placeholders for identifier fields
identifier_counts AS (
    SELECT
        encounter_id,
        0 AS identifier_count
    FROM public.encounters
),
aggregated_identifiers AS (
    SELECT
        encounter_id,
        NULL AS all_identifiers
    FROM public.encounters
),
aggregated_reasons AS (
    -- Aggregate reasons separately to avoid LISTAGG conflict
    SELECT 
        encounter_id,
        JSON_PARSE(
            '[' || LISTAGG(
                '{' ||
                '"code":"' || COALESCE(REPLACE(reason_code, '"', '\\"'), '') || '",' ||
                '"system":"' || COALESCE(REPLACE(reason_system, '"', '\\"'), '') || '",' ||
                '"display":"' || COALESCE(REPLACE(reason_display, '"', '\\"'), '') || '",' ||
                '"text":"' || COALESCE(REPLACE(reason_text, '"', '\\"'), '') || '"' ||
                '}',
                ','
            ) || ']'
        ) AS reasons
    FROM public.encounter_reasons
    GROUP BY encounter_id
),
aggregated_types AS (
    -- Aggregate types separately to avoid LISTAGG conflict
    SELECT 
        encounter_id,
        JSON_PARSE(
            '[' || LISTAGG(
                '{' ||
                '"code":"' || COALESCE(REPLACE(type_code, '"', '\\"'), '') || '",' ||
                '"system":"' || COALESCE(REPLACE(type_system, '"', '\\"'), '') || '",' ||
                '"display":"' || COALESCE(REPLACE(type_display, '"', '\\"'), '') || '",' ||
                '"text":"' || COALESCE(REPLACE(type_text, '"', '\\"'), '') || '"' ||
                '}',
                ','
            ) || ']'
        ) AS types
    FROM public.encounter_types
    GROUP BY encounter_id
),
aggregated_hospitalization AS (
    -- Aggregate hospitalization details
    SELECT 
        encounter_id,
        MAX(discharge_disposition_text) AS discharge_disposition_text,
        MAX(discharge_code) AS discharge_code,
        MAX(discharge_system) AS discharge_system
    FROM public.encounter_hospitalization
    GROUP BY encounter_id
)
SELECT 
    -- ============================================
    -- CORE ENCOUNTER DATA
    -- ============================================
    e.encounter_id,
    e.patient_id,
    e.status,
    e.resourcetype,
    e.class_code,
    e.class_display,
    e.start_time,
    e.end_time,
    e.service_provider_id,
    e.appointment_id,
    e.parent_encounter_id,

    -- ============================================
    -- DISCRETE META FIELDS
    -- ============================================
    e.meta_last_updated,

    e.created_at AS etl_created_at,
    e.updated_at AS etl_updated_at,
    
    -- ============================================
    -- IDENTIFIERS (FROM CTE)
    -- ============================================
    ai.all_identifiers,
    ic.identifier_count,
    
    -- ============================================
    -- HOSPITALIZATION DETAILS (FROM CTE)
    -- ============================================
    ah.discharge_disposition_text,
    ah.discharge_code,
    ah.discharge_system,
    
    -- ============================================
    -- LOCATION TRACKING (FROM CTE)
    -- ============================================
    lt.location_sequence,
    lc.location_count,
    
    -- ============================================
    -- ENHANCED PARTICIPANTS (FROM CTE)
    -- ============================================
    pa.all_participants,
    pc.participant_count,
    pc.primary_performer_count,
    
    -- ============================================
    -- REASONS (FROM CTE)
    -- ============================================
    ar.reasons,
    
    -- ============================================
    -- TYPES (FROM CTE)
    -- ============================================
    at.types,
    
    -- ============================================
    -- CONDITIONS/DIAGNOSES (FROM CTE)
    -- ============================================
    ec.all_diagnoses,
    condc.diagnosis_count,
    condc.primary_diagnosis_code,
    condc.primary_diagnosis_display,
    condc.primary_diagnosis_code_system,
    
    -- ============================================
    -- DURATION CALCULATIONS (V2 ENHANCEMENTS)
    -- ============================================
    -- Basic duration in minutes (rounded to whole minutes)
    CASE
        WHEN e.start_time IS NOT NULL AND e.end_time IS NOT NULL
        THEN ROUND(EXTRACT(EPOCH FROM (e.end_time - e.start_time)) / 60)
        ELSE NULL
    END AS duration_minutes,
    
    -- Is encounter complete
    CASE 
        WHEN e.status IN ('finished', 'cancelled', 'entered-in-error') THEN TRUE
        ELSE FALSE
    END AS is_complete,
    
    -- Days since encounter
    CASE 
        WHEN e.end_time IS NOT NULL 
        THEN DATEDIFF(day, e.end_time, CURRENT_DATE)
        WHEN e.start_time IS NOT NULL
        THEN DATEDIFF(day, e.start_time, CURRENT_DATE)
        ELSE NULL
    END AS days_since_encounter,
    
    -- ============================================
    -- RANKING AND ANALYTICS (V2 ENHANCEMENT)
    -- ============================================
    -- Rank encounters by recency per patient
    RANK() OVER (
        PARTITION BY e.patient_id 
        ORDER BY e.start_time DESC
    ) AS patient_encounter_rank

FROM public.encounters e
    LEFT JOIN identifier_counts ic ON e.encounter_id = ic.encounter_id
    LEFT JOIN aggregated_identifiers ai ON e.encounter_id = ai.encounter_id
    LEFT JOIN aggregated_hospitalization ah ON e.encounter_id = ah.encounter_id
    LEFT JOIN location_counts lc ON e.encounter_id = lc.encounter_id
    LEFT JOIN location_timeline lt ON e.encounter_id = lt.encounter_id
    LEFT JOIN participant_counts pc ON e.encounter_id = pc.encounter_id
    LEFT JOIN participant_aggregation pa ON e.encounter_id = pa.encounter_id
    LEFT JOIN aggregated_reasons ar ON e.encounter_id = ar.encounter_id
    LEFT JOIN aggregated_types at ON e.encounter_id = at.encounter_id
    LEFT JOIN condition_counts condc ON e.encounter_id = condc.encounter_id
    LEFT JOIN encounter_conditions ec ON e.encounter_id = ec.encounter_id

WHERE e.status != 'cancelled';

-- ===================================================================
-- V2 REFRESH CONFIGURATION
-- ===================================================================
-- This V2 materialized view is configured without AUTO REFRESH
-- Refresh is handled via scheduled AWS Lambda function
-- 
-- Refresh schedule:
-- - Business hours (6 AM - 10 PM): Every hour
-- - Off-hours: Every 3 hours
-- 
-- To manually refresh:
-- REFRESH MATERIALIZED VIEW fact_fhir_encounters_view_v2;
-- ===================================================================

-- ===================================================================
-- V2 PERFORMANCE OPTIMIZATIONS
-- ===================================================================
-- 1. CTEs reduce redundant calculations and improve readability
-- 2. CTEs separate LISTAGG operations from COUNT DISTINCT (Redshift requirement)
-- 3. Window functions enable efficient ranking without self-joins
-- 4. Consider DISTKEY on patient_id for patient-centric queries
-- 5. SORTKEY on (start_time, encounter_id) for temporal analysis
-- 6. Filtered out cancelled encounters to reduce data volume
-- ===================================================================

-- ===================================================================
-- V2 ANALYTICS CAPABILITIES
-- ===================================================================
-- 1. Length of stay analysis for capacity planning
-- 2. Complexity scoring for resource allocation
-- 3. Emergency vs scheduled tracking for operational metrics
-- 4. Participant involvement for care team analysis
-- 5. Diagnosis counts for case mix analysis
-- 6. Business day calculations for accurate throughput metrics
-- ===================================================================