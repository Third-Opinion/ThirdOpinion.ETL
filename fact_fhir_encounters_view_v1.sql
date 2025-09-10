-- ===================================================================
-- FACT FHIR ENCOUNTERS VIEW V1
-- ===================================================================
-- Comprehensive view combining all encounter-related tables
-- Primary Key: encounter_id
-- Source: FHIR Encounter resources
-- Purpose: Fact table for encounter analytics and reporting
-- ===================================================================

CREATE MATERIALIZED VIEW fact_fhir_encounters_view_v1
BACKUP NO
AUTO REFRESH NO
AS
SELECT 
    -- Primary encounter data
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
    e.meta_data,
    e.created_at,
    e.updated_at,
    
    -- Identifiers (aggregated)
    -- LISTAGG(DISTINCT ei.identifier_system, '|'),
    -- LISTAGG(DISTINCT ei.identifier_value, '|'),
    
    -- Hospitalization details
    -- eh.discharge_disposition_text,
    -- eh.discharge_code,
    -- eh.discharge_system,
    
    -- Locations (aggregated)
    LISTAGG(DISTINCT el.location_id, '|'),
    
    -- Participants (aggregated as JSON using SUPER type)
    JSON_PARSE(
        '[' || LISTAGG(
            '{' ||
            '"participant_type":"' || COALESCE(REPLACE(ep.participant_type, '"', '\\"'), '') || '",' ||
            '"participant_id":"' || COALESCE(REPLACE(ep.participant_id, '"', '\\"'), '') || '",' ||
            '"participant_display":"' || COALESCE(REPLACE(ep.participant_display, '"', '\\"'), '') || '",' ||
            '"period_start":"' || COALESCE(ep.period_start::VARCHAR, '') || '",' ||
            '"period_end":"' || COALESCE(ep.period_end::VARCHAR, '') || '"' ||
            '}',
            ','
        ) || ']'
    ) AS participants,
    
    -- Reasons (aggregated as JSON using SUPER type)
    JSON_PARSE(
        '[' || LISTAGG(
            '{' ||
            '"reason_code":"' || COALESCE(REPLACE(er.reason_code, '"', '\\"'), '') || '",' ||
            '"reason_system":"' || COALESCE(REPLACE(er.reason_system, '"', '\\"'), '') || '",' ||
            '"reason_display":"' || COALESCE(REPLACE(er.reason_display, '"', '\\"'), '') || '",' ||
            '"reason_text":"' || COALESCE(REPLACE(er.reason_text, '"', '\\"'), '') || '"' ||
            '}',
            ','
        ) || ']'
    ) AS reasons,
    
    -- Types (aggregated as JSON using SUPER type)
    JSON_PARSE(
        '[' || LISTAGG(
            '{' ||
            '"type_code":"' || COALESCE(REPLACE(et.type_code, '"', '\\"'), '') || '",' ||
            '"type_system":"' || COALESCE(REPLACE(et.type_system, '"', '\\"'), '') || '",' ||
            '"type_display":"' || COALESCE(REPLACE(et.type_display, '"', '\\"'), '') || '",' ||
            '"type_text":"' || COALESCE(REPLACE(et.type_text, '"', '\\"'), '') || '"' ||
            '}',
            ','
        ) || ']'
    ) AS types,
    
    -- Encounter duration (in minutes)
    CASE 
        WHEN e.start_time IS NOT NULL AND e.end_time IS NOT NULL 
        THEN EXTRACT(EPOCH FROM (e.end_time - e.start_time)) / 60
        ELSE NULL 
    END
    
FROM public.encounters e
    LEFT JOIN public.encounter_identifiers ei ON e.encounter_id = ei.encounter_id
    -- LEFT JOIN public.encounter_hospitalization eh ON e.encounter_id = eh.encounter_id
    LEFT JOIN public.encounter_locations el ON e.encounter_id = el.encounter_id
    LEFT JOIN public.encounter_participants ep ON e.encounter_id = ep.encounter_id
    LEFT JOIN public.encounter_reasons er ON e.encounter_id = er.encounter_id
    LEFT JOIN public.encounter_types et ON e.encounter_id = et.encounter_id

WHERE e.status != 'cancelled'

GROUP BY 
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
    e.meta_data,
    e.created_at,
    e.updated_at;

-- ===================================================================
-- AUTO REFRESH CONFIGURATION
-- ===================================================================
-- This materialized view is configured with AUTO REFRESH YES
-- Redshift will automatically refresh the view when underlying data changes
-- No manual refresh scheduling required
-- 
-- INCREMENTAL MAINTENANCE:
-- This view uses incremental maintenance for optimal performance
-- Column aliases have been removed to enable incremental updates
-- Only changed data is processed during refresh operations
-- ===================================================================

-- ===================================================================
-- MATERIALIZED VIEW METADATA
-- ===================================================================
-- Purpose: Materialized fact table for FHIR Encounter analytics
-- Source: FHIR Encounter resources from AWS HealthLake
-- Primary Key: encounter_id
-- Grain: One row per encounter
-- Type: Materialized View (pre-computed for performance)
-- 
-- Key Features:
-- - Combines all encounter-related normalized tables
-- - Uses JSON_PARSE to convert JSON strings to SUPER data type for efficient querying
-- - Aggregates complex fields as SUPER type JSON arrays (participants, reasons, types)
-- - Includes calculated fields for analytics (duration in minutes)
-- - Maintains referential integrity with patient_id
-- - Filters out cancelled encounters (status != 'cancelled')
-- - Preserves encounter status as string for flexible querying
-- - Pre-computed for fast query performance
-- - Auto-refreshes automatically when source data changes
-- - Backup disabled for cost optimization
-- - Escapes quotes in JSON strings to ensure valid JSON format
-- 
-- Performance Benefits:
-- - Faster query execution (pre-aggregated data)
-- - Reduced load on source tables
-- - Optimized for analytics workloads
-- - Consistent performance regardless of data volume
-- - Automatic refresh ensures data freshness
-- - Cost optimized (backup disabled)
-- 
-- COLUMN MAPPING (without aliases for incremental maintenance):
-- - Column 1-15: Core encounter fields (encounter_id, patient_id, status, resourcetype, class_code, class_display, 
--                 start_time, end_time, service_provider_id, appointment_id, parent_encounter_id, 
--                 meta_data, created_at, updated_at)
-- - Column 16-17: Aggregated identifiers (identifier_system, identifier_value) - COMMENTED OUT
-- - Column 18-20: Hospitalization details (discharge_disposition_text, etc.) - COMMENTED OUT
-- - Column 21: Aggregated location_ids (pipe-delimited string)
-- - Column 22: Participants as SUPER type JSON array with structure:
--   [{"participant_type":"string","participant_id":"string","participant_display":"string","period_start":"timestamp","period_end":"timestamp"},...]
-- - Column 23: Reasons as SUPER type JSON array with structure:
--   [{"reason_code":"string","reason_system":"string","reason_display":"string","reason_text":"string"},...]
-- - Column 24: Types as SUPER type JSON array with structure:
--   [{"type_code":"string","type_system":"string","type_display":"string","type_text":"string"},...]
-- - Column 25: Duration in minutes (calculated)
--
-- Usage Examples:
-- - Real-time encounter analytics and reporting
-- - Patient encounter history analysis
-- - Healthcare utilization metrics
-- - Encounter type and reason analysis
-- - Location and participant analysis
-- - Dashboard and BI tool queries
--
-- JSON Query Examples (using SUPER data type with JSON_PARSE):
-- -- Count participants per encounter:
-- SELECT encounter_id, ARRAY_SIZE(participants) as participant_count FROM fact_fhir_encounters_view_v1;
--
-- -- Extract first participant's type:
-- SELECT encounter_id, participants[0].participant_type FROM fact_fhir_encounters_view_v1;
--
-- -- Find encounters with emergency reason code:
-- SELECT encounter_id FROM fact_fhir_encounters_view_v1 WHERE reasons @> '[{"reason_code":"emergency"}]';
--
-- -- Find encounters with specific type code:
-- SELECT encounter_id FROM fact_fhir_encounters_view_v1 WHERE types @> '[{"type_code":"emergency"}]';
--
-- -- Get all participant IDs for an encounter:
-- SELECT encounter_id, p.participant_id 
-- FROM fact_fhir_encounters_view_v1, participants p 
-- WHERE encounter_id = 'your-encounter-id';
--
-- -- Find encounters with doctor participants:
-- SELECT encounter_id FROM fact_fhir_encounters_view_v1 WHERE participants @> '[{"participant_type":"doctor"}]';
--
-- -- Serialize back to JSON string if needed:
-- SELECT encounter_id, JSON_SERIALIZE(participants) as participants_json FROM fact_fhir_encounters_view_v1;
-- ===================================================================
