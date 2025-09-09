-- =====================================================
-- View: v_patient_medication_requests_comprehensive
-- Purpose: Comprehensive medication request data with associated dispenses and medication details
-- Category: Medication Views
-- =====================================================

CREATE OR REPLACE VIEW "public"."v_patient_medication_requests_comprehensive" AS
SELECT
    -- Primary identifiers
    mr.patient_id,
    mr.medication_request_id,
    mr.medication_id,
    
    -- Medication request details
    mr.status AS request_status,
    mr.intent,
    mr.reported_boolean,
    mr.authored_on AS prescribed_date,
    mr.medication_display AS medication_name,
    
    -- Medication details (from medications table)
    m.status AS medication_status,
    m.code_text AS medication_code_text,
    m.created_at AS medication_created_at,
    m.updated_at AS medication_updated_at,
    
    -- Medication dispense details (if available)
    md.medication_dispense_id,
    md.status AS dispense_status,
    md.type_system,
    md.type_code,
    md.type_display,
    md.quantity_value AS dispensed_quantity,
    md.when_handed_over AS dispensed_date,
    md.created_at AS dispense_created_at,
    md.updated_at AS dispense_updated_at,
    
    -- Encounter information
    mr.encounter_id,
    e.start_time AS encounter_date,
    
    -- Medication request notes
    mrn.note_text AS request_notes,
    
    -- Calculated fields
    COALESCE(md.when_handed_over, mr.authored_on) AS medication_date,
    GREATEST(
        COALESCE(md.created_at, '1900-01-01'::timestamp),
        COALESCE(mr.created_at, '1900-01-01'::timestamp),
        COALESCE(m.created_at, '1900-01-01'::timestamp)
    ) AS created_at,
    GREATEST(
        COALESCE(md.updated_at, '1900-01-01'::timestamp),
        COALESCE(mr.updated_at, '1900-01-01'::timestamp),
        COALESCE(m.updated_at, '1900-01-01'::timestamp)
    ) AS updated_at,
    
    -- Medication status summary
    CASE 
        WHEN md.when_handed_over IS NOT NULL THEN 'DISPENSED'
        WHEN mr.status = 'active' THEN 'PRESCRIBED'
        WHEN mr.status = 'completed' THEN 'COMPLETED'
        WHEN mr.status = 'cancelled' THEN 'CANCELLED'
        WHEN mr.status = 'stopped' THEN 'STOPPED'
        ELSE 'UNKNOWN'
    END AS medication_status_summary,
    
    -- Days since prescribed
    CASE 
        WHEN mr.authored_on IS NOT NULL 
        THEN DATEDIFF(day, mr.authored_on, CURRENT_DATE)
        ELSE NULL
    END AS days_since_prescribed,
    
    -- Days since dispensed (if applicable)
    CASE 
        WHEN md.when_handed_over IS NOT NULL 
        THEN DATEDIFF(day, md.when_handed_over, CURRENT_DATE)
        ELSE NULL
    END AS days_since_dispensed

FROM
    medication_requests mr
    -- Join with medications table to get medication details
    LEFT JOIN medications m ON mr.medication_id = m.medication_id
    -- Join with medication dispenses through auth prescriptions table
    LEFT JOIN medication_dispense_auth_prescriptions mda ON mr.medication_request_id = mda.authorizing_prescription_id
    LEFT JOIN medication_dispenses md ON mda.medication_dispense_id = md.medication_dispense_id
        AND mr.patient_id = md.patient_id
    -- Join with medication request notes
    LEFT JOIN medication_request_notes mrn ON mr.medication_request_id = mrn.medication_request_id
    -- Join with encounters to get encounter details
    LEFT JOIN encounters e ON mr.encounter_id = e.encounter_id

WHERE
    mr.patient_id IS NOT NULL

ORDER BY
    mr.patient_id,
    mr.authored_on DESC,
    mr.medication_request_id;

-- =====================================================
-- View Documentation
-- =====================================================
-- This view provides a comprehensive view of medication requests with their associated
-- dispense information and medication details. It uses medication_requests as the main
-- table and joins with related tables to provide complete medication information.
--
-- Key Features:
-- - Medication request as the primary data source
-- - Associated dispense information when available
-- - Medication details from the medications table
-- - Encounter and note information
-- - Calculated fields for status summary and timing
-- - Proper handling of NULL values with COALESCE
--
-- Clinical Trials Usage:
-- - Drug-based inclusion/exclusion criteria
-- - Medication adherence analysis
-- - Prescription pattern analysis
-- - Drug interaction screening
--
-- Performance Considerations:
-- - Indexes recommended on: patient_id, medication_request_id, authored_on
-- - Consider materialized view for large datasets
-- =====================================================
