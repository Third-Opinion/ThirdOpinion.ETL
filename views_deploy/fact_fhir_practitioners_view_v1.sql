CREATE MATERIALIZED VIEW fact_fhir_practitioners_view_v1
AUTO REFRESH NO
AS
WITH ranked_names AS (
    -- Subquery to rank and select the best name per practitioner
    -- Adapts the ranking logic from patient view
    SELECT practitioner_id, text, family, given,
           -- Rank names by completeness and quality
           ROW_NUMBER() OVER (PARTITION BY practitioner_id ORDER BY 
               CASE WHEN family IS NOT NULL AND family != '' AND given IS NOT NULL AND given != '' THEN 1    -- Complete names first
                    WHEN family IS NOT NULL AND family != '' THEN 2                                           -- Family name only
                    WHEN given IS NOT NULL AND given != '' THEN 3                                             -- Given name only
                    ELSE 4 END,                                                                               -- Text-only or empty names last
               family, given) as name_rank   -- Tie-break by name alphabetically
    FROM public.practitioner_names
    -- Include any name that has at least some content
    WHERE (family IS NOT NULL AND family != '')
       OR (given IS NOT NULL AND given != '')
       OR (text IS NOT NULL AND text != '')
),
address_counts AS (
    SELECT 
        practitioner_id,
        COUNT(DISTINCT line) AS address_count,
        MAX(city) AS primary_city,
        MAX(state) AS primary_state
    FROM public.practitioner_addresses
    WHERE line IS NOT NULL
    GROUP BY practitioner_id
),
aggregated_addresses AS (
    SELECT 
        pra.practitioner_id,
        JSON_PARSE(
            '[' || LISTAGG(DISTINCT
                '{' ||
                '"line":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(pra.line, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"city":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(pra.city, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"state":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(pra.state, '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"postalCode":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(pra.postal_code, '[\r\n\t]', ''), '"', ''), '') || '"' ||
                '}',
                ','
            ) WITHIN GROUP (ORDER BY pra.city, pra.state) || ']'
        ) AS addresses
    FROM public.practitioner_addresses pra
    WHERE pra.line IS NOT NULL
    GROUP BY pra.practitioner_id
),
telecom_counts AS (
    SELECT 
        practitioner_id,
        COUNT(DISTINCT "value") AS telecom_count,
        MAX(CASE WHEN "system" = 'phone' THEN "value" END) AS primary_phone,
        MAX(CASE WHEN "system" = 'email' THEN "value" END) AS primary_email
    FROM public.practitioner_telecoms
    WHERE "value" IS NOT NULL
    GROUP BY practitioner_id
),
aggregated_telecoms AS (
    SELECT 
        prt.practitioner_id,
        JSON_PARSE(
            '[' || LISTAGG(DISTINCT
                '{' ||
                '"system":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(prt."system", '[\r\n\t]', ''), '"', ''), '') || '",' ||
                '"value":"' || COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(prt."value", '[\r\n\t]', ''), '"', ''), '') || '"' ||
                '}',
                ','
            ) WITHIN GROUP (ORDER BY prt."system", prt."value") || ']'
        ) AS telecoms
    FROM public.practitioner_telecoms prt
    WHERE prt."value" IS NOT NULL
    GROUP BY prt.practitioner_id
)
SELECT 
    -- CORE PRACTITIONER DATA
    -- ============================================
    pr.practitioner_id,              -- Unique practitioner identifier (Primary Key)
    pr.resource_type,                -- FHIR resource type
    pr.active,                       -- Boolean: Is practitioner active?
    
    -- FHIR Metadata
    pr.meta_last_updated,              -- FHIR resource version
    
    -- ETL Audit Fields
    pr.created_at AS etl_created_at,
    pr.updated_at AS etl_updated_at,
    -- PRACTITIONER NAME PROCESSING (ADAPTED FROM PATIENT VIEW)
    -- Constructs a JSON object containing the practitioner's primary name
    -- Uses same name ranking and cleaning logic as patient view
    CASE 
        WHEN rn.practitioner_id IS NOT NULL THEN
            JSON_PARSE(
                '{"primary_name":{"text":"' || 
                -- Clean name text field: remove quotes, backslashes, whitespace
                COALESCE(REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(rn.text, '"', ''),
                        '\\\\', ''
                    ),
                    '[\r\n\t]', ''
                ), '') || 
                '","family":"' || 
                -- Clean family_name: remove quotes, backslashes, whitespace
                COALESCE(REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(rn.family, '"', ''),
                        '\\\\', ''
                    ),
                    '[\r\n\t]', ''
                ), '') || 
                '","given":"' || 
                -- Clean given_names: remove quotes, backslashes, whitespace  
                COALESCE(REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(rn.given, '"', ''),
                        '\\\\', ''
                    ),
                    '[\r\n\t]', ''
                ), '') || 
                '"}}'
            )
        ELSE
            -- Return null structure for practitioners without valid names
            JSON_PARSE('{"primary_name":null}')
    END AS names,
    -- ADDRESSES (AGGREGATED FROM CTE)
    -- ============================================
    aa.addresses,
    -- TELECOMS (AGGREGATED FROM CTE)
    -- ============================================
    at.telecoms,
    tc.telecom_count,
    tc.primary_phone,
    tc.primary_email,
    
    -- Determine if practitioner has complete contact info
    CASE 
        WHEN COALESCE(tc.telecom_count, 0) > 0 AND COALESCE(ac.address_count, 0) > 0 
        THEN TRUE
        ELSE FALSE
    END AS has_complete_contact,
    
    -- Primary address city (from CTE)
    ac.primary_city,
    
    -- Primary address state (from CTE)
    ac.primary_state
-- TABLE JOINS AND NAME RANKING LOGIC
-- ============================================
FROM public.practitioners pr
    LEFT JOIN ranked_names rn ON pr.practitioner_id = rn.practitioner_id AND rn.name_rank = 1  -- Only join rank 1 (best) name
    LEFT JOIN address_counts ac ON pr.practitioner_id = ac.practitioner_id
    LEFT JOIN aggregated_addresses aa ON pr.practitioner_id = aa.practitioner_id
    LEFT JOIN telecom_counts tc ON pr.practitioner_id = tc.practitioner_id
    LEFT JOIN aggregated_telecoms at ON pr.practitioner_id = at.practitioner_id;
-- REFRESH CONFIGURATION
-- This materialized view is configured with AUTO REFRESH NO
-- Manual refresh will be scheduled via AWS Lambda or Airflow
-- Refresh frequency should align with source data update patterns
-- 
-- To manually refresh:
-- REFRESH MATERIALIZED VIEW fact_fhir_practitioners_view_v1;
-- ===================================================================
-- INDEXES AND OPTIMIZATION
-- Redshift automatically creates and maintains sort keys and distribution keys
-- based on query patterns. Monitor query performance and adjust if needed:
-- - Consider DISTKEY on practitioner_id for practitioner-centric queries
-- - Consider SORTKEY on (active, primary_state) for location-based queries
-- ===================================================================
-- DATA QUALITY NOTES
-- 1. Practitioners without names will have NULL in names field
-- 2. Name ranking logic prioritizes complete names (family + given) over partial names
-- 3. Some practitioners may not have addresses or contact information
-- 4. All text fields are sanitized to ensure valid JSON format
-- 5. Contact completeness flag helps identify practitioners with full contact info
-- 6. Primary contact fields (phone, email, city, state) provide quick access to key info
-- 7. Counts provide data quality metrics for monitoring
-- ===================================================================