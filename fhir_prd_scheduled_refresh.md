# Product Requirements Document: Amazon Redshift Materialized Views for FHIR Healthcare Data Entities

## Executive Summary
This Product Requirements Document defines the specifications for implementing materialized views in Amazon Redshift to create standardized fact views for FHIR healthcare data entities. The project will establish a performant, scalable analytics layer supporting healthcare insights with scheduled refresh patterns for data freshness and maintainability.

## 1. Project Overview and Objectives

### 1.1 Project Vision
Create a robust, scalable analytics foundation for FHIR healthcare data by implementing Amazon Redshift materialized views with scheduled refresh patterns that provide timely insights into patient care patterns, clinical outcomes, and operational metrics.

### 1.2 Core Objectives
**Primary Goals:**
- Establish standardized fact views for 8 critical FHIR entities: care_plans, conditions, diagnostic_reports, document_references, medication_requests, observations, practitioners, and procedures
- Create v2 enhanced views for patients and encounters with advanced aggregations
- Implement scheduled refresh capabilities ensuring controlled data freshness
- Implement JSON aggregation patterns optimized for scheduled refresh performance
- Create a maintainable, versioned view architecture supporting future schema evolution

**Business Objectives:**
- Reduce query latency from 30+ seconds to under 5 seconds for complex FHIR data aggregations
- Improve data freshness with scheduled hourly refresh cycles
- Standardize analytics across all healthcare reporting systems
- Enable self-service analytics for clinical and operational teams

### 1.3 Success Metrics
- Query performance: 95% of queries complete in <5 seconds
- Data freshness: Maximum 1-hour latency from source to analytical views via scheduled refreshes
- System availability: 99.9% uptime during business hours
- Scheduled refresh success rate: >99% successful scheduled refreshes

## 2. Technical Requirements

### 2.1 Scheduled Refresh Materialized View Specifications

**Refresh Strategy:**
```sql
-- Scheduled refresh pattern using AWS Lambda or Apache Airflow
-- Refresh frequency: Hourly during business hours (6 AM - 10 PM)
-- Off-hours refresh: Every 3 hours

-- Permitted aggregate functions for materialized views
- MAX(), MIN(), COUNT(), SUM(), AVG()
- DATE(), DATE_PART(), DATE_TRUNC() with specific parameters
- CAST() operations for data type conversion
- REGEXP_REPLACE() for JSON sanitization
- LISTAGG() for string aggregation (now permitted without AUTO REFRESH constraint)
- All JOIN types (INNER, LEFT, RIGHT, FULL)
- DISTINCT aggregates
- Window functions for advanced analytics
- Subqueries for complex logic
- Set operations (UNION, INTERSECT, EXCEPT)
```

### 2.2 JSON Aggregation Pattern
**Enhanced Implementation Approach with Scheduled Refresh:**
```sql
-- Enhanced aggregation pattern with LISTAGG now available
CREATE MATERIALIZED VIEW fact_fhir_observations_view_v1
AS
SELECT 
    patient_id,
    observation_date,
    MAX(CASE WHEN observation_type = 'blood_pressure_systolic' 
        THEN observation_value END) as bp_systolic,
    MAX(CASE WHEN observation_type = 'blood_pressure_diastolic' 
        THEN observation_value END) as bp_diastolic,
    MAX(CASE WHEN observation_type = 'heart_rate' 
        THEN observation_value END) as heart_rate,
    LISTAGG(DISTINCT observation_type, ', ') as observation_types,
    COUNT(DISTINCT observation_id) as observation_count
FROM observations
GROUP BY patient_id, observation_date;

-- Schedule refresh via external scheduler
-- REFRESH MATERIALIZED VIEW fact_fhir_observations_view_v1;
```

### 2.3 SUPER Data Type Requirements
**JSON Processing Specifications:**
```sql
-- SUPER type implementation with enhanced capabilities
CREATE MATERIALIZED VIEW fact_fhir_patients_view_v2
AS
WITH patient_details AS (
    SELECT DISTINCT
        patient_id,
        JSON_PARSE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    patient_json,
                    '"ssn":"[0-9]{3}-[0-9]{2}-[0-9]{4}"',
                    '"ssn":"***-**-****"'
                ),
                '\\b(True|False|None)\\b',
                CASE 
                    WHEN '\\1' = 'True' THEN 'true'
                    WHEN '\\1' = 'False' THEN 'false'
                    WHEN '\\1' = 'None' THEN 'null'
                END
            )
        ) as sanitized_patient_data
    FROM patients
)
SELECT * FROM patient_details;
```

### 2.4 Performance Requirements
- Query Response Time: P95 < 5 seconds, P99 < 10 seconds
- Refresh Duration: Complete refresh within 5 minutes for tables up to 1TB
- Concurrent Users: Support 100+ concurrent analytical queries
- Storage Compression: Achieve >3:1 compression ratio using ZSTD encoding

## 3. Naming Conventions and Patterns

### 3.1 View Naming Standard
Format: `fact_fhir_{entity}s_view_v{version}`

Examples:
- fact_fhir_care_plans_view_v1
- fact_fhir_conditions_view_v1
- fact_fhir_diagnostic_reports_view_v1
- fact_fhir_document_references_view_v1
- fact_fhir_medication_requests_view_v1
- fact_fhir_observations_view_v1
- fact_fhir_practitioners_view_v1
- fact_fhir_procedures_view_v1
- fact_fhir_patients_view_v2 (enhanced version)
- fact_fhir_encounters_view_v2 (enhanced version)

### 3.2 Column Naming Conventions
**Standard Patterns:**
- Primary keys: {entity}_id (e.g., patient_id, encounter_id)
- Foreign keys: {referenced_entity}_id (e.g., practitioner_id)
- Dates: {action}_date or {action}_datetime (e.g., observation_date)
- Codes: {entity}_code (e.g., diagnosis_code, procedure_code)
- Aggregates: {metric}_{aggregation} (e.g., observation_count, bp_max)

### 3.3 Special Case Handling
**Medications Entity:**
- Primary table: medication_requests.ddl (not medications.ddl)
- View name: fact_fhir_medication_requests_view_v1
- Include dosage instructions, prescriber information, and fulfillment status

**Enhanced V2 Views:**
- fact_fhir_patients_view_v2: Includes window functions, advanced aggregations
- fact_fhir_encounters_view_v2: Includes subqueries, complex joins

## 4. Data Processing Approach

### 4.1 JSON Sanitization Strategy
**Enhanced REGEXP_REPLACE Implementation:**
```sql
-- Three-tier sanitization with subquery support
CREATE MATERIALIZED VIEW fact_fhir_{entity}s_view_v1
AS
SELECT 
    entity_id,
    -- Enhanced sanitization with subqueries now available
    (SELECT 
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(json_data, '"ssn":"[0-9-]+"', '"ssn":"REDACTED"'),
                '\\b(True|False)\\b', 
                CASE WHEN '\\1' = 'True' THEN 'true' ELSE 'false' END, 
                1, 'g'
            ),
            '\\bNone\\b', 'null', 1, 'g'
        )
    ) as sanitized_json
FROM source_table;
```

### 4.2 Hierarchical Data Flattening
**Enhanced Parent-Child Relationship Processing for V2 Views:**
```sql
-- Enhanced fact_fhir_patients_view_v2 with full SQL capabilities
CREATE MATERIALIZED VIEW fact_fhir_patients_view_v2
AS
WITH patient_aggregates AS (
    SELECT 
        p.patient_id,
        p.birth_date,
        p.gender,
        p.race,
        p.ethnicity,
        -- Advanced aggregations using window functions
        FIRST_VALUE(pn.family_name) OVER (
            PARTITION BY p.patient_id 
            ORDER BY CASE WHEN pn.name_use = 'official' THEN 1 ELSE 2 END
        ) as primary_family_name,
        FIRST_VALUE(pn.given_name) OVER (
            PARTITION BY p.patient_id 
            ORDER BY CASE WHEN pn.name_use = 'official' THEN 1 ELSE 2 END
        ) as primary_given_name,
        -- LISTAGG for multiple values
        LISTAGG(DISTINCT pn.prefix, ', ') 
            WITHIN GROUP (ORDER BY pn.name_use) as all_prefixes,
        LISTAGG(DISTINCT pa.city, ', ') 
            WITHIN GROUP (ORDER BY pa.address_use) as all_cities,
        -- Complex aggregations
        COUNT(DISTINCT pn.name_id) as name_variations_count,
        COUNT(DISTINCT pa.address_id) as address_count,
        COUNT(DISTINCT pi.identifier_value) as identifier_count,
        -- Ranking functions
        DENSE_RANK() OVER (ORDER BY p.created_date) as patient_creation_rank
    FROM patients p
    LEFT JOIN patient_names pn ON p.patient_id = pn.patient_id
    LEFT JOIN patient_addresses pa ON p.patient_id = pa.patient_id
    LEFT JOIN patient_identifiers pi ON p.patient_id = pi.patient_id
    GROUP BY 
        p.patient_id, p.birth_date, p.gender, p.race, 
        p.ethnicity, p.created_date, pn.family_name, 
        pn.given_name, pn.name_use
),
patient_conditions AS (
    SELECT 
        patient_id,
        COUNT(DISTINCT condition_code) as unique_conditions,
        LISTAGG(DISTINCT condition_code, ', ') 
            WITHIN GROUP (ORDER BY onset_date DESC) as condition_list
    FROM conditions
    GROUP BY patient_id
)
SELECT DISTINCT
    pa.*,
    pc.unique_conditions,
    pc.condition_list
FROM patient_aggregates pa
LEFT JOIN patient_conditions pc ON pa.patient_id = pc.patient_id;

-- Enhanced fact_fhir_encounters_view_v2 with complex logic
CREATE MATERIALIZED VIEW fact_fhir_encounters_view_v2
AS
WITH encounter_metrics AS (
    SELECT 
        e.encounter_id,
        e.patient_id,
        e.encounter_class,
        e.encounter_type,
        e.period_start,
        e.period_end,
        e.status,
        e.service_provider_id,
        -- Duration calculations
        EXTRACT(EPOCH FROM (e.period_end - e.period_start))/3600 as encounter_duration_hours,
        -- Practitioner aggregations
        (SELECT LISTAGG(DISTINCT p.practitioner_name, ', ')
         FROM encounter_participants ep
         JOIN practitioners p ON ep.practitioner_id = p.practitioner_id
         WHERE ep.encounter_id = e.encounter_id) as participating_practitioners,
        -- Diagnosis aggregations
        (SELECT COUNT(DISTINCT diagnosis_code)
         FROM encounter_diagnoses
         WHERE encounter_id = e.encounter_id) as diagnosis_count,
        -- Procedure aggregations
        (SELECT COUNT(DISTINCT procedure_code)
         FROM procedures
         WHERE encounter_id = e.encounter_id) as procedure_count,
        -- Location history
        (SELECT LISTAGG(location_name || ':' || location_period, ' -> ')
                WITHIN GROUP (ORDER BY location_start)
         FROM encounter_locations
         WHERE encounter_id = e.encounter_id) as location_timeline
    FROM encounters e
),
encounter_rankings AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY period_start DESC) as recent_encounter_rank,
        LAG(period_start) OVER (PARTITION BY patient_id ORDER BY period_start) as previous_encounter_date,
        LEAD(period_start) OVER (PARTITION BY patient_id ORDER BY period_start) as next_encounter_date
    FROM encounter_metrics
)
SELECT * FROM encounter_rankings;
```

## 5. Implementation Methodology

### 5.1 Scheduled Refresh Architecture

**Scheduler Setup:**
```bash
# AWS Lambda function for scheduled refresh
import boto3
import json
from datetime import datetime

def lambda_handler(event, context):
    redshift_data = boto3.client('redshift-data')
    
    views = [
        'fact_fhir_care_plans_view_v1',
        'fact_fhir_conditions_view_v1',
        'fact_fhir_diagnostic_reports_view_v1',
        'fact_fhir_document_references_view_v1',
        'fact_fhir_medication_requests_view_v1',
        'fact_fhir_observations_view_v1',
        'fact_fhir_practitioners_view_v1',
        'fact_fhir_procedures_view_v1',
        'fact_fhir_patients_view_v2',
        'fact_fhir_encounters_view_v2'
    ]
    
    for view in views:
        response = redshift_data.execute_statement(
            ClusterIdentifier='healthcare-cluster',
            Database='healthcare_db',
            Sql=f'REFRESH MATERIALIZED VIEW {view};'
        )
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Refresh initiated at {datetime.now()}')
    }

# CloudWatch Events Rule for hourly execution
aws events put-rule \
  --name fhir-mv-refresh-schedule \
  --schedule-expression "rate(1 hour)"
```

## 7. Deployment Strategy

### 7.1 AWS CLI Deployment Process

**Pre-deployment Validation:**
```bash
# Validate view SQL syntax
aws redshift-data execute-statement \
  --cluster-identifier healthcare-cluster \
  --database healthcare_db \
  --sql "EXPLAIN CREATE MATERIALIZED VIEW fact_fhir_patients_view_v2..."

# Create EventBridge schedule for refresh
aws scheduler create-schedule \
  --name fhir-mv-refresh \
  --schedule-expression "rate(1 hour)" \
  --target '{
    "Arn": "arn:aws:lambda:us-east-1:123456789:function:refresh-materialized-views",
    "RoleArn": "arn:aws:iam::123456789:role/scheduler-role"
  }' \
  --flexible-time-window '{"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 15}'
```

**Deployment Execution:**
```bash
#!/bin/bash
# deployment_script.sh

CLUSTER_ID="healthcare-cluster"
DATABASE="healthcare_db"
VIEWS_V1=("care_plans" "conditions" "diagnostic_reports" "document_references" 
          "medication_requests" "observations" "practitioners" "procedures")
VIEWS_V2=("patients" "encounters")

# Deploy V1 views
for entity in "${VIEWS_V1[@]}"; do
    echo "Deploying fact_fhir_${entity}_view_v1..."
    
    # Create materialized view (without AUTO REFRESH)
    aws redshift-data execute-statement \
      --cluster-identifier $CLUSTER_ID \
      --database $DATABASE \
      --sql "$(cat views/fact_fhir_${entity}_view_v1.sql)" \
      --with-event > deployment_${entity}.json
    
    # Wait for completion
    STATEMENT_ID=$(jq -r '.Id' deployment_${entity}.json)
    aws redshift-data wait statement-completed --id $STATEMENT_ID
    
    # Initial refresh
    aws redshift-data execute-statement \
      --cluster-identifier $CLUSTER_ID \
      --database $DATABASE \
      --sql "REFRESH MATERIALIZED VIEW fact_fhir_${entity}_view_v1;"
done

# Deploy V2 views (patients and encounters)
for entity in "${VIEWS_V2[@]}"; do
    echo "Deploying fact_fhir_${entity}_view_v2..."
    
    # Create materialized view with enhanced features
    aws redshift-data execute-statement \
      --cluster-identifier $CLUSTER_ID \
      --database $DATABASE \
      --sql "$(cat views/fact_fhir_${entity}_view_v2.sql)" \
      --with-event > deployment_${entity}_v2.json
    
    # Wait for completion
    STATEMENT_ID=$(jq -r '.Id' deployment_${entity}_v2.json)
    aws redshift-data wait statement-completed --id $STATEMENT_ID
    
    # Initial refresh
    aws redshift-data execute-statement \
      --cluster-identifier $CLUSTER_ID \
      --database $DATABASE \
      --sql "REFRESH MATERIALIZED VIEW fact_fhir_${entity}_view_v2;"
done
```

### 7.2 Rollback Strategy
```bash
# Rollback procedure
# First disable scheduled refresh
aws scheduler delete-schedule --name fhir-mv-refresh

# Drop V2 views
for entity in patients encounters; do
    aws redshift-data execute-statement \
      --cluster-identifier $CLUSTER_ID \
      --database $DATABASE \
      --sql "DROP MATERIALIZED VIEW IF EXISTS fact_fhir_${entity}_view_v2 CASCADE;"
done

# Drop V1 views
for entity in "${VIEWS_V1[@]}"; do
    aws redshift-data execute-statement \
      --cluster-identifier $CLUSTER_ID \
      --database $DATABASE \
      --sql "DROP MATERIALIZED VIEW IF EXISTS fact_fhir_${entity}_view_v1 CASCADE;"
done
```

## 8. Success Criteria and Performance Requirements

### 8.3 Technical Validation Criteria
```sql
-- Success validation query
WITH validation_metrics AS (
    SELECT 
        'scheduled_refresh_configured' as metric,
        COUNT(*) as value,
        COUNT(*) = 10 as success
    FROM stv_mv_info 
    WHERE name LIKE 'fact_fhir_%_view_v%'
    
    UNION ALL
    
    SELECT 
        'recent_refresh_success' as metric,
        COUNT(*) as value,
        COUNT(*) = 10 as success
    FROM svl_mv_refresh_status
    WHERE mv_name LIKE 'fact_fhir_%_view_v%'
    AND status = 'Success'
    AND refresh_end_time > CURRENT_TIMESTAMP - INTERVAL '2 hours'
    
    UNION ALL
    
    SELECT 
        'performance_sla_met' as metric,
        COUNT(*) as value,
        COUNT(*) = 0 as success
    FROM stl_query
    WHERE querytxt LIKE '%fact_fhir_%_view_v%'
    AND EXTRACT(EPOCH FROM (endtime - starttime)) > 5
    AND starttime > CURRENT_DATE - 1
)
SELECT 
    metric,
    value,
    CASE WHEN success THEN 'PASS' ELSE 'FAIL' END as status
FROM validation_metrics;
```

## 9. Risk Assessment

### 9.1 Technical Risks

**Risk 3: JSON Sanitization Failures**
- Impact: Medium - PHI exposure risk
- Probability: Low
- Mitigation: Multi-tier REGEXP_REPLACE patterns, comprehensive testing
- Contingency: Additional sanitization layer in application tier

### 9.2 Data Quality Risks

**Risk 4: Source Data Schema Changes**
- Impact: High - View refresh failures
- Probability: Medium
- Mitigation: Schema change detection, versioned view strategy
- Contingency: Maintain v1 views while developing v2

### 9.3 Risk Matrix

| Risk | Impact | Probability | Score | Priority |
|------|--------|-------------|-------|----------|
| Schema Changes | 4 | 3 | 12 | High |
| JSON Sanitization | 3 | 2 | 6 | Medium |

## 10. Timeline and Resource Allocation

### 10.1 Project Timeline
**12-Week Implementation Schedule:**

| Phase | Duration | Weeks | Key Deliverables |
|-------|----------|-------|------------------|
| Discovery & Analysis | 2 weeks | 1-2 | Table analysis, refresh strategy design |
| Design & Architecture | 2 weeks | 3-4 | View specifications, scheduler architecture |
| Implementation | 6 weeks | 5-10 | All views development and testing |
| Deployment & Go-Live | 2 weeks | 11-12 | Production deployment, scheduler activation |

## 11. Monitoring and Maintenance

### 11.1 Operational Monitoring
```bash
# Monitoring dashboard queries for scheduled refresh
aws redshift-data execute-statement \
  --cluster-identifier healthcare-cluster \
  --database healthcare_db \
  --sql "
  SELECT 
    mv.name as view_name,
    mv.state,
    r.status as last_refresh_status,
    r.refresh_start_time,
    r.refresh_end_time,
    EXTRACT(EPOCH FROM (r.refresh_end_time - r.refresh_start_time)) as refresh_duration_seconds,
    CURRENT_TIMESTAMP - r.refresh_end_time as time_since_refresh
  FROM stv_mv_info mv
  LEFT JOIN svl_mv_refresh_status r ON mv.name = r.mv_name
  WHERE mv.name LIKE 'fact_fhir_%_view_v%'
  AND r.refresh_end_time = (
    SELECT MAX(refresh_end_time) 
    FROM svl_mv_refresh_status 
    WHERE mv_name = mv.name
  )
  ORDER BY time_since_refresh DESC;"
```

## 12. Future Enhancements

### 12.1 Version 3 Roadmap
- **Enhanced Aggregations**: Additional clinical metrics and KPIs using advanced window functions
- **Real-time Streaming**: Integration with Kinesis for near real-time updates
- **ML Integration**: SageMaker integration for predictive analytics
- **Advanced Visualizations**: QuickSight dashboard templates
- **Incremental Refresh**: Implement incremental refresh patterns for large tables

### 12.2 Scalability Considerations
- **Horizontal Scaling**: Plan for cluster resize as data volume grows
- **Partitioning Strategy**: Implement time-based partitioning for historical data
- **Archival Process**: S3 archival for data older than 2 years
- **Cross-Region Replication**: Disaster recovery across AWS regions
- **Refresh Optimization**: Implement incremental refresh where possible

## Appendices

### Appendix A: Sample View Definitions

#### fact_fhir_patients_view_v2
```sql
CREATE MATERIALIZED VIEW fact_fhir_patients_view_v2
DISTKEY (patient_id)
SORTKEY (birth_date, patient_id)
AS
WITH patient_core AS (
    SELECT 
        p.patient_id,
        p.birth_date,
        p.gender,
        p.race,
        p.ethnicity,
        p.marital_status,
        p.deceased_boolean,
        p.deceased_datetime,
        p.multiple_birth_boolean,
        p.multiple_birth_integer,
        -- Calculate age
        DATEDIFF(year, p.birth_date, CURRENT_DATE) as age_years,
        -- Sanitized JSON
        JSON_PARSE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(p.raw_json, '"ssn":"[0-9-]+"', '"ssn":"REDACTED"'),
                    '"mrn":"[^"]*"', '"mrn":"REDACTED"'
                ),
                '\\b(True|False|None)\\b',
                CASE 
                    WHEN '\\1' = 'True' THEN 'true'
                    WHEN '\\1' = 'False' THEN 'false'
                    ELSE 'null'
                END
            )
        ) as fhir_resource
    FROM patients p
),
patient_names_agg AS (
    SELECT 
        patient_id,
        MAX(CASE WHEN name_use = 'official' THEN family_name END) as official_family_name,
        MAX(CASE WHEN name_use = 'official' THEN given_name END) as official_given_name,
        LISTAGG(DISTINCT family_name, ', ') WITHIN GROUP (ORDER BY name_use) as all_family_names,
        COUNT(DISTINCT name_id) as name_count
    FROM patient_names
    GROUP BY patient_id
),
patient_addresses_agg AS (
    SELECT 
        patient_id,
        MAX(CASE WHEN address_use = 'home' THEN city END) as home_city,
        MAX(CASE WHEN address_use = 'home' THEN state END) as home_state,
        MAX(CASE WHEN address_use = 'home' THEN postal_code END) as home_postal_code,
        COUNT(DISTINCT address_id) as address_count
    FROM patient_addresses
    GROUP BY patient_id
),
patient_telecom_agg AS (
    SELECT 
        patient_id,
        MAX(CASE WHEN system = 'phone' AND use = 'home' THEN value END) as home_phone,
        MAX(CASE WHEN system = 'phone' AND use = 'mobile' THEN value END) as mobile_phone,
        MAX(CASE WHEN system = 'email' THEN value END) as email,
        COUNT(DISTINCT telecom_id) as contact_count
    FROM patient_telecoms
    GROUP BY patient_id
)
SELECT 
    pc.*,
    pn.official_family_name,
    pn.official_given_name,
    pn.all_family_names,
    pn.name_count,
    pa.home_city,
    pa.home_state,
    pa.home_postal_code,
    pa.address_count,
    pt.home_phone,
    pt.mobile_phone,
    pt.email,
    pt.contact_count
FROM patient_core pc
LEFT JOIN patient_names_agg pn ON pc.patient_id = pn.patient_id
LEFT JOIN patient_addresses_agg pa ON pc.patient_id = pa.patient_id
LEFT JOIN patient_telecom_agg pt ON pc.patient_id = pt.patient_id;
```

#### fact_fhir_encounters_view_v2
```sql
CREATE MATERIALIZED VIEW fact_fhir_encounters_view_v2
DISTKEY (patient_id)
SORTKEY (period_start, encounter_id)
AS
WITH encounter_base AS (
    SELECT 
        e.encounter_id,
        e.patient_id,
        e.status,
        e.encounter_class,
        e.encounter_type,
        e.priority,
        e.period_start,
        e.period_end,
        e.reason_code,
        e.reason_text,
        e.service_provider_id,
        e.admission_source,
        e.discharge_disposition,
        -- Calculate duration
        EXTRACT(EPOCH FROM (e.period_end - e.period_start))/3600 as duration_hours,
        EXTRACT(DAY FROM (e.period_end - e.period_start)) as duration_days,
        -- Time-based classifications
        EXTRACT(HOUR FROM e.period_start) as admission_hour,
        CASE 
            WHEN EXTRACT(DOW FROM e.period_start) IN (0,6) THEN 'Weekend'
            ELSE 'Weekday'
        END as admission_day_type
    FROM encounters e
),
encounter_participants AS (
    SELECT 
        encounter_id,
        LISTAGG(DISTINCT practitioner_name, ', ') 
            WITHIN GROUP (ORDER BY participant_type) as all_practitioners,
        COUNT(DISTINCT practitioner_id) as practitioner_count,
        MAX(CASE WHEN participant_type = 'primary' THEN practitioner_name END) as primary_practitioner
    FROM (
        SELECT 
            ep.encounter_id,
            ep.practitioner_id,
            ep.participant_type,
            p.practitioner_name
        FROM encounter_participants ep
        JOIN practitioners p ON ep.practitioner_id = p.practitioner_id
    )
    GROUP BY encounter_id
),
encounter_diagnoses AS (
    SELECT 
        encounter_id,
        COUNT(DISTINCT diagnosis_code) as diagnosis_count,
        MAX(CASE WHEN rank = 1 THEN diagnosis_code END) as primary_diagnosis_code,
        MAX(CASE WHEN rank = 1 THEN diagnosis_text END) as primary_diagnosis_text,
        LISTAGG(DISTINCT diagnosis_code, ', ') 
            WITHIN GROUP (ORDER BY rank) as all_diagnosis_codes
    FROM encounter_diagnoses
    GROUP BY encounter_id
),
encounter_procedures AS (
    SELECT 
        encounter_id,
        COUNT(DISTINCT procedure_code) as procedure_count,
        LISTAGG(DISTINCT procedure_code, ', ') 
            WITHIN GROUP (ORDER BY performed_datetime) as all_procedure_codes,
        MIN(performed_datetime) as first_procedure_time,
        MAX(performed_datetime) as last_procedure_time
    FROM procedures
    GROUP BY encounter_id
)
SELECT 
    eb.*,
    ep.all_practitioners,
    ep.practitioner_count,
    ep.primary_practitioner,
    ed.diagnosis_count,
    ed.primary_diagnosis_code,
    ed.primary_diagnosis_text,
    ed.all_diagnosis_codes,
    epr.procedure_count,
    epr.all_procedure_codes,
    epr.first_procedure_time,
    epr.last_procedure_time
FROM encounter_base eb
LEFT JOIN encounter_participants ep ON eb.encounter_id = ep.encounter_id
LEFT JOIN encounter_diagnoses ed ON eb.encounter_id = ed.encounter_id
LEFT JOIN encounter_procedures epr ON eb.encounter_id = epr.encounter_id;
```

### Appendix B: Validation Scripts
```bash
#!/bin/bash
# validate_views.sh

# Function to validate a single view
validate_view() {
    local view_name=$1
    local version=$2
    echo "Validating ${view_name}..."
    
    # Check if view exists
    EXISTS=$(aws redshift-data execute-statement \
        --cluster-identifier healthcare-cluster \
        --database healthcare_db \
        --sql "SELECT COUNT(*) FROM stv_mv_info WHERE name = '${view_name}';" \
        --output text --query 'Records[0][0].longValue')
    
    if [ "$EXISTS" -eq "1" ]; then
        echo "✓ View exists"
        
        # Check last refresh status
        LAST_REFRESH=$(aws redshift-data execute-statement \
            --cluster-identifier healthcare-cluster \
            --database healthcare_db \
            --sql "SELECT 
                    status,
                    EXTRACT(HOUR FROM (CURRENT_TIMESTAMP - refresh_end_time)) as hours_since_refresh
                   FROM svl_mv_refresh_status 
                   WHERE mv_name = '${view_name}'
                   ORDER BY refresh_end_time DESC
                   LIMIT 1;" \
            --output json)
        
        echo "✓ Last refresh status: $(echo $LAST_REFRESH | jq -r '.Records[0][0].stringValue')"
        echo "✓ Hours since refresh: $(echo $LAST_REFRESH | jq -r '.Records[0][1].longValue')"
    else
        echo "✗ View does not exist"
    fi
}

# Validate V1 views
for entity in care_plans conditions diagnostic_reports document_references \
              medication_requests observations practitioners procedures; do
    validate_view "fact_fhir_${entity}_view_v1" "v1"
done

# Validate V2 views
for entity in patients encounters; do
    validate_view "fact_fhir_${entity}_view_v2" "v2"
done

# Check scheduler status
echo "Checking scheduler status..."
aws scheduler get-schedule --name fhir-mv-refresh --output json | \
    jq '.State, .ScheduleExpression'
```

### Appendix C: Change Log

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2024-09-09 | Data Team | Initial PRD creation |
| 2.0 | 2025-09-09 | Data Team | Removed AUTO REFRESH requirement, implemented scheduled refresh pattern |
| 3.0 | 2025-09-09 | Data Team | Added v2 views for patients and encounters, streamlined sections |

**Document Status:** UPDATED - V2 Views Added
**Last Updated:** September 9, 2025
**Next Review Date:** End of Discovery Phase (Week 2)

---

## Key Changes Summary

This version includes:

1. **New V2 Views**: Enhanced fact_fhir_patients_view_v2 and fact_fhir_encounters_view_v2 with advanced SQL features
2. **Streamlined Documentation**: Removed development phases, QA processes, budget sections, and most risk items
3. **Focus on Technical Implementation**: Concentrated on view definitions and deployment strategies
4. **Advanced SQL Features**: V2 views leverage window functions, LISTAGG, subqueries, and complex joins

The V2 views for patients and encounters provide significantly enhanced analytics capabilities while maintaining backward compatibility with existing V1 views.
