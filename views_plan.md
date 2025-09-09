# FHIR Database Views Strategy for Clinical Trials Matching

## Executive Summary

This document outlines a comprehensive strategy for creating database views to transform the normalized FHIR data structure into consolidated, analytics-ready views optimized for clinical trials matching software and QuickSight reporting. The views will reduce query complexity, improve performance, and provide clinically meaningful data aggregations.

**Key Benefits:**
- Simplified data access for clinical researchers
- Improved query performance through pre-aggregated data
- Standardized clinical terminology and coding
- Patient-centric data organization for trials matching
- Enhanced data quality through validation and cleansing

---

## View Categories Overview

| View Category | Purpose | Primary Use Case | QuickSight Value |
|---------------|---------|------------------|------------------|
| **Patient Summary Views** | Consolidated patient demographics and core clinical info | Patient identification and basic eligibility screening | Patient dashboards and demographics reports |
| **Condition Views** | Diagnosis and medical conditions with standardized coding | Primary condition-based trial matching | Disease prevalence and condition timeline reports |
| **Medication Views** | Current and historical medication data | Drug-based inclusion/exclusion criteria | Medication adherence and prescription patterns |
| **Clinical Timeline Views** | Chronological patient care events | Understanding patient journey and care progression | Treatment timeline visualizations |
| **Eligibility Screening Views** | Pre-computed common trial criteria | Rapid patient cohort identification | Trial feasibility dashboards |
| **Data Quality Views** | Data completeness and validation metrics | Ensuring data reliability for trials | Data quality monitoring dashboards |

---

## 1. Patient Summary Views

### 1.1 `v_patient_master`
**Purpose:** Single source of truth for patient demographics and administrative data

**Key Information:**
- Patient demographics (age, gender, birth_date, deceased status)
- Primary contact information (preferred address, phone, email)
- Marital status and communication preferences
- Managing organization and care team references
- Data quality indicators (completeness scores)

**Value Added:**
- Eliminates need to join multiple patient tables
- Provides calculated fields (current_age, age_at_death)
- Standardizes address and contact information
- Includes data completeness metrics

**Clinical Trials Usage:**
- Basic demographic eligibility screening
- Patient contact information for recruitment
- Age-based inclusion/exclusion criteria
- Geographic distribution analysis

```sql
-- Example structure
CREATE VIEW v_patient_master AS
SELECT 
    p.patient_id,
    p.active,
    p.gender,
    p.birth_date,
    DATEDIFF(year, p.birth_date, COALESCE(p.deceased_date, CURRENT_DATE)) as current_age,
    p.deceased,
    p.deceased_date,
    -- Primary name (most recent or preferred)
    pn.family_name as last_name,
    pn.given_names as first_name,
    -- Primary address
    pa.address_line as street_address,
    pa.city,
    pa.state,
    pa.postal_code,
    pa.country,
    -- Primary contact
    pt.telecom_value as primary_phone,
    -- Data quality score
    CASE 
        WHEN p.birth_date IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN pn.family_name IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN pa.city IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN pt.telecom_value IS NOT NULL THEN 1 ELSE 0 END
    ) / 4.0 * 100 as data_completeness_score
FROM patients p
LEFT JOIN patient_names pn ON p.patient_id = pn.patient_id 
    AND (pn.name_use = 'official' OR pn.name_use IS NULL)
LEFT JOIN patient_addresses pa ON p.patient_id = pa.patient_id 
    AND (pa.address_use = 'home' OR pa.address_use IS NULL)
LEFT JOIN patient_telecoms pt ON p.patient_id = pt.patient_id 
    AND pt.telecom_system = 'phone' AND pt.telecom_use = 'home';
```

### 1.2 `v_patient_clinical_summary`
**Purpose:** High-level clinical overview for each patient

**Key Information:**
- Total number of conditions, medications, procedures
- Date of first and last encounter
- Primary care provider information
- Most recent vital signs and lab values
- Care plan status and goals

**Value Added:**
- Provides clinical activity overview without detailed queries
- Identifies patients with sufficient clinical data for trials
- Shows care engagement patterns

---

## 2. Condition Views

### 2.1 `v_patient_conditions_current`
**Purpose:** Current active conditions with standardized coding

**Key Information:**
- Active conditions (clinical_status = 'active')
- Primary condition codes (ICD-10, SNOMED CT)
- Condition severity and onset information  
- Verification status and asserter information
- Body sites and clinical notes

**Value Added:**
- Filters out resolved/inactive conditions
- Standardizes condition coding across different systems
- Provides condition hierarchy (primary vs secondary diagnoses)
- Includes clinical context and severity

**Clinical Trials Usage:**
- Primary diagnosis matching for disease-specific trials
- Comorbidity assessment for exclusion criteria
- Condition severity-based eligibility
- Disease progression tracking

```sql
CREATE VIEW v_patient_conditions_current AS
SELECT 
    c.patient_id,
    c.condition_id,
    c.condition_text,
    -- Primary coding
    cc.code_code as primary_condition_code,
    cc.code_system as primary_condition_system,
    cc.code_display as primary_condition_name,
    -- Clinical details
    c.clinical_status_code,
    c.verification_status_code,
    c.severity_code,
    c.severity_display,
    c.onset_datetime,
    c.recorded_date,
    -- Calculated fields
    DATEDIFF(day, c.onset_datetime, CURRENT_DATE) as days_since_onset,
    CASE 
        WHEN cc.code_system LIKE '%icd%' THEN 'ICD'
        WHEN cc.code_system LIKE '%snomed%' THEN 'SNOMED'
        ELSE 'OTHER'
    END as coding_standard
FROM conditions c
INNER JOIN condition_codes cc ON c.condition_id = cc.condition_id
WHERE c.clinical_status_code = 'active'
    AND c.verification_status_code IN ('confirmed', 'provisional');
```

### 2.2 `v_condition_timeline`
**Purpose:** Complete condition history with progression tracking

**Key Information:**
- All conditions (active, resolved, inactive)
- Condition onset and resolution dates
- Condition progression and stage changes
- Related procedures and treatments

**Value Added:**
- Shows complete medical history
- Tracks condition progression over time
- Links conditions to treatments and outcomes

---

## 3. Medication Views

### 3.1 `v_patient_medications_current`
**Purpose:** Current active medications with dosage and administration details

**Key Information:**
- Active medication requests and dispenses
- Medication names (generic and brand)
- Current dosage, frequency, and route
- Prescribing provider and pharmacy information
- Adherence indicators

**Value Added:**
- Combines medication requests and dispenses
- Provides current medication regimen
- Includes dosage standardization
- Shows medication adherence patterns

**Clinical Trials Usage:**
- Drug-drug interaction screening
- Contraindicated medication exclusion
- Concomitant medication analysis
- Dosage-based eligibility criteria

```sql
CREATE VIEW v_patient_medications_current AS
SELECT 
    mr.patient_id,
    mr.medication_request_id,
    mr.medication_display as medication_name,
    mr.status as request_status,
    mr.authored_on as prescribed_date,
    -- Dosage information
    mrd.dosage_text,
    mrd.dosage_dose_value,
    mrd.dosage_dose_unit,
    mrd.dosage_timing_frequency,
    mrd.dosage_route_display,
    -- Dispense information
    md.when_handed_over as last_dispensed,
    md.quantity_value as dispensed_quantity,
    -- Calculated fields
    DATEDIFF(day, mr.authored_on, CURRENT_DATE) as days_since_prescribed,
    CASE 
        WHEN md.when_handed_over IS NOT NULL THEN 'DISPENSED'
        WHEN mr.status = 'active' THEN 'PRESCRIBED'
        ELSE 'INACTIVE'
    END as medication_status
FROM medication_requests mr
LEFT JOIN medication_request_dosage_instructions mrd 
    ON mr.medication_request_id = mrd.medication_request_id
LEFT JOIN medication_dispenses md 
    ON mr.medication_request_id = md.medication_dispense_id
WHERE mr.status IN ('active', 'completed')
    AND mr.authored_on >= DATEADD(month, -12, CURRENT_DATE); -- Last 12 months
```

### 3.2 `v_medication_adherence`
**Purpose:** Medication adherence patterns and compliance metrics

**Key Information:**
- Prescription vs dispense patterns
- Days supply and refill patterns
- Adherence scores and gaps in therapy
- Medication discontinuation reasons

**Value Added:**
- Calculates adherence metrics
- Identifies medication compliance patterns
- Shows therapy gaps and discontinuations

---

## 4. Clinical Timeline Views

### 4.1 `v_patient_clinical_timeline`
**Purpose:** Chronological view of all clinical events for each patient

**Key Information:**
- All clinical events (encounters, conditions, procedures, medications)
- Event types and dates
- Clinical outcomes and results
- Provider and location information

**Value Added:**
- Provides complete patient journey visualization
- Shows care progression and treatment responses
- Enables temporal analysis of clinical events

**Clinical Trials Usage:**
- Understanding patient care patterns
- Identifying treatment-naive patients
- Assessing disease progression
- Timeline-based eligibility criteria

```sql
CREATE VIEW v_patient_clinical_timeline AS
SELECT patient_id, 'CONDITION' as event_type, condition_text as event_description, 
       onset_datetime as event_date, condition_id as event_id
FROM conditions
WHERE onset_datetime IS NOT NULL
UNION ALL
SELECT patient_id, 'MEDICATION' as event_type, medication_display as event_description,
       authored_on as event_date, medication_request_id as event_id
FROM medication_requests
WHERE authored_on IS NOT NULL
UNION ALL
SELECT patient_id, 'PROCEDURE' as event_type, code_text as event_description,
       performed_date_time as event_date, procedure_id as event_id
FROM procedures
WHERE performed_date_time IS NOT NULL
ORDER BY patient_id, event_date DESC;
```

---

## 5. Eligibility Screening Views

### 5.1 `v_trial_eligibility_base`
**Purpose:** Pre-computed common eligibility criteria for rapid screening

**Key Information:**
- Age ranges and gender requirements
- Common inclusion/exclusion diagnoses
- Contraindicated medications
- Required lab values and vital signs
- Geographic and logistical factors

**Value Added:**
- Speeds up trial matching queries
- Standardizes common eligibility criteria
- Reduces computational overhead
- Enables rapid cohort identification

**Clinical Trials Usage:**
- Rapid patient screening for multiple trials
- Feasibility analysis for trial planning
- Automated eligibility pre-screening
- Population-based trial matching

### 5.2 `v_oncology_eligibility`
**Purpose:** Specialized view for oncology trials with cancer-specific criteria

**Key Information:**
- Cancer diagnoses with staging information
- Performance status indicators
- Prior cancer treatments
- Biomarker and genetic testing results
- Metastasis and progression indicators

**Value Added:**
- Cancer-specific data aggregation
- Staging and progression tracking
- Treatment history analysis
- Biomarker integration

---

## 6. Data Quality Views

### 6.1 `v_data_quality_summary`
**Purpose:** Monitor data completeness and quality across all patients

**Key Information:**
- Data completeness scores by category
- Missing critical data elements
- Data validation errors
- Duplicate record identification
- Temporal data consistency

**Value Added:**
- Ensures data reliability for clinical decisions
- Identifies data improvement opportunities
- Provides confidence metrics for trial matching
- Enables data quality monitoring

**QuickSight Usage:**
- Data quality dashboards
- Completeness trending reports
- Data validation monitoring
- Source system performance tracking

---

## Implementation Strategy

### Phase 1: Core Patient Views (Weeks 1-2)
1. `v_patient_master`
2. `v_patient_conditions_current`
3. `v_patient_medications_current`

### Phase 2: Clinical Analysis Views (Weeks 3-4)
1. `v_patient_clinical_timeline`
2. `v_condition_timeline`
3. `v_medication_adherence`

### Phase 3: Specialized Views (Weeks 5-6)
1. `v_trial_eligibility_base`
2. `v_oncology_eligibility`
3. `v_data_quality_summary`

### Phase 4: Performance Optimization (Week 7)
1. Index optimization for view performance
2. Materialized view implementation for large datasets
3. Query performance testing and tuning

---

## Performance Considerations

### Indexing Strategy
- Create indexes on frequently joined columns (patient_id, condition_id, etc.)
- Index date fields used in timeline views
- Consider composite indexes for multi-column filters

### Materialized Views
- Implement materialized views for computationally expensive aggregations
- Refresh schedules aligned with ETL processes
- Consider incremental refresh for large datasets

### Query Optimization
- Use appropriate WHERE clauses to limit data scope
- Leverage Redshift distribution and sort keys
- Monitor query performance and optimize accordingly

---

## Data Governance and Security

### Access Controls
- Implement row-level security for patient data
- Role-based access for different user types
- Audit logging for data access

### Data Privacy
- PHI protection in all views
- De-identification options for research use
- Compliance with HIPAA and other regulations

### Data Lineage
- Document data transformation logic
- Track data sources and dependencies
- Maintain change history for views

---

## Success Metrics

### Performance Metrics
- Query response time improvements (target: <5 seconds for standard reports)
- Reduction in complex join operations (target: 80% reduction)
- Increased QuickSight dashboard performance

### Clinical Metrics
- Improved trial matching accuracy
- Reduced time for patient cohort identification
- Enhanced data quality scores

### User Adoption Metrics
- Increased usage of standardized views
- Reduced custom query development
- Improved user satisfaction scores

---

## Maintenance and Evolution

### Regular Reviews
- Monthly performance review and optimization
- Quarterly clinical requirements assessment
- Annual view architecture review

### Version Control
- All view definitions stored in source control
- Change management process for view updates
- Documentation of breaking changes

### Continuous Improvement
- User feedback incorporation
- New clinical requirements integration
- Technology advancement adoption

This comprehensive view strategy will transform the complex FHIR data structure into an accessible, performant, and clinically meaningful data layer optimized for clinical trials matching and healthcare analytics.
