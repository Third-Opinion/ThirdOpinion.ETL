# FHIR Clinical Data Views Strategy

## Executive Summary

This document outlines a comprehensive strategy for creating database views to transform the normalized FHIR data structure into consolidated, analytics-ready formats optimized for clinical trials matching and QuickSight reporting. The views will aggregate complex relational data into denormalized structures that provide clear, accessible patient clinical profiles.

## Current State Analysis

The existing database contains 60+ tables following FHIR R4 specifications with proper normalization. While this structure ensures data integrity and compliance, it creates complexity for analytical queries requiring multiple joins across related tables. Clinical trials matching software needs consolidated patient profiles with easily accessible diagnostic, medication, and demographic information.

## View Categories & Strategy

### 1. Patient Master Views

#### 1.1 `v_patient_master`
**Purpose**: Consolidated patient demographic and administrative information
**Value**: Single source of truth for patient identification and basic demographics

**Consolidated Data**:
- Core patient data from `patients` table
- Primary address from `patient_addresses`
- Primary name from `patient_names`
- Primary contact information from `patient_telecoms`
- Preferred communication language from `patient_communications`
- Managing organization details

**Key Fields**:
```sql
- patient_id
- full_name (concatenated given + family)
- birth_date, age_years (calculated)
- gender
- primary_address (formatted)
- primary_phone, primary_email
- preferred_language
- active_status
- managing_organization_id
```

**Clinical Trials Value**: Enables rapid patient identification, demographic filtering, and contact information for recruitment.

#### 1.2 `v_patient_contacts_summary`
**Purpose**: Emergency contacts and care team relationships
**Value**: Consolidated view of patient support network

**Use Case**: Understanding patient support systems for trial participation feasibility.

### 2. Clinical Condition Views

#### 2.1 `v_patient_conditions_current`
**Purpose**: Active and confirmed patient conditions with full diagnostic details
**Value**: Comprehensive diagnosis profile for inclusion/exclusion criteria matching

**Consolidated Data**:
- Primary condition data from `conditions` table
- All condition codes (ICD-10, SNOMED, etc.) from `condition_codes`
- Condition categories from `condition_categories`
- Onset and resolution information
- Severity and clinical status
- Associated body sites from `condition_body_sites`

**Key Fields**:
```sql
- patient_id
- condition_id
- primary_diagnosis_code, secondary_codes
- diagnosis_display_name
- condition_category (problem-list-item, encounter-diagnosis)
- clinical_status (active, resolved, inactive)
- verification_status (confirmed, provisional)
- onset_date, resolution_date
- severity_level
- body_sites_affected
- days_since_onset (calculated)
```

**Clinical Trials Value**: Direct matching against trial inclusion/exclusion criteria based on specific diagnoses, disease duration, and severity.

#### 2.2 `v_patient_conditions_history`
**Purpose**: Complete historical view of all patient conditions
**Value**: Longitudinal disease progression and comorbidity analysis

**Use Case**: Understanding disease progression patterns and identifying patients with specific medical histories.

### 3. Medication Views

#### 3.1 `v_patient_medications_current`
**Purpose**: Currently active medications with dosing and administration details
**Value**: Comprehensive current medication profile for drug interaction screening

**Consolidated Data**:
- Active medication requests from `medication_requests`
- Medication details from `medications` table
- Dosage instructions from `medication_request_dosage_instructions`
- Categories from `medication_request_categories`
- Recent dispenses from `medication_dispenses`

**Key Fields**:
```sql
- patient_id
- medication_name
- medication_codes (RxNorm, NDC)
- current_status (active, stopped, completed)
- dosage_text, structured_dose
- frequency, route_of_administration
- start_date, end_date
- prescribing_provider
- therapeutic_category
- days_on_medication (calculated)
```

**Clinical Trials Value**: Screening for contraindicated medications, identifying patients on specific therapeutic regimens, washout period calculations.

#### 3.2 `v_patient_medication_history`
**Purpose**: Complete medication history including discontinued drugs
**Value**: Historical medication exposure analysis

**Use Case**: Identifying patients with prior exposure to specific drug classes or compounds for trial eligibility.

### 4. Laboratory and Diagnostic Views

#### 4.1 `v_patient_lab_results_latest`
**Purpose**: Most recent laboratory values by test type
**Value**: Current clinical status assessment through lab values

**Consolidated Data**:
- Latest observations by category from `observations`
- Lab categories from `observation_categories`
- Reference ranges from `observation_reference_ranges`
- Interpretation flags from `observation_interpretations`

**Key Fields**:
```sql
- patient_id
- test_name, test_code (LOINC)
- result_value, result_unit
- reference_range_low, reference_range_high
- abnormal_flag (normal, high, low, critical)
- test_date
- days_since_test (calculated)
- trending_direction (if multiple values)
```

**Clinical Trials Value**: Screening based on specific lab criteria (e.g., creatinine levels, liver function, blood counts).

#### 4.2 `v_patient_vital_signs_trends`
**Purpose**: Vital signs trends and patterns
**Value**: Clinical stability assessment and monitoring capabilities

**Use Case**: Identifying patients with stable vital signs or specific physiological parameters.

### 5. Encounter and Care Utilization Views

#### 5.1 `v_patient_encounters_summary`
**Purpose**: Healthcare utilization patterns and encounter history
**Value**: Understanding patient engagement with healthcare system

**Consolidated Data**:
- Encounter details from `encounters` table
- Encounter types and reasons
- Participating providers
- Location information

**Key Fields**:
```sql
- patient_id
- total_encounters_last_year
- last_encounter_date
- encounter_types (inpatient, outpatient, emergency)
- primary_care_provider
- specialist_providers
- hospitalization_count
- average_encounter_frequency
```

**Clinical Trials Value**: Assessing patient compliance likelihood, healthcare engagement, and provider relationships.

### 6. Comprehensive Patient Profile Views

#### 6.1 `v_clinical_trial_patient_profile`
**Purpose**: Comprehensive patient summary optimized for clinical trials screening
**Value**: Single view containing all key clinical trial matching criteria

**Consolidated Data**:
- Demographics from patient master view
- Active conditions with severity and duration
- Current medications with therapeutic classes
- Recent lab values and vital signs
- Healthcare utilization patterns
- Key exclusion indicators (pregnancy, organ transplant, etc.)

**Key Fields**:
```sql
- patient_id, demographics
- primary_diagnoses (top 5 by severity/recency)
- active_medication_classes
- key_lab_values (creatinine, liver enzymes, CBC)
- comorbidity_count
- last_encounter_date
- care_team_providers
- trial_exclusion_flags
- eligibility_score (calculated)
```

**Clinical Trials Value**: Rapid screening of entire patient population against trial criteria with single query.

#### 6.2 `v_patient_clinical_summary_dashboard`
**Purpose**: Executive dashboard view for clinical operations
**Value**: High-level patient population insights for QuickSight dashboards

**Use Case**: Population health management, quality metrics, and operational reporting.

## Implementation Strategy

### Phase 1: Foundation Views (Weeks 1-2)
1. `v_patient_master`
2. `v_patient_conditions_current`
3. `v_patient_medications_current`
4. `v_patient_lab_results_latest`

### Phase 2: Enhanced Analytics Views (Weeks 3-4)
1. `v_patient_encounters_summary`
2. `v_patient_vital_signs_trends`
3. `v_patient_conditions_history`
4. `v_patient_medication_history`

### Phase 3: Clinical Trials Optimization (Weeks 5-6)
1. `v_clinical_trial_patient_profile`
2. `v_patient_clinical_summary_dashboard`
3. Specialized screening views based on common trial criteria

## Technical Considerations

### Performance Optimization
- **Materialized Views**: Consider materializing frequently accessed views with complex aggregations
- **Indexing Strategy**: Create appropriate indexes on view key fields used in WHERE clauses
- **Incremental Refresh**: Implement incremental refresh patterns for large datasets
- **Partitioning**: Consider date-based partitioning for historical views

### Data Quality Features
- **Data Validation**: Include data quality flags (missing values, outliers)
- **Completeness Scores**: Calculate completeness percentages for key fields
- **Data Freshness**: Include last_updated timestamps and data age indicators
- **Standardization**: Apply consistent formatting and terminology across views

### QuickSight Integration
- **Column Naming**: Use business-friendly column names and descriptions
- **Data Types**: Optimize data types for QuickSight visualization capabilities
- **Calculated Fields**: Pre-calculate commonly used metrics and KPIs
- **Hierarchical Data**: Structure data to support drill-down capabilities

## Expected Benefits

### For Clinical Trials Matching
- **Query Performance**: 10x faster screening queries vs. multi-table joins
- **Data Accessibility**: Non-technical users can access clinical data
- **Standardization**: Consistent data representation across all applications
- **Completeness**: Clear visibility into data gaps affecting trial eligibility

### For QuickSight Reporting
- **Simplified Data Model**: Reduced complexity for report builders
- **Faster Dashboard Load Times**: Pre-aggregated data reduces query time
- **Rich Analytics**: Enable advanced analytics and trend analysis
- **Self-Service Capabilities**: Empower business users to create custom reports

### For Clinical Operations
- **Population Health**: Comprehensive patient population insights
- **Quality Metrics**: Standardized clinical quality indicators
- **Care Gap Analysis**: Identify patients missing recommended care
- **Provider Performance**: Support provider scorecards and quality improvement

## Maintenance and Governance

### View Documentation
- Maintain comprehensive documentation for each view
- Include business rules and calculation logic
- Document data lineage and source table relationships
- Provide usage examples and common query patterns

### Change Management
- Version control for view definitions
- Impact analysis for schema changes
- Testing procedures for view modifications
- Rollback procedures for failed deployments

### Monitoring and Optimization
- Monitor view performance and usage patterns
- Regular review of view effectiveness and user feedback
- Optimization recommendations based on query patterns
- Capacity planning for growing data volumes

This comprehensive view strategy will transform the complex FHIR normalized structure into an analytics-ready platform that supports both clinical trials matching and operational reporting while maintaining data integrity and clinical accuracy.
