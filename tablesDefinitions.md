# FHIR Database Table Definitions

This document contains the complete DDL (Data Definition Language) for all tables in the `dev.public` schema of the Redshift cluster `prod-redshift-main-ue2`.

**Database:** `dev`  
**Schema:** `public`  
**Total Tables:** 68  
**Generated:** $(date)

## Table Categories

- [Patient Tables](#patient-tables) - Core patient information and related data
- [Care Plan Tables](#care-plan-tables) - Patient care plans and goals
- [Condition Tables](#condition-tables) - Medical conditions and diagnoses
- [Encounter Tables](#encounter-tables) - Healthcare encounters and visits
- [Observation Tables](#observation-tables) - Clinical observations and measurements
- [Diagnostic Report Tables](#diagnostic-report-tables) - Diagnostic test results
- [Medication Tables](#medication-tables) - Medication prescriptions and dispensing
- [Procedure Tables](#procedure-tables) - Medical procedures performed
- [Document Reference Tables](#document-reference-tables) - Clinical documents
- [Practitioner Tables](#practitioner-tables) - Healthcare providers

---

## Patient Tables

### patients
Main patient demographic and administrative information.

```sql
CREATE TABLE patients (
    patient_id VARCHAR(65535),
    active BOOLEAN,
    gender VARCHAR(65535),
    birth_date DATE,
    deceased BOOLEAN,
    deceased_date TIMESTAMP,
    resourcetype VARCHAR(65535),
    marital_status_code VARCHAR(65535),
    marital_status_display VARCHAR(65535),
    marital_status_system VARCHAR(65535),
    multiple_birth BOOLEAN,
    birth_order INTEGER,
    managing_organization_id VARCHAR(65535),
    photos VARCHAR(65535),
    meta_version_id VARCHAR(65535),
    meta_last_updated TIMESTAMP,
    meta_source VARCHAR(65535),
    meta_security VARCHAR(65535),
    meta_tag VARCHAR(65535),
    extensions VARCHAR(65535),
    created_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone,
    updated_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone,
    dummy VARCHAR(65535)
);
```

### patient_addresses
Patient address information with support for multiple addresses per patient.

```sql
CREATE TABLE patient_addresses (
    patient_id VARCHAR(65535),
    address_use VARCHAR(65535),
    address_type VARCHAR(65535),
    address_text VARCHAR(65535),
    address_line VARCHAR(65535),
    city VARCHAR(65535),
    district VARCHAR(65535),
    state VARCHAR(65535),
    postal_code VARCHAR(65535),
    country VARCHAR(65535),
    period_start DATE,
    period_end DATE,
    dummy VARCHAR(65535)
);
```

### patient_names
Patient name information supporting multiple names per patient.

```sql
CREATE TABLE patient_names (
    patient_id VARCHAR(65535),
    name_use VARCHAR(65535),
    name_text VARCHAR(65535),
    family_name VARCHAR(65535),
    given_names VARCHAR(65535),
    prefix VARCHAR(65535),
    suffix VARCHAR(65535),
    period_start DATE,
    period_end DATE,
    dummy VARCHAR(65535)
);
```

### patient_telecoms
Patient telecommunication contact information.

```sql
CREATE TABLE patient_telecoms (
    patient_id VARCHAR(65535),
    telecom_system VARCHAR(65535),
    telecom_value VARCHAR(65535),
    telecom_use VARCHAR(65535),
    telecom_rank INTEGER,
    period_start DATE,
    period_end DATE,
    dummy VARCHAR(65535)
);
```

### patient_communications
Patient communication preferences and languages.

```sql
CREATE TABLE patient_communications (
    patient_id VARCHAR(65535),
    language_code VARCHAR(65535),
    language_system VARCHAR(65535),
    language_display VARCHAR(65535),
    preferred BOOLEAN,
    extensions VARCHAR(65535),
    dummy VARCHAR(65535)
);
```

### patient_contacts
Patient emergency contacts and related persons.

```sql
CREATE TABLE patient_contacts (
    patient_id VARCHAR(65535),
    contact_relationship_code VARCHAR(65535),
    contact_relationship_system VARCHAR(65535),
    contact_relationship_display VARCHAR(65535),
    contact_name_text VARCHAR(65535),
    contact_name_family VARCHAR(65535),
    contact_name_given VARCHAR(65535),
    contact_telecom_system VARCHAR(65535),
    contact_telecom_value VARCHAR(65535),
    contact_telecom_use VARCHAR(65535),
    contact_gender VARCHAR(65535),
    contact_organization_id VARCHAR(65535),
    period_start DATE,
    period_end DATE,
    dummy VARCHAR(65535)
);
```

### patient_links
Links between related patient records.

```sql
CREATE TABLE patient_links (
    patient_id VARCHAR(65535),
    other_patient_id VARCHAR(65535),
    link_type_code VARCHAR(65535),
    link_type_system VARCHAR(65535),
    link_type_display VARCHAR(65535),
    dummy VARCHAR(65535)
);
```

### patient_practitioners
Links between patients and their healthcare providers.

```sql
CREATE TABLE patient_practitioners (
    patient_id VARCHAR(65535),
    practitioner_id VARCHAR(65535),
    practitioner_role_id VARCHAR(65535),
    organization_id VARCHAR(65535),
    reference_type VARCHAR(65535),
    dummy VARCHAR(65535)
);
```

---

## Care Plan Tables

### care_plans
Main table for patient care plans.

```sql
CREATE TABLE care_plans (
    care_plan_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    status VARCHAR(50),
    intent VARCHAR(50),
    title VARCHAR(500),
    meta_version_id VARCHAR(50),
    meta_last_updated TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

### care_plan_care_teams
Care teams associated with care plans.

```sql
CREATE TABLE care_plan_care_teams (
    care_plan_id VARCHAR(255),
    care_team_id VARCHAR(255)
);
```

### care_plan_categories
Categories for care plans.

```sql
CREATE TABLE care_plan_categories (
    care_plan_id VARCHAR(255),
    category_code VARCHAR(50),
    category_system VARCHAR(255),
    category_display VARCHAR(255),
    category_text VARCHAR(500)
);
```

### care_plan_goals
Goals associated with care plans.

```sql
CREATE TABLE care_plan_goals (
    care_plan_id VARCHAR(255),
    goal_id VARCHAR(255)
);
```

### care_plan_identifiers
External identifiers for care plans.

```sql
CREATE TABLE care_plan_identifiers (
    care_plan_id VARCHAR(255),
    identifier_system VARCHAR(255),
    identifier_value VARCHAR(255)
);
```

---

## Condition Tables

### conditions
Main table for medical conditions and diagnoses.

```sql
CREATE TABLE conditions (
    condition_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    encounter_id VARCHAR(255),
    clinical_status_code VARCHAR(50),
    clinical_status_display VARCHAR(255),
    clinical_status_system VARCHAR(255),
    verification_status_code VARCHAR(50),
    verification_status_display VARCHAR(255),
    verification_status_system VARCHAR(255),
    condition_text VARCHAR(500),
    severity_code VARCHAR(50),
    severity_display VARCHAR(255),
    severity_system VARCHAR(255),
    onset_datetime TIMESTAMP,
    onset_age_value NUMERIC(10,2),
    onset_age_unit VARCHAR(20),
    onset_period_start TIMESTAMP,
    onset_period_end TIMESTAMP,
    onset_text VARCHAR(500),
    abatement_datetime TIMESTAMP,
    abatement_age_value NUMERIC(10,2),
    abatement_age_unit VARCHAR(20),
    abatement_period_start TIMESTAMP,
    abatement_period_end TIMESTAMP,
    abatement_text VARCHAR(500),
    abatement_boolean BOOLEAN,
    recorded_date TIMESTAMP,
    recorder_type VARCHAR(50),
    recorder_id VARCHAR(255),
    asserter_type VARCHAR(50),
    asserter_id VARCHAR(255),
    meta_version_id VARCHAR(50),
    meta_last_updated TIMESTAMP,
    meta_source VARCHAR(255),
    meta_profile VARCHAR(65535),
    meta_security VARCHAR(65535),
    meta_tag VARCHAR(65535),
    created_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone,
    updated_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone
);
```

### condition_body_sites
Body sites associated with conditions.

```sql
CREATE TABLE condition_body_sites (
    condition_id VARCHAR(255),
    body_site_code VARCHAR(50),
    body_site_system VARCHAR(255),
    body_site_display VARCHAR(255),
    body_site_text VARCHAR(500)
);
```

### condition_categories
Categories for medical conditions.

```sql
CREATE TABLE condition_categories (
    condition_id VARCHAR(255),
    category_code VARCHAR(50),
    category_system VARCHAR(255),
    category_display VARCHAR(255),
    category_text VARCHAR(500)
);
```

### condition_codes
Coded representations of conditions.

```sql
CREATE TABLE condition_codes (
    condition_id VARCHAR(255),
    code_code VARCHAR(50),
    code_system VARCHAR(255),
    code_display VARCHAR(255),
    code_text VARCHAR(500)
);
```

### condition_evidence
Evidence supporting the condition diagnosis.

```sql
CREATE TABLE condition_evidence (
    condition_id VARCHAR(255),
    evidence_code VARCHAR(50),
    evidence_system VARCHAR(255),
    evidence_display VARCHAR(255),
    evidence_detail_reference VARCHAR(255)
);
```

### condition_extensions
FHIR extensions for conditions with flexible value types.

```sql
CREATE TABLE condition_extensions (
    condition_id VARCHAR(255) NOT NULL,
    extension_url VARCHAR(500) NOT NULL,
    extension_type VARCHAR(50) NOT NULL,
    value_type VARCHAR(50),
    value_string VARCHAR(65535),
    value_datetime TIMESTAMP,
    value_reference VARCHAR(255),
    value_code VARCHAR(100),
    value_boolean BOOLEAN,
    value_decimal NUMERIC(18,6),
    value_integer INTEGER,
    parent_extension_url VARCHAR(500),
    extension_order INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone,
    updated_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone
);
```

### condition_notes
Clinical notes associated with conditions.

```sql
CREATE TABLE condition_notes (
    condition_id VARCHAR(255),
    note_text VARCHAR(65535),
    note_author_reference VARCHAR(255),
    note_time TIMESTAMP
);
```

### condition_stages
Staging information for conditions (e.g., cancer stages).

```sql
CREATE TABLE condition_stages (
    condition_id VARCHAR(255),
    stage_summary_code VARCHAR(50),
    stage_summary_system VARCHAR(255),
    stage_summary_display VARCHAR(255),
    stage_assessment_code VARCHAR(50),
    stage_assessment_system VARCHAR(255),
    stage_assessment_display VARCHAR(255),
    stage_type_code VARCHAR(50),
    stage_type_system VARCHAR(255),
    stage_type_display VARCHAR(255)
);
```

---

## Encounter Tables

### encounters
Main table for healthcare encounters and visits.

```sql
CREATE TABLE encounters (
    encounter_id VARCHAR(255),
    patient_id VARCHAR(255),
    status VARCHAR(50),
    resourcetype VARCHAR(50),
    class_code VARCHAR(10),
    class_display VARCHAR(255),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    service_provider_id VARCHAR(255),
    appointment_id VARCHAR(255),
    parent_encounter_id VARCHAR(255),
    meta_data VARCHAR(256),
    created_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone,
    updated_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone
);
```

### encounter_hospitalization
Hospitalization details for encounters.

```sql
CREATE TABLE encounter_hospitalization (
    encounter_id VARCHAR(255),
    discharge_disposition_text VARCHAR(500),
    discharge_code VARCHAR(50),
    discharge_system VARCHAR(500)
);
```

### encounter_identifiers
External identifiers for encounters.

```sql
CREATE TABLE encounter_identifiers (
    encounter_id VARCHAR(255),
    identifier_system VARCHAR(500),
    identifier_value VARCHAR(500)
);
```

### encounter_locations
Locations where encounters took place.

```sql
CREATE TABLE encounter_locations (
    encounter_id VARCHAR(255),
    location_id VARCHAR(255)
);
```

### encounter_participants
Healthcare providers and other participants in encounters.

```sql
CREATE TABLE encounter_participants (
    encounter_id VARCHAR(255),
    participant_type VARCHAR(50),
    participant_id VARCHAR(255),
    participant_display VARCHAR(255),
    period_start TIMESTAMP,
    period_end TIMESTAMP
);
```

### encounter_reasons
Reasons for healthcare encounters.

```sql
CREATE TABLE encounter_reasons (
    encounter_id VARCHAR(255),
    reason_code VARCHAR(50),
    reason_system VARCHAR(255),
    reason_display VARCHAR(255),
    reason_text VARCHAR(500)
);
```

### encounter_types
Types and classifications of encounters.

```sql
CREATE TABLE encounter_types (
    encounter_id VARCHAR(255),
    type_code VARCHAR(50),
    type_system VARCHAR(255),
    type_display VARCHAR(255),
    type_text VARCHAR(500)
);
```

---

## Observation Tables

### observations
Main table for clinical observations and measurements.

```sql
CREATE TABLE observations (
    observation_id VARCHAR(65535),
    patient_id VARCHAR(65535),
    encounter_id VARCHAR(65535),
    specimen_id VARCHAR(65535),
    status VARCHAR(65535),
    observation_text VARCHAR(65535),
    primary_code VARCHAR(65535),
    primary_system VARCHAR(65535),
    primary_display VARCHAR(65535),
    value_string VARCHAR(65535),
    value_quantity_value NUMERIC(10,2),
    value_quantity_unit VARCHAR(65535),
    value_quantity_system VARCHAR(65535),
    value_codeable_concept_code VARCHAR(65535),
    value_codeable_concept_system VARCHAR(65535),
    value_codeable_concept_display VARCHAR(65535),
    value_codeable_concept_text VARCHAR(65535),
    value_datetime TIMESTAMP,
    value_boolean BOOLEAN,
    data_absent_reason_code VARCHAR(65535),
    data_absent_reason_display VARCHAR(65535),
    data_absent_reason_system VARCHAR(65535),
    effective_datetime TIMESTAMP,
    effective_period_start TIMESTAMP,
    effective_period_end TIMESTAMP,
    issued TIMESTAMP,
    body_site_code VARCHAR(65535),
    body_site_system VARCHAR(65535),
    body_site_display VARCHAR(65535),
    body_site_text VARCHAR(65535),
    method_code VARCHAR(65535),
    method_system VARCHAR(65535),
    method_display VARCHAR(65535),
    method_text VARCHAR(65535),
    meta_version_id VARCHAR(65535),
    meta_last_updated TIMESTAMP,
    meta_source VARCHAR(65535),
    meta_profile VARCHAR(65535),
    meta_security VARCHAR(65535),
    meta_tag VARCHAR(65535),
    extensions VARCHAR(65535),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

### observation_categories
Categories for observations (e.g., vital-signs, laboratory).

```sql
CREATE TABLE observation_categories (
    observation_id VARCHAR(65535),
    category_code VARCHAR(65535),
    category_system VARCHAR(65535),
    category_display VARCHAR(65535),
    category_text VARCHAR(65535)
);
```

### observation_components
Components of complex observations (e.g., blood pressure systolic/diastolic).

```sql
CREATE TABLE observation_components (
    observation_id VARCHAR(65535),
    component_code VARCHAR(65535),
    component_system VARCHAR(65535),
    component_display VARCHAR(65535),
    component_text VARCHAR(65535),
    component_value_string VARCHAR(65535),
    component_value_quantity_value NUMERIC(10,2),
    component_value_quantity_unit VARCHAR(65535),
    component_value_codeable_concept_code VARCHAR(65535),
    component_value_codeable_concept_system VARCHAR(65535),
    component_value_codeable_concept_display VARCHAR(65535),
    component_data_absent_reason_code VARCHAR(65535),
    component_data_absent_reason_display VARCHAR(65535)
);
```

### observation_derived_from
References to observations that this observation is derived from.

```sql
CREATE TABLE observation_derived_from (
    observation_id VARCHAR(65535),
    derived_from_reference VARCHAR(65535)
);
```

### observation_interpretations
Clinical interpretations of observation values.

```sql
CREATE TABLE observation_interpretations (
    observation_id VARCHAR(65535),
    interpretation_code VARCHAR(65535),
    interpretation_system VARCHAR(65535),
    interpretation_display VARCHAR(65535)
);
```

### observation_members
Member observations for grouped observations.

```sql
CREATE TABLE observation_members (
    observation_id VARCHAR(65535),
    member_observation_id VARCHAR(65535)
);
```

### observation_notes
Clinical notes associated with observations.

```sql
CREATE TABLE observation_notes (
    observation_id VARCHAR(65535),
    note_text VARCHAR(65535),
    note_author_reference VARCHAR(65535),
    note_time TIMESTAMP
);
```

### observation_performers
Healthcare providers who performed the observations.

```sql
CREATE TABLE observation_performers (
    observation_id VARCHAR(65535),
    performer_type VARCHAR(65535),
    performer_id VARCHAR(65535)
);
```

### observation_reference_ranges
Reference ranges for observation values.

```sql
CREATE TABLE observation_reference_ranges (
    observation_id VARCHAR(65535),
    range_low_value NUMERIC(10,2),
    range_low_unit VARCHAR(65535),
    range_high_value NUMERIC(10,2),
    range_high_unit VARCHAR(65535),
    range_type_code VARCHAR(65535),
    range_type_system VARCHAR(65535),
    range_type_display VARCHAR(65535),
    range_text VARCHAR(65535)
);
```

---

## Diagnostic Report Tables

### diagnostic_reports
Main table for diagnostic test reports and results.

```sql
CREATE TABLE diagnostic_reports (
    diagnostic_report_id VARCHAR(65535),
    resource_type VARCHAR(65535),
    status VARCHAR(65535),
    effective_datetime TIMESTAMP,
    issued_datetime TIMESTAMP,
    code_text VARCHAR(65535),
    code_primary_code VARCHAR(65535),
    code_primary_system VARCHAR(65535),
    code_primary_display VARCHAR(65535),
    patient_id VARCHAR(65535),
    encounter_id VARCHAR(65535),
    meta_data VARCHAR(65535),
    extensions VARCHAR(65535),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    meta_last_updated TIMESTAMP,
    effective_date_time TIMESTAMP,
    meta_version_id VARCHAR(65535),
    issued TIMESTAMP
);
```

### diagnostic_report_based_on
Service requests that the diagnostic report is based on.

```sql
CREATE TABLE diagnostic_report_based_on (
    diagnostic_report_id VARCHAR(65535),
    service_request_id VARCHAR(65535)
);
```

### diagnostic_report_categories
Categories for diagnostic reports.

```sql
CREATE TABLE diagnostic_report_categories (
    diagnostic_report_id VARCHAR(65535),
    category_code VARCHAR(65535),
    category_system VARCHAR(65535),
    category_display VARCHAR(65535)
);
```

### diagnostic_report_media
Media attachments for diagnostic reports.

```sql
CREATE TABLE diagnostic_report_media (
    diagnostic_report_id VARCHAR(65535),
    media_id VARCHAR(65535)
);
```

### diagnostic_report_performers
Healthcare providers who performed the diagnostic tests.

```sql
CREATE TABLE diagnostic_report_performers (
    diagnostic_report_id VARCHAR(65535),
    performer_type VARCHAR(65535),
    performer_id VARCHAR(65535)
);
```

### diagnostic_report_presented_forms
Formatted presentations of diagnostic reports.

```sql
CREATE TABLE diagnostic_report_presented_forms (
    diagnostic_report_id VARCHAR(65535),
    content_type VARCHAR(65535),
    url VARCHAR(65535),
    title VARCHAR(65535),
    data VARCHAR(65535)
);
```

### diagnostic_report_results
Observations that are part of the diagnostic report.

```sql
CREATE TABLE diagnostic_report_results (
    diagnostic_report_id VARCHAR(65535),
    observation_id VARCHAR(65535)
);
```

---

## Medication Tables

### medications
Main table for medication information.

```sql
CREATE TABLE medications (
    medication_id VARCHAR(255) NOT NULL,
    resource_type VARCHAR(50),
    code_text VARCHAR(500),
    status VARCHAR(50),
    meta_version_id VARCHAR(50),
    meta_last_updated TIMESTAMP,
    created_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone,
    updated_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone
);
```

### medication_identifiers
External identifiers for medications.

```sql
CREATE TABLE medication_identifiers (
    medication_id VARCHAR(255),
    identifier_system VARCHAR(255),
    identifier_value VARCHAR(255)
);
```

### medication_requests
Medication prescriptions and orders.

```sql
CREATE TABLE medication_requests (
    medication_request_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    encounter_id VARCHAR(255),
    medication_id VARCHAR(255),
    medication_display VARCHAR(500),
    status VARCHAR(50),
    intent VARCHAR(50),
    reported_boolean BOOLEAN,
    authored_on TIMESTAMP,
    meta_version_id VARCHAR(50),
    meta_last_updated TIMESTAMP,
    created_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone,
    updated_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone
);
```

### medication_request_categories
Categories for medication requests.

```sql
CREATE TABLE medication_request_categories (
    medication_request_id VARCHAR(255),
    category_code VARCHAR(50),
    category_system VARCHAR(255),
    category_display VARCHAR(255),
    category_text VARCHAR(500)
);
```

### medication_request_dosage_instructions
Dosage instructions for medication requests.

```sql
CREATE TABLE medication_request_dosage_instructions (
    medication_request_id VARCHAR(255),
    dosage_text VARCHAR(65535),
    dosage_timing_frequency INTEGER,
    dosage_timing_period INTEGER,
    dosage_timing_period_unit VARCHAR(20),
    dosage_route_code VARCHAR(50),
    dosage_route_system VARCHAR(255),
    dosage_route_display VARCHAR(255),
    dosage_dose_value NUMERIC(10,2),
    dosage_dose_unit VARCHAR(100),
    dosage_dose_system VARCHAR(255),
    dosage_dose_code VARCHAR(50),
    dosage_as_needed_boolean BOOLEAN
);
```

### medication_request_identifiers
External identifiers for medication requests.

```sql
CREATE TABLE medication_request_identifiers (
    medication_request_id VARCHAR(255),
    identifier_system VARCHAR(255),
    identifier_value VARCHAR(255)
);
```

### medication_request_notes
Clinical notes for medication requests.

```sql
CREATE TABLE medication_request_notes (
    medication_request_id VARCHAR(255),
    note_text VARCHAR(65535)
);
```

### medication_dispenses
Medication dispensing records.

```sql
CREATE TABLE medication_dispenses (
    medication_dispense_id VARCHAR(255) NOT NULL,
    resource_type VARCHAR(50),
    status VARCHAR(50),
    patient_id VARCHAR(255) NOT NULL,
    medication_id VARCHAR(255),
    medication_display VARCHAR(500),
    type_system VARCHAR(255),
    type_code VARCHAR(50),
    type_display VARCHAR(255),
    quantity_value NUMERIC(10,2),
    when_handed_over TIMESTAMP,
    meta_version_id VARCHAR(50),
    meta_last_updated TIMESTAMP,
    created_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone,
    updated_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone
);
```

### medication_dispense_auth_prescriptions
Authorizing prescriptions for medication dispenses.

```sql
CREATE TABLE medication_dispense_auth_prescriptions (
    medication_dispense_id VARCHAR(255),
    authorizing_prescription_id VARCHAR(255)
);
```

### medication_dispense_dosage_instructions
Dosage instructions for medication dispenses.

```sql
CREATE TABLE medication_dispense_dosage_instructions (
    medication_dispense_id VARCHAR(255),
    dosage_text VARCHAR(65535),
    dosage_timing_frequency INTEGER,
    dosage_timing_period INTEGER,
    dosage_timing_period_unit VARCHAR(20),
    dosage_route_code VARCHAR(50),
    dosage_route_system VARCHAR(255),
    dosage_route_display VARCHAR(255),
    dosage_dose_value NUMERIC(10,2),
    dosage_dose_unit VARCHAR(100),
    dosage_dose_system VARCHAR(255),
    dosage_dose_code VARCHAR(50)
);
```

### medication_dispense_identifiers
External identifiers for medication dispenses.

```sql
CREATE TABLE medication_dispense_identifiers (
    medication_dispense_id VARCHAR(255),
    identifier_system VARCHAR(255),
    identifier_value VARCHAR(255)
);
```

### medication_dispense_performers
Healthcare providers who performed medication dispenses.

```sql
CREATE TABLE medication_dispense_performers (
    medication_dispense_id VARCHAR(255),
    performer_actor_reference VARCHAR(255)
);
```

---

## Procedure Tables

### procedures
Main table for medical procedures performed.

```sql
CREATE TABLE procedures (
    procedure_id VARCHAR(255) NOT NULL,
    resource_type VARCHAR(50),
    status VARCHAR(50),
    patient_id VARCHAR(255) NOT NULL,
    code_text VARCHAR(500),
    performed_date_time TIMESTAMP,
    meta_version_id VARCHAR(50),
    meta_last_updated TIMESTAMP,
    created_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone,
    updated_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone
);
```

### procedure_code_codings
Coded representations of procedures.

```sql
CREATE TABLE procedure_code_codings (
    procedure_id VARCHAR(255),
    code_system VARCHAR(255),
    code_code VARCHAR(100),
    code_display VARCHAR(500)
);
```

### procedure_identifiers
External identifiers for procedures.

```sql
CREATE TABLE procedure_identifiers (
    procedure_id VARCHAR(255),
    identifier_system VARCHAR(255),
    identifier_value VARCHAR(255)
);
```

---

## Document Reference Tables

### document_references
Main table for clinical document references.

```sql
CREATE TABLE document_references (
    document_reference_id VARCHAR(255) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    status VARCHAR(50),
    type_code VARCHAR(50),
    type_system VARCHAR(255),
    type_display VARCHAR(500),
    date TIMESTAMP,
    custodian_id VARCHAR(255),
    description VARCHAR(65535),
    context_period_start TIMESTAMP,
    context_period_end TIMESTAMP,
    meta_version_id VARCHAR(50),
    meta_last_updated TIMESTAMP,
    created_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone,
    updated_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone
);
```

### document_reference_authors
Authors of document references.

```sql
CREATE TABLE document_reference_authors (
    document_reference_id VARCHAR(255),
    author_id VARCHAR(255)
);
```

### document_reference_categories
Categories for document references.

```sql
CREATE TABLE document_reference_categories (
    document_reference_id VARCHAR(255),
    category_code VARCHAR(50),
    category_system VARCHAR(255),
    category_display VARCHAR(255)
);
```

### document_reference_content
Content attachments for document references.

```sql
CREATE TABLE document_reference_content (
    document_reference_id VARCHAR(255),
    attachment_content_type VARCHAR(100),
    attachment_url VARCHAR(65535)
);
```

### document_reference_identifiers
External identifiers for document references.

```sql
CREATE TABLE document_reference_identifiers (
    document_reference_id VARCHAR(255),
    identifier_system VARCHAR(255),
    identifier_value VARCHAR(255)
);
```

---

## Practitioner Tables

### practitioners
Healthcare providers and practitioners.

```sql
CREATE TABLE practitioners (
    practitioner_id VARCHAR(255) NOT NULL,
    resource_type VARCHAR(50),
    active BOOLEAN,
    meta_version_id VARCHAR(50),
    meta_last_updated TIMESTAMP,
    created_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone,
    updated_at TIMESTAMP DEFAULT ('now'::text)::timestamp with time zone
);
```

### practitioner_addresses
Practitioner address information.

```sql
CREATE TABLE practitioner_addresses (
    practitioner_id VARCHAR(255),
    state VARCHAR(50),
    line VARCHAR(500),
    city VARCHAR(100),
    postal_code VARCHAR(20)
);
```

### practitioner_names
Practitioner name information.

```sql
CREATE TABLE practitioner_names (
    practitioner_id VARCHAR(255),
    family VARCHAR(255),
    text VARCHAR(500),
    given VARCHAR(255)
);
```

### practitioner_telecoms
Practitioner telecommunication contact information.

```sql
CREATE TABLE practitioner_telecoms (
    practitioner_id VARCHAR(255),
    system VARCHAR(50),
    value VARCHAR(255)
);
```

---

## Key Observations

### Data Types Used
- **VARCHAR(65535)**: Used for large text fields and flexible content
- **VARCHAR(255)**: Standard for IDs and medium text fields
- **VARCHAR(50)**: Used for codes and short text fields
- **TIMESTAMP**: For date/time fields, some with timezone awareness
- **NUMERIC(10,2)**: For decimal values with 2 decimal places
- **BOOLEAN**: For true/false fields
- **INTEGER**: For whole numbers
- **DATE**: For date-only fields

### Primary Keys
Based on the `NOT NULL` constraints observed:
- Most main entity tables use single column primary keys (e.g., `patient_id`, `condition_id`)
- Some tables appear to have composite primary keys (e.g., `condition_extensions`)
- Detail tables typically reference main entity IDs as foreign keys

### FHIR Compliance
The schema follows FHIR R4 specifications with:
- Standardized resource types
- Extension mechanisms for custom data
- Coded values with system/code/display patterns
- Meta fields for versioning and provenance
- Proper normalization of complex FHIR resources

### Timestamps
Most tables include:
- `created_at` and `updated_at` with default values
- `meta_last_updated` for FHIR metadata
- Various business-specific timestamp fields

This schema represents a comprehensive FHIR-compliant healthcare data warehouse suitable for clinical analytics and interoperability.
