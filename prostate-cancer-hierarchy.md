# Prostate Cancer Hormone Sensitivity Status Decision Hierarchy
## ICD-10-CM Codes: Z19.1 (Hormone Sensitive) vs Z19.2 (Hormone Resistant)

---

## Data Sourcing from FHIR Materialized Views

### Primary Data Sources
| Data Element | Source View | Field/Column | Notes |
|--------------|-------------|--------------|-------|
| **Prostate Cancer Diagnosis (C61)** | fact_fhir_conditions_view_v1 | code_codings (JSON) | Primary diagnosis identification |
| **ADT Medications** | fact_fhir_medication_requests_view_v1 | medication_display, dosage_instructions | Androgen deprivation therapy tracking |
| **PSA Levels** | fact_fhir_observations_view_v1 | value_quantity_value, observation_display | Lab values from observations |
| **PSA Levels** | fact_fhir_diagnostic_reports_view_v1 | content (JSON) | Lab reports and results |
| **Testosterone Levels** | fact_fhir_observations_view_v1 | value_quantity_value, observation_display | Hormone level monitoring |
| **Testosterone Levels** | fact_fhir_diagnostic_reports_view_v1 | content (JSON) | Hormone testing reports |
| **Radiographic Progression** | fact_fhir_diagnostic_reports_view_v1 | content (JSON), description | Imaging studies (CT, MRI, bone scans) |
| **Clinical Notes Inference** | *Not yet deployed* | N/A | **Note: Future capability to infer from clinical notes** |

### Query Examples for Data Extraction

#### Prostate Cancer Patients
```sql
-- Identify patients with prostate cancer diagnosis
SELECT DISTINCT patient_id, condition_text, code_codings
FROM fact_fhir_conditions_view_v1 
WHERE JSON_EXTRACT_PATH_TEXT(code_codings, '[0].code') = 'C61'
   OR condition_text ILIKE '%prostate cancer%';
```

#### ADT Medication Tracking
```sql
-- Find patients on ADT medications
SELECT patient_id, medication_display, status, authored_on
FROM fact_fhir_medication_requests_view_v1
WHERE medication_display ILIKE ANY(ARRAY[
    '%leuprolide%', '%goserelin%', '%triptorelin%', 
    '%degarelix%', '%bicalutamide%', '%enzalutamide%',
    '%abiraterone%', '%lupron%', '%zoladex%'
]);
```

#### PSA and Testosterone Levels
```sql
-- Extract PSA values from observations
SELECT patient_id, effective_datetime, value_quantity_value, value_quantity_unit
FROM fact_fhir_observations_view_v1
WHERE observation_display ILIKE '%PSA%' 
   OR observation_display ILIKE '%prostate specific antigen%';

-- Extract testosterone values
SELECT patient_id, effective_datetime, value_quantity_value, value_quantity_unit  
FROM fact_fhir_observations_view_v1
WHERE observation_display ILIKE '%testosterone%';
```

#### Radiographic Studies
```sql
-- Find imaging studies for progression assessment
SELECT patient_id, date, type_display, description
FROM fact_fhir_diagnostic_reports_view_v1
WHERE type_display ILIKE ANY(ARRAY[
    '%CT%', '%MRI%', '%bone scan%', '%PET%', 
    '%imaging%', '%radiology%'
]);
```

### Data Quality Considerations
- **PSA Progression**: Requires temporal analysis with 25% increase + 2.0 ng/mL absolute increase
- **Testosterone Castrate Levels**: <50 ng/dL threshold critical for CRPC determination  
- **Medication Adherence**: Track active vs discontinued ADT medications
- **Clinical Notes**: Future enhancement to extract unstructured clinical decision data

---

### Starting Point
**Patient with Prostate Cancer (ICD-10: C61)**

### DECISION POINT 1: Is patient receiving Androgen Deprivation Therapy (ADT)?
*Medications + Notes*

#### Path A: NO ADT
- **OUTCOME: Z19.1 - Hormone Sensitive (Treatment Naïve)**

#### Path B: YES - Patient on ADT
- Proceed to testosterone level assessment

### DECISION POINT 2: Check Testosterone Level

#### Path B1: Testosterone ≥50 ng/dL
- Not Castrate Level
- Continue ADT/Adjust therapy
- **OUTCOME: Z19.1 - Hormone Sensitive (Inadequate suppression)**

#### Path B2: Testosterone <50 ng/dL (Castrate Level)
- Proceed to progression assessment

### DECISION POINT 3: Evidence of Progression?

**Progression Criteria (ANY of the following):**
- **PSA Progression:** 25% increase over nadir + absolute increase >2.0 ng/mL (confirmed 3+ weeks later)
- **Radiographic - Soft Tissue:** ≥20% increase in target lesions (RECIST 1.1)
- **Radiographic - Bone:** ≥2 new lesions → confirm 6+ weeks later → ≥2 additional new lesions (PCWG3 "2+2 rule")
- **Clinical Progression:** New symptoms, deteriorating performance status, need for palliative intervention

#### Path B2a: NO Progression
- **OUTCOME: Z19.1 - Hormone Sensitive (Castrate-Sensitive)**

#### Path B2b: YES - Progression Confirmed
- **OUTCOME: Z19.2 - Hormone Resistant (CRPC - Castration-Resistant)**

---

## Critical Requirements for Z19.2 (CRPC)
⚠️ **All must be met:**
- Testosterone MUST be <50 ng/dL (1.7 nmol/L) - verify with lab test *(Source: Observations/DiagnosticReports views)*
- Progression MUST be documented while on ADT *(Source: MedicationRequests view for ADT status)*
- Continue ADT even after CRPC diagnosis
- Consider: nmCRPC (non-metastatic) vs mCRPC (metastatic) for staging *(Source: DiagnosticReports view for imaging)*

### Data Validation Requirements
- **Lab Values**: Query both observations and diagnostic reports for complete testosterone/PSA history
- **Medication Timeline**: Ensure ADT therapy overlap with progression timeframe
- **Imaging Correlation**: Cross-reference radiographic progression with clinical timeline
- **Clinical Documentation**: *Future: Infer from clinical notes when natural language processing capability is deployed*

## Laboratory Monitoring Guidelines
*(Data Sources: fact_fhir_observations_view_v1, fact_fhir_diagnostic_reports_view_v1)*

- **PSA:** Every 3-6 months during ADT - *Query observations for PSA trend analysis*
- **Testosterone:** Confirm castrate levels every 3-6 months - *Critical for CRPC determination*  
- **Optimal Target:** <20 ng/dL (though <50 ng/dL is standard)
- **PSA Doubling Time:** <10 months suggests high risk - *Requires temporal analysis of PSA values*

### Automated Monitoring Queries
```sql
-- PSA trend analysis for doubling time calculation
WITH psa_values AS (
  SELECT patient_id, effective_datetime, value_quantity_value as psa_value
  FROM fact_fhir_observations_view_v1 
  WHERE observation_display ILIKE '%PSA%'
  ORDER BY patient_id, effective_datetime
)
SELECT patient_id, 
       LAG(psa_value) OVER (PARTITION BY patient_id ORDER BY effective_datetime) as previous_psa,
       psa_value as current_psa,
       effective_datetime,
       DATEDIFF(day, LAG(effective_datetime) OVER (PARTITION BY patient_id ORDER BY effective_datetime), effective_datetime) as days_between
FROM psa_values;
```

---

## Summary of Outcomes

### Z19.1 - Hormone Sensitive
- Treatment naïve patients (no ADT)
- Patients with inadequate testosterone suppression (≥50 ng/dL)
- Castrate patients without progression

### Z19.2 - Hormone Resistant
- Castrate testosterone levels (<50 ng/dL) AND documented progression
- Also known as CRPC (Castration-Resistant Prostate Cancer)