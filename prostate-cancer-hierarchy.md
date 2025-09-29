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
  SELECT 
        code_code, 
       patient_id
    FROM fact_fhir_conditions_view_v1 AS c 
    WHERE c.code_code IN ('C61', 'Z19.1', 'Z19.2', 'R97.21') 
        AND EXTRACT(YEAR FROM c.recorded_date) = 2025 
    GROUP BY code_code, patient_id;
```

#### ADT Medication Tracking
```sql
-- Find patients on ADT medications
WITH target_patients AS (
     SELECT 
        code_code, 
       patient_id
    FROM fact_fhir_conditions_view_v1 AS c 
    WHERE c.code_code IN ('C61', 'Z19.1', 'Z19.2', 'R97.21') 
        AND EXTRACT(YEAR FROM c.recorded_date) = 2025 
    GROUP BY code_code, patient_id
)

SELECT 
    tp.patient_id,
    medication_id,
    tp.code_code as condition_code,
  "medication_display",
  authored_on,
  "status"
  FROM target_patients tp
INNER JOIN public.fact_fhir_medication_requests_view_v1 mpv 
    ON tp.patient_id = mpv.patient_id
WHERE
mpv.medication_display ILIKE '%Leuprolide%' OR 
mpv.medication_display ILIKE '%Leuprorelin%' OR 
mpv.medication_display ILIKE '%Leuproreline%' OR 
mpv.medication_display ILIKE '%Lupron%' OR 
mpv.medication_display ILIKE '%Eligard%' OR 
mpv.medication_display ILIKE '%Camcevi%' OR 
mpv.medication_display ILIKE '%Fensolvi%' OR 
mpv.medication_display ILIKE '%Viadur%' OR 
mpv.medication_display ILIKE '%TAP-144%' OR 
mpv.medication_display ILIKE '%A-43818%' OR 
mpv.medication_display ILIKE '%Abbott-43818%' OR 

-- Goserelin
mpv.medication_display ILIKE '%Goserelin%' OR 
mpv.medication_display ILIKE '%Zoladex%' OR 
mpv.medication_display ILIKE '%ICI-118630%' OR 

-- Triptorelin (including international variations)
mpv.medication_display ILIKE '%Triptorelin%' OR 
mpv.medication_display ILIKE '%Triptoréline%' OR 
mpv.medication_display ILIKE '%Triptorelinum%' OR 
mpv.medication_display ILIKE '%Trelstar%' OR 
mpv.medication_display ILIKE '%Triptodur%' OR 
mpv.medication_display ILIKE '%Decapeptyl%' OR 
mpv.medication_display ILIKE '%CL 118532%' OR 

-- LHRH/GnRH Antagonists
-- Degarelix
mpv.medication_display ILIKE '%Degarelix%' OR 
mpv.medication_display ILIKE '%Firmagon%' OR 
mpv.medication_display ILIKE '%FE 200486%' OR 
mpv.medication_display ILIKE '%ASP-3550%' OR 

-- Relugolix
mpv.medication_display ILIKE '%Relugolix%' OR 
mpv.medication_display ILIKE '%Orgovyx%' OR 
mpv.medication_display ILIKE '%Relumina%' OR 
mpv.medication_display ILIKE '%Myfembree%' OR 
mpv.medication_display ILIKE '%Ryeqo%' OR 
mpv.medication_display ILIKE '%TAK-385%' OR 
mpv.medication_display ILIKE '%RVT-601%' OR 

-- Anti-androgens
-- Bicalutamide
mpv.medication_display ILIKE '%Bicalutamide%' OR 
mpv.medication_display ILIKE '%Casodex%' OR 
mpv.medication_display ILIKE '%Cosudex%' OR 
mpv.medication_display ILIKE '%Calutide%' OR 
mpv.medication_display ILIKE '%ICI 176334%' OR 
mpv.medication_display ILIKE '%ICI-176334%' OR 

-- Enzalutamide (CRITICAL: Include MDV3100 variations)
mpv.medication_display ILIKE '%Enzalutamide%' OR 
mpv.medication_display ILIKE '%Xtandi%' OR 
mpv.medication_display ILIKE '%MDV3100%' OR 
mpv.medication_display ILIKE '%MDV 3100%' OR 
mpv.medication_display ILIKE '%MDV-3100%' OR 

-- Abiraterone (including development codes)
mpv.medication_display ILIKE '%Abiraterone%' OR 
mpv.medication_display ILIKE '%Zytiga%' OR 
mpv.medication_display ILIKE '%Yonsa%' OR 
mpv.medication_display ILIKE '%CB-7630%' OR 
mpv.medication_display ILIKE '%CB7630%' OR 
mpv.medication_display ILIKE '%CB 7630%' OR 
mpv.medication_display ILIKE '%JNJ-212082%' OR 
mpv.medication_display ILIKE '%JNJ212082%' OR 
mpv.medication_display ILIKE '%JNJ 212082%' OR 
mpv.medication_display ILIKE '%CB-7598%' OR 

-- Additional first-generation anti-androgens
-- Flutamide
mpv.medication_display ILIKE '%Flutamide%' OR 
mpv.medication_display ILIKE '%Eulexin%' OR 
mpv.medication_display ILIKE '%SCH-13521%' OR 

-- Nilutamide
mpv.medication_display ILIKE '%Nilutamide%' OR 
mpv.medication_display ILIKE '%Nilandron%' OR 
mpv.medication_display ILIKE '%Anandron%' OR 

-- Common ADT abbreviations that might appear
mpv.medication_display ILIKE '%LHRH agonist%' OR 
mpv.medication_display ILIKE '%GnRH agonist%' OR 
mpv.medication_display ILIKE '%LHRH analog%' OR 
mpv.medication_display ILIKE '%GnRH analog%' OR 
mpv.medication_display ILIKE '%androgen deprivation%' OR 
mpv.medication_display ILIKE '%antiandrogen%' OR 
mpv.medication_display ILIKE '%anti-androgen%'
ORDER BY 
    mpv.authored_on;
```

#### PSA and Testosterone Levels

```sql
-- Extract PSA values from observations using LOINC codes and text search
SELECT patient_id, effective_datetime, value_quantity_value, value_quantity_unit,
       observation_code, observation_display
FROM fact_fhir_observations_view_v1
WHERE observation_code IN (
    '2857-1',   -- PSA [Mass/volume] in Serum or Plasma
    '83112-3',  -- PSA [Mass/volume] in Serum or Plasma by Immunoassay
    '10886-0',  -- PSA Free [Mass/volume] in Serum or Plasma
    '12841-3',  -- PSA Free/Total ratio in Serum or Plasma
    '19195-7',  -- PSA [Units/volume] in Serum or Plasma
    '35741-8'   -- PSA [Mass/volume] in Serum or Plasma by Detection limit <= 0.01 ng/mL
)
OR observation_display ILIKE '%PSA%' 
OR observation_display ILIKE '%prostate specific antigen%';

-- Extract testosterone values using LOINC codes and text search
SELECT patient_id, effective_datetime, value_quantity_value, value_quantity_unit,
       observation_code, observation_display
FROM fact_fhir_observations_view_v1
WHERE observation_code IN (
    '2986-8',   -- Testosterone [Mass/volume] in Serum or Plasma
    '14913-8',  -- Testosterone [Moles/volume] in Serum or Plasma  
    '2991-8',   -- Testosterone Free [Mass/volume] in Serum or Plasma
    '1854-9',   -- Testosterone bioavailable [Mass/volume] in Serum or Plasma
    '49041-7',  -- Testosterone Free [Moles/volume] in Serum or Plasma
    '58952-3'   -- Testosterone Free+Weakly bound [Mass/volume] in Serum or Plasma
)
OR observation_display ILIKE '%testosterone%';
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
-- PSA trend analysis for doubling time calculation using LOINC codes
WITH psa_values AS (
  SELECT patient_id, effective_datetime, value_quantity_value as psa_value
  FROM fact_fhir_observations_view_v1 
  WHERE observation_code IN ('2857-1', '83112-3', '19195-7', '35741-8')
     OR observation_display ILIKE '%PSA%'
  ORDER BY patient_id, effective_datetime
)
SELECT patient_id, 
       LAG(psa_value) OVER (PARTITION BY patient_id ORDER BY effective_datetime) as previous_psa,
       psa_value as current_psa,
       effective_datetime,
       DATEDIFF(day, LAG(effective_datetime) OVER (PARTITION BY patient_id ORDER BY effective_datetime), effective_datetime) as days_between
FROM psa_values;

-- Testosterone castrate level monitoring
SELECT patient_id, effective_datetime, 
       value_quantity_value as testosterone_level,
       value_quantity_unit,
       CASE 
         WHEN value_quantity_value < 50 AND value_quantity_unit = 'ng/dL' THEN 'Castrate'
         WHEN value_quantity_value < 1.7 AND value_quantity_unit = 'nmol/L' THEN 'Castrate'
         ELSE 'Non-Castrate'
       END as castration_status
FROM fact_fhir_observations_view_v1
WHERE observation_code IN ('2986-8', '14913-8')
   OR observation_display ILIKE '%testosterone%';
```

### LOINC Code Reference for Prostate Cancer Monitoring

#### PSA (Prostate Specific Antigen) LOINC Codes
| LOINC Code | Description | Common Use |
|------------|-------------|------------|
| **2857-1** | PSA [Mass/volume] in Serum or Plasma | Most common, general PSA |
| **83112-3** | PSA [Mass/volume] in Serum or Plasma by Immunoassay | Method-specific |
| **10886-0** | PSA Free [Mass/volume] in Serum or Plasma | Free PSA component |
| **12841-3** | PSA Free/Total ratio in Serum or Plasma | Ratio calculation |
| **19195-7** | PSA [Units/volume] in Serum or Plasma | Alternative units |
| **35741-8** | PSA [Mass/volume] by Detection limit ≤0.01 ng/mL | Ultrasensitive PSA |

#### Testosterone LOINC Codes
| LOINC Code | Description | Clinical Relevance |
|------------|-------------|-------------------|
| **2986-8** | Testosterone [Mass/volume] in Serum or Plasma | Primary for castrate levels |
| **14913-8** | Testosterone [Moles/volume] in Serum or Plasma | Alternative units (nmol/L) |
| **2991-8** | Testosterone Free [Mass/volume] in Serum or Plasma | Free testosterone |
| **1854-9** | Testosterone bioavailable [Mass/volume] in Serum or Plasma | Bioavailable portion |
| **49041-7** | Testosterone Free [Moles/volume] in Serum or Plasma | Free in molar units |
| **58952-3** | Testosterone Free+Weakly bound [Mass/volume] | Combined measurement |

#### Critical Thresholds
- **Castrate Testosterone**: <50 ng/dL or <1.7 nmol/L
- **PSA Progression**: 25% increase + 2.0 ng/mL absolute increase
- **PSA Doubling Time**: <10 months indicates high risk

---

## Summary of Outcomes

### Z19.1 - Hormone Sensitive
- Treatment naïve patients (no ADT)
- Patients with inadequate testosterone suppression (≥50 ng/dL)
- Castrate patients without progression

### Z19.2 - Hormone Resistant
- Castrate testosterone levels (<50 ng/dL) AND documented progression
- Also known as CRPC (Castration-Resistant Prostate Cancer)