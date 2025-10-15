# HMU SQL Refactoring Summary

## Overview
All observation-based and condition-based queries from the hmu_sql directory have been refactored into materialized views following standardized patterns using fact tables.

---

## Observation-Based Views (14 views)

### Pattern
All observation views follow the pattern from `rpt_fhir_observations_testosterone_total_hmu_v1.sql`:
- Use `fact_fhir_observations_view_v1` as data source
- Include patient demographics (names, birth_date, gender)
- Materialized views with DISTSTYLE KEY, DISTKEY (patient_id), SORTKEY (patient_id, effective_datetime)
- Text-based filtering (removed LOINC/SNOMED code checks)
- No value thresholds (let downstream analysis apply criteria)
- Includes array fields: codes, categories, reference_ranges, interpretations, notes

### Created Views

**Laboratory Tests (3):**
- `rpt_fhir_observations_absolute_neutrophil_count_hmu_v1.sql`
- `rpt_fhir_observations_platelet_count_hmu_v1.sql`
- `rpt_fhir_observations_hemoglobin_hmu_v1.sql`

**Liver & Kidney Function (7):**
- `rpt_fhir_observations_creatinine_hmu_v1.sql`
- `rpt_fhir_observations_egfr_hmu_v1.sql`
- `rpt_fhir_observations_alt_hmu_v1.sql`
- `rpt_fhir_observations_ast_hmu_v1.sql`
- `rpt_fhir_observations_total_bilirubin_hmu_v1.sql`
- `rpt_fhir_observations_serum_albumin_hmu_v1.sql`
- `rpt_fhir_observations_serum_potassium_hmu_v1.sql`

**HIV & Diabetes Monitoring (4):**
- `rpt_fhir_observations_hba1c_hmu_v1.sql`
- `rpt_fhir_observations_bmi_hmu_v1.sql`
- `rpt_fhir_observations_cd4_count_hmu_v1.sql`
- `rpt_fhir_observations_hiv_viral_load_hmu_v1.sql`

**Other (2):**
- `rpt_fhir_observations_testosterone_total_hmu_v1.sql` (original pattern)
- `rpt_fhir_observations_psa_total_hmu_v1.sql` (pre-existing)

---

## Condition-Based Views (3 views)

### Pattern
All condition views use `fact_fhir_conditions_view_v1` as data source:
- Include patient demographics (names, birth_date, gender)
- Materialized views with DISTSTYLE KEY, DISTKEY (patient_id), SORTKEY (patient_id, recorded_date)
- Text-based and ICD-10 code filtering
- Includes aggregated JSON fields: body_sites, categories, evidence, notes, stages
- Includes code_rank for primary/secondary code identification
- Includes is_active calculated field

### Created Views

1. **`rpt_fhir_conditions_additional_malignancy_hmu_v1.sql`**
   - Replaces: 11_additional_malignancy.sql
   - Captures all malignancy conditions (ICD-10 C00-C97, D00-D09)
   - Removed hardcoded exception logic for skin cancer, in situ, remission
   - AI can assess exceptions based on code_code, stages, clinical_status_code, dates

2. **`rpt_fhir_conditions_active_liver_disease_hmu_v1.sql`**
   - Replaces: 12_active_liver_disease.sql
   - Captures hepatitis (B15-B19) and liver disease (K70-K77) conditions
   - Removed hepatitis type categorization
   - AI can determine disease type and severity from codes and notes

3. **`rpt_fhir_conditions_cns_metastases_hmu_v1.sql`**
   - Replaces: 16_active_cns_metastases.sql
   - Captures CNS metastases (C79.3x, C79.4x, C79.5x)
   - Removed treatment status assessment
   - AI can evaluate treatment status from clinical_status_code and notes

---

## Key Benefits of Refactoring

### 1. Performance
- Uses pre-aggregated fact tables instead of raw tables with multiple joins
- Materialized views provide instant query performance
- Optimized distribution and sort keys for common access patterns

### 2. Standardization
- Consistent naming: `rpt_fhir_observations_*_hmu_v1` and `rpt_fhir_conditions_*_hmu_v1`
- Consistent structure across all views
- Predictable column sets

### 3. Flexibility
- Removed hardcoded thresholds and business rules
- AI can make nuanced clinical decisions
- Easier to adapt to changing requirements
- Comprehensive data preserved for analysis

### 4. Maintainability
- Single source of truth (fact tables)
- Simpler queries (no complex CTEs or CASE statements)
- Clear separation: views for data retrieval, AI for interpretation

### 5. AI-Friendly
- All relevant context preserved (codes, notes, dates, status)
- No premature filtering
- Rich metadata for informed decisions

---

## Original Files Status

### Deleted
All observation and condition-based hmu_sql files have been deleted after successful refactoring:
- 02-10: Lab test observations (ANC, Platelet, Hemoglobin, Creatinine, ALT, AST, Bilirubin, Albumin, Potassium)
- 11: Additional malignancy conditions
- 12: Active liver disease conditions
- 13: HbA1c observations
- 14: HIV observations (CD4, viral load)
- 15: BMI observations
- 16: CNS metastases conditions

### Archived
- `.archived_01_serum_testosterone.sql` - Original testosterone query before refactoring

---

## Migration Path

For any downstream processes using the old hmu_sql queries:

1. **Update query references** from `hmu_sql/XX_*.sql` to `views/rpt_fhir_*_hmu_v1.sql`
2. **Update column references** - views use fact table column names
3. **Apply thresholds in WHERE clauses** if needed (e.g., `value_quantity_value >= 1500` for ANC)
4. **Use AI for clinical interpretations** instead of relying on hardcoded status flags

---

## Date Completed
October 14, 2025
