# Observations Normalization Gaps Analysis

**Date**: 2025-01-XX  
**Status**: Analysis Complete - Ready for Implementation

## Summary

Based on the [Comprehensive Normalization Strategy](./COMPREHENSIVE_NORMALIZATION_STRATEGY.md), the following code-based fields in Observations are **still denormalized** and should be normalized using the shared `codes` table or specialized tables.

---

## ✅ Already Normalized

1. **Observation Codes** - ✅ Normalized via `codes` table + `observation_codes` junction table
2. **Categories** - ✅ Normalized via `categories` table + `observation_categories` junction table
3. **Interpretations** - ✅ Normalized via `interpretations` table + `observation_interpretations` junction table

---

## ❌ Still Denormalized (Should Normalize)

### Phase 1: High Priority (Immediate Impact)

#### 1. **Body Sites** (Observations)
**Current State:**
- ❌ Denormalized in `observations` table: `body_site_code`, `body_site_system`, `body_site_display`, `body_site_text`
- ✅ `body_sites` table exists (used by conditions)
- ❌ No `observation_body_sites` junction table

**Action Required:**
- Create `observation_body_sites` junction table (similar to `condition_body_sites`)
- Remove body site columns from `observations` table
- Update ETL to populate `body_sites` table and `observation_body_sites` junction table

**Storage Impact:** Low cardinality (3 unique values), but consistency with conditions is valuable

---

#### 2. **Component Codes** (Observation Components)
**Current State:**
- ❌ Denormalized in `observation_components` table:
  - `component_code VARCHAR(50)`
  - `component_system VARCHAR(255)`
  - `component_display VARCHAR(255)`

**Action Required:**
- Replace with `component_code_id BIGINT` referencing `codes.code_id`
- Update ETL to:
  1. Extract unique component codes → upsert to `codes` table
  2. Get `code_id` from `codes` table
  3. Store `component_code_id` in `observation_components` table
- Remove `component_code`, `component_system`, `component_display` columns

**Storage Impact:** Medium-high (component codes like "systolic", "diastolic" likely repeat)

**ETL Pattern:** Use same pattern as `observation_codes` transformation

---

#### 3. **Range Type Codes** (Reference Ranges)
**Current State:**
- ❌ Denormalized in `observation_reference_ranges` table:
  - `range_type_code VARCHAR(50)`
  - `range_type_system VARCHAR(255)`
  - `range_type_display VARCHAR(255)`

**Action Required:**
- Replace with `range_type_code_id BIGINT` referencing `codes.code_id`
- Update ETL to:
  1. Extract unique range type codes → upsert to `codes` table
  2. Get `code_id` from `codes` table
  3. Store `range_type_code_id` in `observation_reference_ranges` table
- Remove `range_type_code`, `range_type_system`, `range_type_display` columns

**Storage Impact:** Medium (range types like "normal", "recommended" likely repeat)

**ETL Pattern:** Use same pattern as `observation_codes` transformation

---

### Phase 2: Medium Priority (When Data Available)

#### 4. **Data Absent Reason Codes** (Observations & Components)
**Current State:**
- ❌ Denormalized in `observations` table:
  - `data_absent_reason_code VARCHAR(50)`
  - `data_absent_reason_system VARCHAR(255)`
  - `data_absent_reason_display VARCHAR(255)`
- ❌ Denormalized in `observation_components` table:
  - `component_data_absent_reason_code VARCHAR(50)`
  - `component_data_absent_reason_display VARCHAR(255)` (missing system)

**Action Required:**
- Replace with `data_absent_reason_code_id BIGINT` in both tables
- Update ETL to normalize data absent reason codes via `codes` table
- Remove denormalized columns

**Storage Impact:** Medium (codes like "not-performed", "unsupported" likely repeat)

---

#### 5. **Method Codes** (Observations)
**Current State:**
- ❌ Denormalized in `observations` table:
  - `method_code VARCHAR(50)`
  - `method_system VARCHAR(255)`
  - `method_display VARCHAR(255)`
- ⚠️ Currently set to `NULL` in ETL (data may not exist)

**Action Required:**
- Replace with `method_code_id BIGINT` referencing `codes.code_id`
- Update ETL to normalize method codes (if data exists)
- Remove denormalized columns

**Storage Impact:** Unknown (depends on data availability)

---

#### 6. **Value Codeable Concept Codes** (Observations & Components)
**Current State:**
- ❌ Denormalized in `observations` table:
  - `value_codeable_concept_code VARCHAR(50)`
  - `value_codeable_concept_system VARCHAR(255)`
  - `value_codeable_concept_display VARCHAR(255)`
- ❌ Denormalized in `observation_components` table:
  - `component_value_codeable_concept_code VARCHAR(50)`
  - `component_value_codeable_concept_system VARCHAR(255)`
  - `component_value_codeable_concept_display VARCHAR(255)`

**Action Required:**
- Replace with `value_code_id BIGINT` in both tables
- Update ETL to normalize value codes via `codes` table
- Remove denormalized columns
- **Note:** Keep `value_codeable_concept_text` denormalized (unique text per observation)

**Storage Impact:** Medium-high (if value codes repeat)

---

## Implementation Checklist

### Phase 1: High Priority

- [ ] **Body Sites Normalization**
  - [ ] Create `observation_body_sites` DDL
  - [ ] Update `observations.sql` DDL (remove body site columns)
  - [ ] Create body sites transformation function
  - [ ] Update main ETL pipeline to use body sites normalization
  - [ ] Test and validate

- [ ] **Component Codes Normalization**
  - [ ] Update `observation_components.sql` DDL (add `component_code_id`, remove denormalized columns)
  - [ ] Create component codes transformation function (reuse `codes_transformation.py` pattern)
  - [ ] Update `child_tables.py` to use normalized component codes
  - [ ] Test and validate

- [ ] **Range Type Codes Normalization**
  - [ ] Update `observation_reference_ranges.sql` DDL (add `range_type_code_id`, remove denormalized columns)
  - [ ] Create range type codes transformation function (reuse `codes_transformation.py` pattern)
  - [ ] Update `child_tables.py` to use normalized range type codes
  - [ ] Test and validate

### Phase 2: Medium Priority

- [ ] **Data Absent Reason Codes Normalization**
  - [ ] Update `observations.sql` DDL (add `data_absent_reason_code_id`, remove denormalized columns)
  - [ ] Update `observation_components.sql` DDL (add `component_data_absent_reason_code_id`, remove denormalized columns)
  - [ ] Update ETL transformations
  - [ ] Test and validate

- [ ] **Method Codes Normalization**
  - [ ] Update `observations.sql` DDL (add `method_code_id`, remove denormalized columns)
  - [ ] Update ETL transformations (if data exists)
  - [ ] Test and validate

- [ ] **Value Codeable Concept Codes Normalization**
  - [ ] Update `observations.sql` DDL (add `value_code_id`, remove denormalized columns)
  - [ ] Update `observation_components.sql` DDL (add `component_value_code_id`, remove denormalized columns)
  - [ ] Update ETL transformations
  - [ ] Test and validate

---

## Implementation Pattern

All code normalizations should follow the same pattern as `observation_codes`:

1. **Extract unique codes** from source data
2. **Generate `code_id`** using hash of `code_code + code_system` (via `generate_code_id_native()`)
3. **Upsert to `codes` table** (if new code)
4. **Store `code_id`** in the child table (e.g., `component_code_id`, `range_type_code_id`)
5. **Remove denormalized columns** from DDL

**Reference Implementation:**
- `v2/HMUObservation/transformations/codes_transformation.py` - Code normalization utilities
- `v2/HMUObservation/transformations/child_tables.py` - Observation codes transformation (lines 23-200)

---

## Files to Update

### DDL Files
- `v2/ddl/observations.sql` - Remove body site, method, data absent reason, value codeable concept columns
- `v2/ddl/observation_components.sql` - Remove component code, data absent reason, value codeable concept columns
- `v2/ddl/observation_reference_ranges.sql` - Remove range type code columns
- `v2/ddl/observation_body_sites.sql` - **NEW** - Create junction table

### ETL Files
- `v2/HMUObservation/transformations/main_observation.py` - Update main observation transformation
- `v2/HMUObservation/transformations/child_tables.py` - Update component and reference range transformations
- `v2/HMUObservation/HMUObservation.py` - Update pipeline to use new transformations

### View Files (if applicable)
- `views/fact_fhir_observations_view_v1.sql` - Update to join with normalized tables

---

## Expected Benefits

1. **Storage Efficiency**: 
   - Component codes: ~50-70% reduction (estimated)
   - Range type codes: ~60-80% reduction (estimated)
   - Body sites: Consistency with conditions (already normalized)

2. **Consistency**: Same normalization pattern across all code-based fields

3. **Maintainability**: One pattern to understand and maintain

4. **Query Performance**: Smaller tables, faster joins (with proper DISTKEY/SORTKEY)

---

## Migration Strategy

1. **Create new normalized tables/columns** (additive change)
2. **Run ETL to populate both old and new columns** (dual-write)
3. **Validate data matches**
4. **Update views/queries to use new columns**
5. **Remove old denormalized columns** (breaking change - coordinate with downstream consumers)

---

**Next Steps**: Start with Phase 1 items (Body Sites, Component Codes, Range Type Codes) as they have the highest impact and are most straightforward to implement.


