# Comprehensive Normalization Strategy

## Overview

This document outlines a comprehensive normalization strategy for all ETL jobs, ensuring consistency across all resources (Observations, Conditions, etc.).

## Core Principle

**Use a SINGLE shared `codes` table for ALL codes, regardless of context. The context is determined by the column name (e.g., `component_code_id`, `range_type_code_id`), not by separate tables.**

### Key Rules

1. **ONE `codes` table for all codes**: Component codes, range type codes, method codes, data absent reason codes, evidence codes, stage codes, etc. all go into the same `codes` table.

2. **Reference via `code_id`**: Each table that needs a code references `codes.code_id` with a descriptive column name:
   - `observation_components.component_code_id` → references `codes.code_id`
   - `observation_reference_ranges.range_type_code_id` → references `codes.code_id`
   - `observations.method_code_id` → references `codes.code_id`
   - `observations.data_absent_reason_code_id` → references `codes.code_id`

3. **Separate tables only for special cases**: 
   - `categories` table (categories have specific meaning/context)
   - `interpretations` table (interpretations have specific meaning/context)
   - `body_sites` table (has additional context: `body_site_text`)

4. **Keep denormalized**: Unique data (text, IDs, timestamps, values) remains denormalized.

---

## Normalization Rules

### ✅ **SHOULD BE NORMALIZED** (Code-Based, Repetitive)

**Key Principle**: Use the **shared `codes` table** for ALL codes, regardless of context. The context is determined by the column name (e.g., `component_code_id`, `range_type_code_id`), not by separate tables.

### Code-Based Fields (Use Shared `codes` Table)

1. **Observation Codes** (`code_code`, `code_system`, `code_display`)
   - ✅ Already normalized: `codes` table + `observation_codes` with `code_id`
   - High cardinality, significant storage savings

2. **Condition Codes** (`code_code`, `code_system`, `code_display`)
   - ✅ Already normalized: `codes` table + `condition_codes` with `code_id`
   - High cardinality, significant storage savings

3. **Component Codes** (`component_code`, `component_system`, `component_display`)
   - ❌ **Not normalized**: Currently in `observation_components` table
   - **Should normalize**: Replace with `component_code_id` referencing `codes.code_id`
   - Component codes like "systolic", "diastolic" likely repeat

4. **Range Type Codes** (`range_type_code`, `range_type_system`, `range_type_display`)
   - ❌ **Not normalized**: Currently in `observation_reference_ranges` table
   - **Should normalize**: Replace with `range_type_code_id` referencing `codes.code_id`
   - Range types like "normal", "recommended" likely repeat

5. **Data Absent Reason Codes** (`data_absent_reason_code`, `data_absent_reason_system`, `data_absent_reason_display`)
   - ❌ **Not normalized**: Currently in `observations` and `observation_components` tables
   - **Should normalize**: Replace with `data_absent_reason_code_id` referencing `codes.code_id`
   - These codes likely repeat (e.g., "not-performed", "unsupported")

6. **Method Codes** (`method_code`, `method_system`, `method_display`)
   - ❌ **Not normalized**: Currently in `observations` table (set to None)
   - **Should normalize**: Replace with `method_code_id` referencing `codes.code_id`
   - If method codes exist and repeat

7. **Value Codeable Concept Codes** (`value_codeable_concept_code`, `value_codeable_concept_system`, `value_codeable_concept_display`)
   - ❌ **Not normalized**: Currently in `observations` and `observation_components` tables
   - **Should normalize**: Replace with `value_code_id` referencing `codes.code_id`
   - If value codes repeat

8. **Evidence Codes** (`evidence_code`, `evidence_system`, `evidence_display`) - Conditions
   - ⚠️ **Review needed**: Currently in `condition_evidence` table
   - **Should normalize**: Replace with `evidence_code_id` referencing `codes.code_id`
   - If evidence codes repeat across conditions

9. **Stage Codes** (`stage_*_code`, `stage_*_system`, `stage_*_display`) - Conditions
   - ⚠️ **Review needed**: Currently in `condition_stages` table
   - **Should normalize**: Replace with `stage_*_code_id` referencing `codes.code_id`
   - If stage codes repeat (e.g., cancer staging codes)

### Special Cases (Separate Tables)

10. **Categories** (`category_code`, `category_system`, `category_display`)
    - ✅ Already normalized: `categories` table (separate because categories have specific meaning/context)
    - Medium cardinality, good storage savings

11. **Interpretations** (`interpretation_code`, `interpretation_system`, `interpretation_display`)
    - ✅ Already normalized: `interpretations` table (separate because interpretations have specific meaning/context)
    - Low-medium cardinality, good storage savings

12. **Body Sites** (`body_site_code`, `body_site_system`, `body_site_display`)
    - ⚠️ **Partially normalized**: `body_sites` table exists, used by conditions
    - ❌ **Missing**: Observations should also use it
    - **Note**: Could use `codes` table, but `body_sites` has additional context (body_site_text)
    - Low cardinality (3 unique values), but consistency is valuable

### ❌ **SHOULD NOT BE NORMALIZED** (Unique Data)

These fields contain unique data per record and should remain denormalized:

1. **Notes** (`note_text`, `note_author_reference`, `note_time`)
   - ✅ Keep denormalized: Unique text per observation/condition
   - Tables: `observation_notes`, `condition_notes`

2. **Extensions** (`extension_url`, `value_*`)
   - ✅ Keep denormalized: Highly variable, unique per resource
   - Tables: `condition_extensions` (observations doesn't have extensions table)

3. **Performers** (`performer_type`, `performer_id`)
   - ✅ Keep denormalized: Unique performer IDs per observation
   - Table: `observation_performers`

4. **Members** (`member_id`, `member_reference`)
   - ✅ Keep denormalized: Unique member references
   - Table: `observation_members`

5. **Derived From** (`derived_from_reference`)
   - ✅ Keep denormalized: Unique references per observation
   - Table: `observation_derived_from`

6. **Reference Range Values** (`range_low_value`, `range_high_value`, `range_text`)
   - ✅ Keep denormalized: Unique numeric values and text per observation
   - Table: `observation_reference_ranges` (but `range_type_code` should be normalized)

7. **Component Values** (`component_value_*`)
   - ✅ Keep denormalized: Unique values per component
   - Table: `observation_components` (but `component_code` should be normalized)

---

## Normalization Priority

### Phase 1: High Priority (Immediate)

1. ✅ **Body Sites** - Normalize observations to match conditions
   - Create `observation_body_sites` table
   - Use shared `body_sites` table
   - Remove body site columns from `observations` table

2. ⚠️ **Component Codes** - Normalize using shared `codes` table
   - Replace `component_code`, `component_system`, `component_display` with `component_code_id`
   - Use existing `codes` table (no new table needed)
   - Keep component values in `observation_components` (denormalized)

3. ⚠️ **Range Type Codes** - Normalize using shared `codes` table
   - Replace `range_type_code`, `range_type_system`, `range_type_display` with `range_type_code_id`
   - Use existing `codes` table (no new table needed)
   - Keep range values/text in `observation_reference_ranges` (denormalized)

### Phase 2: Medium Priority (When Data Available)

4. **Data Absent Reason Codes** - Normalize using shared `codes` table
   - Replace `data_absent_reason_code`, `data_absent_reason_system`, `data_absent_reason_display` with `data_absent_reason_code_id`
   - Use existing `codes` table (no new table needed)
   - Update `observations` and `observation_components` tables

5. **Method Codes** - Normalize using shared `codes` table
   - Replace `method_code`, `method_system`, `method_display` with `method_code_id`
   - Use existing `codes` table (no new table needed)
   - Update `observations` table

6. **Value Codeable Concept Codes** - Normalize using shared `codes` table
   - Replace `value_codeable_concept_code`, `value_codeable_concept_system`, `value_codeable_concept_display` with `value_code_id`
   - Use existing `codes` table (no new table needed)
   - Update `observations` and `observation_components` tables

7. **Evidence Codes** - Normalize using shared `codes` table
   - Replace `evidence_code`, `evidence_system`, `evidence_display` with `evidence_code_id`
   - Use existing `codes` table (no new table needed)
   - Update `condition_evidence` table

8. **Stage Codes** - Normalize using shared `codes` table
   - Replace `stage_*_code`, `stage_*_system`, `stage_*_display` with `stage_*_code_id`
   - Use existing `codes` table (no new table needed)
   - Update `condition_stages` table

### Phase 3: Keep Denormalized (No Action)

8. **Notes, Extensions, Performers, Members, Derived From, Values**
   - These remain denormalized as they contain unique data

---

## Implementation Pattern

### Standard Normalization Pattern (Using Shared `codes` Table)

For any code-based field that should be normalized:

1. **Use Existing `codes` Table** (no need to create new tables):
   - The `codes` table already exists with: `code_id`, `code_code`, `code_system`, `code_display`, `code_text`, `normalized_code_text`
   - All codes (regardless of context) go into this single table

2. **Update Table with Code Fields** (e.g., `observation_components`):
   ```sql
   -- Remove denormalized columns:
   ALTER TABLE public.observation_components 
   DROP COLUMN component_code,
   DROP COLUMN component_system,
   DROP COLUMN component_display;
   
   -- Add normalized reference:
   ALTER TABLE public.observation_components 
   ADD COLUMN component_code_id BIGINT;  -- References codes.code_id
   ```

3. **ETL Transformation**:
   - Extract unique codes from source → add to `codes` table (if not exists)
   - Get `code_id` from `codes` table (using hash-based lookup)
   - Store `code_id` in the child table (e.g., `observation_components.component_code_id`)
   - Use same pattern as `observation_codes` and `condition_codes`

### Example: Normalizing Component Codes

**Before (Denormalized):**
```sql
CREATE TABLE observation_components (
    observation_id VARCHAR(255),
    component_code VARCHAR(50),      -- Denormalized
    component_system VARCHAR(255),    -- Denormalized
    component_display VARCHAR(255),   -- Denormalized
    component_value_string VARCHAR(65535)
);
```

**After (Normalized):**
```sql
CREATE TABLE observation_components (
    observation_id VARCHAR(255),
    component_code_id BIGINT,  -- References codes.code_id
    component_value_string VARCHAR(65535)
);
```

**ETL Logic:**
1. Extract `component_code`, `component_system`, `component_display` from source
2. Generate `code_id` using hash of `code_code + code_system` (same as observation codes)
3. Upsert to `codes` table (if new code)
4. Store `code_id` in `observation_components.component_code_id`

---

## Benefits of Comprehensive Normalization

1. **Consistency**: Same pattern across all code-based fields
2. **Storage Efficiency**: Codes stored once, referenced many times
3. **Maintainability**: One pattern to understand and maintain
4. **Query Performance**: Smaller tables, faster joins (with proper DISTKEY/SORTKEY)
5. **Data Integrity**: Single source of truth for code definitions

---

## Trade-offs

### Pros
- ✅ Consistent pattern across all tables
- ✅ Storage efficiency for repetitive codes
- ✅ Easier to maintain (one pattern)
- ✅ Better data integrity

### Cons
- ⚠️ More tables to manage
- ⚠️ More joins for queries
- ⚠️ More ETL code complexity
- ⚠️ Overhead for very low cardinality (but consistency is valuable)

---

## Decision Framework

**Normalize if:**
- Field contains codes (code, system, display structure)
- Codes can repeat across records
- **Use shared `codes` table** - don't create separate code tables
- Storage savings justify complexity (or consistency is valuable)

**Use Separate Tables if:**
- Field has additional context beyond code/system/display (e.g., `body_sites` has `body_site_text`)
- Field represents a specific domain concept (e.g., `categories`, `interpretations`)

**Denormalize if:**
- Field contains unique text/values per record
- Field contains unique IDs/references
- Field contains timestamps/dates
- Field is highly variable (extensions)

---

## Current Status

### ✅ Already Normalized
- Codes (observations, conditions)
- Categories (observations, conditions)
- Interpretations (observations)
- Body Sites (conditions only - observations missing)

### ⚠️ Should Normalize (Use Shared `codes` Table)
- Body Sites (observations) - Use `body_sites` table (separate due to additional context)
- Component Codes (observations) - Use `codes` table via `component_code_id`
- Range Type Codes (observations) - Use `codes` table via `range_type_code_id`
- Data Absent Reason Codes (observations, conditions) - Use `codes` table via `data_absent_reason_code_id`
- Method Codes (observations - if data exists) - Use `codes` table via `method_code_id`
- Value Codeable Concept Codes (observations, components) - Use `codes` table via `value_code_id`
- Evidence Codes (conditions - if codes repeat) - Use `codes` table via `evidence_code_id`
- Stage Codes (conditions - if codes repeat) - Use `codes` table via `stage_*_code_id`

### ❌ Keep Denormalized
- Notes
- Extensions
- Performers
- Members
- Derived From
- Values (numeric, text, etc.)

---

**Last Updated**: 2025-12-04  
**Status**: Strategy Document - Ready for Implementation

