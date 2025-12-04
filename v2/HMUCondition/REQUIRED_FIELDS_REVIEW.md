# Required Fields Review - HMUCondition ETL

## Executive Summary

This document reviews the current HMUCondition ETL process against the required database fields and provides implementation recommendations.

**Date:** 2025-01-XX  
**Status:** Review Complete - Ready for Implementation

---

## 1. ICD-10 vs ICD-10-CM Clarification

### Answer: They are related but distinct

- **ICD-10**: World Health Organization's international standard (3-5 characters)
- **ICD-10-CM**: US-specific Clinical Modification (up to 7 characters, more detailed)
- **For ETL purposes**: Treat both `http://hl7.org/fhir/sid/icd-10` and `http://hl7.org/fhir/sid/icd-10-cm` as valid ICD-10 codes
- **Current data**: Uses `http://hl7.org/fhir/sid/icd-10-cm` (US standard)

### Recommendation
- Support both system identifiers when matching/looking up ICD-10 codes
- Normalize both to a common identifier for matching purposes
- Document that ICD-10-CM is the US variant of ICD-10

---

## 2. Code Coverage Analysis

### Current State
✅ **100% Code Coverage**: All 3,200,699 conditions in Redshift have associated codes in `condition_codes` table

### Source Data Verification
- All HealthLake Condition resources sampled have `code.coding[]` arrays
- Transformation logic properly handles:
  - Conditions with multiple codes (exploded to multiple rows)
  - Conditions with empty/null coding arrays (filtered out)

### Edge Cases to Handle
1. **Empty coding array**: Current logic filters these out - should we create a placeholder?
2. **Code without system**: Current logic requires `code_code` to be not null
3. **Code without display**: Currently allowed (display can be null)

### Recommendation
- **Current approach is sufficient**: Codes are normalized in `condition_codes` table
- **No changes needed** for code extraction
- **Consider**: Adding a check to log conditions without codes (if any appear in future)

---

## 3. Required Fields Mapping

### ✅ Already Available

| Required Field | Current Field | Status | Notes |
|---------------|--------------|--------|-------|
| **Diagnosis Name** | `condition_text` | ✅ Available | From `code.text` in FHIR |
| **Effective Date/Time** | `recorded_date`, `onset_datetime` | ✅ Available | Need to coalesce |
| **ICD-10 Code** | `condition_codes.code_code` (where system = ICD-10-CM) | ✅ Available | Normalized in child table |
| **SNOMED CT Code** | `condition_codes.code_code` (where system = SNOMED) | ✅ Available | Normalized in child table |
| **Severity** | `severity_display` | ✅ Available | Optional field |
| **Stage** | `condition_stages.stage_summary_display` | ✅ Available | Optional, in child table |

### ❌ Missing/Needs Enhancement

| Required Field | Current State | Action Needed |
|---------------|--------------|---------------|
| **Status** (computed) | Only `clinical_status_code` and `verification_status_code` exist | **ADD**: Computed `status` field with values: `"current"`, `"past"`, `"primary"`, `"secondary"`, `"history_of"` |
| **Effective Date/Time** (single field) | Two separate fields | **ADD**: `effective_datetime` = COALESCE(`onset_datetime`, `recorded_date`) |
| **Diagnosis Name** (explicit) | Exists as `condition_text` | **RENAME/ALIAS**: Add `diagnosis_name` as alias or computed column |

---

## 4. Status Field Computation Logic

### Required Values
- `"current"` - Active and confirmed condition
- `"past"` - Inactive/resolved condition
- `"primary"` - Primary diagnosis (problem list item, current)
- `"secondary"` - Secondary diagnosis (encounter diagnosis, current)
- `"history_of"` - Historical condition

### Computation Rules

```python
def compute_status(clinical_status, verification_status, category_code, category_text, code_display):
    """
    Compute status field based on multiple FHIR fields
    """
    # Priority 1: Check for history_of
    if (category_text and 'history' in category_text.lower()) or \
       (code_display and 'history' in code_display.lower()) or \
       (category_code and category_code.startswith('history')):
        return 'history_of'
    
    # Priority 2: Check verification status (entered-in-error = exclude)
    if verification_status == 'entered-in-error':
        return None  # Or 'entered-in-error' if we want to keep these
    
    # Priority 3: Determine current vs past
    is_current = (clinical_status == 'active' and verification_status == 'confirmed')
    is_past = (clinical_status == 'inactive' and verification_status == 'confirmed')
    
    # Priority 4: Determine primary vs secondary
    if is_current:
        if category_code == 'problem-list-item':
            return 'primary'
        elif category_code == 'encounter-diagnosis':
            return 'secondary'
        else:
            return 'current'
    elif is_past:
        return 'past'
    else:
        # Default: active but unconfirmed = current
        return 'current' if clinical_status == 'active' else 'past'
```

### Implementation Location
- **File**: `transformations/main_condition.py`
- **Method**: Add status computation in `transform_main_condition_data()`
- **Keep original fields**: Maintain `clinical_status_code` and `verification_status_code` for reference

---

## 5. Implementation Plan

### Step 1: Enhance Main Condition Transformation

**File**: `HMUCondition/v2/transformations/main_condition.py`

**Changes**:
1. Add `diagnosis_name` column (alias of `condition_text`)
2. Add `effective_datetime` column (COALESCE of `onset_datetime` and `recorded_date`)
3. Add `status` column (computed using logic above)
4. Keep `clinical_status_code` and `verification_status_code` (as requested)

**Join Requirements**:
- Need to join with `condition_categories` to get category information for status computation
- Need to join with `condition_codes` to check for "history" in code display
- OR: Pre-compute status before transformation (better performance)

### Step 2: Update Database Schema

**DDL Changes Needed**:
```sql
ALTER TABLE public.conditions ADD COLUMN IF NOT EXISTS diagnosis_name VARCHAR;
ALTER TABLE public.conditions ADD COLUMN IF NOT EXISTS effective_datetime TIMESTAMP;
ALTER TABLE public.conditions ADD COLUMN IF NOT EXISTS status VARCHAR(50);
```

**Note**: `clinical_status_code` and `verification_status_code` already exist.

### Step 3: Handle ICD-10 System Variants

**In code lookups/queries**:
- Support both `http://hl7.org/fhir/sid/icd-10` and `http://hl7.org/fhir/sid/icd-10-cm`
- Normalize to a common identifier for matching: `icd-10` (without -cm suffix)

**Example**:
```python
# Normalize ICD-10 system identifier
icd10_systems = [
    'http://hl7.org/fhir/sid/icd-10',
    'http://hl7.org/fhir/sid/icd-10-cm'
]
normalized_system = 'icd-10' if code_system in icd10_systems else code_system
```

### Step 4: Code Access Pattern

**Since codes remain normalized** (as requested), document the access pattern:

```sql
-- Get ICD-10 code for a condition
SELECT code_code, code_display 
FROM condition_codes 
WHERE condition_id = ? 
  AND code_system IN ('http://hl7.org/fhir/sid/icd-10', 'http://hl7.org/fhir/sid/icd-10-cm');

-- Get SNOMED CT code for a condition
SELECT code_code, code_display 
FROM condition_codes 
WHERE condition_id = ? 
  AND code_system = 'http://snomed.info/sct';

-- Get all codes for a condition
SELECT code_code, code_system, code_display 
FROM condition_codes 
WHERE condition_id = ?;
```

---

## 6. Data Quality Checks

### Current Coverage
- ✅ 100% of conditions have codes
- ✅ All conditions have `condition_text` (diagnosis name)
- ✅ Most conditions have `recorded_date` or `onset_datetime`

### Recommended Validation
1. **Status computation**: Verify all conditions get a valid status value
2. **Effective datetime**: Ensure at least one of `onset_datetime` or `recorded_date` exists
3. **Code systems**: Verify ICD-10-CM and SNOMED CT codes are properly identified
4. **History detection**: Test that "history_of" status is correctly identified

---

## 7. Summary of Required Changes

### Must Implement
1. ✅ Add `diagnosis_name` field (alias of `condition_text`)
2. ✅ Add `effective_datetime` field (COALESCE of onset/recorded)
3. ✅ Add `status` field (computed from clinical/verification status + category)
4. ✅ Keep `clinical_status_code` and `verification_status_code` (original fields)
5. ✅ Support both ICD-10 and ICD-10-CM system identifiers

### Already Satisfied
- ✅ Codes normalized in `condition_codes` table (as requested)
- ✅ All conditions have codes (100% coverage)
- ✅ ICD-10 and SNOMED CT codes available via normalized table

### Optional Enhancements
- Consider adding computed columns for quick access to ICD-10/SNOMED codes (but not required per requirements)
- Add logging for conditions without codes (edge case handling)
- Add validation for status computation completeness

---

## 8. Next Steps

1. **Review this document** with stakeholders
2. **Implement changes** to `transformations/main_condition.py`
3. **Update DDL** for new columns
4. **Test with sample data** to verify status computation logic
5. **Deploy and validate** with full dataset

---

## Appendix: Status Computation Examples

| Clinical Status | Verification Status | Category | Code Display | Computed Status |
|----------------|-------------------|----------|--------------|----------------|
| active | confirmed | problem-list-item | Diabetes | primary |
| active | confirmed | encounter-diagnosis | Hypertension | secondary |
| active | confirmed | health-concern | - | current |
| inactive | confirmed | problem-list-item | - | past |
| active | unconfirmed | - | - | current |
| - | entered-in-error | - | - | (exclude or mark) |
| active | confirmed | - | History of cancer | history_of |
| - | - | history-of | - | history_of |

---

**Document Status**: ✅ Review Complete - Ready for Implementation

