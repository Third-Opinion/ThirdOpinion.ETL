# Implementation Summary - Required Fields

## Date: 2025-01-XX
## Status: ✅ Implemented

---

## Changes Implemented

### 1. ✅ Diagnosis Name Field
- **Field**: `diagnosis_name`
- **Type**: String (VARCHAR)
- **Source**: Alias of `condition_text` (from FHIR `code.text`)
- **Location**: `transformations/main_condition.py` line 89
- **Status**: ✅ Implemented

### 2. ✅ Effective Date/Time Field
- **Field**: `effective_datetime`
- **Type**: Timestamp
- **Source**: COALESCE(`onset_datetime`, `recorded_date`)
- **Logic**: Uses `onsetDateTime` if available, otherwise falls back to `recordedDate`
- **Location**: `transformations/main_condition.py` line 142
- **Status**: ✅ Implemented

### 3. ✅ Computed Status Field
- **Field**: `status`
- **Type**: String (VARCHAR)
- **Values**: `"current"`, `"past"`, `"primary"`, `"secondary"`, `"history_of"`, or `NULL`
- **Location**: `transformations/main_condition.py` lines 175-233
- **Status**: ✅ Implemented

#### Status Computation Logic:
1. **Priority 1 - History Detection**: 
   - If category text/code contains "history" OR code display contains "history" → `"history_of"`

2. **Priority 2 - Entered in Error**:
   - If `verification_status_code == "entered-in-error"` → `NULL` (excluded)

3. **Priority 3 & 4 - Current/Past/Primary/Secondary**:
   - If `clinical_status_code == "active"` AND `verification_status_code == "confirmed"`:
     - If `category_code == "problem-list-item"` → `"primary"`
     - Else if `category_code == "encounter-diagnosis"` → `"secondary"`
     - Else → `"current"`
   - Else if `clinical_status_code == "inactive"` AND `verification_status_code == "confirmed"` → `"past"`
   - Else if `clinical_status_code == "active"` → `"current"`
   - Else → `"past"`

### 4. ✅ Original Status Fields Preserved
- **Fields**: `clinical_status_code`, `verification_status_code`
- **Status**: ✅ Kept as-is (no changes)
- **Note**: Both original fields remain available alongside the computed `status` field

---

## Files Modified

### 1. `transformations/main_condition.py`
- Added `diagnosis_name` field (line 89)
- Added `effective_datetime` field (line 142)
- Added status computation logic (lines 155-233)
- Extracts category and code display information for status computation

### 2. `HMUCondition.py`
- Updated `convert_to_dynamic_frames()` to include new fields:
  - `diagnosis_name` (line 192)
  - `effective_datetime` (line 210)
  - `status` (line 211)

---

## Database Schema Changes Required

### Option 1: Automatic Migration (Recommended)
The deployment script (`scripts/deploy_ddl_to_redshift.sh`) will automatically detect and apply schema changes. Simply run:

```bash
./scripts/deploy_ddl_to_redshift.sh
```

The script will:
- Compare current schema with `ddl/conditions.sql`
- Automatically generate and execute ALTER TABLE statements for new columns
- Handle table postfixes if configured

### Option 2: Manual Migration
If you need to run the migration manually, use the migration script:

```sql
-- See: ddl/migrations/add_required_fields_to_conditions.sql
ALTER TABLE IF EXISTS public.conditions ADD COLUMN IF NOT EXISTS diagnosis_name VARCHAR(500);
ALTER TABLE IF EXISTS public.conditions ADD COLUMN IF NOT EXISTS effective_datetime TIMESTAMP;
ALTER TABLE IF EXISTS public.conditions ADD COLUMN IF NOT EXISTS status VARCHAR(50);
```

**Note**: 
- `clinical_status_code` and `verification_status_code` already exist (no changes needed)
- The table name may have a postfix (e.g., `conditions_v2`) depending on configuration
- The DDL file (`ddl/conditions.sql`) has been updated with the new columns

---

## Code Access Patterns

### ICD-10 and SNOMED CT Codes
Codes remain normalized in the `condition_codes` table (as requested). Access patterns:

```sql
-- Get ICD-10 code (supports both icd-10 and icd-10-cm)
SELECT code_code, code_display 
FROM condition_codes 
WHERE condition_id = ? 
  AND code_system IN (
    'http://hl7.org/fhir/sid/icd-10',
    'http://hl7.org/fhir/sid/icd-10-cm'
  );

-- Get SNOMED CT code
SELECT code_code, code_display 
FROM condition_codes 
WHERE condition_id = ? 
  AND code_system = 'http://snomed.info/sct';
```

### Required Fields Query Example

```sql
SELECT 
    condition_id,
    patient_id,
    diagnosis_name,           -- ✅ New field
    status,                   -- ✅ New field
    clinical_status_code,     -- ✅ Original field (kept)
    verification_status_code, -- ✅ Original field (kept)
    effective_datetime,       -- ✅ New field
    recorded_date,           -- Original field (still available)
    onset_datetime           -- Original field (still available)
FROM conditions
WHERE patient_id = ?;
```

---

## Testing Recommendations

1. **Status Computation**:
   - Verify all status values are correctly computed
   - Test edge cases (null values, missing categories)
   - Verify "history_of" detection works correctly

2. **Effective Datetime**:
   - Verify COALESCE logic (onset_datetime preferred over recorded_date)
   - Test cases where both are null

3. **Diagnosis Name**:
   - Verify it matches `condition_text`
   - Test cases where `code.text` is null

4. **Data Integrity**:
   - Verify all 3,200,699+ conditions have the new fields populated
   - Check for any NULL values in required fields

---

## Next Steps

1. ✅ **Code Implementation**: Complete
2. ⏳ **DDL Execution**: Run ALTER TABLE statements on Redshift
3. ⏳ **Testing**: Test with sample data before full deployment
4. ⏳ **Deployment**: Deploy to production environment
5. ⏳ **Validation**: Verify data quality after deployment

---

## Notes

- **ICD-10 vs ICD-10-CM**: Both system identifiers are supported in code lookups
- **Code Normalization**: Codes remain in normalized `condition_codes` table (as requested)
- **100% Code Coverage**: All conditions have codes, no additional extraction needed
- **Backward Compatibility**: Original fields (`clinical_status_code`, `verification_status_code`) are preserved

---

**Implementation Status**: ✅ Complete - Ready for Testing

