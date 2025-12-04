# DDL Update Summary - Required Fields

## Date: 2025-01-XX
## Status: ✅ Updated

---

## Changes Made

### 1. Updated `ddl/conditions.sql`

Added three new columns to the conditions table definition:

1. **`status`** (VARCHAR(50))
   - Location: After `verification_status_system` (line 16)
   - Purpose: Computed status field with values: "current", "past", "primary", "secondary", "history_of"

2. **`diagnosis_name`** (VARCHAR(500))
   - Location: After `condition_text` (line 18)
   - Purpose: Alias of condition_text for required field mapping

3. **`effective_datetime`** (TIMESTAMP)
   - Location: After `recorded_date` (line 36)
   - Purpose: COALESCE of onset_datetime and recorded_date

### Column Order in DDL

The columns are now ordered as follows:
```sql
-- Status fields (grouped together)
clinical_status_code
clinical_status_display
clinical_status_system
verification_status_code
verification_status_display
verification_status_system
status                    -- ✅ NEW: Computed status

-- Condition information
condition_text
diagnosis_name            -- ✅ NEW: Alias of condition_text

-- Severity
severity_code
severity_display
severity_system

-- Onset/Abatement dates
onset_datetime
...
abatement_datetime
...

-- Recorded/Effective dates
recorded_date
effective_datetime        -- ✅ NEW: COALESCE(onset_datetime, recorded_date)
```

---

## Deployment Options

### Option 1: Automatic Migration (Recommended)

The deployment script will automatically detect and apply schema changes:

```bash
# Deploy all DDL files (will auto-migrate existing tables)
./scripts/deploy_ddl_to_redshift.sh

# With table postfix (e.g., for v2 tables)
TABLE_POSTFIX="_v2" ./scripts/deploy_ddl_to_redshift.sh
```

**How it works:**
1. Script compares current table schema with DDL file
2. Detects new columns (`diagnosis_name`, `effective_datetime`, `status`)
3. Automatically generates and executes ALTER TABLE statements
4. Reports migration status

### Option 2: Manual Migration

If you need to run the migration manually:

```sql
-- For base table
ALTER TABLE IF EXISTS public.conditions 
ADD COLUMN IF NOT EXISTS diagnosis_name VARCHAR(500);

ALTER TABLE IF EXISTS public.conditions 
ADD COLUMN IF NOT EXISTS effective_datetime TIMESTAMP;

ALTER TABLE IF EXISTS public.conditions 
ADD COLUMN IF NOT EXISTS status VARCHAR(50);

-- For v2 table (if using postfix)
ALTER TABLE IF EXISTS public.conditions_v2 
ADD COLUMN IF NOT EXISTS diagnosis_name VARCHAR(500);

ALTER TABLE IF EXISTS public.conditions_v2 
ADD COLUMN IF NOT EXISTS effective_datetime TIMESTAMP;

ALTER TABLE IF EXISTS public.conditions_v2 
ADD COLUMN IF NOT EXISTS status VARCHAR(50);
```

**Or use the migration script:**
```bash
# Edit ddl/migrations/add_required_fields_to_conditions.sql to set correct table name
# Then execute via Redshift Data API or psql
```

---

## Files Created/Modified

### Modified Files
- ✅ `ddl/conditions.sql` - Added three new columns

### New Files
- ✅ `ddl/migrations/add_required_fields_to_conditions.sql` - Standalone migration script
- ✅ `HMUCondition/v2/DDL_UPDATE_SUMMARY.md` - This file

---

## Verification

After deployment, verify the columns exist:

```sql
-- Check columns in conditions table
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'public' 
  AND table_name = 'conditions'  -- or 'conditions_v2' if using postfix
  AND column_name IN ('diagnosis_name', 'effective_datetime', 'status')
ORDER BY column_name;
```

Expected output:
```
diagnosis_name        | character varying | 500
effective_datetime    | timestamp         | NULL
status                 | character varying | 50
```

---

## Notes

- **Backward Compatibility**: All existing columns remain unchanged
- **Original Fields Preserved**: `clinical_status_code` and `verification_status_code` are kept as-is
- **Table Postfix Support**: The deployment script handles table postfixes automatically
- **No Data Migration Needed**: New columns will be populated by the ETL process

---

## Next Steps

1. ✅ **DDL Updated**: Complete
2. ⏳ **Deploy DDL**: Run deployment script or manual migration
3. ⏳ **Verify Schema**: Confirm columns exist in Redshift
4. ⏳ **Run ETL**: Execute HMUCondition ETL to populate new fields
5. ⏳ **Validate Data**: Verify new fields are populated correctly

---

**Status**: ✅ DDL Files Updated - Ready for Deployment

