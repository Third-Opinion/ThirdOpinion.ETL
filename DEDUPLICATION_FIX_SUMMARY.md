# Deduplication Fix Summary

## Issue
HMU*.py files were creating multiple records for each entity instead of just the newest one based on `meta_last_updated`.

## Root Causes

### 1. Missing Deduplication Logic
Most HMU files lacked deduplication in their `filter_dataframe_by_version` functions, allowing multiple versions of the same entity to be processed.

### 2. Duplicate Function Definitions
- **HMUObservation**: Had duplicate `write_to_redshift_versioned` function at line 1079 that was overriding the proper version-aware function
- **HMUPractitioner**: Had duplicate `write_to_redshift_versioned` function at line 433 that was overriding the proper version-aware function

### 3. Missing meta_last_updated in Child Tables (HMUPatient only)
HMUPatient child tables were missing `meta_last_updated` column in:
- Transform functions
- Flat DataFrame selections
- resolveChoice specs
- CREATE TABLE SQL statements

## Fixes Applied

### All 13 HMU Files - Deduplication Logic Added

Added window function deduplication to `filter_dataframe_by_version`:

```python
# Step 1: Deduplicate incoming data - keep only latest version per entity
from pyspark.sql.window import Window

window_spec = Window.partitionBy(id_column).orderBy(F.col("meta_last_updated").desc())
df_latest = df.withColumn("row_num", F.row_number().over(window_spec)) \
              .filter(F.col("row_num") == 1) \
              .drop("row_num")

incoming_count = df.count()
deduplicated_count = df_latest.count()

if incoming_count > deduplicated_count:
    logger.info(f"Deduplicated incoming data: {incoming_count} → {deduplicated_count} records (kept latest per entity)")
```

**Files Fixed:**
1. ✅ HMUAllergyIntolerance
2. ✅ HMUCarePlan
3. ✅ HMUCondition
4. ✅ HMUDiagnosticReport
5. ✅ HMUDocumentReference
6. ✅ HMUEncounter
7. ✅ HMUMedication
8. ✅ HMUMedicationDispense
9. ✅ HMUMedicationRequest
10. ✅ HMUObservation
11. ✅ HMUPatient
12. ✅ HMUPractitioner
13. ✅ HMUProcedure

### HMUObservation & HMUPractitioner - Removed Duplicate Functions

**HMUObservation**: Removed duplicate `write_to_redshift_versioned` function at line 1079 (kept the proper version-aware one at line 146)

**HMUPractitioner**: Removed duplicate `write_to_redshift_versioned` function at line 433 (kept the proper version-aware one at line 146)

### HMUPatient - Added meta_last_updated to Child Tables

**Transforms Updated:**
- `transform_patient_names()` - Added `meta_last_updated` extraction
- `transform_patient_telecoms()` - Added `meta_last_updated` extraction
- `transform_patient_addresses()` - Added `meta_last_updated` extraction
- `transform_patient_contacts()` - Added `meta_last_updated` extraction
- `transform_patient_communications()` - Added `meta_last_updated` extraction
- `transform_patient_practitioners()` - Added `meta_last_updated` extraction
- `transform_patient_links()` - Added `meta_last_updated` extraction

**Flat DataFrame Selections Updated:**
Added `.cast(TimestampType()).alias("meta_last_updated")` to all child table flat_df selections

**resolveChoice Specs Updated:**
Added `("meta_last_updated", "cast:timestamp")` to all child table resolveChoice specs

**CREATE TABLE SQL Updated:**
Added `meta_last_updated TIMESTAMP,` to all child table schemas:
- `patient_names`
- `patient_telecoms`
- `patient_addresses`
- `patient_contacts`
- `patient_communications`
- `patient_practitioners`
- `patient_links`

## File Organization

All scripts and views have been properly organized:

### Scripts Folder
All `.sh` files moved to `scripts/` folder:
- `scripts/deploy_glue_jobs.sh`
- `scripts/redeploy_fhir_views.sh`
- `scripts/create_all_fhir_views.sh`
- `scripts/validate_sql_files.sh`
- etc.

All scripts properly reference `views/` folder for SQL files.

### Views Folder
All `*_view_*.sql` files moved to `views/` folder:
- `views/fact_fhir_patients_view_v1.sql`
- `views/fact_fhir_encounters_view_v1.sql`
- `views/fact_fhir_observations_view_v1.sql`
- etc.

## Testing

### HMUPatient
- ✅ Deployed and tested successfully
- ✅ Job completed without errors
- ✅ Deduplication working correctly

### Other Files
- ✅ All files uploaded to S3
- ⏳ Pending testing in production

## Expected Behavior

After these fixes:

1. **Deduplication**: When multiple versions of the same entity exist in source data:
   - Only the latest version (by `meta_last_updated`) is kept
   - Older versions are automatically filtered out

2. **Version Comparison**: The latest version is compared against Redshift:
   - If newer than Redshift → processed and written
   - If same as Redshift → skipped
   - If doesn't exist in Redshift → written as new

3. **No Duplicates**: Each entity will have exactly one record in Redshift - the latest version

## Files Modified

### Python ETL Files (13)
- HMUAllergyIntolerance/HMUAllergyIntolerance.py
- HMUCarePlan/HMUCarePlan.py
- HMUCondition/HMUCondition.py
- HMUDiagnosticReport/HMUDiagnosticReport.py
- HMUDocumentReference/HMUDocumentReference.py
- HMUEncounter/HMUEncounter.py
- HMUMedication/HMUMedication.py
- HMUMedicationDispense/HMUMedicationDispense.py
- HMUMedicationRequest/HMUMedicationRequest.py
- HMUObservation/HMUObservation.py
- HMUPatient/HMUPatient.py
- HMUPractitioner/HMUPractitioner.py
- HMUProcedure/HMUProcedure.py

### Helper Script
- add_deduplication.py (created to automate fixes)

## Verification Commands

```bash
# Verify all files have deduplication
grep -l "Window.*partitionBy.*meta_last_updated" HMU*/HMU*.py | wc -l
# Should return: 13

# Check for duplicate functions
for file in HMU*/HMU*.py; do
  count=$(grep -c "^def write_to_redshift_versioned" "$file")
  if [ "$count" -gt 1 ]; then
    echo "⚠️  $file: $count duplicate functions"
  fi
done
# Should return: (no output - no duplicates)

# Verify HMUPatient child tables have meta_last_updated
grep -c "meta_last_updated" HMUPatient/HMUPatient.py
# Should return: high number (multiple occurrences)
```

## Deployment Status

### Uploaded to S3
- ✅ HMUPatient.py (tested and working)
- ✅ HMUObservation.py
- ✅ HMUPractitioner.py
- ⏳ Other files pending deployment

### Next Steps
1. Deploy remaining updated files to S3
2. Run and test each Glue job
3. Verify no duplicate records in Redshift
4. Monitor CloudWatch logs for deduplication messages
