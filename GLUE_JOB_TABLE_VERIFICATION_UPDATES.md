# Glue Job Updates: Remove Table Creation, Add Table Verification

## Overview

All Glue jobs are being updated to **check for table existence** instead of creating tables. Tables must be created via DDL scripts (in `./ddl/` folder) before Glue jobs run.

## Changes Required

### 1. Add Table Verification Functions

Add these two functions (after `get_entities_to_delete` function, before `write_to_redshift_versioned`):

```python
def verify_table_exists(table_name):
    """Verify that a Redshift table exists, raise error if it doesn't"""
    logger.info(f"Verifying table exists: public.{table_name}")

    try:
        # Try to read from the table to verify it exists
        test_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "redshiftTmpDir": S3_TEMP_DIR,
                "useConnectionProperties": "true",
                "dbtable": f"public.{table_name}",
                "connectionName": REDSHIFT_CONNECTION
            },
            transformation_ctx=f"verify_{table_name}"
        )

        # If we can convert to DF, table exists
        df = test_frame.toDF()
        logger.info(f"✅ Table 'public.{table_name}' exists and is accessible")
        return True

    except Exception as e:
        logger.error(f"❌ Table 'public.{table_name}' does NOT exist or is not accessible")
        logger.error(f"   Error: {str(e)}")
        logger.error(f"   Tables must be created using DDL scripts before running Glue jobs")
        raise RuntimeError(f"Required table 'public.{table_name}' does not exist. Please run DDL scripts first.")

def verify_all_required_tables(required_tables):
    """Verify all required tables exist before processing"""
    logger.info(f"\n{'='*80}")
    logger.info(f"🔍 VERIFYING REQUIRED TABLES EXIST")
    logger.info("=" * 80)

    all_exist = True
    for table in required_tables:
        try:
            verify_table_exists(table)
        except RuntimeError:
            all_exist = False

    if not all_exist:
        logger.error(f"\n{'='*80}")
        logger.error("❌ MISSING REQUIRED TABLES")
        logger.error("=" * 80)
        logger.error("Please run the DDL scripts to create all required tables before running this Glue job")
        logger.error("DDL scripts should be located in the ddl/ folder")
        raise RuntimeError("Missing required tables. Cannot proceed with ETL job.")

    logger.info(f"\n{'='*80}")
    logger.info(f"✅ ALL REQUIRED TABLES VERIFIED")
    logger.info("=" * 80)
    return True
```

### 2. Remove CREATE TABLE SQL Functions

Delete all `create_*_sql()` function definitions. For example:
- `create_redshift_tables_sql()`
- `create_patient_names_table_sql()`
- `create_observations_table_sql()`
- etc.

### 3. Update `write_to_redshift_versioned()` Function Signature

**OLD:**
```python
def write_to_redshift_versioned(dynamic_frame, table_name, id_column, preactions=""):
```

**NEW:**
```python
def write_to_redshift_versioned(dynamic_frame, table_name, id_column):
```

### 4. Update `write_to_redshift_versioned()` Empty Records Handling

**OLD:**
```python
if total_records == 0:
    logger.info(f"No records to process for {table_name}, but ensuring table exists")
    # Execute preactions to create table even if no data
    if preactions:
        logger.info(f"Executing preactions to create empty {table_name} table")
        # ... (dummy record creation logic)
    return
```

**NEW:**
```python
if total_records == 0:
    logger.info(f"No records to process for {table_name} - skipping")
    return
```

### 5. Update Preactions Building Logic

**OLD:**
```python
# Step 4: Build preactions for selective deletion
selective_preactions = preactions
if entities_to_delete:
    # ... (deletion logic)
    if selective_preactions:
        selective_preactions = delete_clause + " " + selective_preactions
    else:
        selective_preactions = delete_clause
```

**NEW:**
```python
# Step 4: Build preactions for selective deletion
if entities_to_delete:
    # ... (deletion logic)
    logger.info(f"Will delete {num_entities} existing entities before inserting updated versions")
else:
    delete_clause = ""
```

### 6. Update Write Options

**OLD:**
```python
glueContext.write_dynamic_frame.from_options(
    frame=filtered_dynamic_frame,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": S3_TEMP_DIR,
        "useConnectionProperties": "true",
        "dbtable": f"public.{table_name}",
        "connectionName": REDSHIFT_CONNECTION,
        "preactions": selective_preactions or ""
    },
    transformation_ctx=f"write_{table_name}_versioned_to_redshift"
)
```

**NEW:**
```python
connection_options = {
    "redshiftTmpDir": S3_TEMP_DIR,
    "useConnectionProperties": "true",
    "dbtable": f"public.{table_name}",
    "connectionName": REDSHIFT_CONNECTION
}

# Only add preactions if we have deletions to perform
if delete_clause:
    connection_options["preactions"] = delete_clause

glueContext.write_dynamic_frame.from_options(
    frame=filtered_dynamic_frame,
    connection_type="redshift",
    connection_options=connection_options,
    transformation_ctx=f"write_{table_name}_versioned_to_redshift"
)
```

### 7. Update write_to_redshift_versioned() Calls

**OLD:**
```python
logger.info("📝 Creating main patients table with version checking...")
patients_table_sql = create_redshift_tables_sql()
write_to_redshift_versioned(main_resolved_frame, "patients", "patient_id", patients_table_sql)
logger.info("✅ Main patients table created and written successfully")
```

**NEW:**
```python
logger.info("📝 Writing main patients table with version checking...")
write_to_redshift_versioned(main_resolved_frame, "patients", "patient_id")
logger.info("✅ Main patients table written successfully")
```

### 8. Add Table Verification Before Writes

Add this call before the first `write_to_redshift_versioned()` call:

```python
# Verify all required tables exist before processing
required_tables = [
    "patients",
    "patient_names",
    "patient_telecoms",
    # ... (all tables used by this job)
]
verify_all_required_tables(required_tables)
```

## Jobs Status

| Job Name | Status | Tables | Notes |
|----------|--------|--------|-------|
| HMUPatient | ✅ **COMPLETED** | patients, patient_names, patient_telecoms, patient_addresses, patient_contacts, patient_communications, patient_practitioners, patient_links | Template for all other jobs |
| HMUObservation | ⏳ Pending | observations | Single table, simpler pattern |
| HMUCondition | ⏳ Pending | conditions, condition_codes, condition_body_sites, condition_categories, condition_evidence, condition_extensions, condition_notes, condition_stages | Multiple tables |
| HMUEncounter | ⏳ Pending | encounters, encounter_class_history, encounter_diagnoses, encounter_participants, encounter_locations | Multiple tables |
| HMUProcedure | ⏳ Pending | procedures, procedure_codes, procedure_body_sites, procedure_performers, procedure_focal_devices | Multiple tables |
| HMUAllergyIntolerance | ⏳ Pending | allergy_intolerance, allergy_intolerance_reactions | Multiple tables |
| HMUCarePlan | ⏳ Pending | care_plans, care_plan_activities | Multiple tables |
| HMUDiagnosticReport | ⏳ Pending | diagnostic_reports, diagnostic_report_codes, diagnostic_report_results | Multiple tables |
| HMUDocumentReference | ⏳ Pending | document_references, document_reference_content | Multiple tables |
| HMUMedication | ⏳ Pending | medications, medication_codes, medication_ingredients | Multiple tables |
| HMUMedicationDispense | ⏳ Pending | medication_dispenses, medication_dispense_performers | Multiple tables |
| HMUMedicationRequest | ⏳ Pending | medication_requests, medication_request_dosage_instructions | Multiple tables |
| HMUPractitioner | ⏳ Pending | practitioners, practitioner_qualifications, practitioner_telecoms, practitioner_addresses, practitioner_names | Multiple tables |

## Testing

After updating each job:

1. **Syntax check:**
   ```bash
   python3 -m py_compile HMU{JobName}/HMU{JobName}.py
   ```

2. **Create DDL tables first:**
   ```bash
   # Run DDL scripts to create all required tables
   ./scripts/run_ddl_scripts.sh
   ```

3. **Upload to S3:**
   ```bash
   AWS_PROFILE=to-prd-admin aws s3 cp HMU{JobName}/HMU{JobName}.py \
       s3://aws-glue-assets-442042533707-us-east-2/scripts/HMU{JobName}.py \
       --region us-east-2
   ```

4. **Test run:**
   ```bash
   ./scripts/run_all_glue_jobs.sh --include "HMU{JobName}"
   ```

## Reference Implementation

See `/Users/ken/code/ThirdOpinion/ThirdOpinion.ETL/HMUPatient/HMUPatient.py` for the complete reference implementation.

Key sections:
- **Lines 608-671**: Verification functions
- **Line 812**: Updated `write_to_redshift_versioned()` signature
- **Lines 821-823**: Simplified empty records handling
- **Lines 840-861**: Updated preactions logic
- **Lines 869-885**: Conditional connection_options with preactions
- **Line 1369**: Table verification call before writes
- **Lines 1373-1403**: Updated write calls without SQL parameters
