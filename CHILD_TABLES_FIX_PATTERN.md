# Child Tables Fix Pattern

## Issue
Child tables in HMU files are missing `meta_last_updated` column, causing errors when `filter_dataframe_by_version` tries to deduplicate by this column.

## Files Needing Fix

### ✅ Fixed
- HMUPatient (all 7 child tables)
- HMUPractitioner (all 3 child tables)
- HMUDocumentReference (2 child tables: identifiers, authors)
- HMUCondition (all 6 child tables)
- HMUEncounter (3 child tables: types, participants, locations)
- HMUMedicationDispense (all 3 child tables)

### ⏳ Pending
- HMUObservation (9 child tables) - also has exit code 137 (memory issue)

## Fix Pattern

For EACH child table, you need to make 4 changes:

### 1. Transform Function - Add meta extraction in first select

```python
# BEFORE
names_df = df.select(
    F.col("id").alias("entity_id"),
    F.explode(F.col("name")).alias("name_item")
)

# AFTER
names_df = df.select(
    F.col("id").alias("entity_id"),
    F.col("meta").getField("lastUpdated").alias("meta_last_updated"),  # ADD THIS
    F.explode(F.col("name")).alias("name_item")
)
```

### 2. Transform Function - Include meta in final select

```python
# BEFORE
names_final = names_df.select(
    F.col("entity_id"),
    F.col("name_item.text").alias("text"),
    # ...other fields
)

# AFTER
names_final = names_df.select(
    F.col("entity_id"),
    F.col("meta_last_updated"),  # ADD THIS
    F.col("name_item.text").alias("text"),
    # ...other fields
)
```

### 3. resolveChoice - Add meta_last_updated spec

```python
# BEFORE
names_resolved_frame = names_dynamic_frame.resolveChoice(
    specs=[('entity_id', 'cast:string')]
)

# AFTER
names_resolved_frame = names_dynamic_frame.resolveChoice(
    specs=[
        ('entity_id', 'cast:string'),
        ('meta_last_updated', 'cast:timestamp')  # ADD THIS
    ]
)
```

### 4. CREATE TABLE SQL - Add meta_last_updated column

```sql
-- BEFORE
CREATE TABLE public.entity_names (
    entity_id VARCHAR(255),
    text VARCHAR(500),
    -- ...other columns
) SORTKEY (entity_id);

-- AFTER
CREATE TABLE public.entity_names (
    entity_id VARCHAR(255),
    meta_last_updated TIMESTAMP,  -- ADD THIS
    text VARCHAR(500),
    -- ...other columns
) SORTKEY (entity_id);
```

## File-Specific Details

### HMUDocumentReference
**Error**: Line 690 - `document_reference_id, identifier_system, identifier_value`

**Child tables to fix**:
1. `transform_document_identifiers()`
2. `transform_document_authors()`
3. `transform_document_context()`

### HMUCondition
**Error**: Line 1703 - `condition_id, category_code, category_system, category_text, category_display`

**Child tables to fix**:
1. `transform_condition_categories()`
2. `transform_condition_codes()`
3. `transform_condition_body_sites()`
4. `transform_condition_stages()`
5. `transform_condition_evidence()`
6. `transform_condition_notes()`

### HMUEncounter
**Error**: Line 1158 - `encounter_id, type_code, type_display, type_system, type_text`

**Child tables to fix**:
1. `transform_encounter_types()`
2. `transform_encounter_diagnoses()`
3. `transform_encounter_participants()`
4. `transform_encounter_locations()`

### HMUMedicationDispense
**Error**: Line 1000 - `medication_dispense_id, identifier_system, identifier_value`

**Child tables to fix**:
1. `transform_dispense_identifiers()`
2. `transform_dispense_performers()`
3. `transform_dispense_dosage_instructions()`

### HMUObservation
**Error**: Exit code 137 (out of memory)

This file has 9 child tables - may need to be handled separately or with memory optimization.

## Quick Fix Script Template

```python
# For each file, run these fixes:

# 1. Find all transform functions
grep "def transform_" FILE.py

# 2. For each transform, add meta extraction
# Find: F.col("id").alias("entity_id"),
# Add after: F.col("meta").getField("lastUpdated").alias("meta_last_updated"),

# 3. For each transform final select, add meta column
# Find: F.col("entity_id"),
# Add after: F.col("meta_last_updated"),

# 4. Update resolveChoice specs
# Find: specs=[('entity_id', 'cast:string')]
# Replace: specs=[('entity_id', 'cast:string'), ('meta_last_updated', 'cast:timestamp')]

# 5. Update CREATE TABLE SQL
# Find: entity_id VARCHAR(255),
# Add after: meta_last_updated TIMESTAMP,
```

## Testing

After fixing:
1. Upload to S3
2. Run Glue job
3. Check for `Column 'meta_last_updated' does not exist` errors
4. Verify deduplication messages in CloudWatch logs

## Status

- ✅ HMUPractitioner - Fixed and uploaded
- ✅ HMUDocumentReference - Fixed and uploaded
- ✅ HMUCondition - Fixed and uploaded
- ✅ HMUEncounter - Fixed and uploaded
- ✅ HMUMedicationDispense - Fixed and uploaded
- ⚠️ HMUObservation - Memory issue (exit 137) - needs separate investigation
