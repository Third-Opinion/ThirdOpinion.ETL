# Condition ETL v2 Refactoring Status

## Overview
This document tracks the progress of refactoring the Condition ETL job to match the improved v2 structure used in HMUObservation.

## Completed ✅

### 1. Folder Structure
- ✅ Created `HMUCondition/v2/` directory structure
- ✅ Created `utils/`, `transformations/`, and `database/` subdirectories
- ✅ Added `__init__.py` files for package structure

### 2. Configuration (`config.py`)
- ✅ Created `SparkConfig`, `DatabaseConfig`, `ProcessingConfig`, and `ETLConfig` dataclasses
- ✅ Created `TableNames` class with support for:
  - `CONDITIONS` (main table)
  - `CONDITION_CODES`
  - `CONDITION_CATEGORIES`
  - `CONDITION_BODY_SITES`
  - `CONDITION_EVIDENCE`
  - `CONDITION_EXTENSIONS`
  - `CONDITION_NOTES`
  - `CONDITION_STAGES`
- ✅ Added table name postfix support for testing
- ✅ Environment variable and job argument support

### 3. Utility Modules (`utils/`)
- ✅ `timestamp_utils.py` - FHIR timestamp parsing utilities
- ✅ `bookmark_utils.py` - Incremental processing with bookmark support
- ✅ `deduplication_utils.py` - Window function-based deduplication
- ✅ `version_utils.py` - Version comparison and filtering
- ✅ `deletion_utils.py` - Deletion operations for deleted conditions (isDelete field)

## Remaining Work 🔨

### 4. Transformations (`transformations/`)
**Status:** Needs to be created

**Files needed:**
- `__init__.py`
- `main_condition.py` - Main condition transformation (from `transform_main_condition_data`)
- `child_tables.py` - All child table transformations:
  - `transform_condition_codes`
  - `transform_condition_categories`
  - `transform_condition_body_sites`
  - `transform_condition_evidence`
  - `transform_condition_extensions`
  - `transform_condition_notes`
  - `transform_condition_stages`

**Source:** Extract from `HMUCondition/HMUCondition.py`:
- Lines 509-664: `transform_main_condition_data`
- Lines 665-699: `transform_condition_categories`
- Lines 700-736: `transform_condition_notes`
- Lines 737-780: `transform_condition_body_sites`
- Lines 781-830: `transform_condition_stages`
- Lines 953-1051: `transform_condition_codes`
- Lines 1052-1095: `transform_condition_evidence`
- Lines 1096-1210: `transform_condition_extensions`

### 5. Database Operations (`database/`)
**Status:** Needs to be created

**Files needed:**
- `__init__.py`
- `redshift_operations.py` - Redshift read/write operations:
  - `write_to_redshift_simple` - For child tables (append only)
  - `write_to_redshift_versioned` - For main table (version-aware)

**Source:** Can be adapted from `HMUObservation/v2/database/redshift_operations.py`

### 6. Main Orchestrator (`HMUCondition.py`)
**Status:** Needs to be created

**Key features to implement:**
- Read data from Iceberg catalog
- Filter deleted records (isDelete field)
- Apply bookmark filtering for incremental processing
- Deduplicate conditions
- Transform main condition data
- Transform all child tables
- Version-aware writes to Redshift
- Handle deleted conditions
- Clean up old child records for updated conditions

**Source:** Adapt from:
- `HMUObservation/v2/HMUObservation.py` (structure and flow)
- `HMUCondition/HMUCondition.py` (Condition-specific transformations)

### 7. Table Name Postfix
**Status:** Already implemented in config ✅

The table name postfix feature is already built into the `TableNames` class and will work automatically once all modules use `TableNames` properties.

## Key Differences from Observation

1. **Deletion Field:** Conditions use `isDelete` instead of `status == "entered-in-error"`
2. **Table Names:** Different table names (conditions vs observations)
3. **Child Tables:** Different child tables (7 child tables for conditions vs 9 for observations)
4. **ID Column:** Uses `condition_id` instead of `observation_id`

## Next Steps

1. Extract transformation functions from original `HMUCondition.py`
2. Create `transformations/main_condition.py` and `transformations/child_tables.py`
3. Create `database/redshift_operations.py` (adapt from Observation version)
4. Create main `HMUCondition.py` orchestrator
5. Test with `TABLE_NAME_POSTFIX=_v2` to ensure it works without affecting production

## Testing Strategy

1. Create test tables in Redshift with `_v2` postfix
2. Run job with `--TABLE_NAME_POSTFIX=_v2`
3. Verify data loads correctly
4. Compare results with v1 implementation
5. Once validated, can switch production to use v2

