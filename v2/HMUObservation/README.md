# HMUObservation ETL v2

## Overview

This is a refactored and improved version of the HMUObservation ETL job. The v2 implementation addresses key issues from the original:

- **Modular Architecture**: Code split into logical modules for better maintainability
- **Removed Debug Code**: All production debug code removed
- **Optimized Performance**: Improved deduplication using window functions
- **Externalized Configuration**: Configuration management separated from business logic
- **Better Error Handling**: More specific error handling with proper logging
- **Type Hints**: Added type hints for better code clarity
- **Standardized Utilities**: Reusable utility functions for common operations
- **Code Enrichment**: Automatic mapping of text-based codes to LOINC codes

## Structure

```
v2/HMUObservation/
├── __init__.py
├── config.py                    # Configuration management
├── README.md                    # This file
├── HMUObservation.py            # Main ETL orchestrator
├── CODE_MAPPINGS_AND_NORMALIZATION.md  # Code mappings reference
├── database/
│   ├── __init__.py
│   └── redshift_operations.py  # Redshift read/write operations
├── transformations/
│   ├── __init__.py
│   ├── main_observation.py     # Main observation transformation
│   ├── child_tables.py         # Child table transformations
│   ├── codes_transformation.py # Codes table transformation
│   └── ...                     # Other transformation modules
└── utils/
    ├── __init__.py
    ├── code_enrichment.py      # LOINC mapping dictionary
    ├── code_enrichment_native.py  # Code enrichment logic
    ├── deletion_utils.py       # Deletion operations
    └── ...                     # Other utility modules
```

## Key Features

### 1. Configuration Management (`config.py`)
- Centralized configuration using dataclasses
- Environment variable support
- Type-safe configuration objects
- Table name constants

### 2. Code Enrichment (`utils/code_enrichment.py`)

The ETL automatically enriches observation codes by mapping text-based codes to standardized LOINC codes:

- **~200+ LOINC mappings** for common lab tests, abbreviations, and terms
- **Normalization patterns** for handling variations (suffixes, typos, etc.)
- **Synthetic codes** created when no LOINC match exists (ensures 100% code coverage)
- **Impact:** ~171,222 observations mapped from synthetic to LOINC codes

See `CODE_MAPPINGS_AND_NORMALIZATION.md` for complete mapping reference.

### 3. Utility Modules (`utils/`)
- **Code Enrichment**: Automatic LOINC code mapping with normalization
- **Timestamp Parsing**: Standardized FHIR timestamp parsing with multiple format support
- **Bookmark Management**: Incremental processing with bookmark support
- **Deduplication**: Optimized window function-based deduplication
- **Version Management**: Entity versioning for updates
- **Deletion Operations**: Efficient deletion with batch support

### 4. Database Operations (`database/`)
- Separated read/write operations
- Version-aware writes
- Simple append writes for child tables
- Proper error handling

### 5. Transformations (`transformations/`)
- Clean transformation functions
- Removed all debug code
- Standardized patterns
- Helper functions for common operations
- Code deduplication to prevent duplicate codes

### 6. Main Orchestrator (`HMUObservation.py`)
- Clean main function
- Improved error handling
- Better logging
- Step-by-step processing with clear separation

## Usage

The v2 implementation maintains the same interface as v1 but with improved internals. To use:

1. Update the Glue job script location to point to `v2/HMUObservation.py`
2. Configuration can be adjusted via environment variables or by modifying `config.py`
3. All functionality remains the same, but with better performance and maintainability

### Table Name Postfix for Testing

You can add an optional postfix to all table names to test v2 without affecting production tables:

**Via Environment Variable:**
```bash
export TABLE_NAME_POSTFIX="_v2"
```

**Via Glue Job Arguments:**
```
--TABLE_NAME_POSTFIX=_v2
```

**Example:**
- Without postfix: `observations`, `observation_codes`, etc.
- With `_v2` postfix: `observations_v2`, `observation_codes_v2`, etc.

This allows you to:
- Test v2 implementation safely
- Run parallel tests without conflicts
- Compare v1 vs v2 results side-by-side

**Note:** Make sure the postfixed tables exist in Redshift before running the job, or create them using the same DDL as the production tables.

## Code Enrichment

The ETL automatically enriches observation codes by:

1. **Extracting code text** from observation `code.text`
2. **Normalizing text** (lowercase, strip suffixes, handle typos, etc.)
3. **Looking up in mapping table** (`LAB_TEST_LOINC_MAPPING`)
4. **Creating code:**
   - If LOINC match found → use LOINC code
   - If no match → create synthetic code

### Normalization Patterns

- **Numeric Suffix Stripping:** `shbg_1` → `shbg`
- **Parentheses Stripping:** `specimen_source_(3)` → `specimen_source`
- **Space-Comma Normalization:** `"PSA ,total"` → `"PSA,total"`
- **Plural Variations:** `nitrites` → same LOINC as `nitrite`
- **Typo Corrections:** `appearnace` → `appearance`

### Code Mappings

The ETL includes mappings for:

- **Lab Test Abbreviations:** platelet, hgb, bun, co2, etc. (~25K observations)
- **CBC Differential Components:** neutrophil_%, lymphocyte_%, etc. (~15K observations)
- **Antibiotic Susceptibility:** ampicillin, levofloxacin, etc. (~25K observations)
- **Generic Terms:** marital_status, prostate_biopsy, etc. (~91K observations)
- **Common Urine Tests:** leukocytes, glucose, protein, nitrite, etc.
- **PSA Tests:** Standard and free PSA variations

See `CODE_MAPPINGS_AND_NORMALIZATION.md` for complete reference.

## Migration Notes

- All debug code removed from `extract_ai_evidence` UDF (which was removed entirely as it wasn't used)
- Deduplication now uses window functions instead of SQL JOINs
- Configuration is externalized - update `config.py` or use environment variables
- Error handling is more specific - check logs for specific error types
- Code enrichment automatically maps text to LOINC codes
- Deduplication prevents duplicate codes per observation

## Performance Improvements

1. **Deduplication**: Window functions are more efficient than SQL JOINs for large datasets
2. **Caching**: Consider adding DataFrame caching for expensive operations
3. **Reduced Counts**: Minimized unnecessary `count()` operations
4. **Optimized Timestamp Parsing**: Reusable timestamp parser reduces code duplication
5. **Code Enrichment**: Broadcast joins for efficient mapping lookups
6. **Normalized Codes Table**: Reduces storage from ~21GB to ~1.1GB (95% reduction)

## Future Enhancements

- Add unit tests for transformation functions
- Add integration tests
- Add CloudWatch metrics
- Add data quality checks
- Add retry logic for transient failures
- Continue expanding LOINC mappings based on synthetic code analysis

## Documentation

- **`CODE_MAPPINGS_AND_NORMALIZATION.md`**: Complete reference for code mappings, normalization patterns, and validation queries
- **`README.md`**: This file - overview and usage guide

## Related Files

- `v2/HMUObservation/utils/code_enrichment.py` - Main LOINC mapping dictionary
- `v2/HMUObservation/utils/code_enrichment_native.py` - Code enrichment and normalization logic
- `v2/HMUObservation/transformations/codes_transformation.py` - Code transformation with deduplication
