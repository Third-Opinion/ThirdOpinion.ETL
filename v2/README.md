# V2 ETL Jobs - Shared Architecture

## Structure

```
v2/
├── shared/                          # Shared code used by all ETL jobs
│   ├── config.py                    # Shared configuration classes (SparkConfig, DatabaseConfig, ProcessingConfig, ETLConfig)
│   ├── utils/                       # Shared utility functions
│   │   ├── __init__.py
│   │   ├── timestamp_utils.py        # FHIR timestamp parsing
│   │   ├── bookmark_utils.py        # Incremental processing bookmarks
│   │   ├── deduplication_utils.py   # Generic entity deduplication
│   │   └── version_utils.py          # Version comparison utilities
│   └── database/                    # Shared database operations
│       ├── __init__.py
│       └── redshift_operations.py   # Redshift read/write operations
│
├── HMUObservation/                   # Observation ETL job
│   ├── config.py                    # Observation-specific config (imports from shared, adds TableNames)
│   ├── HMUObservation.py            # Main job entry point
│   ├── transformations/             # Observation-specific transformations
│   ├── utils/                       # Observation-specific utilities (code_enrichment, deletion_utils, etc.)
│   └── database/                    # (can be removed - use shared)
│
└── HMUCondition/                    # Condition ETL job
    ├── config.py                    # Condition-specific config (imports from shared, adds TableNames)
    ├── HMUCondition.py              # Main job entry point
    ├── transformations/             # Condition-specific transformations
    └── utils/                       # Condition-specific utilities (deletion_utils, etc.)
```

## Shared Code

### Configuration (`shared/config.py`)
- `SparkConfig`: Spark settings (shuffle partitions, adaptive execution, etc.)
- `DatabaseConfig`: Database connection settings
- `ProcessingConfig`: Processing options (test mode, backdate, sampling, etc.)
- `ETLConfig`: Main configuration container

### Utilities (`shared/utils/`)
- **timestamp_utils.py**: FHIR timestamp parsing (handles multiple formats)
- **bookmark_utils.py**: Incremental processing (get bookmark, filter by bookmark)
- **deduplication_utils.py**: Generic entity deduplication (`deduplicate_entities(df, entity_type)`)
- **version_utils.py**: Version comparison for updates

### Database (`shared/database/`)
- **redshift_operations.py**: 
  - `write_to_redshift_simple()`: Simple append writes
  - `write_to_redshift_versioned()`: Version-aware writes with updates

## Job-Specific Code

Each job (HMUObservation, HMUCondition) has:
- **config.py**: Imports shared config classes, adds job-specific `TableNames` class
- **Transformations**: Job-specific data transformations
- **Job-specific utilities**: 
  - Observation: `code_enrichment.py`, `reference_range_parser.py`, observation-specific `deletion_utils.py`
  - Condition: Condition-specific `deletion_utils.py`

## Import Patterns

### In Job Config Files
```python
from shared.config import SparkConfig, DatabaseConfig, ProcessingConfig, ETLConfig

# Then add job-specific TableNames class
class TableNames:
    # ... job-specific table names
```

### In Job Main Files
```python
from config import TableNames, ObservationETLConfig  # or ConditionETLConfig
from shared.utils import (
    get_bookmark_from_redshift,
    filter_by_bookmark,
    deduplicate_entities
)
from shared.database import write_to_redshift_simple, write_to_redshift_versioned
```

### In Job Utilities
```python
from shared.utils.timestamp_utils import create_timestamp_parser
from shared.config import DatabaseConfig, ProcessingConfig
```

## Migration Status

✅ **Completed:**
- Created `v2/shared/` folder structure
- Created shared config, utils, and database modules
- Copied job files to `v2/HMUObservation/` and `v2/HMUCondition/`
- Updated Observation config.py to use shared config

🔄 **In Progress:**
- Update all imports in job files to use shared utilities
- Remove duplicate utils/database folders from jobs (keep only job-specific code)
- Update Condition config.py to use shared config

## Next Steps

1. Update imports in `HMUObservation/HMUObservation.py` and `HMUCondition/HMUCondition.py`
2. Update imports in transformation files
3. Remove duplicate shared utilities from job folders (keep only job-specific utilities)
4. Test each job to ensure imports work correctly

## Benefits

- **Code Reuse**: Common utilities shared across all ETL jobs
- **Consistency**: Same patterns and implementations across jobs
- **Maintainability**: Fix bugs once, benefit everywhere
- **Easier Testing**: Test shared code once, use everywhere
- **Scalability**: Easy to add new ETL jobs using the same shared foundation

