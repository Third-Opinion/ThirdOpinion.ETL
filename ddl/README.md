# Redshift Table DDL Files

This directory contains externalized DDL (Data Definition Language) files for all Redshift tables used by the Glue ETL jobs.

## Overview

Previously, table creation logic was embedded directly in the Glue job Python files. This approach has been refactored to:

1. **Externalize DDL**: All `CREATE TABLE` statements are now in separate `.sql` files in this directory
2. **Centralized Management**: Table schemas are managed in one location for easier maintenance
3. **Pre-deployment**: Tables are created before Glue jobs run, not during job execution
4. **Schema Validation**: Glue jobs check table existence and log schemas at startup

## Directory Structure

```
ddl/
├── README.md                              # This file
├── patients.sql                           # Main patient table
├── patient_names.sql                      # Patient names (1-to-many)
├── patient_addresses.sql                  # Patient addresses (1-to-many)
├── practitioners.sql                      # Main practitioner table
├── practitioner_names.sql                 # Practitioner names (1-to-many)
├── ...                                    # Additional tables
└── [78 total table DDL files]
```

## Table Organization by Source Job

| Glue Job | Table Count | Main Table(s) |
|----------|-------------|---------------|
| HMUPatient | 8 | patients |
| HMUPractitioner | 4 | practitioners |
| HMUObservation | 10 | observations |
| HMUCondition | 8 | conditions |
| HMUEncounter | 6 | encounters |
| HMUDiagnosticReport | 7 | diagnostic_reports |
| HMUDocumentReference | 5 | document_references |
| HMUMedicationRequest | 5 | medication_requests |
| HMUMedicationDispense | 5 | medication_dispenses |
| HMUCarePlan | 5 | care_plans |
| HMUProcedure | 3 | procedures |
| HMUMedication | 2 | medications |
| HMUAllergyIntolerance | 2 | allergy_intolerance |
| **Total** | **78 tables** | |

## Deployment

### Deploy All Tables

Use the provided deployment script to create all tables in Redshift:

```bash
# Deploy all DDL files to Redshift
./scripts/deploy_ddl_to_redshift.sh
```

The script will:
- ✅ Read each SQL file from `./ddl/`
- ✅ Execute CREATE TABLE statements via Redshift Data API
- ✅ Track deployment status for each table
- ✅ Provide summary of successful/failed deployments

### Deploy Individual Table

To deploy a single table manually:

```bash
AWS_PROFILE=to-prd-admin aws redshift-data execute-statement \
  --workgroup-name to-prd-redshift-serverless \
  --database dev \
  --sql "$(cat ddl/patients.sql)" \
  --region us-east-2
```

## Glue Job Integration

### Table Existence Checking

All Glue jobs now:

1. **Import table utilities** at startup:
```python
import sys
sys.path.append('./glue_utils')
from table_utils import check_all_tables
```

2. **Check table existence** before processing:
```python
table_names = ["patients", "patient_names", "patient_addresses", ...]
check_all_tables(glueContext, table_names, REDSHIFT_CONNECTION, S3_TEMP_DIR)
```

3. **Log table schemas** for debugging:
```
🔍 Checking table: public.patients
✅ Table 'public.patients' EXISTS

📋 Table Schema:
   Column Name                              Data Type
   ---------------------------------------- --------------------
   patient_id                               StringType
   active                                   BooleanType
   gender                                   StringType
   ...
```

### Write Operations

Glue jobs now assume tables exist:

```python
# OLD: CREATE TABLE in preactions
preactions = create_patients_table_sql() + "; TRUNCATE TABLE patients;"

# NEW: Tables assumed to exist, only data operations in preactions
preactions = "TRUNCATE TABLE patients;" if is_initial_load else ""
```

## Maintenance

### Adding a New Table

1. Create DDL file: `ddl/new_table_name.sql`
2. Include CREATE TABLE statement with proper schema
3. Deploy: `./scripts/deploy_ddl_to_redshift.sh`
4. Update Glue job to include table in check list

### Modifying Existing Table

1. Update the `.sql` file in `ddl/` directory
2. **Important**: For schema changes, consider migration strategy:
   - Add new columns with ALTER TABLE
   - Create new table version if breaking changes
   - Migrate data as needed
3. Redeploy DDL (will use `IF NOT EXISTS` so table won't be recreated)

### Regenerating DDL from Glue Jobs

If Glue jobs are updated with new table schemas:

```bash
# Extract all CREATE TABLE statements from Glue jobs
python3 scripts/extract_ddl_from_glue_jobs.py

# This will:
# ✅ Scan all HMU*/HMU*.py files
# ✅ Extract CREATE TABLE statements
# ✅ Generate individual .sql files
# ✅ Overwrite existing files in ddl/
```

## File Naming Convention

- Table names use snake_case: `patient_names.sql`
- Main entity tables: `{entity}.sql` (e.g., `patients.sql`)
- Related tables: `{entity}_{detail}.sql` (e.g., `patient_names.sql`)

## SQL File Format

Each `.sql` file contains:

```sql
-- Table: table_name
-- Source: HMUJobName.py (function_name)
-- Extracted: extract_ddl_from_glue_jobs.py

CREATE TABLE IF NOT EXISTS public.table_name (
    column1 VARCHAR(255) NOT NULL,
    column2 TIMESTAMP,
    ...
) DISTKEY (column1) SORTKEY (column1);
```

## Benefits of Externalized DDL

✅ **Version Control**: Table schemas tracked in git separately from ETL logic
✅ **Schema Documentation**: Easy to review all table structures in one place
✅ **Deployment Control**: Tables can be created/modified independently of Glue jobs
✅ **Reduced Job Complexity**: Glue jobs focus on data transformation, not table management
✅ **Debugging**: Table schemas logged at job startup for troubleshooting
✅ **Testing**: Can recreate database schema without running Glue jobs

## Troubleshooting

### Table Not Found Error

If a Glue job reports table doesn't exist:

1. Check if table was deployed: `./scripts/deploy_ddl_to_redshift.sh`
2. Verify table in Redshift:
```bash
AWS_PROFILE=to-prd-admin aws redshift-data execute-statement \
  --workgroup-name to-prd-redshift-serverless \
  --database dev \
  --sql "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name='your_table_name';" \
  --region us-east-2
```

### Schema Mismatch

If data types don't match between Glue job and table:

1. Check DDL file: `cat ddl/table_name.sql`
2. Compare with Glue job transformation logic
3. Update DDL file if needed
4. Redeploy table (may require DROP TABLE first for schema changes)

## Related Files

- `glue_utils/table_utils.py`: Table checking utility functions
- `scripts/extract_ddl_from_glue_jobs.py`: DDL extraction script
- `scripts/update_glue_jobs_for_external_ddl.py`: Glue job update script
- `scripts/deploy_ddl_to_redshift.sh`: DDL deployment script
