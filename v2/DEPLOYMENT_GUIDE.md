# V2 ETL Jobs - Deployment Guide

## Overview

The v2 ETL jobs use a shared architecture where common utilities are in `v2/shared/` and job-specific code is in `v2/HMUObservation/` and `v2/HMUCondition/`.

## Structure

```
v2/
├── shared/                    # Shared code (included in all job deployments)
│   ├── config.py
│   ├── utils/
│   └── database/
│
├── HMUObservation/            # Observation ETL job
│   ├── HMUObservation.py      # Main entry point (uploaded separately)
│   ├── config.py              # Job-specific config
│   ├── transformations/       # Job-specific transformations
│   ├── utils/                # Job-specific utilities
│   ├── create_deployment_zip.py
│   └── deploy.ps1
│
└── HMUCondition/             # Condition ETL job
    └── ...
```

## Deployment Process

### Unified Deployment (Recommended)

All jobs use the same deployment script from the `v2/` directory. You can use either PowerShell or Python:

#### Option 1: Python Script (Cross-platform)

```bash
cd v2
python deploy.py HMUObservation
python deploy.py HMUMedication
python deploy.py HMUCondition
```

**With options:**
```bash
python deploy.py HMUObservation --create-job    # Also create/update Glue job
python deploy.py HMUObservation --skip-zip     # Skip zip creation
python deploy.py HMUObservation --skip-upload  # Skip S3 upload
```

#### Option 2: PowerShell Script (Windows)

```powershell
cd v2
.\deploy.ps1 -JobName HMUObservation
.\deploy.ps1 -JobName HMUCondition
```

**With options:**
```powershell
.\deploy.ps1 -JobName HMUObservation -CreateJob    # Also create/update Glue job
.\deploy.ps1 -JobName HMUObservation -SkipZip      # Skip zip creation
.\deploy.ps1 -JobName HMUObservation -SkipUpload   # Skip S3 upload
```

**What the deployment script does:**
1. ✅ Creates deployment zip with job-specific and shared modules
2. ✅ Uploads zip to S3
3. ✅ Uploads main script to S3
4. ✅ Optionally creates/updates Glue job (with `--create-job` or `-CreateJob`)

#### Manual Deployment (If needed)

If you prefer to do it manually:

```bash
# 1. Create zip
cd v2
python create_deployment_zip.py HMUObservation

# 2. Upload zip
aws s3 cp ../HMUObservation_v2.zip s3://aws-glue-assets-442042533707-us-east-2/python-libs/HMUObservation_v2.zip

# 3. Upload main script
aws s3 cp HMUObservation/HMUObservation.py s3://aws-glue-assets-442042533707-us-east-2/scripts/HMUObservation_v2.py
```

## Zip File Structure

When Glue extracts the zip, the structure will be:

```
/
├── HMUObservation.py          # (not in zip, uploaded separately)
├── config.py                 # Job-specific config
├── transformations/
│   ├── main_observation.py
│   └── child_tables.py
├── utils/                    # Job-specific utils
│   ├── code_enrichment.py
│   ├── deletion_utils.py
│   └── reference_range_parser.py
└── shared/                   # Shared modules
    ├── config.py
    ├── utils/
    │   ├── bookmark_utils.py
    │   ├── deduplication_utils.py
    │   ├── timestamp_utils.py
    │   └── version_utils.py
    └── database/
        └── redshift_operations.py
```

## Import Patterns

### In Job Files

```python
# Import from shared
from shared.config import SparkConfig, DatabaseConfig, ProcessingConfig
from shared.utils.bookmark_utils import get_bookmark_from_redshift
from shared.database.redshift_operations import write_to_redshift_versioned

# Import from job-specific modules
from config import TableNames, ObservationETLConfig
from utils.code_enrichment import enrich_observation_code
from transformations.main_observation import transform_main_observation_data
```

### In Shared Modules

```python
# Shared modules can import from other shared modules
from shared.utils.timestamp_utils import create_timestamp_parser
```

## Glue Job Configuration

The Glue job needs:

1. **Main script location:**
   ```
   s3://aws-glue-assets-442042533707-us-east-2/scripts/HMUObservation_v2.py
   ```

2. **Extra Python files:**
   ```
   --extra-py-files s3://aws-glue-assets-442042533707-us-east-2/python-libs/HMUObservation_v2.zip
   ```

3. **Job parameters:**
   ```
   --TABLE_NAME_POSTFIX=_v2
   ```

## Verification

After deployment, verify:

1. **Zip file structure:**
   ```bash
   unzip -l HMUObservation_v2.zip | grep shared
   ```

2. **Files in S3:**
   ```bash
   aws s3 ls s3://aws-glue-assets-442042533707-us-east-2/python-libs/HMUObservation_v2.zip
   aws s3 ls s3://aws-glue-assets-442042533707-us-east-2/scripts/HMUObservation_v2.py
   ```

3. **Test job run:**
   - Run the job in Glue console
   - Check CloudWatch logs for import errors
   - Verify all modules import correctly

## Troubleshooting

### Import Errors

If you see `ModuleNotFoundError: No module named 'shared'`:

1. Verify the zip includes the `shared/` folder:
   ```bash
   unzip -l HMUObservation_v2.zip | grep "shared/"
   ```

2. Check that `--extra-py-files` is set correctly in Glue job

3. Verify the zip file was uploaded to S3

### Missing Files

If files are missing from the zip:

1. Check `create_deployment_zip.py` includes all required files
2. Verify file paths are correct (relative to job directory)
3. Check that shared files exist in `v2/shared/`

## Adding New Jobs

To add a new ETL job:

1. Create job folder: `v2/NewJob/`
2. Add job-specific code (config.py, transformations/, utils/, etc.)
3. Update imports to use `shared.*` modules
4. Deploy using unified script:
   ```powershell
   cd v2
   .\deploy.ps1 -JobName NewJob
   ```
5. No need to create job-specific deployment scripts - the unified script handles everything!

