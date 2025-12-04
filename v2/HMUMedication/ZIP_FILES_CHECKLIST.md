# ZIP File Contents Checklist for HMUMedication

## Required Files in ZIP

The ZIP file must include all these modules for the ETL job to work:

### Job-Specific Files:
- ✅ `config.py` - Job configuration
- ✅ `HMUMedication.py` - Main job script (uploaded separately to scripts/)
- ✅ `__init__.py` - Package marker

### Transformations Module:
- ✅ `transformations/__init__.py`
- ✅ `transformations/main_medication.py` - Main transformation
- ✅ `transformations/child_tables.py` - Child table transformations
- ✅ **`transformations/enrich_medication.py`** - **ENRICHMENT MODULE (REQUIRED!)**

### Utils Module:
- ✅ `utils/__init__.py`
- ✅ `utils/enrichment.py` - Enrichment utilities
- ✅ **`utils/lookup_table.py`** - Lookup table management

### Shared Module:
- ✅ `shared/__init__.py`
- ✅ `shared/config.py`
- ✅ `shared/utils/__init__.py`
- ✅ `shared/utils/bookmark_utils.py`
- ✅ `shared/utils/deduplication_utils.py`
- ✅ `shared/utils/timestamp_utils.py`
- ✅ `shared/utils/version_utils.py`
- ✅ `shared/database/__init__.py`
- ✅ `shared/database/redshift_operations.py`

## Current Status

✅ The `create_deployment_zip.py` script now includes:
- `transformations/enrich_*.py` pattern (includes `enrich_medication.py`)
- All utils files via glob pattern (includes `lookup_table.py`)

## To Recreate ZIP

```bash
cd v2
python create_deployment_zip.py HMUMedication
```

This will create `HMUMedication_v2.zip` in the project root with all required modules.

## Verify ZIP Contents

After creating the ZIP, verify it includes:
```bash
# Check if enrich_medication.py is in the ZIP
unzip -l HMUMedication_v2.zip | grep enrich_medication.py

# Check if lookup_table.py is in the ZIP  
unzip -l HMUMedication_v2.zip | grep lookup_table.py
```

## Upload to S3

```bash
aws s3 cp HMUMedication_v2.zip s3://aws-glue-assets-442042533707-us-east-2/python-libs/HMUMedication_v2.zip
```




