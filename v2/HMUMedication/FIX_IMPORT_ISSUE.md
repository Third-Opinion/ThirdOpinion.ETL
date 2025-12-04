# Fix: Missing enrichment module in ZIP

## Issue

The error `No module named 'transformations.enrich_medication'` indicates that `enrich_medication.py` is not in the ZIP file.

## Solution

### Step 1: Verify ZIP Creation Script

The `create_deployment_zip.py` script now includes:
```python
"transformations/enrich_*.py",  # Pattern for enrichment modules
```

This should include `enrich_medication.py`.

### Step 2: Recreate the ZIP File

Recreate the ZIP file to ensure all modules are included:

```bash
cd v2
python create_deployment_zip.py HMUMedication
```

### Step 3: Verify ZIP Contents

Check if `enrich_medication.py` is in the ZIP:

```bash
# On Windows (PowerShell):
Expand-Archive -Path HMUMedication_v2.zip -DestinationPath temp_zip -Force
Get-ChildItem -Path temp_zip -Recurse -Filter "*enrich*.py"
Remove-Item -Path temp_zip -Recurse

# Or using Python:
python -c "import zipfile; z=zipfile.ZipFile('HMUMedication_v2.zip'); print([f for f in z.namelist() if 'enrich' in f])"
```

### Step 4: Upload Updated ZIP to S3

```bash
aws s3 cp HMUMedication_v2.zip s3://aws-glue-assets-442042533707-us-east-2/python-libs/HMUMedication_v2.zip
```

### Step 5: Verify Required Files

The ZIP should contain:
- ✅ `transformations/enrich_medication.py`
- ✅ `utils/enrichment.py`
- ✅ `utils/lookup_table.py`

## Current ZIP Location

S3 Location: `s3://aws-glue-assets-442042533707-us-east-2/python-libs/HMUMedication_v2.zip`

## Next Steps

1. Recreate ZIP with updated script
2. Upload to S3
3. Re-run the Glue job
4. The import error should be resolved




