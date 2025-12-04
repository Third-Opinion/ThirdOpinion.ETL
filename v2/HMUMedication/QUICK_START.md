# Quick Start: Local Medication Enrichment

## Setup (One Time)

### 1. Install Dependencies

```bash
pip install boto3 requests
```

### 2. Configure AWS Credentials

```bash
aws configure
# OR set environment variables
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

## Run Enrichment

### Default Configuration (Already Set)

The script is pre-configured with your Redshift cluster:

- **Cluster**: `prod-redshift-main-ue2`
- **Database**: `dev`
- **Secret ARN**: Already configured
- **Region**: `us-east-2`

### Run Script

```bash
cd v2/HMUMedication
python enrich_medications_locally.py
```

### With Options

```bash
# Enrich 500 medications
python enrich_medications_locally.py --limit 500

# Use RxNav only
python enrich_medications_locally.py --mode rxnav_only

# Use Comprehend Medical only
python enrich_medications_locally.py --mode comprehend_only
```

## What Happens

1. ✅ Reads medications from Redshift (missing codes)
2. ✅ Calls APIs to enrich (RxNav/Comprehend Medical)
3. ✅ Writes results to `medication_code_lookup` table
4. ✅ Next Glue ETL run uses the lookup table automatically!

## Next Steps

After running the local script:

1. ✅ Results are in `medication_code_lookup` table
2. **Run Glue ETL job** - it will use the lookup table automatically
3. Medications will be enriched from cache (fast!)

## Troubleshooting

**"Could not import enrichment utilities"**
- Make sure you're in `v2/HMUMedication` directory

**"Query failed"**
- Check AWS credentials are configured
- Verify you have access to the Redshift cluster

**"Network is unreachable"**
- This is fine! The script runs locally with internet access



