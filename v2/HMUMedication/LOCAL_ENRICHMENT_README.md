# Local Medication Enrichment Script

## Overview

This script runs **locally** (with internet access) to enrich medications via API calls and write results to Redshift lookup table.

**Why Local?**
- Glue jobs run in VPC without internet access
- Local machine has internet for API calls (RxNav, Comprehend Medical)
- Results are written to Redshift lookup table
- Glue job then uses the lookup table (fast, no API calls needed)

## Setup

### 1. Install Dependencies

```bash
pip install boto3 requests
```

### 2. Configure Redshift Connection

Set environment variables for Redshift cluster connection:

```bash
export REDSHIFT_CLUSTER_ID="prod-redshift-main-ue2"
export REDSHIFT_DATABASE="dev"
export REDSHIFT_SECRET_ARN="arn:aws:secretsmanager:us-east-2:442042533707:secret:redshift!prod-redshift-main-ue2-awsuser-yp5Lq4"
export AWS_REGION="us-east-2"
```

Or for serverless:

```bash
export REDSHIFT_WORKGROUP="to-prd-redshift-serverless"
export REDSHIFT_DATABASE="dev"
export AWS_REGION="us-east-2"
```

### 3. Configure AWS Credentials

Make sure your AWS credentials are configured:

```bash
aws configure
# OR
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

## Usage

### Basic Usage

```bash
cd v2/HMUMedication
python enrich_medications_locally.py
```

### With Options

```bash
# Enrich up to 500 medications
python enrich_medications_locally.py --limit 500

# Use RxNav only (no Comprehend Medical)
python enrich_medications_locally.py --mode rxnav_only

# Use Comprehend Medical only
python enrich_medications_locally.py --mode comprehend_only

# Hybrid mode (try RxNav first, then Comprehend Medical)
python enrich_medications_locally.py --mode hybrid
```

## How It Works

1. **Reads medications from Redshift** that need enrichment (missing RxNorm codes)
2. **Calls APIs** (RxNav/Comprehend Medical) to enrich medications
3. **Writes results to lookup table** in Redshift (`medication_code_lookup`)
4. **Next Glue ETL run** will use the enriched lookup table (fast!)

## Workflow

```
1. Run local enrichment script
   ↓
2. Script reads medications from Redshift
   ↓
3. Calls APIs to enrich (RxNav/Comprehend Medical)
   ↓
4. Writes to medication_code_lookup table
   ↓
5. Run Glue ETL job
   ↓
6. Glue job uses lookup table (fast, no API calls)
   ↓
7. Medications are enriched automatically!
```

## Environment Variables

### Required (Cluster Connection)
- `REDSHIFT_CLUSTER_ID`: Redshift cluster identifier
- `REDSHIFT_SECRET_ARN`: AWS Secrets Manager ARN with credentials
- `REDSHIFT_DATABASE`: Database name (default: `dev`)

### Optional
- `AWS_REGION`: AWS region (default: `us-east-2`)
- `REDSHIFT_WORKGROUP`: For serverless (alternative to cluster)

## Rate Limiting

- **RxNav API**: 20 requests/second (automatically handled)
- **Comprehend Medical**: AWS service limits apply
- Script processes in batches to respect limits

## Output

The script will:
- Log progress every 50 successful enrichments
- Write results to `medication_code_lookup` table
- Show summary of enriched medications

## Next Steps

After running the local script:

1. ✅ Enrichment results are in Redshift lookup table
2. **Run Glue ETL job** - it will automatically use the lookup table
3. Medications will be enriched from the lookup table (fast!)

## Troubleshooting

### "Network is unreachable"
- ✅ This is expected - you're running locally now!
- Make sure you have internet access on your local machine

### "Redshift configuration not found"
- Set the required environment variables (see Setup section)

### "Could not import enrichment utilities"
- Make sure you're running from `v2/HMUMedication` directory
- Check that `utils/enrichment.py` exists

### "Query failed"
- Check AWS credentials are configured
- Verify Secret ARN is correct
- Check IAM permissions for Redshift Data API



