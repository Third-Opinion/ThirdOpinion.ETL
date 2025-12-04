# Medication Enrichment Workflow

## Architecture

The medication enrichment has been split into two parts:

### 1. **Local API Enrichment Script** (This Repository)
- Runs locally on your machine (with internet access)
- Reads medications from Redshift
- Calls RxNav/Comprehend Medical APIs
- Writes results to `medication_code_lookup` table in Redshift

### 2. **Glue ETL Job** (AWS Glue)
- Runs in AWS Glue environment (VPC, no internet)
- Reads from `medication_code_lookup` table (fast, no API calls)
- Enriches medications automatically using lookup cache

## Workflow

```
┌─────────────────────────────────────────────────────────┐
│  LOCAL ENRICHMENT (enrich_medications_locally.py)       │
├─────────────────────────────────────────────────────────┤
│  1. Read medications from Redshift (missing codes)      │
│  2. Call RxNav/Comprehend Medical APIs                  │
│  3. Write results to medication_code_lookup table       │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│  REDSHIFT: medication_code_lookup table                 │
├─────────────────────────────────────────────────────────┤
│  Stores enrichment results for reuse                    │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│  GLUE ETL JOB (HMUMedication.py)                        │
├─────────────────────────────────────────────────────────┤
│  1. Read lookup table from Redshift (fast!)             │
│  2. Enrich medications using lookup cache               │
│  3. Write enriched medications to medications table     │
└─────────────────────────────────────────────────────────┘
```

## Running the Local Script

### Setup Environment Variables

```bash
export REDSHIFT_CLUSTER_ID="prod-redshift-main-ue2"
export REDSHIFT_DATABASE="dev"
export REDSHIFT_SECRET_ARN="arn:aws:secretsmanager:us-east-2:442042533707:secret:redshift!prod-redshift-main-ue2-awsuser-yp5Lq4"
export AWS_REGION="us-east-2"
```

### Run Script

```bash
cd v2/HMUMedication
python enrich_medications_locally.py --limit 1000 --mode hybrid
```

## Benefits

1. **No Network Issues**: Local script has internet, Glue job doesn't need it
2. **Fast Glue Jobs**: Uses lookup table cache (no API calls)
3. **Reusable**: Lookup table stores results for future runs
4. **Flexible**: Can enrich in batches, monitor progress

## Next Steps

1. ✅ API enrichment removed from Glue job
2. ✅ Local script created
3. **Run local script** to populate lookup table
4. **Run Glue ETL job** - it will use the lookup table automatically



