# Medication Code Enrichment for HMUMedication ETL v2

## Overview

This document describes the medication code enrichment feature added to the v2 HMUMedication ETL job. Enrichment adds missing RxNorm codes to medications using external APIs, following the strategy in `Medication_Code_Enrichment_Strategy.md`.

## Architecture

```
HealthLake (FHIR Medication Resources) - READ-ONLY
    ↓
HMUMedication.py ETL Job
    ├── Read from Iceberg
    ├── Filter by bookmark
    ├── Deduplicate
    ├── Transform
    ├── [NEW] Enrich missing codes (optional)
    │   ├── Check Redshift lookup table (cache)
    │   ├── Call RxNav API (if not cached)
    │   └── Optionally: Call Comprehend Medical (fallback)
    └── Write to Redshift medications table
    ↓
Redshift: medications table (with enriched codes)
    ↓ (JOIN in fact_fhir_medication_requests_view_v1)
All medication requests automatically get enriched codes!
```

## Files Added/Modified

### New Files:
1. **`utils/enrichment.py`** - Enrichment utility functions
   - `normalize_medication_name()` - Normalizes medication names for lookup
   - `lookup_rxnorm_code()` - RxNav API integration with rate limiting
   - `enrich_with_comprehend_medical()` - Comprehend Medical integration
   - `enrich_medication_code()` - Main enrichment function with fallback chain
   - `get_lookup_table_schema()` - SQL schema for lookup table

2. **`utils/lookup_table.py`** - Lookup table management
   - `ensure_lookup_table_exists()` - Creates lookup table if it doesn't exist
   - `check_lookup_table_exists()` - Checks if lookup table exists

3. **`transformations/enrich_medication.py`** - Enrichment transformation
   - `enrich_medications_with_codes()` - DataFrame-level enrichment function

4. **`MEDICATION_ENRICHMENT_README.md`** - This documentation

### Modified Files:
1. **`config.py`** - Added `MedicationEnrichmentConfig` class
2. **`transformations/main_medication.py`** - Added `transform_and_enrich_medication_data()` function
3. **`HMUMedication.py`** - Integrated enrichment into main ETL flow

## Configuration

Enrichment is **ENABLED by default**. To disable:

### Environment Variables:
```bash
ENABLE_MEDICATION_ENRICHMENT=true   # Default: true (enabled)
MEDICATION_ENRICHMENT_MODE=hybrid   # Options: "rxnav_only", "comprehend_only", "hybrid"
USE_MEDICATION_LOOKUP_TABLE=true    # Use Redshift lookup table for caching
```

To disable enrichment:
```bash
ENABLE_MEDICATION_ENRICHMENT=false
```

### Job Arguments (Glue):
```json
{
  "--ENABLE_MEDICATION_ENRICHMENT": "true",
  "--MEDICATION_ENRICHMENT_MODE": "hybrid",
  "--USE_MEDICATION_LOOKUP_TABLE": "true"
}
```

## Enrichment Strategy

### Multi-Tier Fallback Chain:

1. **Redshift Lookup Table** (Tier 1 - Fastest, Free)
   - Checks `public.medication_code_lookup` table first
   - Caches previous enrichment results
   - No API calls needed

2. **RxNav REST API** (Tier 2 - Free, Rate Limited)
   - Free NLM service
   - Rate limit: 20 requests/second per IP
   - Good for standard medication names
   - Endpoints:
     - `/drugs.json?name={name}` - Exact match
     - `/approximateTerm.json?term={name}` - Fuzzy match

3. **Amazon Comprehend Medical** (Tier 3 - Paid, Better Accuracy)
   - AWS service: ~$0.001 per API call
   - Better for complex medication descriptions
   - Higher accuracy for clinical text
   - Requires AWS credentials (IAM role for Glue)

## Lookup Table

The lookup table stores enrichment results for future use. **The table is automatically created** when enrichment runs if it doesn't exist.

### Table Schema:

```sql
CREATE TABLE IF NOT EXISTS public.medication_code_lookup (
    normalized_name VARCHAR(500) NOT NULL,
    rxnorm_code VARCHAR(50),
    rxnorm_system VARCHAR(200) DEFAULT 'http://www.nlm.nih.gov/research/umls/rxnorm',
    medication_name VARCHAR(500),
    confidence_score DECIMAL(3,2),
    enrichment_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
DISTSTYLE KEY
DISTKEY (normalized_name)
SORTKEY (normalized_name);
```

### How It's Created:

1. **Automatic Creation**: When enrichment is enabled, the ETL job automatically:
   - Checks if `medication_code_lookup` table exists (Step 3.5)
   - Creates it if missing using `CREATE TABLE IF NOT EXISTS` via preactions
   - Uses a dummy write operation to trigger the CREATE TABLE statement

2. **Creation Method**:
   - Uses `ensure_lookup_table_exists()` function from `utils/lookup_table.py`
   - Executes CREATE TABLE SQL via Glue preactions mechanism
   - Safe to run multiple times (uses IF NOT EXISTS)

3. **No Manual Setup Required**: The table is created automatically on first enrichment run

The lookup table is automatically checked before making API calls, reducing API usage by ~70-80% after initial enrichment runs.

## Usage

### Enable Enrichment in ETL Job:

1. Set environment variable: `ENABLE_MEDICATION_ENRICHMENT=true`
2. Run the ETL job as normal
3. Enrichment happens during transformation step
4. Enriched codes are written to Redshift `medications` table
5. All medication requests automatically benefit via JOIN

### Recommended Approach:

**Current Setup (Enrichment Enabled by Default):**
- Enrichment runs during main ETL for new medications (using bookmarks)
- Lookup table caches results to minimize API calls
- Rate limiting built-in to respect RxNav API limits

**For Production:**
- Monitor enrichment success rates and costs
- Consider separate batch job for backfilling historical data if needed
- Adjust enrichment mode based on cost/accuracy requirements

## Impact on Medication Requests

When medications are enriched in `HMUMedication.py`:

1. ✅ Medications table gets enriched codes (`primary_code`, `primary_system`)
2. ✅ `fact_fhir_medication_requests_view_v1` automatically shows enriched codes
   - Via JOIN: `mr.medication_id = m.medication_id`
   - Medication codes come from: `m.primary_code`, `m.primary_system`
3. ✅ No changes needed to MedicationRequest ETL
4. ✅ All downstream queries benefit immediately

## Rate Limiting

**RxNav API: 20 requests/second per IP address**

- The enrichment code includes rate limiting
- For large batches, consider:
  - Using lookup table first (reduces API calls)
  - Processing in smaller batches
  - Using a separate enrichment job with controlled parallelism

## Cost Estimates

### RxNav API: **FREE**
- No cost for API calls
- Rate limited to 20 req/sec

### Comprehend Medical: **~$0.001 per call**
- For ~2,868 unique medications: ~$2.87
- Only used if RxNav fails (fallback)

### AWS Glue: **Minimal**
- Adds ~5-10% processing time if enrichment enabled
- Use separate batch job to avoid slowing main ETL

## Testing

To test enrichment:

1. Set `ENABLE_MEDICATION_ENRICHMENT=true`
2. Use `USE_SAMPLE=true` and `SAMPLE_SIZE=100` for testing
3. Check CloudWatch logs for enrichment activity
4. Verify enriched codes in Redshift `medications` table

## Next Steps

1. ✅ Enrichment infrastructure added
2. ⏭️ Create lookup table in Redshift (first enrichment run will attempt this)
3. ⏭️ Run initial enrichment on unique medication names (separate job recommended)
4. ⏭️ Monitor enrichment success rates and costs
5. ⏭️ Integrate into production ETL pipeline when ready

## References

- `Medication_Code_Enrichment_Strategy.md` - Full enrichment strategy document
- [RxNav API Documentation](https://lhncbc.nlm.nih.gov/RxNav/)
- [Amazon Comprehend Medical](https://docs.aws.amazon.com/comprehend-medical/)

