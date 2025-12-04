# Medication Code Systems - Quick Summary

## Current Status ✅

Your system currently enriches medications with **RxNorm codes only** via:
- **Lookup Table**: `public.medication_code_lookup` (caches enrichment results)
- **Enrichment APIs**: RxNav API (free) + Amazon Comprehend Medical (paid fallback)
- **Storage**: RxNorm codes stored in `medications.primary_code` and `medications.primary_system`

## What Each Code System Provides

| Code System | Drug Level | Ingredient Level | Brand Level | Package Level | Status |
|------------|:----------:|:----------------:|:-----------:|:-------------:|:------:|
| **RxNorm** | ✅ | ✅ | ✅ | ❌ | ✅ **Implemented** |
| **SNOMED CT** | ✅ | ✅ | ✅ | ❌ | ❌ Not implemented |
| **NDC** | ❌ | ❌ | ❌ | ✅ | 📋 In identifiers table |
| **UNII** | ❌ | ✅ | ❌ | ❌ | ❌ Not implemented |
| **MED-RT** | ✅ | ❌ | ❌ | ❌ | ❌ Not implemented |

## Quick Queries

### Check Enrichment Coverage
```sql
-- See how many medications have RxNorm codes
SELECT 
    COUNT(*) AS total,
    COUNT(CASE WHEN primary_system = 'http://www.nlm.nih.gov/research/umls/rxnorm' 
          THEN 1 END) AS has_rxnorm,
    ROUND(100.0 * COUNT(CASE WHEN primary_system = 'http://www.nlm.nih.gov/research/umls/rxnorm' 
          THEN 1 END) / COUNT(*), 2) AS pct_enriched
FROM public.medications;
```

### Find All Code Systems Present
```sql
-- See what code systems are already in your data (may include SNOMED, NDC, etc.)
SELECT DISTINCT
    JSON_EXTRACT_PATH_TEXT(codes.code_entry, 'system') AS code_system
FROM public.medications m,
LATERAL FLATTEN(INPUT => JSON_PARSE(m.code)) AS codes(code_entry)
WHERE m.code IS NOT NULL AND m.code != 'null';
```

### View Enrichment Lookup Table
```sql
-- Check enrichment cache
SELECT * FROM public.medication_code_lookup 
WHERE rxnorm_code IS NOT NULL 
ORDER BY updated_at DESC 
LIMIT 100;
```

### Find Medications Missing Codes
```sql
-- Medications that need enrichment
SELECT medication_id, primary_text, primary_code, primary_system
FROM public.medications
WHERE primary_code IS NULL 
   OR primary_system != 'http://www.nlm.nih.gov/research/umls/rxnorm'
LIMIT 100;
```

## For Clinical Trials: Recommendations

### ✅ **RxNorm is Sufficient for Most Use Cases**
- Provides drug-level and ingredient-level identification
- Free API access via RxNav
- Good for standardizing medication names across sites

### 📋 **NDC Codes Already Available**
- Check `medication_identifiers` table for NDC codes
- No additional enrichment needed - just query:
  ```sql
  SELECT m.*, mi.identifier_value AS ndc_code
  FROM medications m
  JOIN medication_identifiers mi ON m.medication_id = mi.medication_id
  WHERE mi.identifier_system LIKE '%ndc%';
  ```

### 🔍 **Other Code Systems**
- **SNOMED CT**: Only needed if you have international trials or need detailed medication representations
- **UNII**: Only needed for ingredient-level analysis (can map from RxNorm if needed)
- **MED-RT**: Only needed for drug class hierarchies

## Next Steps

1. **✅ Run Queries**: Use `QUERY_MEDICATION_CODES.sql` to explore your data
2. **✅ Check Coverage**: See how many medications have RxNorm codes
3. **✅ Verify NDC**: Check if NDC codes are in `medication_identifiers` table
4. **⏭️ Consider Expansion**: Only if you need SNOMED CT, UNII, or MED-RT for specific use cases

## Full Documentation

See `MEDICATION_CODE_SYSTEMS_GUIDE.md` for detailed information about each code system and how to use them.

