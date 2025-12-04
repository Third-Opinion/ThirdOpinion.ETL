# Truncate HMUMedication Tables

## Purpose

This script truncates all tables related to the HMUMedication ETL job to reset bookmarks and force a full re-import of all data.

## Tables Affected

1. **`public.medications`** - Main medications table (truncating this resets the bookmark)
2. **`public.medication_identifiers`** - Child table for medication identifiers
3. **`public.medication_code_lookup`** - Optional enrichment lookup cache (commented out by default)

## How Bookmarks Work

The bookmark system uses the maximum `meta_last_updated` timestamp from the `medications` table:
- If the table is empty → No bookmark → Full load (all records)
- If the table has data → Bookmark = max timestamp → Only process newer records

By truncating the `medications` table, the bookmark will be reset to `None`, causing the next ETL run to process ALL records from Iceberg.

## Usage

### Option 1: Run SQL Script in Redshift Query Editor

1. Open Redshift Query Editor
2. Copy and paste the contents of `truncate_all_tables.sql`
3. Execute the script
4. Verify all tables are empty (query will show 0 records)

### Option 2: Run via AWS Glue/CLI

```bash
# Using AWS CLI to execute SQL via Redshift Data API
aws redshift-data execute-statement \
  --cluster-identifier <your-cluster-id> \
  --database <your-database> \
  --db-user <your-user> \
  --sql "$(cat v2/HMUMedication/truncate_all_tables.sql)"
```

### Option 3: Using MCP AWS Tools

If you have AWS Tools MCP configured, you can execute the SQL directly.

## After Truncating

1. **Next ETL Run** - Will process ALL records from Iceberg (full load)
2. **Processing Time** - Will take longer as it processes all historical data
3. **Bookmark** - Will be re-established after the first successful run

## Important Notes

⚠️ **WARNING**: Truncating these tables will:
- Delete ALL medication data currently in Redshift
- Reset the bookmark (next run processes everything)
- Clear identifier data
- Optionally clear enrichment cache (if uncommented)

✅ **Safe to Run**:
- If you want to re-process all data
- If you've made schema changes
- If you want to test full load performance
- Before initial production deployment

## Verification

After running the truncate script, verify tables are empty:

```sql
SELECT 'medications' as table_name, COUNT(*) as count FROM public.medications
UNION ALL
SELECT 'medication_identifiers', COUNT(*) FROM public.medication_identifiers;
```

Both should return `0` records.

## Next Steps

1. Run the truncate script
2. Verify tables are empty
3. Run the HMUMedication ETL job
4. Monitor CloudWatch logs for full load progress
5. Verify data is loaded correctly




