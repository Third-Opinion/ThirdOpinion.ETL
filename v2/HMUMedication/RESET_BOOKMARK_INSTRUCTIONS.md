# Reset Bookmark to Fix 0 Records Issue

## Problem

When the bookmark timestamp is more recent than all data in Iceberg, it filters out all records, resulting in 0 records processed.

## Solution

Truncate the `medications` table to reset the bookmark. This will cause the next ETL run to process ALL records (full load).

## Quick Fix

Run this SQL in Redshift Query Editor:

```sql
TRUNCATE TABLE public.medications;
TRUNCATE TABLE public.medication_identifiers;
```

Or use the provided script: `reset_bookmark.sql`

## After Truncating

1. **Next ETL Run** will process ALL records from Iceberg (full load)
2. **Bookmark will be reset** to NULL
3. **Processing will take longer** as it processes all historical data

## Verify Bookmark Reset

After truncating, verify the table is empty:

```sql
SELECT 
    COUNT(*) as record_count,
    MAX(meta_last_updated) as max_timestamp
FROM public.medications;
```

Both should return NULL/0, indicating bookmark is reset.




