# Database Migrations

## Overview

This directory contains all database schema migrations. All database schema changes go here as timestamped SQL files with Up/Down sections for forward application and rollback support, similar to Entity Framework.

## Migration File Format

### Up/Down Structure

Each migration file contains two sections:

```sql
-- Migration: Create example table
-- Date: 2024-01-15 12:00:00
-- Description: Creates the example table

-- Up Migration
-- ============================================
-- Forward migration: applies the changes
-- ============================================

CREATE TABLE IF NOT EXISTS public.example (
    id VARCHAR(255) NOT NULL,
    name VARCHAR(255)
);

ALTER TABLE public.example ADD PRIMARY KEY (id);

-- Down Migration
-- ============================================
-- Rollback migration: reverses the changes
-- ============================================

DROP TABLE IF EXISTS public.example CASCADE;
```

**Section Markers:**
- `-- Up Migration`: Everything after this marker until `-- Down Migration`
- `-- Down Migration`: Everything after this marker

**Backward Compatibility:** Files without `-- Down Migration` markers are treated as "Up-only" migrations (can be applied but not rolled back).

## File Naming Convention

**Format**: `YYYYMMDD_HHMMSS_description.sql`

- Timestamp ensures correct ordering (applied chronologically)
- Description explains what the migration does
- Example: `20240115_143022_add_stage_rank_to_condition_stages.sql`

## Usage

### Apply All Pending Migrations

```bash
bash v2/scripts/apply_migrations.sh
```

Applies all migrations that haven't been applied yet, in timestamp order.

### Apply Specific Migration

```bash
bash v2/scripts/apply_migrations.sh --migration 20240115_143022_add_stage_rank_column
```

### Check Migration Status

```bash
bash v2/scripts/apply_migrations.sh --status
```

Shows which migrations have been applied and their status.

### Rollback Last Migration

```bash
bash v2/scripts/apply_migrations.sh --rollback
```

Rolls back the most recently applied migration (if it has a Down section).

### Rollback Specific Migration

```bash
bash v2/scripts/apply_migrations.sh --rollback --migration 20240115_143022_add_stage_rank_column
```

**⚠️ Safety Rule**: You can only rollback the **latest applied migration**. Attempting to rollback an older migration will fail to prevent breaking the migration sequence.

## Creating New Migrations

### Using the Helper Script (Recommended)

```bash
bash v2/scripts/create_migration.sh "add_new_column_to_observations"
```

This creates a template file like: `20240115_143022_add_new_column_to_observations.sql`

### Manual Creation

```bash
# Generate timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Create file
touch v2/migrations/${TIMESTAMP}_your_description.sql
```

Then add your Up/Down SQL sections.

## Rollback Safety

### Safety Rule: Reverse Order Only

✅ **You can only rollback the latest applied migration**

This prevents:
- **Breaking the migration sequence** - Creates gaps in history
- **Dependency issues** - Later migrations may depend on earlier changes
- **Inconsistent database state** - Schema won't match migration history

### Example: Attempting Out-of-Order Rollback

```bash
# Current state: Migration 1 → 2 → 3 (all applied)
# Try to rollback Migration 1 (oldest)

bash v2/scripts/apply_migrations.sh --rollback --migration 20240101_000000_migration_1
```

**Result**: ❌ **Will FAIL with error:**

```
❌ Cannot rollback 20240101_000000_migration_1: it is not the latest applied migration
   Latest applied migration: 20240101_000300_migration_3

   ⚠️  Safety Rule: You can only rollback migrations in reverse order
   This prevents breaking the migration sequence and dependency issues.

   To rollback 20240101_000000_migration_1, you must first rollback all newer migrations:
     - 20240101_000300_migration_3
     - 20240101_000200_migration_2
```

### Rollback Strategy: Reverse Order

To rollback to a specific migration, rollback in reverse order:

```bash
# Current state: 1 → 2 → 3 → 4 (all applied)
# Goal: Rollback to migration 1

# Step 1: Rollback latest (4)
bash v2/scripts/apply_migrations.sh --rollback

# Step 2: Rollback next (3)
bash v2/scripts/apply_migrations.sh --rollback

# Step 3: Rollback next (2)
bash v2/scripts/apply_migrations.sh --rollback

# Now at migration 1 ✅
```

## Migration Status Tracking

The `schema_migrations` table tracks:

- **success**: Migration applied successfully
- **failed**: Migration failed during application
- **rolled_back**: Migration was rolled back

## Best Practices

1. ✅ **Always Include Down Migrations**: Write reverse SQL for every Up migration
2. ✅ **Test Rollbacks**: Test rollback SQL before applying to production
3. ✅ **Idempotent Operations**: Use `IF NOT EXISTS` and `IF EXISTS` clauses for safety
4. ✅ **Order Matters**: Migrations are applied in timestamp order, rollbacks happen in reverse
5. ✅ **One Change Per Migration**: Easier to debug and rollback if needed
6. ✅ **Clear Descriptions**: Use descriptive filenames and comments
7. ✅ **Data Safety**: Be careful with rollbacks that drop tables with data - consider data migration

## Example Workflow

### 1. Create Migration

```bash
bash v2/scripts/create_migration.sh "add_stage_rank_column"
```

### 2. Edit Migration File

Edit `20240115_143022_add_stage_rank_column.sql`:

```sql
-- Up Migration
ALTER TABLE public.condition_stages ADD COLUMN stage_rank INTEGER;

-- Down Migration
ALTER TABLE public.condition_stages DROP COLUMN IF EXISTS stage_rank;
```

### 3. Apply Migration

```bash
bash v2/scripts/apply_migrations.sh
```

### 4. If Needed, Rollback

```bash
bash v2/scripts/apply_migrations.sh --rollback
```

## Troubleshooting

### Migration Has No Down Section

```
⚠️  Cannot rollback: migration file doesn't contain '-- Down Migration' section
```

**Solution**: Add a Down migration section to the file with reverse SQL.

### Migration Not Found for Rollback

```
⚠️  Migration not found in applied migrations
```

**Solution**: Check the migration was successfully applied first using `--status`.

### Migration Already Rolled Back

```
⚠️  Migration has already been rolled back
```

**Solution**: The migration is already rolled back - no action needed.

### Cannot Rollback Non-Latest Migration

```
❌ Cannot rollback: it is not the latest applied migration
```

**Solution**: You must rollback migrations in reverse order, starting with the latest one.

## Migration Types

### Table Creation

```sql
-- Up Migration
CREATE TABLE IF NOT EXISTS public.example (
    id VARCHAR(255) NOT NULL,
    name VARCHAR(255)
) DISTKEY (id) SORTKEY (id);

-- Down Migration
DROP TABLE IF EXISTS public.example CASCADE;
```

### Schema Changes

```sql
-- Up Migration
ALTER TABLE public.condition_stages ADD COLUMN stage_rank INTEGER;

-- Down Migration
ALTER TABLE public.condition_stages DROP COLUMN IF EXISTS stage_rank;
```

## Directory Structure

```
v2/migrations/
├── 20240101_000000_create_schema_migrations_table.sql
├── 20240101_000100_create_shared_reference_tables.sql
├── 20240101_000200_create_observation_tables.sql
├── 20240101_000300_create_condition_tables.sql
└── README.md (this file)
```

## Files

- **Migration files**: `YYYYMMDD_HHMMSS_description.sql` - SQL files with Up/Down sections
- **Scripts**: `v2/scripts/apply_migrations.sh` - Apply/rollback migrations
- **Helper**: `v2/scripts/create_migration.sh` - Create new migration templates
