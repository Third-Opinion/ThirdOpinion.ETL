#!/bin/bash

# Create a new migration file with Up/Down sections
# Usage: bash v2/scripts/create_migration.sh "description_of_migration"
# Example: bash v2/scripts/create_migration.sh "add_index_to_observations"

set -e

MIGRATIONS_DIR="${MIGRATIONS_DIR:-./v2/migrations}"

if [ $# -eq 0 ]; then
    echo "Usage: $0 \"description_of_migration\""
    echo "Example: $0 \"add_index_to_observations\""
    exit 1
fi

DESCRIPTION="$1"

# Generate timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Create migration filename (sanitize description)
SAFE_DESCRIPTION=$(echo "$DESCRIPTION" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_]/_/g' | sed 's/__*/_/g' | sed 's/^_\|_$//g')
MIGRATION_FILE="${MIGRATIONS_DIR}/${TIMESTAMP}_${SAFE_DESCRIPTION}.sql"

# Create directory if it doesn't exist
mkdir -p "$MIGRATIONS_DIR"

# Create migration template
cat > "$MIGRATION_FILE" << EOF
-- Migration: $DESCRIPTION
-- Date: $(date +"%Y-%m-%d %H:%M:%S")
-- Description: [Add description here]

-- Up Migration
-- ============================================
-- Forward migration: applies the changes
-- ============================================

-- TODO: Add your Up migration SQL here
-- Example:
-- CREATE TABLE IF NOT EXISTS public.example (
--     id VARCHAR(255) NOT NULL,
--     name VARCHAR(255)
-- );

-- Down Migration
-- ============================================
-- Rollback migration: reverses the changes
-- ============================================

-- TODO: Add your Down migration SQL here
-- Example:
-- DROP TABLE IF EXISTS public.example CASCADE;
EOF

echo "✅ Created migration file: $MIGRATION_FILE"
echo ""
echo "Next steps:"
echo "1. Edit the migration file and add your Up/Down SQL"
echo "2. Test the migration: bash v2/scripts/apply_migrations.sh --migration $(basename "$MIGRATION_FILE" .sql)"
echo "3. Apply all migrations: bash v2/scripts/apply_migrations.sh"

