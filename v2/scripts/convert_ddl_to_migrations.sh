#!/bin/bash

# Convert existing DDL files from v2/ddl/ to migration format in v2/migrations/
# This script helps migrate from the old "base tables" approach to migrations-only

set -e

DDL_DIR="${DDL_DIR:-./v2/ddl}"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-./v2/migrations}"
BASE_TIMESTAMP="${BASE_TIMESTAMP:-20240101}"
INTERVAL_SECONDS=100  # 100 seconds between migrations

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}🔄 Converting DDL Files to Migrations${NC}"
echo -e "${BLUE}========================================================================${NC}"
echo ""

# Check directories
if [ ! -d "$DDL_DIR" ]; then
    echo -e "${RED}❌ DDL directory not found: $DDL_DIR${NC}"
    exit 1
fi

if [ ! -d "$MIGRATIONS_DIR" ]; then
    echo -e "${YELLOW}Creating migrations directory: $MIGRATIONS_DIR${NC}"
    mkdir -p "$MIGRATIONS_DIR"
fi

# Get all SQL files except schema_migrations and markdown files
DDL_FILES=($(find "$DDL_DIR" -name "*.sql" -type f ! -name "schema_migrations.sql" | sort))

if [ ${#DDL_FILES[@]} -eq 0 ]; then
    echo -e "${YELLOW}⚠️  No DDL files found in $DDL_DIR${NC}"
    exit 0
fi

echo -e "${BLUE}Found ${#DDL_FILES[@]} DDL file(s) to convert${NC}"
echo ""

# Start timestamp
TIMESTAMP_OFFSET=0
MIGRATION_COUNT=0

# Process each DDL file
for ddl_file in "${DDL_FILES[@]}"; do
    TABLE_NAME=$(basename "$ddl_file" .sql)
    
    # Skip if it's the migrations table (will be first migration)
    if [ "$TABLE_NAME" = "schema_migrations" ]; then
        echo -e "${YELLOW}   ⏭️  Skipping schema_migrations.sql (will be first migration)${NC}"
        continue
    fi
    
    # Calculate timestamp
    HOURS=$((TIMESTAMP_OFFSET / 3600))
    MINUTES=$(((TIMESTAMP_OFFSET % 3600) / 60))
    SECONDS=$((TIMESTAMP_OFFSET % 60))
    
    TIMESTAMP=$(printf "%s_%02d%02d%02d" "$BASE_TIMESTAMP" "$HOURS" "$MINUTES" "$SECONDS")
    MIGRATION_FILE="$MIGRATIONS_DIR/${TIMESTAMP}_create_${TABLE_NAME}_table.sql"
    
    # Check if migration already exists
    if [ -f "$MIGRATION_FILE" ]; then
        echo -e "${YELLOW}   ⏭️  Skipping $TABLE_NAME (migration already exists)${NC}"
        ((TIMESTAMP_OFFSET += INTERVAL_SECONDS))
        continue
    fi
    
    # Read DDL content
    DDL_CONTENT=$(cat "$ddl_file")
    
    # Create migration file
    cat > "$MIGRATION_FILE" <<EOF
-- Migration: Create ${TABLE_NAME} table
-- Date: $(date -d "$BASE_TIMESTAMP" +"%Y-%m-%d" 2>/dev/null || echo "$BASE_TIMESTAMP") $(printf "%02d:%02d:%02d" "$HOURS" "$MINUTES" "$SECONDS")
-- Converted from: $ddl_file
-- 
-- This migration creates the ${TABLE_NAME} table.
-- Converted from base DDL file during migration system setup.

${DDL_CONTENT}
EOF

    echo -e "${GREEN}   ✓ Created: $(basename "$MIGRATION_FILE")${NC}"
    ((MIGRATION_COUNT++))
    ((TIMESTAMP_OFFSET += INTERVAL_SECONDS))
done

# Create schema_migrations table as first migration if it doesn't exist
SCHEMA_MIGRATIONS_DDL="$DDL_DIR/schema_migrations.sql"
FIRST_MIGRATION="$MIGRATIONS_DIR/${BASE_TIMESTAMP}_000000_create_schema_migrations_table.sql"

if [ -f "$SCHEMA_MIGRATIONS_DDL" ] && [ ! -f "$FIRST_MIGRATION" ]; then
    echo ""
    echo -e "${BLUE}Creating first migration: schema_migrations table${NC}"
    
    DDL_CONTENT=$(cat "$SCHEMA_MIGRATIONS_DDL")
    
    cat > "$FIRST_MIGRATION" <<EOF
-- Migration: Create schema_migrations tracking table
-- Date: ${BASE_TIMESTAMP} 00:00:00
-- 
-- This is the first migration that creates the tracking table
-- for all subsequent migrations.

${DDL_CONTENT}
EOF

    echo -e "${GREEN}   ✓ Created: $(basename "$FIRST_MIGRATION")${NC}"
    ((MIGRATION_COUNT++))
fi

# Summary
echo ""
echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}📊 SUMMARY${NC}"
echo -e "${BLUE}========================================================================${NC}"
echo -e "${GREEN}✅ Created $MIGRATION_COUNT migration file(s)${NC}"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "  1. Review the generated migrations in: $MIGRATIONS_DIR"
echo "  2. Apply migrations: ./v2/scripts/apply_migrations.sh"
echo "  3. Verify: ./v2/scripts/apply_migrations.sh --status"
echo ""

