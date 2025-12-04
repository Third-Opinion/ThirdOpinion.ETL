#!/bin/bash

# Create grouped migrations from existing DDL files
# Groups: 1) schema_migrations, 2) shared tables, 3) observations, 4) conditions

set -e

DDL_DIR="${DDL_DIR:-./v2/ddl}"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-./v2/migrations}"

# Use current date/time for migrations, or allow override via BASE_DATE env var
if [ -z "$BASE_DATE" ]; then
    BASE_DATE=$(date +"%Y%m%d")
    BASE_TIME_HOUR=$(date +"%H")
    BASE_TIME_MIN=$(date +"%M")
else
    # If BASE_DATE is set, use it (format: YYYYMMDD)
    BASE_TIME_HOUR="00"
    BASE_TIME_MIN="00"
fi

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}🔄 Creating Grouped Migrations from DDL Files${NC}"
echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}Using date: ${BASE_DATE}${NC}"
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

# Define table groups
declare -a SHARED_TABLES=("codes" "categories" "body_sites" "interpretations")
declare -a OBSERVATION_TABLES=("observations" "observation_codes" "observation_categories" "observation_body_sites" "observation_interpretations" "observation_components" "observation_reference_ranges" "observation_performers" "observation_derived_from" "observation_members" "observation_notes")
declare -a CONDITION_TABLES=("conditions" "condition_codes" "condition_categories" "condition_body_sites" "condition_stages" "condition_stage_types" "condition_stage_summaries" "condition_evidence" "condition_extensions" "condition_notes")

# Function to read and combine DDL files
combine_ddl_files() {
    local table_list=("$@")
    local combined=""
    
    for table in "${table_list[@]}"; do
        local ddl_file="$DDL_DIR/${table}.sql"
        if [ -f "$ddl_file" ]; then
            combined+="$(cat "$ddl_file")"$'\n'$'\n'
        else
            echo -e "${YELLOW}   ⚠️  Warning: DDL file not found: $ddl_file${NC}" >&2
        fi
    done
    
    echo "$combined"
}

# Migration 1: Schema migrations table
echo -e "${BLUE}Creating Migration 1: Schema migrations tracking table...${NC}"
MIGRATION_1="$MIGRATIONS_DIR/${BASE_DATE}_000000_create_schema_migrations_table.sql"
if [ ! -f "$MIGRATION_1" ]; then
    MIGRATION_DATE=$(date -d "${BASE_DATE}" +"%Y-%m-%d" 2>/dev/null || echo "${BASE_DATE:0:4}-${BASE_DATE:4:2}-${BASE_DATE:6:2}")
    cat > "$MIGRATION_1" <<EOF
-- Migration: Create schema_migrations tracking table
-- Date: ${MIGRATION_DATE} 00:00:00
-- Description: Creates the table that tracks all applied migrations
--
-- This must be the first migration as it tracks all subsequent migrations.

CREATE TABLE IF NOT EXISTS public.schema_migrations (
    migration_id VARCHAR(255) NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT GETDATE(),
    applied_by VARCHAR(255),
    execution_time_seconds INTEGER,
    status VARCHAR(50) NOT NULL DEFAULT 'success',
    error_message TEXT,
    checksum VARCHAR(64),
    migration_file_path VARCHAR(500)
) DISTKEY (migration_id) SORTKEY (applied_at);

-- Add comments
COMMENT ON TABLE public.schema_migrations IS 'Tracks all applied database migrations with execution details';
COMMENT ON COLUMN public.schema_migrations.migration_id IS 'Unique identifier: YYYYMMDD_HHMMSS_description';
COMMENT ON COLUMN public.schema_migrations.applied_at IS 'Timestamp when migration was applied';
COMMENT ON COLUMN public.schema_migrations.applied_by IS 'AWS user/role that applied the migration';
COMMENT ON COLUMN public.schema_migrations.execution_time_seconds IS 'Time taken to execute migration in seconds';
COMMENT ON COLUMN public.schema_migrations.status IS 'Migration status: success, failed, rolled_back';
COMMENT ON COLUMN public.schema_migrations.error_message IS 'Error message if migration failed';
COMMENT ON COLUMN public.schema_migrations.checksum IS 'SHA256 hash of migration file for verification';
COMMENT ON COLUMN public.schema_migrations.migration_file_path IS 'Path to migration file relative to repo root';

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_schema_migrations_status 
ON public.schema_migrations(status);

CREATE INDEX IF NOT EXISTS idx_schema_migrations_applied_at 
ON public.schema_migrations(applied_at DESC);
EOF
    echo -e "${GREEN}   ✅ Created: $(basename "$MIGRATION_1")${NC}"
else
    echo -e "${YELLOW}   ⏭️  Skipped: Already exists${NC}"
fi

# Migration 2: Shared reference tables
echo ""
echo -e "${BLUE}Creating Migration 2: Shared reference tables...${NC}"
MIGRATION_2="$MIGRATIONS_DIR/${BASE_DATE}_000100_create_shared_reference_tables.sql"
if [ ! -f "$MIGRATION_2" ]; then
    MIGRATION_DATE=$(date -d "${BASE_DATE}" +"%Y-%m-%d" 2>/dev/null || echo "${BASE_DATE:0:4}-${BASE_DATE:4:2}-${BASE_DATE:6:2}")
    {
        echo "-- Migration: Create shared reference tables"
        echo "-- Date: ${MIGRATION_DATE} 00:01:00"
        echo "-- Description: Creates normalized reference tables used by both observations and conditions"
        echo "-- Tables: codes, categories, body_sites, interpretations"
        echo ""
        combine_ddl_files "${SHARED_TABLES[@]}"
    } > "$MIGRATION_2"
    echo -e "${GREEN}   ✅ Created: $(basename "$MIGRATION_2")${NC}"
    echo -e "${BLUE}      Tables: ${SHARED_TABLES[*]}${NC}"
else
    echo -e "${YELLOW}   ⏭️  Skipped: Already exists${NC}"
fi

# Migration 3: Observation tables
echo ""
echo -e "${BLUE}Creating Migration 3: Observation tables...${NC}"
MIGRATION_3="$MIGRATIONS_DIR/${BASE_DATE}_000200_create_observation_tables.sql"
if [ ! -f "$MIGRATION_3" ]; then
    MIGRATION_DATE=$(date -d "${BASE_DATE}" +"%Y-%m-%d" 2>/dev/null || echo "${BASE_DATE:0:4}-${BASE_DATE:4:2}-${BASE_DATE:6:2}")
    {
        echo "-- Migration: Create observation tables"
        echo "-- Date: ${MIGRATION_DATE} 00:02:00"
        echo "-- Description: Creates all observation-related tables"
        echo "-- Tables: observations + all observation_* junction and detail tables"
        echo ""
        combine_ddl_files "${OBSERVATION_TABLES[@]}"
    } > "$MIGRATION_3"
    echo -e "${GREEN}   ✅ Created: $(basename "$MIGRATION_3")${NC}"
    echo -e "${BLUE}      Tables: ${#OBSERVATION_TABLES[@]} tables${NC}"
    echo -e "${BLUE}      ${OBSERVATION_TABLES[*]}${NC}"
else
    echo -e "${YELLOW}   ⏭️  Skipped: Already exists${NC}"
fi

# Migration 4: Condition tables
echo ""
echo -e "${BLUE}Creating Migration 4: Condition tables...${NC}"
MIGRATION_4="$MIGRATIONS_DIR/${BASE_DATE}_000300_create_condition_tables.sql"
if [ ! -f "$MIGRATION_4" ]; then
    MIGRATION_DATE=$(date -d "${BASE_DATE}" +"%Y-%m-%d" 2>/dev/null || echo "${BASE_DATE:0:4}-${BASE_DATE:4:2}-${BASE_DATE:6:2}")
    {
        echo "-- Migration: Create condition tables"
        echo "-- Date: ${MIGRATION_DATE} 00:03:00"
        echo "-- Description: Creates all condition-related tables"
        echo "-- Tables: conditions + all condition_* junction and detail tables"
        echo ""
        combine_ddl_files "${CONDITION_TABLES[@]}"
    } > "$MIGRATION_4"
    echo -e "${GREEN}   ✅ Created: $(basename "$MIGRATION_4")${NC}"
    echo -e "${BLUE}      Tables: ${#CONDITION_TABLES[@]} tables${NC}"
    echo -e "${BLUE}      ${CONDITION_TABLES[*]}${NC}"
else
    echo -e "${YELLOW}   ⏭️  Skipped: Already exists${NC}"
fi

# Summary
echo ""
echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}📊 SUMMARY${NC}"
echo -e "${BLUE}========================================================================${NC}"
echo -e "${GREEN}✅ Created grouped migrations:${NC}"
echo ""
echo -e "  1. ${GREEN}${BASE_DATE}_000000_create_schema_migrations_table.sql${NC}"
echo -e "     → Schema migrations tracking table"
echo ""
echo -e "  2. ${GREEN}${BASE_DATE}_000100_create_shared_reference_tables.sql${NC}"
echo -e "     → ${#SHARED_TABLES[@]} shared tables (codes, categories, body_sites, interpretations)"
echo ""
echo -e "  3. ${GREEN}${BASE_DATE}_000200_create_observation_tables.sql${NC}"
echo -e "     → ${#OBSERVATION_TABLES[@]} observation tables"
echo ""
echo -e "  4. ${GREEN}${BASE_DATE}_000300_create_condition_tables.sql${NC}"
echo -e "     → ${#CONDITION_TABLES[@]} condition tables"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "  1. Review the generated migrations in: $MIGRATIONS_DIR"
echo "  2. Apply migrations: ./v2/scripts/apply_migrations.sh"
echo "  3. Verify: ./v2/scripts/apply_migrations.sh --status"
echo ""

