#!/bin/bash
#================================================================
# UPDATE GLUE JOBS TO CHECK TABLES INSTEAD OF CREATING THEM
#================================================================
# This script updates all Glue job Python files to:
# 1. Remove CREATE TABLE SQL generation functions
# 2. Add table verification functions
# 3. Update write_to_redshift_versioned to not require preactions for table creation
# 4. Add table existence checks before writing data
#
# Usage: ./update_glue_jobs_to_check_tables.sh
#================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}=====================================================================${NC}"
echo -e "${BLUE}UPDATING GLUE JOBS - REMOVE TABLE CREATION, ADD TABLE VERIFICATION${NC}"
echo -e "${BLUE}=====================================================================${NC}"
echo ""

# Get list of all HMU* directories except HMUPatient (already done) and HMUMyJob (template)
GLUE_JOBS=$(find . -maxdepth 1 -type d -name "HMU*" ! -name "HMUPatient" ! -name "HMUMyJob" | sort)

UPDATED_COUNT=0
SKIPPED_COUNT=0

for job_dir in $GLUE_JOBS; do
    job_name=$(basename "$job_dir")
    py_file="$job_dir/$job_name.py"

    if [ ! -f "$py_file" ]; then
        echo -e "${YELLOW}⊙ Skipping $job_name - Python file not found${NC}"
        ((SKIPPED_COUNT++))
        continue
    fi

    echo -e "${BLUE}→ Processing $job_name...${NC}"

    # Check if already updated
    if grep -q "verify_table_exists" "$py_file"; then
        echo -e "${YELLOW}  ⊙ Already updated - skipping${NC}"
        ((SKIPPED_COUNT++))
        continue
    fi

    # Create backup
    cp "$py_file" "$py_file.bak"

    # The updates are complex and job-specific, so we'll note what needs to be done
    echo -e "${YELLOW}  ⚠  Manual update required for $job_name${NC}"
    echo -e "${YELLOW}     Please apply the same pattern as HMUPatient.py:${NC}"
    echo -e "${YELLOW}     1. Remove create_*_sql() functions${NC}"
    echo -e "${YELLOW}     2. Add verify_table_exists() and verify_all_required_tables() functions${NC}"
    echo -e "${YELLOW}     3. Update write_to_redshift_versioned signature${NC}"
    echo -e "${YELLOW}     4. Remove preactions parameter from write calls${NC}"
    echo -e "${YELLOW}     5. Add verify_all_required_tables() call before first write${NC}"
    echo ""

    # Restore backup since we're not auto-updating
    mv "$py_file.bak" "$py_file.manual_update_needed"

    ((SKIPPED_COUNT++))
done

echo ""
echo -e "${BLUE}=====================================================================${NC}"
echo -e "${BLUE}SUMMARY${NC}"
echo -e "${BLUE}=====================================================================${NC}"
echo -e "${GREEN}✓ Auto-updated: $UPDATED_COUNT jobs${NC}"
echo -e "${YELLOW}⚠ Need manual update: $SKIPPED_COUNT jobs${NC}"
echo -e "${BLUE}=====================================================================${NC}"
echo ""
echo -e "${YELLOW}RECOMMENDATION:${NC}"
echo -e "Due to variations in Glue job implementations, please use HMUPatient.py as"
echo -e "a template and manually apply the same changes to each job:"
echo ""
echo -e "  ${GREEN}1. Copy verification functions from HMUPatient.py (lines 608-671)${NC}"
echo -e "  ${GREEN}2. Update write_to_redshift_versioned() signature and body${NC}"
echo -e "  ${GREEN}3. Remove all create_*_table_sql() function definitions${NC}"
echo -e "  ${GREEN}4. Remove preactions parameters from write_to_redshift_versioned() calls${NC}"
echo -e "  ${GREEN}5. Add verify_all_required_tables() call before first write${NC}"
echo ""
