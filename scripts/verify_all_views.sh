#!/bin/bash

# ===================================================================
# VERIFY ALL VIEWS SCRIPT
# ===================================================================
# This script verifies that all fact_* and rpt_* SQL files are
# included in the deployment script and checks for common issues.
#
# Usage:
#   ./verify_all_views.sh
# ===================================================================

# Ensure we're running from project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT" || {
    echo "Error: Cannot change to project root directory"
    exit 1
}

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  FHIR VIEW VERIFICATION${NC}"
echo -e "${BLUE}========================================${NC}"
echo

# Check 1: Compare filesystem vs deployment script
echo -e "${YELLOW}Check 1: Comparing filesystem with deployment script...${NC}"

SCRIPT_VIEWS=$(grep -E '^\s+"(fact_|rpt_)' scripts/redeploy_fhir_views.sh | sed 's/.*"\(.*\)".*/\1/' | sort | uniq)
FILE_VIEWS=$(ls views/fact_*.sql views/rpt_*.sql 2>/dev/null | xargs -n1 basename | sed 's/.sql$//' | sort)

MISSING_FROM_SCRIPT=$(comm -13 <(echo "$SCRIPT_VIEWS") <(echo "$FILE_VIEWS"))
MISSING_FROM_FILES=$(comm -23 <(echo "$SCRIPT_VIEWS") <(echo "$FILE_VIEWS"))

if [ -z "$MISSING_FROM_SCRIPT" ] && [ -z "$MISSING_FROM_FILES" ]; then
    echo -e "  ${GREEN}✓ All views synchronized${NC}"
    echo "    Total fact_ views: $(echo "$FILE_VIEWS" | grep "^fact_" | wc -l | tr -d ' ')"
    echo "    Total rpt_ views: $(echo "$FILE_VIEWS" | grep "^rpt_" | wc -l | tr -d ' ')"
    echo "    Total views: $(echo "$FILE_VIEWS" | wc -l | tr -d ' ')"
else
    echo -e "  ${RED}✗ Synchronization issues found${NC}"
    if [ ! -z "$MISSING_FROM_SCRIPT" ]; then
        echo -e "  ${RED}Views in filesystem but NOT in script:${NC}"
        echo "$MISSING_FROM_SCRIPT" | sed 's/^/    /'
    fi
    if [ ! -z "$MISSING_FROM_FILES" ]; then
        echo -e "  ${RED}Views in script but NOT in filesystem:${NC}"
        echo "$MISSING_FROM_FILES" | sed 's/^/    /'
    fi
    exit 1
fi

echo

# Check 2: Verify all SQL files have CREATE statements
echo -e "${YELLOW}Check 2: Verifying CREATE statements...${NC}"

MISSING_CREATE=0
for file in views/fact_*.sql views/rpt_*.sql; do
    if ! grep -q "^CREATE" "$file"; then
        echo -e "  ${RED}✗ Missing CREATE: $(basename "$file")${NC}"
        MISSING_CREATE=$((MISSING_CREATE + 1))
    fi
done

if [ $MISSING_CREATE -eq 0 ]; then
    echo -e "  ${GREEN}✓ All files have CREATE statements${NC}"
else
    echo -e "  ${RED}✗ $MISSING_CREATE files missing CREATE statements${NC}"
    exit 1
fi

echo

# Check 3: Check for non-standard characters at start of files
echo -e "${YELLOW}Check 3: Checking for syntax issues...${NC}"

SYNTAX_ISSUES=0
for file in views/fact_*.sql views/rpt_*.sql; do
    # Check for non-ASCII characters in first line with CREATE
    create_line=$(grep -n "^CREATE" "$file" | head -1)
    if [ ! -z "$create_line" ]; then
        line_num=$(echo "$create_line" | cut -d: -f1)
        line_content=$(sed -n "${line_num}p" "$file")

        # Check for common issues
        if [[ "$line_content" =~ [^[:print:][:space:]] ]]; then
            echo -e "  ${RED}✗ Non-ASCII characters in $(basename "$file"):${line_num}${NC}"
            SYNTAX_ISSUES=$((SYNTAX_ISSUES + 1))
        fi

        if [[ ! "$line_content" =~ ^CREATE\ (MATERIALIZED\ )?VIEW\ [a-z_0-9]+ ]]; then
            echo -e "  ${YELLOW}⚠ Unusual CREATE format in $(basename "$file"):${line_num}${NC}"
            echo "    ${line_content:0:80}"
        fi
    fi
done

if [ $SYNTAX_ISSUES -eq 0 ]; then
    echo -e "  ${GREEN}✓ No syntax issues found${NC}"
else
    echo -e "  ${RED}✗ $SYNTAX_ISSUES files with syntax issues${NC}"
    exit 1
fi

echo

# Check 4: Verify naming convention
echo -e "${YELLOW}Check 4: Verifying naming conventions...${NC}"

NAMING_ISSUES=0
for file in views/fact_*.sql views/rpt_*.sql; do
    basename=$(basename "$file" .sql)

    # Check for _v1 suffix on fact_ and rpt_ views
    if [[ "$basename" =~ ^(fact_|rpt_) ]] && [[ ! "$basename" =~ _v[0-9]+$ ]]; then
        echo -e "  ${YELLOW}⚠ Missing version suffix: $basename${NC}"
        NAMING_ISSUES=$((NAMING_ISSUES + 1))
    fi
done

if [ $NAMING_ISSUES -eq 0 ]; then
    echo -e "  ${GREEN}✓ All views follow naming convention${NC}"
else
    echo -e "  ${YELLOW}⚠ $NAMING_ISSUES views without version suffix${NC}"
    echo "    (This is a warning, not an error)"
fi

echo

# Check 5: Verify dependency levels are complete
echo -e "${YELLOW}Check 5: Verifying dependency levels...${NC}"

for level in {0..8}; do
    count=$(grep -A 100 "^DEPENDENCY_LEVEL_${level}=" scripts/redeploy_fhir_views.sh | grep -E '^\s+"' | wc -l | tr -d ' ')
    if [ "$count" -gt 0 ]; then
        echo "  Level $level: $count views"
    fi
done

echo

# Summary
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  ✓ ALL CHECKS PASSED${NC}"
echo -e "${GREEN}========================================${NC}"
echo
echo "All fact_* and rpt_* views are properly configured"
echo "and ready for deployment via ./scripts/redeploy_fhir_views.sh"
echo

exit 0
