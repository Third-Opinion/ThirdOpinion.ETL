#!/bin/bash

# Check naming consistency across all Glue jobs

echo "🔍 Checking Naming Consistency Across Glue Jobs"
echo "================================================"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Check for camelCase violations (should use snake_case)
echo -e "\n${YELLOW}Checking for camelCase field names (should be snake_case):${NC}"
for job in HMU*/HMU*.py; do
  job_name=$(basename "$job" .py)

  # Look for alias names that are camelCase
  camel_case=$(grep -E '\.alias\(".*[a-z][A-Z].*"\)' "$job" | grep -v "resourcetype" || true)

  if [[ ! -z "$camel_case" ]]; then
    echo -e "${RED}❌ ${job_name}:${NC}"
    echo "$camel_case" | head -5
  fi
done

# Check for consistent resource type field
echo -e "\n${YELLOW}Checking resource type field consistency:${NC}"
for job in HMU*/HMU*.py; do
  job_name=$(basename "$job" .py)
  resource_type=$(grep -E 'F\.lit.*\.alias\("resource' "$job" | head -1 || true)

  if [[ ! -z "$resource_type" ]]; then
    # Check if it uses "resourcetype" (correct) or "resource_type" (incorrect)
    if echo "$resource_type" | grep -q 'alias("resourcetype")'; then
      echo -e "${GREEN}✅ ${job_name}: Uses 'resourcetype' (correct)${NC}"
    else
      echo -e "${RED}❌ ${job_name}: Uses different naming${NC}"
      echo "   $resource_type"
    fi
  fi
done

# Check for consistent ID field naming
echo -e "\n${YELLOW}Checking ID field naming patterns:${NC}"
for job in HMU*/HMU*.py; do
  job_name=$(basename "$job" .py)

  # Extract resource name from job name
  resource=$(echo "$job_name" | sed 's/HMU//' | tr '[:upper:]' '[:lower:]')

  # Check for primary ID field
  id_field=$(grep -E "\.alias\(\"${resource}_id\"\)" "$job" | head -1 || true)

  if [[ ! -z "$id_field" ]]; then
    echo -e "${GREEN}✅ ${job_name}: Has ${resource}_id field${NC}"
  else
    # Check if it has any ID field
    any_id=$(grep -E '\.alias\(".*_id"\)' "$job" | head -1 || true)
    if [[ ! -z "$any_id" ]]; then
      echo -e "${YELLOW}⚠️  ${job_name}: Has ID field but may not match pattern${NC}"
      echo "   $any_id"
    fi
  fi
done

# Check for consistent date field naming
echo -e "\n${YELLOW}Checking date field naming patterns:${NC}"
echo "Should use: _date for dates, _datetime for timestamps, _at for audit timestamps"
for job in HMU*/HMU*.py; do
  job_name=$(basename "$job" .py)

  # Check for incorrect patterns
  incorrect=$(grep -E '\.alias\(".*Date.*"\)' "$job" | grep -v "_date" || true)

  if [[ ! -z "$incorrect" ]]; then
    echo -e "${RED}❌ ${job_name}: Has camelCase date fields${NC}"
    echo "$incorrect" | head -3
  fi
done

# Check for consistent metadata field naming
echo -e "\n${YELLOW}Checking metadata field naming:${NC}"
for job in HMU*/HMU*.py; do
  job_name=$(basename "$job" .py)

  # Check for meta_ prefix
  meta_fields=$(grep -E '\.alias\("meta_' "$job" | wc -l)

  if [[ $meta_fields -gt 0 ]]; then
    echo -e "${GREEN}✅ ${job_name}: Has ${meta_fields} meta_ prefixed fields${NC}"
  fi
done

# Check for audit fields
echo -e "\n${YELLOW}Checking for standard audit fields (created_at, updated_at):${NC}"
for job in HMU*/HMU*.py; do
  job_name=$(basename "$job" .py)

  has_created=$(grep -q 'alias("created_at")' "$job" && echo "yes" || echo "no")
  has_updated=$(grep -q 'alias("updated_at")' "$job" && echo "yes" || echo "no")

  if [[ "$has_created" == "yes" && "$has_updated" == "yes" ]]; then
    echo -e "${GREEN}✅ ${job_name}: Has both audit fields${NC}"
  else
    echo -e "${YELLOW}⚠️  ${job_name}: Missing audit fields (created_at: $has_created, updated_at: $has_updated)${NC}"
  fi
done

echo -e "\n${GREEN}================================================${NC}"
echo -e "${GREEN}✨ Naming consistency check complete!${NC}"