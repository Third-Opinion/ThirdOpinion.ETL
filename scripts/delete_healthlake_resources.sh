#!/bin/bash

# AWS HealthLake Resource Deletion Script
# This script generates and executes awscurl DELETE commands for each resource_id
#
# Usage:
#   ./delete_healthlake_resources.sh [OPTIONS]
#
# Options:
#   --csv FILE              CSV file containing resource IDs (overrides hardcoded array)
#   --resource-type TYPE    FHIR resource type to delete (default: Condition)
#                          Examples: Condition, Observation, Patient, Encounter, etc.
#   --datastore-id ID      HealthLake datastore ID (default: 836e877666cebf177ce6370ec1478a92)
#   --region REGION        AWS region (default: us-east-2)
#   --batch-size SIZE      Number of resources to process in parallel (default: 10)
#   --help                 Show this help message
#
# CSV File Format:
#   - First row is treated as header (any column names)
#   - Resource IDs should be in the first column
#   - Additional columns are ignored
#
# Examples:
#   ./delete_healthlake_resources.sh --csv resources.csv --resource-type Observation
#   ./delete_healthlake_resources.sh --resource-type Patient
#   ./delete_healthlake_resources.sh --csv ids.csv --batch-size 20

# Default values
DATASTORE_ID="836e877666cebf177ce6370ec1478a92"
REGION="us-east-2"
RESOURCE_TYPE="Condition"
CSV_FILE=""
BATCH_SIZE=10

# Array of resource IDs to delete (used if no CSV provided)
resource_ids=(

)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Parse command line arguments
show_help() {
    head -n 30 "$0" | grep "^#" | sed 's/^# \?//'
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --csv)
            CSV_FILE="$2"
            shift 2
            ;;
        --resource-type)
            RESOURCE_TYPE="$2"
            shift 2
            ;;
        --datastore-id)
            DATASTORE_ID="$2"
            shift 2
            ;;
        --region)
            REGION="$2"
            shift 2
            ;;
        --batch-size)
            BATCH_SIZE="$2"
            shift 2
            ;;
        --help|-h)
            show_help
            ;;
        *)
            echo -e "${RED}Error: Unknown option $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Build BASE_URL with the resource type
BASE_URL="https://healthlake.${REGION}.amazonaws.com/datastore/${DATASTORE_ID}/r4/${RESOURCE_TYPE}"

# Load resource IDs from CSV if provided
if [ -n "$CSV_FILE" ]; then
    if [ ! -f "$CSV_FILE" ]; then
        echo -e "${RED}Error: CSV file not found: $CSV_FILE${NC}"
        exit 1
    fi

    echo -e "${CYAN}Loading resource IDs from CSV file: $CSV_FILE${NC}"

    # Read CSV file, skip header, extract first column, remove quotes and whitespace
    resource_ids=()
    while IFS=',' read -r id rest || [ -n "$id" ]; do
        # Skip empty lines
        if [ -z "$id" ]; then
            continue
        fi

        # Remove quotes and whitespace
        id=$(echo "$id" | sed 's/^[[:space:]]*"//;s/"[[:space:]]*$//;s/^[[:space:]]*//;s/[[:space:]]*$//')

        # Skip if it's empty after cleanup
        if [ -z "$id" ]; then
            continue
        fi

        resource_ids+=("$id")
    done < <(tail -n +2 "$CSV_FILE")  # Skip first line (header)

    echo -e "${GREEN}Loaded ${#resource_ids[@]} resource IDs from CSV${NC}"
    echo ""
fi

# Check if we have any resource IDs to process
if [ ${#resource_ids[@]} -eq 0 ]; then
    echo -e "${RED}Error: No resource IDs to process${NC}"
    echo "Either provide a CSV file with --csv or hardcode resource IDs in the script"
    exit 1
fi

# Counters
count=0
success=0
failed=0

echo "================================"
echo "HealthLake Resource Deletion"
echo "================================"
echo "Datastore ID: $DATASTORE_ID"
echo "Region: $REGION"
echo "Resource Type: $RESOURCE_TYPE"
echo "Batch Size: $BATCH_SIZE"
echo "Total resources to process: ${#resource_ids[@]}"
echo "================================"
echo ""

# Process resource IDs in batches
total_ids=${#resource_ids[@]}

for ((i=0; i<total_ids; i+=BATCH_SIZE)); do
    # Get the batch
    batch=("${resource_ids[@]:$i:$BATCH_SIZE}")
    batch_start=$((i+1))
    batch_end=$((i+BATCH_SIZE))
    if [ $batch_end -gt $total_ids ]; then
        batch_end=$total_ids
    fi

    echo -e "${YELLOW}Processing batch $((i/BATCH_SIZE + 1)): Resources $batch_start-$batch_end${NC}"
    echo "---"

    # Arrays to track results for this batch
    declare -A batch_results

    # Process each resource in the batch in parallel
    for resource_id in "${batch[@]}"; do
        ((count++))

        # Skip empty entries
        if [ -z "$resource_id" ]; then
            continue
        fi

        # Execute awscurl command in background
        (
            echo -n "[$count/$total_ids] Deleting: $resource_id ... "

            response=$(awscurl -k -X DELETE \
                "${BASE_URL}/${resource_id}" \
                --service healthlake \
                --region "${REGION}" \
                -H "Accept: */*" \
                2>&1)

            exit_code=$?

            if [ $exit_code -eq 0 ]; then
                echo -e "${GREEN}SUCCESS${NC}"
                echo "success" > "/tmp/healthlake_delete_${resource_id}.result"
            else
                echo -e "${RED}FAILED${NC}"
                echo "  Error: $response"
                echo "failed" > "/tmp/healthlake_delete_${resource_id}.result"
            fi
        ) &
    done

    # Wait for all background jobs in this batch to complete
    wait

    # Count successes and failures for this batch
    for resource_id in "${batch[@]}"; do
        if [ -z "$resource_id" ]; then
            continue
        fi

        if [ -f "/tmp/healthlake_delete_${resource_id}.result" ]; then
            result=$(cat "/tmp/healthlake_delete_${resource_id}.result")
            if [ "$result" = "success" ]; then
                ((success++))
            else
                ((failed++))
            fi
            rm -f "/tmp/healthlake_delete_${resource_id}.result"
        fi
    done

    echo ""
done

echo ""
echo "================================"
echo "Summary:"
echo -e "  Total Processed: $count"
echo -e "  ${GREEN}Successful: $success${NC}"
echo -e "  ${RED}Failed: $failed${NC}"
echo "================================"

# Exit with appropriate code
if [ $failed -eq 0 ]; then
    exit 0
else
    exit 1
fi
