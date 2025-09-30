#!/bin/bash

# Monitor a specific Glue job run
# Usage: ./monitor_job.sh <job-name> <run-id>

JOB_NAME=${1:-"HMUMedicationRequest"}
RUN_ID=${2:-"jr_fb1c3939a77e6df90dba7764a3c94aa41c92c731264400884e06196a77f81a1e"}
AWS_PROFILE="${AWS_PROFILE:-to-prd-admin}"
AWS_REGION="${AWS_REGION:-us-east-2}"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}📊 Monitoring Glue Job: $JOB_NAME${NC}"
echo -e "${YELLOW}📋 Run ID: $RUN_ID${NC}"
echo "=========================================================="

while true; do
    # Get job status
    STATUS=$(aws glue get-job-run \
        --job-name "$JOB_NAME" \
        --run-id "$RUN_ID" \
        --profile "$AWS_PROFILE" \
        --region "$AWS_REGION" \
        --query "JobRun.{JobRunState: JobRunState, StartedOn: StartedOn, CompletedOn: CompletedOn, ErrorMessage: ErrorMessage}" \
        --output json)

    STATE=$(echo "$STATUS" | jq -r '.JobRunState')
    STARTED=$(echo "$STATUS" | jq -r '.StartedOn')
    COMPLETED=$(echo "$STATUS" | jq -r '.CompletedOn')
    ERROR=$(echo "$STATUS" | jq -r '.ErrorMessage')

    echo -n "$(date '+%H:%M:%S') - Status: "

    case "$STATE" in
        "SUCCEEDED")
            echo -e "${GREEN}✅ SUCCEEDED${NC}"
            echo "Completed at: $COMPLETED"
            break
            ;;
        "FAILED")
            echo -e "${RED}❌ FAILED${NC}"
            echo "Error: $ERROR"
            break
            ;;
        "STOPPED")
            echo -e "${YELLOW}⏹️  STOPPED${NC}"
            break
            ;;
        "RUNNING")
            echo -e "${YELLOW}🔄 RUNNING${NC}"
            ;;
        "STARTING")
            echo -e "${YELLOW}🚀 STARTING${NC}"
            ;;
        *)
            echo -e "${YELLOW}📊 $STATE${NC}"
            ;;
    esac

    # Break if job is complete
    if [[ "$STATE" == "SUCCEEDED" || "$STATE" == "FAILED" || "$STATE" == "STOPPED" ]]; then
        break
    fi

    # Wait 30 seconds before checking again
    sleep 30
done

echo "=========================================================="
echo -e "${GREEN}✨ Monitoring complete!${NC}"