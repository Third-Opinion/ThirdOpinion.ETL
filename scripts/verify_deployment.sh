#!/bin/bash

# Verify AWS Glue Jobs deployment status
# This script checks all HMU jobs and their configurations

set -e  # Exit on error

# Configuration
AWS_PROFILE="${AWS_PROFILE:-to-prd-admin}"
AWS_REGION="${AWS_REGION:-us-east-2}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}===================================================${NC}"
echo -e "${BLUE}🔍 AWS GLUE JOBS DEPLOYMENT VERIFICATION${NC}"
echo -e "${BLUE}===================================================${NC}"
echo

echo -e "${GREEN}AWS Profile:${NC} $AWS_PROFILE"
echo -e "${GREEN}AWS Region:${NC} $AWS_REGION"
echo

# List all HMU jobs
echo -e "${YELLOW}📋 Deployed Glue Jobs:${NC}"
jobs=$(aws glue list-jobs --profile "$AWS_PROFILE" --region "$AWS_REGION" --query "JobNames[?starts_with(@, 'HMU')]" --output json | jq -r '.[]' | sort)

if [ -z "$jobs" ]; then
    echo -e "${RED}❌ No HMU jobs found${NC}"
    exit 1
fi

job_count=0
for job in $jobs; do
    # Get job details
    details=$(aws glue get-job --job-name "$job" --profile "$AWS_PROFILE" --region "$AWS_REGION" --query "Job.{Workers: NumberOfWorkers, Type: WorkerType, Version: GlueVersion, Timeout: Timeout}" --output json)

    workers=$(echo "$details" | jq -r '.Workers')
    worker_type=$(echo "$details" | jq -r '.Type')
    version=$(echo "$details" | jq -r '.Version')
    timeout=$(echo "$details" | jq -r '.Timeout')

    # Get latest job run status
    latest_run=$(aws glue get-job-runs --job-name "$job" --profile "$AWS_PROFILE" --region "$AWS_REGION" --max-results 1 --query "JobRuns[0].{JobRunState: JobRunState, StartedOn: StartedOn, CompletedOn: CompletedOn, ErrorMessage: ErrorMessage}" --output json 2>/dev/null || echo '{}')

    if [ "$latest_run" != "{}" ] && [ "$latest_run" != "null" ]; then
        job_state=$(echo "$latest_run" | jq -r '.JobRunState // "UNKNOWN"')
        started_on=$(echo "$latest_run" | jq -r '.StartedOn // "Never"')
        completed_on=$(echo "$latest_run" | jq -r '.CompletedOn // "N/A"')
        error_message=$(echo "$latest_run" | jq -r '.ErrorMessage // ""')

        # Format dates if they exist
        if [ "$started_on" != "Never" ] && [ "$started_on" != "null" ]; then
            started_on=$(date -j -f "%Y-%m-%dT%H:%M:%S" "$(echo "$started_on" | cut -d'.' -f1)" "+%m/%d %H:%M" 2>/dev/null || echo "$started_on")
        fi

        # Choose status color and icon
        case "$job_state" in
            "SUCCEEDED")
                status_color="$GREEN"
                status_icon="✅"
                ;;
            "RUNNING")
                status_color="$BLUE"
                status_icon="🔄"
                ;;
            "FAILED")
                status_color="$RED"
                status_icon="❌"
                ;;
            "TIMEOUT")
                status_color="$YELLOW"
                status_icon="⏰"
                ;;
            "STOPPED")
                status_color="$YELLOW"
                status_icon="⏹️"
                ;;
            *)
                status_color="$NC"
                status_icon="❓"
                ;;
        esac

        echo -e "  ${GREEN}📦${NC} $job"
        echo -e "      Config: $workers x $worker_type | Glue: v$version | Timeout: ${timeout}min"
        echo -e "      Status: ${status_color}${status_icon} $job_state${NC} | Started: $started_on"

        # Show error if present
        if [ -n "$error_message" ] && [ "$error_message" != "null" ] && [ "$error_message" != "" ]; then
            # Truncate long error messages
            if [ ${#error_message} -gt 100 ]; then
                error_message="${error_message:0:100}..."
            fi
            echo -e "      ${RED}Error: $error_message${NC}"
        fi
    else
        echo -e "  ${GREEN}📦${NC} $job"
        echo -e "      Config: $workers x $worker_type | Glue: v$version | Timeout: ${timeout}min"
        echo -e "      Status: ${YELLOW}Never run${NC}"
    fi

    echo

    ((job_count++))
done

echo
echo -e "${BLUE}===================================================${NC}"
echo -e "${GREEN}✨ VERIFICATION COMPLETE${NC}"
echo -e "${GREEN}Total jobs deployed: $job_count${NC}"

# Count job statuses
succeeded_count=0
failed_count=0
running_count=0
never_run_count=0
other_count=0

for job in $jobs; do
    latest_run=$(aws glue get-job-runs --job-name "$job" --profile "$AWS_PROFILE" --region "$AWS_REGION" --max-results 1 --query "JobRuns[0].JobRunState" --output text 2>/dev/null || echo "NEVER_RUN")

    case "$latest_run" in
        "SUCCEEDED")
            ((succeeded_count++))
            ;;
        "FAILED")
            ((failed_count++))
            ;;
        "RUNNING")
            ((running_count++))
            ;;
        "NEVER_RUN"|"None")
            ((never_run_count++))
            ;;
        *)
            ((other_count++))
            ;;
    esac
done

echo
echo -e "${BLUE}📊 Job Status Summary:${NC}"
[ $succeeded_count -gt 0 ] && echo -e "  ${GREEN}✅ Succeeded: $succeeded_count${NC}"
[ $failed_count -gt 0 ] && echo -e "  ${RED}❌ Failed: $failed_count${NC}"
[ $running_count -gt 0 ] && echo -e "  ${BLUE}🔄 Running: $running_count${NC}"
[ $never_run_count -gt 0 ] && echo -e "  ${YELLOW}⭕ Never run: $never_run_count${NC}"
[ $other_count -gt 0 ] && echo -e "  ${YELLOW}❓ Other: $other_count${NC}"
echo

# Check for expected jobs
expected_jobs=(
    "HMUAllergyIntolerance"
    "HMUCarePlan"
    "HMUCondition"
    "HMUDiagnosticReport"
    "HMUDocumentReference"
    "HMUEncounter"
    "HMUMedication"
    "HMUMedicationDispense"
    "HMUMedicationRequest"
    "HMUObservation"
    "HMUPatient"
    "HMUPractitioner"
    "HMUProcedure"
)

missing_jobs=()
for expected in "${expected_jobs[@]}"; do
    if ! echo "$jobs" | grep -q "^$expected$"; then
        missing_jobs+=("$expected")
    fi
done

if [ ${#missing_jobs[@]} -gt 0 ]; then
    echo -e "${YELLOW}⚠️  Missing expected jobs:${NC}"
    for job in "${missing_jobs[@]}"; do
        echo -e "  - $job"
    done
else
    echo -e "${GREEN}✅ All expected jobs are deployed${NC}"
fi

echo
echo -e "${BLUE}Next Steps:${NC}"
echo "1. Test individual jobs with: aws glue start-job-run --job-name <job-name>"
echo "2. Monitor job runs in AWS Glue Console"
echo "3. Check CloudWatch logs for any execution errors"