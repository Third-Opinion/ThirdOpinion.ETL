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

    echo -e "  ${GREEN}✅${NC} $job"
    echo -e "      Workers: $workers x $worker_type | Glue: v$version | Timeout: ${timeout}min"

    ((job_count++))
done

echo
echo -e "${BLUE}===================================================${NC}"
echo -e "${GREEN}✨ VERIFICATION COMPLETE${NC}"
echo -e "${GREEN}Total jobs deployed: $job_count${NC}"
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