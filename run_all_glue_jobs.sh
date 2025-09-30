#!/bin/bash

# ===================================================================
# RUN ALL GLUE JOBS SCRIPT
# ===================================================================
# This script runs all HMU Glue jobs to populate Redshift tables
# Jobs are dynamically discovered from AWS Glue
# ===================================================================
#
# Usage:
#   ./run_all_glue_jobs.sh              # Run all discovered HMU* jobs
#   ./run_all_glue_jobs.sh --deploy     # Deploy jobs first, then run
#   ./run_all_glue_jobs.sh --help       # Show this help message
#
# ===================================================================

# Configuration
AWS_PROFILE="${AWS_PROFILE:-to-prd-admin}"
AWS_REGION="${AWS_REGION:-us-east-2}"

# Parse command line arguments
DEPLOY_FIRST=false
SHOW_HELP=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --deploy)
            DEPLOY_FIRST=true
            shift
            ;;
        --help|-h)
            SHOW_HELP=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Show help message if requested
if [ "$SHOW_HELP" = true ]; then
    echo "Usage: ./run_all_glue_jobs.sh [OPTIONS]"
    echo ""
    echo "Run all HMU* Glue jobs to populate Redshift tables."
    echo ""
    echo "Options:"
    echo "  --deploy     Deploy Glue jobs from JSON templates before running"
    echo "  --help, -h   Show this help message and exit"
    echo ""
    echo "Environment Variables:"
    echo "  AWS_PROFILE  AWS profile to use (default: to-prd-admin)"
    echo "  AWS_REGION   AWS region (default: us-east-2)"
    echo ""
    echo "Examples:"
    echo "  ./run_all_glue_jobs.sh                    # Run existing jobs"
    echo "  ./run_all_glue_jobs.sh --deploy           # Deploy and run jobs"
    echo "  AWS_PROFILE=my-profile ./run_all_glue_jobs.sh  # Use different profile"
    exit 0
fi

# Array to store discovered Glue jobs
declare -a GLUE_JOBS=()

# Function to deploy Glue jobs from JSON templates
deploy_glue_jobs() {
    echo -e "${BLUE}Deploying Glue jobs from JSON templates...${NC}"
    echo ""

    local deployed_count=0
    local failed_count=0

    # Find all HMU* folders with JSON templates
    for json_file in HMU*/HMU*.json; do
        if [ -f "$json_file" ]; then
            local folder_name=$(dirname "$json_file")
            local job_name=$(basename "$folder_name")

            echo -e "${BLUE}→ Deploying job: $job_name${NC}"
            echo "  From template: $json_file"

            # Check if deploy script exists
            if [ -f "./deploy_glue_job.sh" ]; then
                # Use existing deploy script if available
                if ./deploy_glue_job.sh "$folder_name"; then
                    echo -e "${GREEN}  ✓ Successfully deployed${NC}"
                    ((deployed_count++))
                else
                    echo -e "${RED}  ✗ Failed to deploy${NC}"
                    ((failed_count++))
                fi
            else
                # Direct AWS CLI deployment
                if aws glue create-job \
                    --profile "$AWS_PROFILE" \
                    --region "$AWS_REGION" \
                    --cli-input-json "file://$json_file" &>/dev/null; then
                    echo -e "${GREEN}  ✓ Successfully created${NC}"
                    ((deployed_count++))
                elif aws glue update-job \
                    --profile "$AWS_PROFILE" \
                    --region "$AWS_REGION" \
                    --job-name "$job_name" \
                    --job-update "$(cat $json_file | jq 'del(.Name)')" &>/dev/null; then
                    echo -e "${GREEN}  ✓ Successfully updated${NC}"
                    ((deployed_count++))
                else
                    echo -e "${RED}  ✗ Failed to deploy${NC}"
                    ((failed_count++))
                fi
            fi
            echo ""
        fi
    done

    echo -e "${BLUE}====================================================================="
    echo "DEPLOYMENT SUMMARY"
    echo "====================================================================="
    echo -e "${GREEN}Successfully deployed: $deployed_count jobs${NC}"
    echo -e "${RED}Failed: $failed_count jobs${NC}"
    echo "=====================================================================${NC}"
    echo ""

    if [ $failed_count -gt 0 ]; then
        echo -e "${YELLOW}⚠ Some deployments failed. Continue anyway? (y/n): ${NC}"
        read -r response
        if [[ ! "$response" =~ ^[Yy]$ ]]; then
            echo "Aborting."
            exit 1
        fi
    fi
}

# Function to discover HMU* Glue jobs
discover_glue_jobs() {
    echo -e "${BLUE}Discovering HMU* Glue jobs...${NC}"

    # Query AWS Glue for all jobs starting with HMU
    local jobs_json=$(aws glue get-jobs \
        --profile "$AWS_PROFILE" \
        --region "$AWS_REGION" \
        --query 'Jobs[?starts_with(Name, `HMU`)].Name' \
        --output json 2>/dev/null)

    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ Failed to query Glue jobs${NC}"
        return 1
    fi

    # Parse JSON array and populate GLUE_JOBS array
    if [ ! -z "$jobs_json" ] && [ "$jobs_json" != "[]" ]; then
        # Convert JSON array to bash array using while loop
        while IFS= read -r job_name; do
            GLUE_JOBS+=("$job_name")
        done < <(echo "$jobs_json" | jq -r '.[]' | sort)

        if [ ${#GLUE_JOBS[@]} -gt 0 ]; then
            echo -e "${GREEN}✓ Discovered ${#GLUE_JOBS[@]} HMU* Glue jobs${NC}"
            echo ""
            echo "Jobs found:"
            for job in "${GLUE_JOBS[@]}"; do
                echo "  - $job"
            done
            return 0
        else
            echo -e "${YELLOW}⚠ No HMU* jobs found after parsing${NC}"
            return 1
        fi
    else
        echo -e "${YELLOW}⚠ No HMU* Glue jobs found${NC}"
        return 1
    fi
}

# Arrays to track job runs
declare -a JOB_RUN_IDS=()
declare -a JOB_NAMES=()

# Function to check AWS credentials
check_aws_credentials() {
    echo -e "${BLUE}Checking AWS credentials...${NC}"

    if aws sts get-caller-identity --profile "$AWS_PROFILE" --region "$AWS_REGION" &>/dev/null; then
        echo -e "${GREEN}✓ AWS credentials are valid${NC}"
        ACCOUNT_ID=$(aws sts get-caller-identity --profile "$AWS_PROFILE" --region "$AWS_REGION" --query 'Account' --output text)
        echo -e "  Account ID: ${ACCOUNT_ID}"
        return 0
    else
        echo -e "${RED}✗ AWS credentials are not valid${NC}"
        echo -e "${YELLOW}Attempting to login via SSO...${NC}"
        aws sso login --profile "$AWS_PROFILE"

        if aws sts get-caller-identity --profile "$AWS_PROFILE" --region "$AWS_REGION" &>/dev/null; then
            echo -e "${GREEN}✓ Successfully authenticated${NC}"
            return 0
        else
            echo -e "${RED}✗ Failed to authenticate. Please check your AWS configuration.${NC}"
            exit 1
        fi
    fi
}

# Function to check if a Glue job exists
check_job_exists() {
    local job_name=$1

    if aws glue get-job \
        --job-name "$job_name" \
        --profile "$AWS_PROFILE" \
        --region "$AWS_REGION" &>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Function to start a Glue job
start_glue_job() {
    local job_name=$1

    echo -e "${BLUE}→ Starting job: $job_name${NC}"

    # Check if job exists
    if ! check_job_exists "$job_name"; then
        echo -e "${RED}  ✗ Job does not exist: $job_name${NC}"
        return 1
    fi

    # Start the job
    RUN_ID=$(aws glue start-job-run \
        --job-name "$job_name" \
        --profile "$AWS_PROFILE" \
        --region "$AWS_REGION" \
        --query 'JobRunId' \
        --output text 2>/dev/null)

    if [ $? -eq 0 ] && [ ! -z "$RUN_ID" ]; then
        echo -e "${GREEN}  ✓ Started successfully${NC}"
        echo "    Run ID: $RUN_ID"
        JOB_RUN_IDS+=("$RUN_ID")
        JOB_NAMES+=("$job_name")
        return 0
    else
        echo -e "${RED}  ✗ Failed to start job${NC}"
        return 1
    fi
}

# Function to get job status
get_job_status() {
    local job_name=$1
    local run_id=$2

    aws glue get-job-run \
        --job-name "$job_name" \
        --run-id "$run_id" \
        --profile "$AWS_PROFILE" \
        --region "$AWS_REGION" \
        --query 'JobRun.JobRunState' \
        --output text 2>/dev/null
}

# Function to get job execution time
get_job_execution_time() {
    local job_name=$1
    local run_id=$2

    aws glue get-job-run \
        --job-name "$job_name" \
        --run-id "$run_id" \
        --profile "$AWS_PROFILE" \
        --region "$AWS_REGION" \
        --query 'JobRun.ExecutionTime' \
        --output text 2>/dev/null
}

# Function to get job error message
get_job_error() {
    local job_name=$1
    local run_id=$2

    aws glue get-job-run \
        --job-name "$job_name" \
        --run-id "$run_id" \
        --profile "$AWS_PROFILE" \
        --region "$AWS_REGION" \
        --query 'JobRun.ErrorMessage' \
        --output text 2>/dev/null
}

# Main execution
echo -e "${BLUE}====================================================================="
echo "RUNNING ALL HMU GLUE JOBS"
echo "====================================================================="
echo "AWS Profile: $AWS_PROFILE"
echo "AWS Region: $AWS_REGION"
echo "=====================================================================${NC}"
echo ""

# Check AWS credentials
check_aws_credentials
echo ""

# Deploy jobs if requested
if [ "$DEPLOY_FIRST" = true ]; then
    deploy_glue_jobs
fi

# Discover HMU* Glue jobs
if ! discover_glue_jobs; then
    echo -e "${RED}No HMU* Glue jobs found to run. Exiting.${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}====================================================================="
echo "Jobs to run: ${#GLUE_JOBS[@]}"
echo "=====================================================================${NC}"
echo ""

# Start time
START_TIME=$(date +%s)

# Submit all jobs
echo -e "${YELLOW}Starting Glue jobs...${NC}"
echo ""

SUBMITTED_COUNT=0
FAILED_TO_START=0

for job_name in "${GLUE_JOBS[@]}"; do
    if start_glue_job "$job_name"; then
        ((SUBMITTED_COUNT++))
    else
        ((FAILED_TO_START++))
    fi
    echo ""
done

if [ ${#JOB_RUN_IDS[@]} -eq 0 ]; then
    echo -e "${RED}No jobs were started successfully. Exiting.${NC}"
    exit 1
fi

echo -e "${YELLOW}====================================================================="
echo "MONITORING JOB EXECUTION"
echo "====================================================================="
echo "Successfully started: $SUBMITTED_COUNT jobs"
echo "Failed to start: $FAILED_TO_START jobs"
echo ""
echo "Monitoring job progress (this may take several minutes)..."
echo "=====================================================================${NC}"
echo ""

# Monitor all jobs
SUCCESS_COUNT=0
FAILED_COUNT=0
RUNNING_COUNT=${#JOB_RUN_IDS[@]}

while [ $RUNNING_COUNT -gt 0 ]; do
    RUNNING_COUNT=0

    for i in "${!JOB_RUN_IDS[@]}"; do
        RUN_ID="${JOB_RUN_IDS[$i]}"
        JOB_NAME="${JOB_NAMES[$i]}"

        # Skip if already marked as done (empty run ID)
        if [ -z "$RUN_ID" ]; then
            continue
        fi

        STATUS=$(get_job_status "$JOB_NAME" "$RUN_ID")

        case $STATUS in
            SUCCEEDED)
                EXEC_TIME=$(get_job_execution_time "$JOB_NAME" "$RUN_ID")
                echo -e "${GREEN}✓ $JOB_NAME: Completed successfully (${EXEC_TIME}s)${NC}"
                ((SUCCESS_COUNT++))
                JOB_RUN_IDS[$i]=""  # Mark as done
                ;;
            FAILED)
                ERROR_MSG=$(get_job_error "$JOB_NAME" "$RUN_ID")
                echo -e "${RED}✗ $JOB_NAME: Failed${NC}"
                if [ "$ERROR_MSG" != "None" ] && [ ! -z "$ERROR_MSG" ]; then
                    echo -e "${RED}  Error: $ERROR_MSG${NC}"
                fi
                ((FAILED_COUNT++))
                JOB_RUN_IDS[$i]=""  # Mark as done
                ;;
            STOPPED)
                echo -e "${YELLOW}⚠ $JOB_NAME: Stopped${NC}"
                ((FAILED_COUNT++))
                JOB_RUN_IDS[$i]=""  # Mark as done
                ;;
            RUNNING|STARTING|STOPPING)
                ((RUNNING_COUNT++))
                ;;
        esac
    done

    if [ $RUNNING_COUNT -gt 0 ]; then
        echo -e "${BLUE}Jobs still running: $RUNNING_COUNT${NC}"
        sleep 30  # Check every 30 seconds
    fi
done

# Calculate total execution time
END_TIME=$(date +%s)
TOTAL_TIME=$((END_TIME - START_TIME))
TOTAL_MIN=$((TOTAL_TIME / 60))
TOTAL_SEC=$((TOTAL_TIME % 60))

# Summary
echo ""
echo -e "${BLUE}====================================================================="
echo "EXECUTION SUMMARY"
echo "====================================================================="
echo -e "${GREEN}Successfully completed: $SUCCESS_COUNT jobs${NC}"
echo -e "${RED}Failed: $FAILED_COUNT jobs${NC}"
echo "Total execution time: ${TOTAL_MIN}m ${TOTAL_SEC}s"
echo "=====================================================================${NC}"

# If all jobs succeeded, suggest next steps
if [ $FAILED_COUNT -eq 0 ] && [ $SUCCESS_COUNT -gt 0 ]; then
    echo ""
    echo -e "${GREEN}✓ All jobs completed successfully!${NC}"
    echo ""
    echo -e "${YELLOW}Next steps:${NC}"
    echo "1. Run ./create_all_fhir_views.sh to create the materialized views"
    echo "2. Run ./refresh_all_fhir_views.sh to refresh the views with latest data"
else
    echo ""
    echo -e "${RED}⚠ Some jobs failed. Please check the CloudWatch logs for details.${NC}"
    exit 1
fi

echo ""
echo "Completed at: $(date '+%Y-%m-%d %H:%M:%S')"