#!/bin/bash

# ===================================================================
# RUN ALL GLUE JOBS SCRIPT
# ===================================================================
# This script runs HMU Glue jobs to populate Redshift tables
# By default, only runs jobs that previously failed or stopped
# Jobs are dynamically discovered from AWS Glue
# ===================================================================
#
# Usage:
#   ./run_all_glue_jobs.sh                      # Upload scripts & run only failed/stopped jobs
#   ./run_all_glue_jobs.sh --force              # Upload scripts & run all jobs regardless of status
#   ./run_all_glue_jobs.sh --deploy             # Upload scripts, deploy jobs first, then run
#   ./run_all_glue_jobs.sh --truncate           # Truncate tables before running jobs (prompts for confirmation)
#   ./run_all_glue_jobs.sh --truncate --no-prompt  # Truncate tables without prompting
#   ./run_all_glue_jobs.sh --include "HMUPatient,HMUObservation"  # Run only specific job(s)
#   ./run_all_glue_jobs.sh --skip HMUObservation  # Skip specific job(s)
#   ./run_all_glue_jobs.sh --skip "HMUObservation,HMUPatient"  # Skip multiple jobs
#   ./run_all_glue_jobs.sh --help               # Show this help message
#
# ===================================================================

# Configuration
AWS_PROFILE="${AWS_PROFILE:-to-prd-admin}"
AWS_REGION="${AWS_REGION:-us-east-2}"

# Parse command line arguments
DEPLOY_FIRST=false
SHOW_HELP=false
FORCE_RUN=false
TRUNCATE_TABLES=false
NO_PROMPT=false
SKIP_JOBS=""
INCLUDE_JOBS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --deploy)
            DEPLOY_FIRST=true
            shift
            ;;
        --force)
            FORCE_RUN=true
            shift
            ;;
        --truncate)
            TRUNCATE_TABLES=true
            shift
            ;;
        --no-prompt)
            NO_PROMPT=true
            shift
            ;;
        --include)
            INCLUDE_JOBS="$2"
            shift 2
            ;;
        --skip)
            SKIP_JOBS="$2"
            shift 2
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
    echo "Upload Python scripts to S3 and run HMU* Glue jobs to populate Redshift tables."
    echo ""
    echo "Options:"
    echo "  --deploy           Deploy Glue jobs from JSON templates before running"
    echo "  --force            Run all jobs regardless of their current state"
    echo "  --truncate         Truncate tables before running jobs (prompts for confirmation)"
    echo "  --no-prompt        Skip confirmation prompts (use with --truncate)"
    echo "  --include JOB_NAME Run only specific job(s), comma-separated for multiple"
    echo "  --skip JOB_NAME    Skip specific job(s), comma-separated for multiple"
    echo "  --help, -h         Show this help message and exit"
    echo ""
    echo "Environment Variables:"
    echo "  AWS_PROFILE  AWS profile to use (default: to-prd-admin)"
    echo "  AWS_REGION   AWS region (default: us-east-2)"
    echo ""
    echo "Examples:"
    echo "  ./run_all_glue_jobs.sh                              # Run only failed/stopped jobs"
    echo "  ./run_all_glue_jobs.sh --force                      # Run all jobs regardless of state"
    echo "  ./run_all_glue_jobs.sh --deploy                     # Deploy and run jobs"
    echo "  ./run_all_glue_jobs.sh --truncate                   # Truncate tables and run (with prompt)"
    echo "  ./run_all_glue_jobs.sh --truncate --no-prompt       # Truncate tables without prompt"
    echo "  ./run_all_glue_jobs.sh --include \"HMUPatient,HMUObservation\"  # Run only these jobs"
    echo "  ./run_all_glue_jobs.sh --skip HMUObservation        # Skip Observation job"
    echo "  ./run_all_glue_jobs.sh --skip \"HMUObservation,HMUPatient\"  # Skip multiple jobs"
    echo "  AWS_PROFILE=my-profile ./run_all_glue_jobs.sh       # Use different profile"
    exit 0
fi

# Array to store discovered Glue jobs
declare -a GLUE_JOBS=()

# Function to check if a job should be included (when --include is used)
should_include_job() {
    local job_name=$1

    # If no include list, include all
    if [ -z "$INCLUDE_JOBS" ]; then
        return 0
    fi

    # Convert comma-separated list to array
    IFS=',' read -ra INCLUDE_ARRAY <<< "$INCLUDE_JOBS"

    # Check if job is in include list
    for include_job in "${INCLUDE_ARRAY[@]}"; do
        # Trim whitespace
        include_job=$(echo "$include_job" | xargs)
        if [ "$job_name" = "$include_job" ]; then
            return 0
        fi
    done

    return 1
}

# Function to check if a job should be skipped
should_skip_job() {
    local job_name=$1

    # If no skip list, don't skip
    if [ -z "$SKIP_JOBS" ]; then
        return 1
    fi

    # Convert comma-separated list to array
    IFS=',' read -ra SKIP_ARRAY <<< "$SKIP_JOBS"

    # Check if job is in skip list
    for skip_job in "${SKIP_ARRAY[@]}"; do
        # Trim whitespace
        skip_job=$(echo "$skip_job" | xargs)
        if [ "$job_name" = "$skip_job" ]; then
            return 0
        fi
    done

    return 1
}

# Function to convert job name to table prefix
# HMUDiagnosticReport -> diagnostic_report
# HMUObservation -> observation
# HMUCarePlan -> care_plan
job_name_to_table_prefix() {
    local job_name=$1

    # Remove HMU prefix
    local name_without_prefix="${job_name#HMU}"

    # Convert CamelCase to snake_case
    # Insert underscore before uppercase letters and convert to lowercase
    local snake_case=$(echo "$name_without_prefix" | sed 's/\([A-Z]\)/_\1/g' | sed 's/^_//' | tr '[:upper:]' '[:lower:]')

    echo "$snake_case"
}

# Function to find DDL files for a job
find_ddl_files() {
    local job_name=$1
    local table_prefix=$(job_name_to_table_prefix "$job_name")

    # Find all DDL files in ddl/ folder that start with the table prefix
    local ddl_files=()
    if [ -d "ddl" ]; then
        while IFS= read -r file; do
            ddl_files+=("$file")
        done < <(find ddl -name "${table_prefix}*.sql" -type f 2>/dev/null | sort)
    fi

    # Return as array (one per line)
    printf '%s\n' "${ddl_files[@]}"
}

# Function to extract table name from DDL file
extract_table_name() {
    local ddl_file=$1

    # Extract table name from CREATE TABLE statement
    # Handles both "CREATE TABLE table_name" and "CREATE TABLE IF NOT EXISTS table_name"
    # Also handles "CREATE TABLE public.table_name" or "CREATE TABLE IF NOT EXISTS public.table_name"
    local table_name=$(grep -i "CREATE TABLE" "$ddl_file" | head -1 | sed -E 's/.*CREATE TABLE( IF NOT EXISTS)?[ ]+([a-zA-Z0-9_]*\.)?([a-zA-Z0-9_]+).*/\3/')

    echo "$table_name"
}

# Function to truncate tables for jobs that will run
truncate_tables_for_jobs() {
    local jobs_array=("$@")

    echo -e "${BLUE}====================================================================="
    echo "TRUNCATE TABLES FOR GLUE JOBS"
    echo "====================================================================="
    echo "Finding tables to truncate for ${#jobs_array[@]} jobs..."
    echo "=====================================================================${NC}"
    echo ""

    # Collect all tables to truncate (using indexed arrays instead of associative)
    local all_tables=()
    local total_tables=0

    for job_name in "${jobs_array[@]}"; do
        local table_prefix=$(job_name_to_table_prefix "$job_name")
        echo -e "${BLUE}→ $job_name (table prefix: ${table_prefix})${NC}"

        # Find DDL files
        local ddl_files=($(find_ddl_files "$job_name"))

        if [ ${#ddl_files[@]} -eq 0 ]; then
            echo -e "${YELLOW}  ! No DDL files found in ddl/${table_prefix}*.sql${NC}"
            continue
        fi

        for ddl_file in "${ddl_files[@]}"; do
            local table_name=$(extract_table_name "$ddl_file")
            if [ ! -z "$table_name" ]; then
                all_tables+=("$table_name")
                echo "  - $table_name (from $(basename $ddl_file))"
                ((total_tables++))
            fi
        done
        echo ""
    done

    if [ $total_tables -eq 0 ]; then
        echo -e "${YELLOW}No tables found to truncate. Skipping truncation.${NC}"
        return 0
    fi

    echo -e "${YELLOW}====================================================================="
    echo "TABLES TO TRUNCATE: $total_tables tables"
    echo "=====================================================================${NC}"
    echo ""

    # Prompt for confirmation unless --no-prompt is set
    if [ "$NO_PROMPT" = false ]; then
        echo -e "${RED}⚠  WARNING: This will delete all data from the above tables!${NC}"
        echo -e "${YELLOW}Do you want to continue? (yes/no): ${NC}"
        read -r response

        if [[ ! "$response" =~ ^[Yy][Ee][Ss]$ ]]; then
            echo "Truncation cancelled."
            exit 1
        fi
    fi

    echo ""
    echo -e "${BLUE}Truncating tables...${NC}"
    echo ""

    local truncated_count=0
    local failed_count=0

    # Truncate all collected tables
    for table_name in "${all_tables[@]}"; do
        echo -n "  Truncating $table_name... "

        # Execute TRUNCATE TABLE via Redshift Data API
        local statement_id=$(aws redshift-data execute-statement \
            --cluster-identifier prod-redshift-main-ue2 \
            --database dev \
            --db-user awsuser \
            --sql "TRUNCATE TABLE public.$table_name;" \
            --profile "$AWS_PROFILE" \
            --region "$AWS_REGION" \
            --query "Id" \
            --output text 2>/dev/null)

        if [ $? -eq 0 ] && [ ! -z "$statement_id" ]; then
            # Wait for statement to complete
            sleep 1
            local status=$(aws redshift-data describe-statement \
                --id "$statement_id" \
                --profile "$AWS_PROFILE" \
                --region "$AWS_REGION" \
                --query "Status" \
                --output text 2>/dev/null)

            if [ "$status" = "FINISHED" ]; then
                echo -e "${GREEN}✓ Success${NC}"
                ((truncated_count++))
            else
                local error=$(aws redshift-data describe-statement \
                    --id "$statement_id" \
                    --profile "$AWS_PROFILE" \
                    --region "$AWS_REGION" \
                    --query "Error" \
                    --output text 2>/dev/null)
                echo -e "${RED}✗ Failed: $error${NC}"
                ((failed_count++))
            fi
        else
            echo -e "${RED}✗ Failed to submit${NC}"
            ((failed_count++))
        fi
    done

    echo ""
    echo -e "${BLUE}====================================================================="
    echo "TRUNCATION SUMMARY"
    echo "====================================================================="
    echo -e "${GREEN}Successfully truncated: $truncated_count tables${NC}"
    echo -e "${RED}Failed: $failed_count tables${NC}"
    echo "=====================================================================${NC}"
    echo ""

    if [ $failed_count -gt 0 ]; then
        echo -e "${YELLOW}⚠ Some tables failed to truncate. Continue anyway? (y/n): ${NC}"
        read -r response
        if [[ ! "$response" =~ ^[Yy]$ ]]; then
            echo "Aborting."
            exit 1
        fi
    fi
}

# Function to upload Python scripts to S3
upload_scripts_to_s3() {
    echo -e "${BLUE}Uploading Python scripts to S3...${NC}"
    echo ""

    local upload_count=0
    local failed_count=0

    # Upload utility modules first
    echo -e "${BLUE}→ Uploading utility modules...${NC}"

    # Upload fhir_version_utils.py if it exists
    if [[ -f "fhir_version_utils.py" ]]; then
        echo "  Uploading fhir_version_utils.py..."
        if aws s3 cp "fhir_version_utils.py" "s3://aws-glue-assets-442042533707-us-east-2/scripts/fhir_version_utils.py" \
            --profile "$AWS_PROFILE" \
            --region "$AWS_REGION" &>/dev/null; then
            echo -e "${GREEN}  ✓ Successfully uploaded fhir_version_utils.py${NC}"
            ((upload_count++))
        else
            echo -e "${RED}  ✗ Failed to upload fhir_version_utils.py${NC}"
            ((failed_count++))
        fi
    else
        echo -e "${YELLOW}  ! fhir_version_utils.py not found${NC}"
    fi

    echo ""

    # Upload each Python script
    for job_dir in HMU*/; do
        job_name=$(basename "$job_dir")
        py_file="${job_dir}${job_name}.py"

        # Skip if Python file doesn't exist
        if [[ ! -f "$py_file" ]]; then
            echo -e "${YELLOW}  ! $job_name: Python file not found${NC}"
            continue
        fi

        # Upload to S3
        echo -e "${BLUE}→ Uploading $job_name.py...${NC}"
        if aws s3 cp "$py_file" "s3://aws-glue-assets-442042533707-us-east-2/scripts/${job_name}.py" \
            --profile "$AWS_PROFILE" \
            --region "$AWS_REGION" &>/dev/null; then
            echo -e "${GREEN}  ✓ Successfully uploaded $job_name.py${NC}"
            ((upload_count++))
        else
            echo -e "${RED}  ✗ Failed to upload $job_name.py${NC}"
            ((failed_count++))
        fi
    done

    echo ""
    echo -e "${BLUE}====================================================================="
    echo "SCRIPT UPLOAD SUMMARY"
    echo "====================================================================="
    echo -e "${GREEN}Successfully uploaded: $upload_count scripts${NC}"
    echo -e "${RED}Failed: $failed_count scripts${NC}"
    echo "=====================================================================${NC}"
    echo ""

    if [ $failed_count -gt 0 ]; then
        echo -e "${YELLOW}⚠ Some script uploads failed. Continue anyway? (y/n): ${NC}"
        read -r response
        if [[ ! "$response" =~ ^[Yy]$ ]]; then
            echo "Aborting."
            exit 1
        fi
    fi
}

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

    # Arrays to track all jobs and filtered jobs
    declare -a ALL_JOBS=()
    declare -a JOBS_TO_RUN=()
    declare -a JOBS_SKIPPED=()

    # Parse JSON array and populate ALL_JOBS array
    if [ ! -z "$jobs_json" ] && [ "$jobs_json" != "[]" ]; then
        # Convert JSON array to bash array using while loop
        while IFS= read -r job_name; do
            ALL_JOBS+=("$job_name")
        done < <(echo "$jobs_json" | jq -r '.[]' | sort)

        if [ ${#ALL_JOBS[@]} -gt 0 ]; then
            echo -e "${GREEN}✓ Discovered ${#ALL_JOBS[@]} HMU* Glue jobs${NC}"
            echo ""

            # Filter jobs based on their status (unless --force is used), include list, and skip list
            if [ "$FORCE_RUN" = true ]; then
                echo -e "${YELLOW}Force mode: Running all jobs regardless of status${NC}"
                # Still apply include and skip filters even in force mode
                for job in "${ALL_JOBS[@]}"; do
                    # Check include list first (if specified)
                    if ! should_include_job "$job"; then
                        JOBS_SKIPPED+=("$job")
                        echo -e "${BLUE}  ⊘ $job - not in include list${NC}"
                        continue
                    fi

                    if should_skip_job "$job"; then
                        JOBS_SKIPPED+=("$job")
                        echo -e "${BLUE}  ⊘ $job - skipped (--skip flag)${NC}"
                    else
                        GLUE_JOBS+=("$job")
                    fi
                done
            else
                echo -e "${BLUE}Checking job statuses to determine which jobs need to run...${NC}"
                for job in "${ALL_JOBS[@]}"; do
                    # Check include list first (if specified)
                    if ! should_include_job "$job"; then
                        JOBS_SKIPPED+=("$job")
                        echo -e "${BLUE}  ⊘ $job - not in include list${NC}"
                        continue
                    fi

                    # Check if job is in skip list
                    if should_skip_job "$job"; then
                        JOBS_SKIPPED+=("$job")
                        echo -e "${BLUE}  ⊘ $job - skipped (--skip flag)${NC}"
                        continue
                    fi

                    local status=$(get_latest_job_run_status "$job")
                    if should_run_job "$job"; then
                        GLUE_JOBS+=("$job")
                        echo -e "${YELLOW}  ✓ $job (last status: ${status:-never run}) - will run${NC}"
                    else
                        JOBS_SKIPPED+=("$job")
                        echo -e "${GREEN}  ✓ $job (last status: $status) - skipping${NC}"
                    fi
                done
            fi

            echo ""
            if [ ${#GLUE_JOBS[@]} -gt 0 ]; then
                echo -e "${GREEN}Jobs to run: ${#GLUE_JOBS[@]}${NC}"
                for job in "${GLUE_JOBS[@]}"; do
                    echo "  - $job"
                done
            else
                echo -e "${YELLOW}No jobs need to run (all are in successful/running state)${NC}"
                if [ ${#JOBS_SKIPPED[@]} -gt 0 ]; then
                    echo ""
                    echo -e "${BLUE}Skipped jobs (use --force to run anyway):${NC}"
                    for job in "${JOBS_SKIPPED[@]}"; do
                        echo "  - $job"
                    done
                fi
                return 1
            fi

            if [ ${#JOBS_SKIPPED[@]} -gt 0 ]; then
                echo ""
                echo -e "${BLUE}Skipped ${#JOBS_SKIPPED[@]} jobs with successful/running status${NC}"
            fi

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

# Function to get the latest job run status
get_latest_job_run_status() {
    local job_name=$1

    # Get the most recent job run for this job
    local latest_run=$(aws glue get-job-runs \
        --job-name "$job_name" \
        --profile "$AWS_PROFILE" \
        --region "$AWS_REGION" \
        --max-results 1 \
        --query 'JobRuns[0].JobRunState' \
        --output text 2>/dev/null)

    echo "$latest_run"
}

# Function to check if job should be run based on its latest status
should_run_job() {
    local job_name=$1

    # If force flag is set, always run
    if [ "$FORCE_RUN" = true ]; then
        return 0
    fi

    # Get latest job run status
    local status=$(get_latest_job_run_status "$job_name")

    # Run if status is FAILED, STOPPED, or if no previous runs exist (status is empty/None)
    case "$status" in
        FAILED|STOPPED|""|"None")
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

# Main execution
echo -e "${BLUE}====================================================================="
echo "RUNNING ALL HMU GLUE JOBS"
echo "====================================================================="
echo "AWS Profile: $AWS_PROFILE"
echo "AWS Region: $AWS_REGION"
if [ ! -z "$INCLUDE_JOBS" ]; then
    echo -e "${YELLOW}Only including jobs: $INCLUDE_JOBS${NC}"
fi
if [ ! -z "$SKIP_JOBS" ]; then
    echo -e "${YELLOW}Skipping jobs: $SKIP_JOBS${NC}"
fi
echo "=====================================================================${NC}"
echo ""

# Check AWS credentials
check_aws_credentials
echo ""

# Always upload scripts to S3 before running jobs
upload_scripts_to_s3

# Deploy jobs if requested
if [ "$DEPLOY_FIRST" = true ]; then
    deploy_glue_jobs
fi

# Discover HMU* Glue jobs and filter by status
if ! discover_glue_jobs; then
    if [ "$FORCE_RUN" = false ]; then
        echo ""
        echo -e "${BLUE}💡 Tip: Use --force to run all jobs regardless of their status${NC}"
    fi
    echo -e "${RED}No jobs need to run. Exiting.${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}====================================================================="
echo "Jobs to run: ${#GLUE_JOBS[@]}"
echo "=====================================================================${NC}"
echo ""

# Truncate tables if requested
if [ "$TRUNCATE_TABLES" = true ]; then
    truncate_tables_for_jobs "${GLUE_JOBS[@]}"
fi

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