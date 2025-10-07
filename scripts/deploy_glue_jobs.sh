#!/bin/bash

# Deploy AWS Glue Jobs from HMU*.json configuration files
# This script creates or updates Glue jobs using the JSON configuration files

set -e  # Exit on error

# Configuration
AWS_PROFILE="${AWS_PROFILE:-to-prd-admin}"
AWS_REGION="${AWS_REGION:-us-east-2}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to log messages
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Function to check if a Glue job exists
job_exists() {
    local job_name="$1"
    aws glue get-job --name "$job_name" --profile "$AWS_PROFILE" --region "$AWS_REGION" &>/dev/null
    return $?
}

# Function to create or update a Glue job
deploy_job() {
    local json_file="$1"
    local job_dir=$(dirname "$json_file")
    local job_name=$(basename "$job_dir")

    # Skip non-job directories
    if [[ "$job_name" == "HMUMyJob" ]]; then
        log_warning "Skipping $job_name (test job)"
        return 0
    fi

    # Check if JSON file exists
    if [[ ! -f "$json_file" ]]; then
        log_error "JSON file not found: $json_file"
        return 1
    fi

    # Extract job name from JSON file
    local actual_job_name=$(jq -r '.Name' "$json_file" 2>/dev/null || echo "$job_name")

    log_info "Processing $actual_job_name from $json_file"

    # Copy Python script to S3
    local python_file="${job_dir}/${actual_job_name}.py"
    if [ -f "$python_file" ]; then
        local s3_location=$(jq -r '.Command.ScriptLocation' "$json_file")
        if [ "$s3_location" != "null" ] && [ ! -z "$s3_location" ]; then
            # Add deployment timestamp to the Python file
            local temp_python="/tmp/${actual_job_name}_deploy.py"
            local timestamp=$(date -u +"%Y-%m-%d %H:%M:%S UTC")

            # Check if file already has a deployment timestamp comment
            if head -n 3 "$python_file" | grep -q "# Deployed:"; then
                # Update existing timestamp
                sed "1,3s/# Deployed: .*/# Deployed: $timestamp/" "$python_file" > "$temp_python"
            else
                # Add new timestamp at the top
                echo "# Deployed: $timestamp" > "$temp_python"
                cat "$python_file" >> "$temp_python"
            fi

            log_info "Uploading Python script to $s3_location (with timestamp: $timestamp)"
            if aws s3 cp "$temp_python" "$s3_location" --profile "$AWS_PROFILE" --region "$AWS_REGION"; then
                log_info "✅ Successfully uploaded script to S3"
                # Also update the local file with the timestamp
                cp "$temp_python" "$python_file"
                rm -f "$temp_python"
            else
                log_error "Failed to upload script to S3"
                rm -f "$temp_python"
                return 1
            fi
        fi
    else
        log_warning "Python file not found: $python_file"
    fi

    # Check if job exists
    if job_exists "$actual_job_name"; then
        log_info "Job $actual_job_name exists. Updating..."
        # Create temporary file without Name field
        local temp_file="/tmp/${actual_job_name}_update.json"
        jq 'del(.Name)' "$json_file" > "$temp_file"
        # Update existing job
        if aws glue update-job \
            --job-name "$actual_job_name" \
            --job-update "file://$temp_file" \
            --profile "$AWS_PROFILE" \
            --region "$AWS_REGION"; then
            log_info "✅ Successfully updated $actual_job_name"
            rm -f "$temp_file"
        else
            log_error "Failed to update $actual_job_name"
            rm -f "$temp_file"
            return 1
        fi
    else
        log_info "Job $actual_job_name does not exist. Creating..."
        # Create new job
        if aws glue create-job \
            --cli-input-json "file://$json_file" \
            --profile "$AWS_PROFILE" \
            --region "$AWS_REGION"; then
            log_info "✅ Successfully created $actual_job_name"
        else
            log_error "Failed to create $actual_job_name"
            return 1
        fi
    fi

    return 0
}

# Main execution
main() {
    log_info "Starting Glue job deployment"
    log_info "AWS Profile: $AWS_PROFILE"
    log_info "AWS Region: $AWS_REGION"

    # Refresh SSO token if needed
    log_info "Refreshing AWS SSO token..."
    if aws sso login --profile "$AWS_PROFILE" 2>/dev/null; then
        log_info "SSO token refreshed"
    else
        log_warning "Could not refresh SSO token - may already be valid"
    fi

    echo

    # Array to store results
    declare -a successful_jobs=()
    declare -a failed_jobs=()

    # Find all HMU job JSON files, excluding HMUTemplate.json
    while IFS= read -r json_file; do
        if [[ "$(basename "$json_file")" == "HMUTemplate.json" ]]; then
            continue
        fi

        if deploy_job "$json_file"; then
            successful_jobs+=("$(basename "$(dirname "$json_file")")")
        else
            failed_jobs+=("$(basename "$(dirname "$json_file")")")
        fi
        echo
    done < <(find . -name "HMU*.json" -path "*/HMU*/*" -type f | grep -v HMUTemplate.json | sort)

    # Print summary
    echo
    log_info "==================== DEPLOYMENT SUMMARY ===================="

    if [[ ${#successful_jobs[@]} -gt 0 ]]; then
        log_info "Successfully deployed ${#successful_jobs[@]} job(s):"
        for job in "${successful_jobs[@]}"; do
            echo "  ✅ $job"
        done
    fi

    if [[ ${#failed_jobs[@]} -gt 0 ]]; then
        log_error "Failed to deploy ${#failed_jobs[@]} job(s):"
        for job in "${failed_jobs[@]}"; do
            echo "  ❌ $job"
        done
        exit 1
    else
        log_info "All jobs deployed successfully!"
    fi
}

# Run main function
main "$@"