#!/bin/bash

# Upload all Glue job Python scripts to S3
# This script uploads the .py files to the S3 location referenced in the JSON configs

set -e  # Exit on error

# Configuration
AWS_PROFILE="${AWS_PROFILE:-to-prd-admin}"
AWS_REGION="${AWS_REGION:-us-east-2}"
S3_BUCKET="s3://aws-glue-assets-442042533707-us-east-2/scripts/"

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

# Main execution
main() {
    log_info "Starting script upload to S3"
    log_info "AWS Profile: $AWS_PROFILE"
    log_info "AWS Region: $AWS_REGION"
    log_info "S3 Bucket: $S3_BUCKET"
    echo

    # Upload utility modules first
    log_info "Uploading utility modules..."

    # Upload fhir_version_utils.py if it exists
    if [[ -f "fhir_version_utils.py" ]]; then
        # Add deployment timestamp to the file
        temp_file="/tmp/fhir_version_utils_deploy.py"
        timestamp=$(date -u +"%Y-%m-%d %H:%M:%S UTC")

        # Check if file already has a deployment timestamp comment
        if head -n 3 "fhir_version_utils.py" | grep -q "# Deployed:"; then
            # Update existing timestamp
            sed "1,3s/# Deployed: .*/# Deployed: $timestamp/" "fhir_version_utils.py" > "$temp_file"
        else
            # Add new timestamp at the top
            echo "# Deployed: $timestamp" > "$temp_file"
            cat "fhir_version_utils.py" >> "$temp_file"
        fi

        log_info "Uploading fhir_version_utils.py (timestamp: $timestamp)..."
        if aws s3 cp "$temp_file" "${S3_BUCKET}fhir_version_utils.py" \
            --profile "$AWS_PROFILE" \
            --region "$AWS_REGION"; then
            log_info "✅ Successfully uploaded fhir_version_utils.py"
            # Update local file with timestamp
            cp "$temp_file" "fhir_version_utils.py"
            rm -f "$temp_file"
        else
            log_error "Failed to upload fhir_version_utils.py"
            rm -f "$temp_file"
        fi
    else
        log_warning "fhir_version_utils.py not found"
    fi

    echo

    # Upload each Python script
    for job_dir in HMU*/; do
        job_name=$(basename "$job_dir")
        py_file="${job_dir}${job_name}.py"

        # Skip if Python file doesn't exist
        if [[ ! -f "$py_file" ]]; then
            log_warning "$job_name: Python file not found"
            continue
        fi

        # Add deployment timestamp to the Python file
        temp_python="/tmp/${job_name}_deploy.py"
        timestamp=$(date -u +"%Y-%m-%d %H:%M:%S UTC")

        # Check if file already has a deployment timestamp comment
        if head -n 3 "$py_file" | grep -q "# Deployed:"; then
            # Update existing timestamp
            sed "1,3s/# Deployed: .*/# Deployed: $timestamp/" "$py_file" > "$temp_python"
        else
            # Add new timestamp at the top
            echo "# Deployed: $timestamp" > "$temp_python"
            cat "$py_file" >> "$temp_python"
        fi

        # Upload to S3
        log_info "Uploading $job_name.py (timestamp: $timestamp)..."
        if aws s3 cp "$temp_python" "${S3_BUCKET}${job_name}.py" \
            --profile "$AWS_PROFILE" \
            --region "$AWS_REGION"; then
            log_info "✅ Successfully uploaded $job_name.py"
            # Update local file with timestamp
            cp "$temp_python" "$py_file"
            rm -f "$temp_python"
        else
            log_error "Failed to upload $job_name.py"
            rm -f "$temp_python"
        fi
    done

    echo
    log_info "✨ Script upload complete!"
}

# Execute main function
main "$@"