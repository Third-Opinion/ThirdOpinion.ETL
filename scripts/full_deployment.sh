#!/bin/bash

# Complete deployment script for AWS Glue jobs
# 1. Fixes JSON casing
# 2. Refreshes SSO token
# 3. Uploads Python scripts to S3
# 4. Creates/Updates Glue jobs

set -e  # Exit on error

# Configuration
AWS_PROFILE="${AWS_PROFILE:-to-prd-admin}"
AWS_REGION="${AWS_REGION:-us-east-2}"
S3_BUCKET="s3://aws-glue-assets-442042533707-us-east-2/scripts/"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

log_section() {
    echo
    echo -e "${BLUE}===================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}===================================================${NC}"
}

# Main execution
main() {
    log_section "🚀 FULL GLUE JOB DEPLOYMENT PROCESS"
    log_info "AWS Profile: $AWS_PROFILE"
    log_info "AWS Region: $AWS_REGION"

    # Step 1: Fix JSON casing
    log_section "📝 Step 1: Fixing JSON File Casing"
    if [[ -f "fix_json_casing.py" ]]; then
        python3 fix_json_casing.py
    else
        log_warning "fix_json_casing.py not found, skipping JSON fix"
    fi

    # Step 2: Refresh SSO token
    log_section "🔐 Step 2: AWS SSO Authentication"
    log_info "Please complete SSO login in your browser if prompted..."

    # First, try to get current identity
    if aws sts get-caller-identity --profile "$AWS_PROFILE" --region "$AWS_REGION" &>/dev/null; then
        log_info "✅ AWS credentials are valid"
    else
        log_warning "SSO token expired or invalid. Initiating login..."
        aws sso login --profile "$AWS_PROFILE"

        # Verify login worked
        if aws sts get-caller-identity --profile "$AWS_PROFILE" --region "$AWS_REGION" &>/dev/null; then
            log_info "✅ Successfully authenticated"
        else
            log_error "Authentication failed. Please check your AWS SSO configuration."
            exit 1
        fi
    fi

    # Step 3: Upload Python scripts to S3
    log_section "☁️  Step 3: Uploading Python Scripts to S3"
    log_info "Target S3 bucket: $S3_BUCKET"

    upload_count=0
    upload_failed=0

    for job_dir in HMU*/; do
        job_name=$(basename "$job_dir")
        py_file="${job_dir}${job_name}.py"

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

        echo -n "Uploading $job_name.py (${timestamp})... "
        if aws s3 cp "$temp_python" "${S3_BUCKET}${job_name}.py" \
            --profile "$AWS_PROFILE" \
            --region "$AWS_REGION" &>/dev/null; then
            echo -e "${GREEN}✅${NC}"
            # Update local file with timestamp
            cp "$temp_python" "$py_file"
            rm -f "$temp_python"
            ((upload_count++))
        else
            echo -e "${RED}❌${NC}"
            rm -f "$temp_python"
            ((upload_failed++))
        fi
    done

    log_info "Uploaded $upload_count scripts successfully"
    if [[ $upload_failed -gt 0 ]]; then
        log_warning "$upload_failed scripts failed to upload"
    fi

    # Step 4: Deploy Glue jobs
    log_section "🔧 Step 4: Deploying Glue Jobs"

    # Execute the deployment script
    if [[ -f "deploy_glue_jobs.sh" ]]; then
        # Remove SSO login from deploy script since we already did it
        ./deploy_glue_jobs.sh | grep -v "Refreshing AWS SSO"
    else
        log_error "deploy_glue_jobs.sh not found"
        exit 1
    fi

    # Final summary
    log_section "✨ DEPLOYMENT COMPLETE"
    log_info "Process completed. Check the summary above for any failures."
    log_info ""
    log_info "Next steps:"
    log_info "1. Verify jobs in AWS Glue Console"
    log_info "2. Test each job with sample data"
    log_info "3. Monitor CloudWatch logs for any issues"
}

# Execute main function
main "$@"