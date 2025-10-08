#!/bin/bash

# Interactive Glue Job Runner
# This script lists Glue jobs, shows status/errors, updates code, and allows interactive execution
# Usage: ./run_glue_job.sh [--update-only] [--status|-s]

set -e

# Parse command line arguments
UPDATE_ONLY=false
STATUS_ONLY=false

for arg in "$@"; do
    case $arg in
        --update-only)
            UPDATE_ONLY=true
            shift
            ;;
        --status|-s)
            STATUS_ONLY=true
            shift
            ;;
        *)
            echo "Unknown option: $arg"
            echo "Usage: $0 [--update-only] [--status|-s]"
            exit 1
            ;;
    esac
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== AWS Glue Job Runner ===${NC}"
if [[ "$UPDATE_ONLY" == "true" ]]; then
    echo -e "${YELLOW}Running in UPDATE-ONLY mode${NC}"
elif [[ "$STATUS_ONLY" == "true" ]]; then
    echo -e "${YELLOW}Running in STATUS-ONLY mode${NC}"
fi
echo

# Function to get list of Glue jobs
get_glue_jobs() {
    echo -e "${YELLOW}Fetching Glue jobs...${NC}"
    aws glue get-jobs --query 'Jobs[].Name' --output table
}

# Function to get job run history and status
get_job_status() {
    local job_name=$1
    echo -e "${YELLOW}Getting status for job: ${job_name}${NC}"
    echo
    
    # Get the most recent job run
    local job_run_info=$(aws glue get-job-runs --job-name "$job_name" --max-results 1 --query 'JobRuns[0]')
    
    if [[ "$job_run_info" == "null" ]]; then
        echo -e "${YELLOW}No previous runs found for this job.${NC}"
        return
    fi
    
    # Parse job run details
    local job_run_id=$(echo "$job_run_info" | jq -r '.Id // "N/A"')
    local job_state=$(echo "$job_run_info" | jq -r '.JobRunState // "N/A"')
    local started_on=$(echo "$job_run_info" | jq -r '.StartedOn // "N/A"')
    local completed_on=$(echo "$job_run_info" | jq -r '.CompletedOn // "N/A"')
    local error_message=$(echo "$job_run_info" | jq -r '.ErrorMessage // ""')
    
    echo -e "${BLUE}Last Job Run Details:${NC}"
    echo "  Job Run ID: $job_run_id"
    echo "  Status: $job_state"
    echo "  Started: $started_on"
    echo "  Completed: $completed_on"
    
    if [[ "$job_state" == "FAILED" && -n "$error_message" && "$error_message" != "null" ]]; then
        echo -e "${RED}Error Message:${NC}"
        echo "  $error_message"
    fi
    
    echo
}

# Function to show status summary for all jobs
show_status_summary() {
    echo -e "${BLUE}=== Glue Jobs Status Summary ===${NC}"
    echo
    
    # Get list of all jobs
    local jobs_list=$(aws glue get-jobs --query 'Jobs[].Name' --output text | tr '\t' '\n')
    
    if [[ -z "$jobs_list" ]]; then
        echo -e "${RED}No Glue jobs found${NC}"
        return 1
    fi
    
    # Header
    printf "%-25s %-12s %-20s %-20s %s\n" "JOB NAME" "STATUS" "STARTED" "COMPLETED" "ERROR"
    printf "%-25s %-12s %-20s %-20s %s\n" "--------" "------" "-------" "---------" "-----"
    
    # Process each job
    while IFS= read -r job_name; do
        if [[ -n "$job_name" ]]; then
            # Get the most recent job run
            local job_run_info=$(aws glue get-job-runs --job-name "$job_name" --max-results 1 --query 'JobRuns[0]' 2>/dev/null)
            
            if [[ "$job_run_info" == "null" ]] || [[ -z "$job_run_info" ]]; then
                printf "%-25s %-12s %-20s %-20s %s\n" "$job_name" "NO RUNS" "-" "-" "-"
            else
                local job_state=$(echo "$job_run_info" | jq -r '.JobRunState // "UNKNOWN"')
                local started_on=$(echo "$job_run_info" | jq -r '.StartedOn // "N/A"')
                local completed_on=$(echo "$job_run_info" | jq -r '.CompletedOn // "N/A"')
                local error_message=$(echo "$job_run_info" | jq -r '.ErrorMessage // ""')
                
                # Format dates
                local started_formatted="N/A"
                local completed_formatted="N/A"
                
                if [[ "$started_on" != "N/A" && "$started_on" != "null" ]]; then
                    started_formatted=$(date -d "$started_on" "+%m/%d %H:%M" 2>/dev/null || echo "$started_on")
                fi
                
                if [[ "$completed_on" != "N/A" && "$completed_on" != "null" ]]; then
                    completed_formatted=$(date -d "$completed_on" "+%m/%d %H:%M" 2>/dev/null || echo "$completed_on")
                fi
                
                # Truncate job name if too long
                local display_name="$job_name"
                if [[ ${#job_name} -gt 24 ]]; then
                    display_name="${job_name:0:21}..."
                fi
                
                # Truncate error message if too long
                local display_error=""
                if [[ "$job_state" == "FAILED" && -n "$error_message" && "$error_message" != "null" ]]; then
                    if [[ ${#error_message} -gt 50 ]]; then
                        display_error="${error_message:0:47}..."
                    else
                        display_error="$error_message"
                    fi
                fi
                
                # Color code status
                local colored_status
                case "$job_state" in
                    "SUCCEEDED")
                        colored_status="${GREEN}$job_state${NC}"
                        ;;
                    "FAILED")
                        colored_status="${RED}$job_state${NC}"
                        ;;
                    "RUNNING")
                        colored_status="${BLUE}$job_state${NC}"
                        ;;
                    "STARTING")
                        colored_status="${YELLOW}$job_state${NC}"
                        ;;
                    *)
                        colored_status="$job_state"
                        ;;
                esac
                
                printf "%-25s %-20s %-20s %-20s %s\n" "$display_name" "$colored_status" "$started_formatted" "$completed_formatted" "$display_error"
            fi
        fi
    done <<< "$jobs_list"
    
    echo
}

# Function to get mapped folder name for a job
# Returns the mapped folder name or empty string if no mapping exists
get_mapped_folder() {
    local job_name="$1"

    # Define mappings using a case statement for compatibility
    # New job names follow HMU* pattern without spaces
    case "$job_name" in
        "HMUObservation")
            echo "HMUObservation"
            ;;
        "HMUPractitioner")
            echo "HMUPractitioner"
            ;;
        "HMUProcedure")
            echo "HMUProcedure"
            ;;
        "HMUDocumentReference")
            echo "HMUDocumentReference"
            ;;
        "HMUMedicationDispense")
            echo "HMUMedicationDispense"
            ;;
        "HMUMedicationRequest")
            echo "HMUMedicationRequest"
            ;;
        "HMUMedication")
            echo "HMUMedication"
            ;;
        "HMUPatient")
            echo "HMUPatient"
            ;;
        "HMUEncounter")
            echo "HMUEncounter"
            ;;
        "HMUCondition")
            echo "HMUCondition"
            ;;
        "HMUCarePlan")
            echo "HMUCarePlan"
            ;;
        "HMUDiagnosticReport")
            echo "HMUDiagnosticReport"
            ;;
        "HMUAllergyIntolerance")
            echo "HMUAllergyIntolerance"
            ;;
        # Legacy mappings for backward compatibility
        "HMU Observation")
            echo "HMUObservation"
            ;;
        "HMU Practitioner")
            echo "HMUPractitioner"
            ;;
        "HMU Procedure")
            echo "HMUProcedure"
            ;;
        "HMU DocumentReference")
            echo "HMUDocumentReference"
            ;;
        "HMU MedicationDispense")
            echo "HMUMedicationDispense"
            ;;
        "HMU Patient"|"HMU Encounter"|"HMU Condition"|"HMU MedicationRequest")
            # Convert space-separated names to camelCase format
            echo "${job_name// /}"
            ;;
        *)
            # No mapping found, try direct folder match
            echo "$job_name"
            ;;
    esac
}

# Function to find Python file for job
find_job_python_file() {
    local job_name=$1
    local python_file=""
    local mapped_folder=""
    
    # Check if there's a mapping for this job name
    mapped_folder=$(get_mapped_folder "$job_name")
    if [[ -n "$mapped_folder" ]]; then
        echo -e "${BLUE}Using mapped folder: $mapped_folder${NC}" >&2
        if [[ -d "$mapped_folder" ]]; then
            # First try to find a file matching the folder name
            local folder_name=$(basename "$mapped_folder")
            python_file=$(find "$mapped_folder" -name "${folder_name}.py" | head -1)
            # If not found, fall back to any .py file (excluding common utility files)
            if [[ -z "$python_file" ]]; then
                python_file=$(find "$mapped_folder" -name "*.py" ! -name "spark_utils.py" ! -name "fhir_*" | head -1)
            fi
        fi
    fi
    
    # If not found via mapping, try exact folder match
    if [[ -z "$python_file" ]] && [[ -d "$job_name" ]]; then
        python_file=$(find "$job_name" -name "*.py" | head -1)
    fi
    
    # If not found, try case-insensitive match
    if [[ -z "$python_file" ]]; then
        for dir in */; do
            if [[ "${dir%/}" =~ ^${job_name}$ ]] || [[ "${dir%/,,}" =~ ^${job_name,,}$ ]]; then
                python_file=$(find "$dir" -name "*.py" | head -1)
                break
            fi
        done
    fi
    
    # If still not found, try partial match
    if [[ -z "$python_file" ]]; then
        for dir in */; do
            if [[ "${dir%/}" == *"$job_name"* ]] || [[ "$job_name" == *"${dir%/}"* ]]; then
                python_file=$(find "$dir" -name "*.py" | head -1)
                break
            fi
        done
    fi
    
    echo "$python_file"
}

# Function to update Glue job code
update_glue_job() {
    local job_name=$1
    echo -e "${YELLOW}Updating Glue job code for: ${job_name}${NC}"
    
    # Get current git branch
    local current_branch=$(git branch --show-current)
    echo "Current branch: $current_branch"
    
    # Ensure we're up to date with remote
    echo -e "${YELLOW}Pulling latest changes from remote...${NC}"
    git pull origin "$current_branch" || {
        echo -e "${RED}Warning: Could not pull from remote. Proceeding with local code.${NC}"
    }
    
    # Find the Python file for this job
    local python_file=$(find_job_python_file "$job_name")
    
    if [[ -z "$python_file" ]]; then
        echo -e "${RED}Error: Could not find Python file for job '$job_name'${NC}"
        echo "Looked for folder named '$job_name' containing a .py file"
        return 1
    fi
    
    echo "Found Python file: $python_file"
    
    # Check if file exists and is readable
    if [[ ! -f "$python_file" ]]; then
        echo -e "${RED}Error: Python file '$python_file' not found${NC}"
        return 1
    fi
    
    # Get current S3 script location from the job
    local current_s3_location=$(aws glue get-job --job-name "$job_name" --query 'Job.Command.ScriptLocation' --output text 2>/dev/null)
    
    if [[ "$current_s3_location" == "None" ]] || [[ -z "$current_s3_location" ]]; then
        echo -e "${RED}Error: Could not get current S3 script location for job${NC}"
        return 1
    fi
    
    echo "Current S3 location: $current_s3_location"
    
    # Upload the updated script to S3
    echo -e "${YELLOW}Uploading script to S3...${NC}"
    aws s3 cp "$python_file" "$current_s3_location"
    
    if [[ $? -ne 0 ]]; then
        echo -e "${RED}Failed to upload script to S3${NC}"
        return 1
    fi
    
    echo -e "${GREEN}Job code updated successfully!${NC}"
    echo "Script location: $current_s3_location"
}

# Function to monitor job status
monitor_job_status() {
    local job_name=$1
    local job_run_id=$2
    
    echo -e "${YELLOW}Monitoring job status (polling every 30 seconds)...${NC}"
    echo -e "${BLUE}Press Ctrl+C to stop monitoring and exit${NC}"
    echo
    
    local start_time=$(date +%s)
    local last_state=""
    
    while true; do
        # Get current job run status
        local job_info=$(aws glue get-job-run --job-name "$job_name" --run-id "$job_run_id" --query 'JobRun' 2>/dev/null)
        
        if [[ $? -ne 0 ]] || [[ "$job_info" == "null" ]]; then
            echo -e "${RED}Error: Could not retrieve job status${NC}"
            break
        fi
        
        local current_state=$(echo "$job_info" | jq -r '.JobRunState // "UNKNOWN"')
        local started_on=$(echo "$job_info" | jq -r '.StartedOn // "N/A"')
        local completed_on=$(echo "$job_info" | jq -r '.CompletedOn // "N/A"')
        local error_message=$(echo "$job_info" | jq -r '.ErrorMessage // ""')
        local execution_time=$(echo "$job_info" | jq -r '.ExecutionTime // 0')
        
        # Calculate elapsed time
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        local elapsed_min=$((elapsed / 60))
        local elapsed_sec=$((elapsed % 60))
        
        # Clear previous line and print current status
        echo -ne "\r\033[K"
        
        case "$current_state" in
            "STARTING")
                echo -ne "${YELLOW}[${elapsed_min}m ${elapsed_sec}s] Status: STARTING...${NC}"
                ;;
            "RUNNING")
                echo -ne "${BLUE}[${elapsed_min}m ${elapsed_sec}s] Status: RUNNING...${NC}"
                ;;
            "SUCCEEDED")
                echo -e "\n${GREEN}✓ Job completed successfully!${NC}"
                echo "Execution time: ${execution_time} seconds"
                echo "Completed at: $completed_on"
                break
                ;;
            "FAILED")
                echo -e "\n${RED}✗ Job failed!${NC}"
                if [[ -n "$error_message" && "$error_message" != "null" ]]; then
                    echo -e "${RED}Error: $error_message${NC}"
                fi
                echo "Failed at: $completed_on"
                break
                ;;
            "STOPPED")
                echo -e "\n${YELLOW}⚠ Job was stopped${NC}"
                echo "Stopped at: $completed_on"
                break
                ;;
            "TIMEOUT")
                echo -e "\n${RED}⏰ Job timed out${NC}"
                echo "Timed out at: $completed_on"
                break
                ;;
            *)
                echo -ne "${YELLOW}[${elapsed_min}m ${elapsed_sec}s] Status: $current_state${NC}"
                ;;
        esac
        
        # Break if job is in a terminal state
        if [[ "$current_state" =~ ^(SUCCEEDED|FAILED|STOPPED|TIMEOUT)$ ]]; then
            break
        fi
        
        # Store last state for change detection
        last_state="$current_state"
        
        # Wait 30 seconds before next poll
        sleep 30
    done
    
    echo
}

# Function to run a Glue job
run_glue_job() {
    local job_name=$1
    echo -e "${GREEN}Starting Glue job: ${job_name}${NC}"
    
    local job_run_id=$(aws glue start-job-run --job-name "$job_name" --query 'JobRunId' --output text)
    
    if [[ $? -eq 0 ]]; then
        echo -e "${GREEN}Job started successfully!${NC}"
        echo "Job Run ID: $job_run_id"
        echo
        
        # Ask if user wants to monitor the job
        echo -e "${YELLOW}Do you want to monitor job progress? (Y/n):${NC}"
        read -p "> " monitor_choice
        
        if [[ ! "$monitor_choice" =~ ^[Nn]$ ]]; then
            monitor_job_status "$job_name" "$job_run_id"
        else
            echo -e "${YELLOW}Job is running in background.${NC}"
            echo -e "${YELLOW}You can monitor manually using:${NC}"
            echo "aws glue get-job-run --job-name \"$job_name\" --run-id \"$job_run_id\""
        fi
    else
        echo -e "${RED}Failed to start job${NC}"
        exit 1
    fi
}

# Main script logic
main() {
    # Check if AWS CLI is configured
    if ! aws sts get-caller-identity >/dev/null 2>&1; then
        echo -e "${RED}Error: AWS CLI not configured or no valid credentials${NC}"
        exit 1
    fi
    
    # If status-only mode, show summary and exit
    if [[ "$STATUS_ONLY" == "true" ]]; then
        show_status_summary
        exit 0
    fi
    
    # Get list of Glue jobs
    echo -e "${BLUE}Available Glue Jobs:${NC}"
    
    # Get jobs as separate lines instead of tab-separated
    jobs_list=$(aws glue get-jobs --query 'Jobs[].Name' --output text | tr '\t' '\n')
    
    if [[ -z "$jobs_list" ]]; then
        echo -e "${RED}No Glue jobs found${NC}"
        exit 1
    fi
    
    # Convert to array
    jobs_array=()
    while IFS= read -r line; do
        if [[ -n "$line" ]]; then
            jobs_array+=("$line")
        fi
    done <<< "$jobs_list"
    
    echo
    echo "Select a job to run:"
    for i in "${!jobs_array[@]}"; do
        echo "  $((i+1)). ${jobs_array[$i]}"
    done
    
    echo
    read -p "Enter job number (1-${#jobs_array[@]}): " selection
    
    # Validate selection
    if ! [[ "$selection" =~ ^[0-9]+$ ]] || [ "$selection" -lt 1 ] || [ "$selection" -gt "${#jobs_array[@]}" ]; then
        echo -e "${RED}Invalid selection${NC}"
        exit 1
    fi
    
    # Get selected job name
    selected_job="${jobs_array[$((selection-1))]}"
    echo
    echo -e "${BLUE}Selected job: ${selected_job}${NC}"
    echo
    
    # Show job status
    get_job_status "$selected_job"
    
    # Update job code with latest from current branch
    echo -e "${YELLOW}Updating job with latest code...${NC}"
    if ! update_glue_job "$selected_job"; then
        echo -e "${RED}Failed to update job code. Exiting.${NC}"
        exit 1
    fi
    echo
    
    # If update-only mode, exit here
    if [[ "$UPDATE_ONLY" == "true" ]]; then
        echo -e "${GREEN}Job updated successfully. Exiting (update-only mode).${NC}"
        exit 0
    fi
    
    # Ask to run the job
    echo -e "${YELLOW}Do you want to run this job? (y/N):${NC}"
    read -p "> " confirm
    
    if [[ "$confirm" =~ ^[Yy]$ ]]; then
        run_glue_job "$selected_job"
    else
        echo -e "${YELLOW}Job execution cancelled${NC}"
    fi
}

# Run main function
main "$@"