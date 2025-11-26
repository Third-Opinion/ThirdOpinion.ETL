#!/bin/bash
# Create Glue Triggers for ETL Jobs
#
# High-frequency jobs (HMUObservation, HMUCondition, HMUDocumentReference):
#   - Weekend (Sat-Sun): Every 3 hours, 7AM-11:59AM UTC-3 (10AM-2PM UTC)
#   - Weekdays (Mon-Fri): Every hour, 2AM-10PM UTC-3 (5AM-1AM UTC)
#
# All other jobs:
#   - Mon-Sat at 11PM UTC-3 (2AM UTC next day)
#
# Usage:
#   ./create_glue_schedules.sh                  # Create all triggers
#   ./create_glue_schedules.sh --delete-existing # Delete and recreate triggers
#   ./create_glue_schedules.sh --pause-all      # Pause all HMU triggers
#   ./create_glue_schedules.sh --resume-all     # Resume all HMU triggers
#   ./create_glue_schedules.sh --status         # Show status of all triggers

set -e

# Configuration
AWS_REGION="${AWS_REGION:-us-east-2}"
ACTION="${1:-create}"

# High-frequency jobs
HIGH_FREQ_JOBS=(
    "HMUObservation"
    "HMUCondition"
    "HMUDocumentReference"
)

# All other jobs (excluding HMUMyJob which appears to be a template)
OTHER_JOBS=(
    "HMUAllergyIntolerance"
    "HMUCarePlan"
    "HMUDiagnosticReport"
    "HMUEncounter"
    "HMUMedication"
    "HMUMedicationDispense"
    "HMUMedicationRequest"
    "HMUPatient"
    "HMUPractitioner"
    "HMUProcedure"
)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to get all HMU trigger names
get_all_hmu_triggers() {
    aws glue list-triggers --region "$AWS_REGION" \
        --query 'TriggerNames[?contains(@, `HMU`)]' \
        --output text | tr '\t' '\n'
}

# Function to pause all triggers
pause_all_triggers() {
    echo "=============================================="
    echo "  Pausing All HMU Glue Triggers"
    echo "=============================================="
    echo ""

    local triggers
    triggers=$(get_all_hmu_triggers)

    if [ -z "$triggers" ]; then
        log_warn "No HMU triggers found"
        return
    fi

    for trigger in $triggers; do
        log_info "Pausing: $trigger"
        aws glue stop-trigger --name "$trigger" --region "$AWS_REGION" >/dev/null 2>&1 \
            && log_info "  Paused successfully" \
            || log_warn "  Already paused or not found"
    done

    echo ""
    log_info "All triggers paused"
}

# Function to resume all triggers
resume_all_triggers() {
    echo "=============================================="
    echo "  Resuming All HMU Glue Triggers"
    echo "=============================================="
    echo ""

    local triggers
    triggers=$(get_all_hmu_triggers)

    if [ -z "$triggers" ]; then
        log_warn "No HMU triggers found"
        return
    fi

    for trigger in $triggers; do
        log_info "Resuming: $trigger"
        aws glue start-trigger --name "$trigger" --region "$AWS_REGION" >/dev/null 2>&1 \
            && log_info "  Resumed successfully" \
            || log_warn "  Already running or not found"
    done

    echo ""
    log_info "All triggers resumed"
}

# Function to show status of all triggers
show_trigger_status() {
    echo "=============================================="
    echo "  HMU Glue Trigger Status"
    echo "=============================================="
    echo ""

    local triggers
    triggers=$(get_all_hmu_triggers)

    if [ -z "$triggers" ]; then
        log_warn "No HMU triggers found"
        return
    fi

    printf "%-45s %-12s %s\n" "TRIGGER NAME" "STATE" "SCHEDULE"
    printf "%-45s %-12s %s\n" "------------" "-----" "--------"

    for trigger in $triggers; do
        local info
        info=$(aws glue get-trigger --name "$trigger" --region "$AWS_REGION" \
            --query 'Trigger.[State,Schedule]' --output text 2>/dev/null) || continue
        local state schedule
        state=$(echo "$info" | cut -f1)
        schedule=$(echo "$info" | cut -f2)

        # Color code the state
        if [ "$state" == "ACTIVATED" ]; then
            printf "%-45s ${GREEN}%-12s${NC} %s\n" "$trigger" "$state" "$schedule"
        else
            printf "%-45s ${YELLOW}%-12s${NC} %s\n" "$trigger" "$state" "$schedule"
        fi
    done
    echo ""
}

# Function to delete a trigger if it exists
delete_trigger_if_exists() {
    local trigger_name="$1"

    if aws glue get-trigger --name "$trigger_name" --region "$AWS_REGION" >/dev/null 2>&1; then
        log_warn "Deleting existing trigger: $trigger_name"
        aws glue delete-trigger --name "$trigger_name" --region "$AWS_REGION"
        sleep 2  # Wait for deletion to propagate
    fi
}

# Function to create a trigger
create_trigger() {
    local trigger_name="$1"
    local job_name="$2"
    local schedule="$3"
    local description="$4"

    if [ "$ACTION" == "--delete-existing" ]; then
        delete_trigger_if_exists "$trigger_name"
    fi

    log_info "Creating trigger: $trigger_name"
    log_info "  Job: $job_name"
    log_info "  Schedule: $schedule"
    log_info "  Description: $description"

    aws glue create-trigger \
        --name "$trigger_name" \
        --type SCHEDULED \
        --schedule "$schedule" \
        --actions "JobName=$job_name" \
        --description "$description" \
        --start-on-creation \
        --region "$AWS_REGION" \
        || { log_error "Failed to create trigger: $trigger_name"; return 1; }

    echo ""
}

# Verify AWS credentials
if ! aws sts get-caller-identity --region "$AWS_REGION" >/dev/null 2>&1; then
    log_error "AWS credentials not configured or invalid"
    exit 1
fi

# Handle different actions
case "$ACTION" in
    --pause-all)
        pause_all_triggers
        exit 0
        ;;
    --resume-all)
        resume_all_triggers
        exit 0
        ;;
    --status)
        show_trigger_status
        exit 0
        ;;
    --delete-existing|create|"")
        # Continue with trigger creation below
        ;;
    --help|-h)
        echo "Usage: $0 [OPTION]"
        echo ""
        echo "Options:"
        echo "  (no option)        Create all triggers"
        echo "  --delete-existing  Delete existing triggers before creating"
        echo "  --pause-all        Pause all HMU triggers"
        echo "  --resume-all       Resume all HMU triggers"
        echo "  --status           Show status of all triggers"
        echo "  --help, -h         Show this help message"
        exit 0
        ;;
    *)
        log_error "Unknown option: $ACTION"
        echo "Use --help for usage information"
        exit 1
        ;;
esac

echo "=============================================="
echo "  AWS Glue Trigger Creation Script"
echo "  Region: $AWS_REGION"
echo "=============================================="
echo ""

log_info "AWS credentials verified"
echo ""

# ============================================
# HIGH-FREQUENCY JOBS
# ============================================
echo "=============================================="
echo "  Creating triggers for HIGH-FREQUENCY jobs"
echo "=============================================="
echo ""

for job in "${HIGH_FREQ_JOBS[@]}"; do
    # Schedule 1: Weekend - Every 3 hours from 10AM-2PM UTC (7AM-11AM UTC-3)
    # Cron: At minute 0, every 3rd hour from 10-13, on Saturday and Sunday
    create_trigger \
        "${job}-weekend-schedule" \
        "$job" \
        "cron(0 10,13 ? * SAT,SUN *)" \
        "Weekend schedule: Every 3 hours 7AM-11:59AM UTC-3 (Sat-Sun)"

    # Schedule 2: Weekdays - Every hour from 5AM-1AM UTC (2AM-10PM UTC-3)
    # This spans midnight, so we need: 5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,0,1
    # Simplified: Every hour from 5-23 and 0-1
    # Cron format: 0 5-23,0-1 means at minute 0, hours 5-23 and 0-1
    create_trigger \
        "${job}-weekday-schedule" \
        "$job" \
        "cron(0 0,1,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23 ? * MON-FRI *)" \
        "Weekday schedule: Every hour 2AM-10PM UTC-3 (Mon-Fri)"
done

# ============================================
# OTHER JOBS
# ============================================
echo "=============================================="
echo "  Creating triggers for OTHER jobs"
echo "=============================================="
echo ""

for job in "${OTHER_JOBS[@]}"; do
    # Schedule: Mon-Sat at 11PM UTC-3 = 2AM UTC (next day)
    # Mon 11PM UTC-3 = Tue 2AM UTC
    # Sat 11PM UTC-3 = Sun 2AM UTC
    # So we run at 2AM UTC on Tue-Sun
    create_trigger \
        "${job}-nightly-schedule" \
        "$job" \
        "cron(0 2 ? * TUE-SUN *)" \
        "Nightly schedule: 11PM UTC-3 Mon-Sat (2AM UTC Tue-Sun)"
done

echo "=============================================="
echo "  Trigger Creation Complete!"
echo "=============================================="
echo ""
log_info "Created ${#HIGH_FREQ_JOBS[@]} high-frequency jobs with 2 schedules each"
log_info "Created ${#OTHER_JOBS[@]} other jobs with 1 schedule each"
echo ""
log_info "To list all triggers:"
echo "  aws glue list-triggers --region $AWS_REGION"
echo ""
log_info "To delete a trigger:"
echo "  aws glue delete-trigger --name <trigger-name> --region $AWS_REGION"
