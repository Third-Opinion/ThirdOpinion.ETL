#!/bin/bash

# Script to fix syntax errors and redeploy Glue jobs
# Handles indentation errors and orphaned lines

AWS_PROFILE="to-prd-admin"
REGION="us-east-2"
S3_BUCKET="aws-glue-assets-442042533707-us-east-2"

echo "🔧 Fixing syntax errors and redeploying Glue jobs..."

# List of jobs that need fixing based on errors
JOBS_TO_FIX=(
    "HMUObservation"
    "HMUCarePlan"
    "HMUDiagnosticReport"
    "HMUDocumentReference"
    "HMUMedication"
    "HMUMedicationDispense"
    "HMUMedicationRequest"
    "HMUPractitioner"
    "HMUProcedure"
    "HMUAllergyIntolerance"
    "HMUPatient"
)

for job in "${JOBS_TO_FIX[@]}"; do
    echo ""
    echo "🔍 Processing job: $job"

    # Find the Python file (handle spaces in directory names)
    if [ -d "$job" ]; then
        py_file="$job/$job.py"
    elif [ -d "HMU $job" ]; then
        py_file="HMU $job/HMU $job.py"
    else
        echo "❌ Directory not found for $job"
        continue
    fi

    if [ ! -f "$py_file" ]; then
        echo "❌ Python file not found: $py_file"
        continue
    fi

    echo "📝 Checking syntax for: $py_file"

    # Check current syntax
    if python3 -m py_compile "$py_file" 2>/dev/null; then
        echo "✅ Syntax is already valid"
    else
        echo "🔧 Fixing syntax errors..."

        # Create backup
        cp "$py_file" "$py_file.backup.$(date +%s)"

        # Fix common syntax issues
        # Remove orphaned lines that cause indentation errors
        # Look for lines that start with just spaces/tabs and are orphaned
        python3 -c "
import re
import sys

def fix_syntax_errors(filename):
    try:
        with open(filename, 'r') as f:
            lines = f.readlines()

        fixed_lines = []
        i = 0
        while i < len(lines):
            line = lines[i]

            # Skip empty lines and comments
            if line.strip() == '' or line.strip().startswith('#'):
                fixed_lines.append(line)
                i += 1
                continue

            # Check if this line is orphaned (indented but doesn't belong anywhere)
            if line.startswith('    ') or line.startswith('\t'):
                # Look ahead to see if this makes sense in context
                # If the next line is at a lower indent level, this might be orphaned
                next_non_empty = i + 1
                while next_non_empty < len(lines) and lines[next_non_empty].strip() == '':
                    next_non_empty += 1

                if next_non_empty < len(lines):
                    next_line = lines[next_non_empty]
                    # If next line is at base level or is a def/class, current line might be orphaned
                    if (not next_line.startswith('    ') and not next_line.startswith('\t')) or \
                       next_line.strip().startswith('def ') or next_line.strip().startswith('class '):
                        # Check if current line looks like an orphaned fragment
                        if any(fragment in line for fragment in ['.show(', '.printSchema()', 'logger.info']):
                            print(f'Removing orphaned line {i+1}: {line.strip()}')
                            i += 1
                            continue

            fixed_lines.append(line)
            i += 1

        # Write fixed content
        with open(filename, 'w') as f:
            f.writelines(fixed_lines)

        print(f'Fixed syntax errors in {filename}')
        return True

    except Exception as e:
        print(f'Error fixing {filename}: {e}')
        return False

# Fix the file
fix_syntax_errors('$py_file')
" || echo "❌ Python script failed"

        # Check if fix worked
        if python3 -m py_compile "$py_file" 2>/dev/null; then
            echo "✅ Syntax fixed successfully"
        else
            echo "❌ Syntax errors remain, skipping deployment"
            continue
        fi
    fi

    # Deploy to S3
    echo "📤 Deploying to S3..."
    if AWS_PROFILE="$AWS_PROFILE" aws s3 cp "$py_file" "s3://$S3_BUCKET/scripts/$job.py" --region "$REGION"; then
        echo "✅ Deployed successfully"

        # Start the job
        echo "🚀 Starting job run..."
        run_result=$(AWS_PROFILE="$AWS_PROFILE" aws glue start-job-run --job-name "$job" --region "$REGION" 2>&1)
        if echo "$run_result" | grep -q "JobRunId"; then
            job_run_id=$(echo "$run_result" | grep -o '"JobRunId": "[^"]*"' | cut -d'"' -f4)
            echo "✅ Job started successfully: $job_run_id"
        else
            echo "❌ Failed to start job: $run_result"
        fi
    else
        echo "❌ Failed to deploy to S3"
    fi

    # Small delay between jobs
    sleep 2
done

echo ""
echo "🎉 Deployment complete!"
echo "Use ./check_job_status.sh to monitor job progress"