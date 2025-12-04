# Unified deployment script for all v2 ETL jobs
# This script packages and deploys any ETL job to AWS Glue
#
# Usage:
#   .\deploy.ps1 -JobName HMUObservation
#   .\deploy.ps1 -JobName HMUCondition -CreateJob

param(
    [Parameter(Mandatory=$true)]
    [string]$JobName,
    
    [string]$S3Bucket = "s3://aws-glue-assets-442042533707-us-east-2",
    [string]$JobNameSuffix = "_v2",
    [switch]$SkipZip = $false,
    [switch]$SkipUpload = $false,
    [switch]$CreateJob = $false
)

$ErrorActionPreference = "Stop"

Write-Host "🚀 V2 ETL Job Deployment Script" -ForegroundColor Green
Write-Host "=" * 60 -ForegroundColor Gray
Write-Host "   Job: $JobName" -ForegroundColor Cyan
Write-Host ""

# Get script directory
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$v2Dir = $scriptDir
$jobDir = Join-Path $v2Dir $JobName
$projectRoot = Split-Path -Parent $v2Dir

# Validate job directory exists
if (-not (Test-Path $jobDir)) {
    Write-Host "❌ Job directory not found: $jobDir" -ForegroundColor Red
    $availableJobs = Get-ChildItem -Path $v2Dir -Directory | Where-Object { $_.Name -ne "shared" -and $_.Name -ne "__pycache__" } | Select-Object -ExpandProperty Name
    Write-Host "   Available jobs: $($availableJobs -join ', ')" -ForegroundColor Yellow
    exit 1
}

$zipFilename = "${JobName}${JobNameSuffix}.zip"
$zipPath = Join-Path $projectRoot $zipFilename
$mainScript = "${JobName}.py"
$mainScriptPath = Join-Path $jobDir $mainScript
$s3ZipPath = "$S3Bucket/python-libs/$zipFilename"
$s3ScriptPath = "$S3Bucket/scripts/${JobName}${JobNameSuffix}.py"
$glueJobName = "${JobName}${JobNameSuffix}"

# Step 1: Create zip
if (-not $SkipZip) {
    Write-Host "📦 Step 1: Creating deployment zip..." -ForegroundColor Yellow
    
    # Check if Python is available
    $pythonCmd = Get-Command python -ErrorAction SilentlyContinue
    if (-not $pythonCmd) {
        Write-Host "❌ Python not found. Please install Python or use --SkipZip" -ForegroundColor Red
        exit 1
    }
    
    # Run Python script to create zip
    Push-Location $v2Dir
    try {
        python create_deployment_zip.py $JobName
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ Failed to create zip" -ForegroundColor Red
            exit 1
        }
    } finally {
        Pop-Location
    }
    
    Write-Host "✅ Zip created successfully" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "⏭️  Skipping zip creation (--SkipZip)" -ForegroundColor Gray
    Write-Host ""
}

# Step 2: Upload to S3
if (-not $SkipUpload) {
    Write-Host "☁️  Step 2: Uploading to S3..." -ForegroundColor Yellow
    
    if (-not (Test-Path $zipPath)) {
        Write-Host "❌ Zip file not found: $zipPath" -ForegroundColor Red
        Write-Host "   Run without --SkipZip to create it" -ForegroundColor Yellow
        exit 1
    }
    
    # Upload zip
    Write-Host "   Uploading zip: $s3ZipPath" -ForegroundColor Cyan
    aws s3 cp $zipPath $s3ZipPath --profile to-prod-admin
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Failed to upload zip to S3" -ForegroundColor Red
        exit 1
    }
    
    # Upload main script
    if (-not (Test-Path $mainScriptPath)) {
        Write-Host "❌ Main script not found: $mainScriptPath" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "   Uploading script: $s3ScriptPath" -ForegroundColor Cyan
    aws s3 cp $mainScriptPath $s3ScriptPath --profile to-prod-admin
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Failed to upload script to S3" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "✅ Files uploaded successfully" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "⏭️  Skipping S3 upload (--SkipUpload)" -ForegroundColor Gray
    Write-Host ""
}

# Step 3: Create/Update Glue Job (optional)
if ($CreateJob) {
    Write-Host "⚙️  Step 3: Creating/updating Glue job..." -ForegroundColor Yellow
    
    $jobConfigPath = Join-Path $jobDir "${glueJobName}.json"
    
    if (-not (Test-Path $jobConfigPath)) {
        Write-Host "⚠️  Job config not found: $jobConfigPath" -ForegroundColor Yellow
        Write-Host "   Skipping job creation. Create the job manually or add the JSON config file." -ForegroundColor Yellow
    } else {
        Write-Host "   Creating/updating job: $glueJobName" -ForegroundColor Cyan
        
        # Check if job already exists
        $jobExists = aws glue get-job --job-name $glueJobName --profile to-prod-admin 2>$null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "   Job already exists. Updating..." -ForegroundColor Yellow
            aws glue update-job --job-name $glueJobName --job-update file://$jobConfigPath --profile to-prod-admin --region us-east-2
        } else {
            Write-Host "   Creating new job..." -ForegroundColor Cyan
            aws glue create-job --job-input file://$jobConfigPath --profile to-prod-admin --region us-east-2
        }
        
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ Failed to create/update Glue job" -ForegroundColor Red
            exit 1
        }
        
        Write-Host "✅ Job created/updated successfully" -ForegroundColor Green
        Write-Host ""
    }
} else {
    Write-Host "⏭️  Skipping job creation (use --CreateJob to enable)" -ForegroundColor Gray
    Write-Host ""
}

# Summary
Write-Host "=" * 60 -ForegroundColor Gray
Write-Host "✅ Deployment complete!" -ForegroundColor Green
Write-Host ""
Write-Host "📋 Summary:" -ForegroundColor Cyan
Write-Host "   Job: $JobName"
Write-Host "   Zip location: $s3ZipPath"
Write-Host "   Script location: $s3ScriptPath"
Write-Host "   Glue job name: $glueJobName"
Write-Host ""
Write-Host "⚠️  Next steps:" -ForegroundColor Yellow
Write-Host "   1. Verify files in S3 bucket"
if (-not $CreateJob) {
    Write-Host "   2. Create/update Glue job:"
    Write-Host "      .\deploy.ps1 -JobName $JobName -CreateJob"
}
Write-Host "   3. Test the job in Glue console"
Write-Host "   4. Check CloudWatch logs for import errors"
Write-Host ""

