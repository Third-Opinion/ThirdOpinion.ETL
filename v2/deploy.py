#!/usr/bin/env python3
"""
Unified deployment script for all v2 ETL jobs

This script automates the complete deployment process:
1. Create deployment zip
2. Upload zip to S3
3. Upload main script to S3
4. Optionally create/update Glue job

Usage:
    python deploy.py HMUObservation
    python deploy.py HMUMedication --create-job
    python deploy.py HMUCondition --skip-upload
"""
import argparse
import subprocess
import sys
from pathlib import Path

# Set UTF-8 encoding for Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')


def run_command(cmd, check=True):
    """Run a shell command and return the result"""
    print(f"   Running: {' '.join(cmd) if isinstance(cmd, list) else cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"   ❌ Error: {result.stderr}")
        sys.exit(1)
    return result


def deploy_job(job_name, s3_bucket, job_suffix, skip_zip, skip_upload, create_job):
    """Deploy an ETL job to AWS Glue"""
    
    print("🚀 V2 ETL Job Deployment Script")
    print("=" * 60)
    print(f"   Job: {job_name}")
    print()
    
    # Get paths
    script_dir = Path(__file__).parent
    v2_dir = script_dir
    job_dir = v2_dir / job_name
    project_root = v2_dir.parent
    
    # Validate job directory
    if not job_dir.exists():
        print(f"❌ Job directory not found: {job_dir}")
        # List available jobs
        available = [d.name for d in v2_dir.iterdir() 
                     if d.is_dir() and d.name not in ['shared', '__pycache__']]
        print(f"   Available jobs: {', '.join(available)}")
        sys.exit(1)
    
    zip_filename = f"{job_name}{job_suffix}.zip"
    zip_path = project_root / zip_filename
    main_script = f"{job_name}.py"
    main_script_path = job_dir / main_script
    s3_zip_path = f"{s3_bucket}/python-libs/{zip_filename}"
    s3_script_path = f"{s3_bucket}/scripts/{job_name}{job_suffix}.py"
    glue_job_name = f"{job_name}{job_suffix}"
    
    # Step 1: Create zip
    if not skip_zip:
        print("📦 Step 1: Creating deployment zip...")
        
        # Run Python script to create zip
        result = run_command(
            f'cd "{v2_dir}" && python create_deployment_zip.py {job_name}',
            check=False
        )
        
        if result.returncode != 0:
            print("❌ Failed to create zip")
            print(result.stderr)
            sys.exit(1)
        
        if not zip_path.exists():
            print(f"❌ Zip file not found: {zip_path}")
            sys.exit(1)
        
        print("✅ Zip created successfully")
        print()
    else:
        print("⏭️  Skipping zip creation (--skip-zip)")
        print()
    
    # Step 2: Upload to S3
    if not skip_upload:
        print("☁️  Step 2: Uploading to S3...")
        
        # Upload zip
        print(f"   Uploading zip: {s3_zip_path}")
        run_command(f'aws s3 cp "{zip_path}" {s3_zip_path} --profile to-prod-admin')
        
        # Upload main script
        if not main_script_path.exists():
            print(f"❌ Main script not found: {main_script_path}")
            sys.exit(1)
        
        print(f"   Uploading script: {s3_script_path}")
        run_command(f'aws s3 cp "{main_script_path}" {s3_script_path} --profile to-prod-admin')
        
        print("✅ Files uploaded successfully")
        print()
    else:
        print("⏭️  Skipping S3 upload (--skip-upload)")
        print()
    
    # Step 3: Create/Update Glue Job
    if create_job:
        print("⚙️  Step 3: Creating/updating Glue job...")
        
        job_config_path = job_dir / f"{glue_job_name}.json"
        
        if not job_config_path.exists():
            print(f"⚠️  Job config not found: {job_config_path}")
            print("   Skipping job creation. Create the job manually or add the JSON config file.")
        else:
            print(f"   Creating/updating job: {glue_job_name}")
            
            # Check if job exists
            check_result = run_command(
                f'aws glue get-job --job-name {glue_job_name} --profile to-prod-admin --region us-east-2',
                check=False
            )
            
            if check_result.returncode == 0:
                print("   Job already exists. Updating...")
                run_command(
                    f'aws glue update-job --job-name {glue_job_name} --job-update file://"{job_config_path}" --profile to-prod-admin --region us-east-2'
                )
            else:
                print("   Creating new job...")
                # Use job_update.json if available, otherwise use the main JSON
                update_config = job_dir / "job_update.json"
                if update_config.exists():
                    run_command(
                        f'aws glue create-job --name {glue_job_name} --role arn:aws:iam::442042533707:role/service-role/AWSGlueServiceRole --command Name=glueetl,ScriptLocation={s3_script_path},PythonVersion=3 --glue-version 4.0 --worker-type G.2X --number-of-workers 6 --execution-class FLEX --timeout 720 --max-retries 0 --connections Connections="Redshift connection" --default-arguments file://"{update_config}" --profile to-prod-admin --region us-east-2'
                    )
                else:
                    print("   ⚠️  job_update.json not found, using basic configuration")
                    run_command(
                        f'aws glue create-job --name {glue_job_name} --role arn:aws:iam::442042533707:role/service-role/AWSGlueServiceRole --command Name=glueetl,ScriptLocation={s3_script_path},PythonVersion=3 --glue-version 4.0 --worker-type G.2X --number-of-workers 6 --execution-class FLEX --timeout 720 --max-retries 0 --connections Connections="Redshift connection" --default-arguments \'{{"--extra-py-files":"{s3_zip_path}","--TABLE_NAME_POSTFIX":"{job_suffix}","--enable-continuous-cloudwatch-log":"true"}}\' --profile to-prod-admin --region us-east-2'
                    )
            
            print("✅ Job created/updated successfully")
            print()
    else:
        print("⏭️  Skipping job creation (use --create-job to enable)")
        print()
    
    # Summary
    print("=" * 60)
    print("✅ Deployment complete!")
    print()
    print("📋 Summary:")
    print(f"   Job: {job_name}")
    print(f"   Zip location: {s3_zip_path}")
    print(f"   Script location: {s3_script_path}")
    print(f"   Glue job name: {glue_job_name}")
    print()
    if not create_job:
        print("⚠️  Next steps:")
        print("   1. Verify files in S3 bucket")
        print(f"   2. Create/update Glue job:")
        print(f"      python deploy.py {job_name} --create-job")
        print("   3. Test the job in Glue console")
        print("   4. Check CloudWatch logs for import errors")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Deploy v2 ETL job to AWS Glue')
    parser.add_argument('job_name', help='Job name (e.g., HMUObservation, HMUMedication)')
    parser.add_argument('--s3-bucket', default='s3://aws-glue-assets-442042533707-us-east-2',
                       help='S3 bucket for Glue assets')
    parser.add_argument('--job-suffix', default='_v2',
                       help='Suffix for job name and files (default: _v2)')
    parser.add_argument('--skip-zip', action='store_true',
                       help='Skip zip creation')
    parser.add_argument('--skip-upload', action='store_true',
                       help='Skip S3 upload')
    parser.add_argument('--create-job', action='store_true',
                       help='Create or update Glue job')
    
    args = parser.parse_args()
    
    try:
        deploy_job(
            args.job_name,
            args.s3_bucket,
            args.job_suffix,
            args.skip_zip,
            args.skip_upload,
            args.create_job
        )
    except KeyboardInterrupt:
        print("\n\n⚠️  Deployment cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Deployment failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)




