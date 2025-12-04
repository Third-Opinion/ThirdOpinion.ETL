"""
Create deployment zip for AWS Glue ETL jobs

This script packages all required Python modules into a zip file
that can be uploaded to S3 and used as a Python library in AWS Glue.

Usage:
    python create_deployment_zip.py <job_name>
    
Example:
    python create_deployment_zip.py HMUObservation
    python create_deployment_zip.py HMUCondition
"""
import os
import sys
import zipfile
import argparse
import ast
from pathlib import Path

# Set UTF-8 encoding for Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')


def validate_python_file(file_path: Path) -> tuple[bool, str]:
    """Validate a Python file for syntax errors"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            source = f.read()
        ast.parse(source)
        return True, ""
    except SyntaxError as e:
        return False, f"SyntaxError: {e.msg} at line {e.lineno}"
    except Exception as e:
        return False, f"Error: {str(e)}"

def create_deployment_zip(job_name: str):
    """Create deployment zip for AWS Glue job"""
    # Validate job name
    v2_dir = Path(__file__).parent
    job_dir = v2_dir / job_name
    
    if not job_dir.exists():
        print(f"[ERROR] Job directory not found: {job_dir}")
        print(f"   Available jobs: {[d.name for d in v2_dir.iterdir() if d.is_dir() and d.name not in ['shared', '__pycache__']]}")
        sys.exit(1)
    
    # Validate Python files before creating zip
    print("Validating Python files for syntax errors...")
    validation_errors = []
    
    # Check job files
    job_python_files = list(job_dir.rglob("*.py"))
    for py_file in job_python_files:
        if '__pycache__' in str(py_file):
            continue
        is_valid, error_msg = validate_python_file(py_file)
        if not is_valid:
            rel_path = py_file.relative_to(v2_dir.parent)
            validation_errors.append(f"{rel_path}: {error_msg}")
            print(f"  [ERROR] {rel_path}: {error_msg}")
    
    # Check shared files
    shared_dir = v2_dir / "shared"
    if shared_dir.exists():
        shared_python_files = list(shared_dir.rglob("*.py"))
        for py_file in shared_python_files:
            if '__pycache__' in str(py_file):
                continue
            is_valid, error_msg = validate_python_file(py_file)
            if not is_valid:
                rel_path = py_file.relative_to(v2_dir.parent)
                validation_errors.append(f"{rel_path}: {error_msg}")
                print(f"  [ERROR] {rel_path}: {error_msg}")
    
    if validation_errors:
        print(f"\n[FAILED] Found {len(validation_errors)} syntax error(s). Fix errors before deploying.")
        sys.exit(1)
    
    print("  [OK] All Python files validated successfully\n")
    
    # Project root (parent of v2 folder)
    project_root = v2_dir.parent
    zip_filename = f"{job_name}_v2.zip"
    zip_path = project_root / zip_filename
    
    # Automatically discover all Python files in job directory
    # Exclude __pycache__, .pyc files, and backup files
    def should_include_file(file_path: Path) -> bool:
        """Determine if a file should be included in the zip"""
        # Exclude __pycache__ directories
        if '__pycache__' in file_path.parts:
            return False
        # Exclude .pyc files
        if file_path.suffix == '.pyc':
            return False
        # Exclude backup files
        if file_path.name.endswith('.backup') or file_path.name.endswith('.bak'):
            return False
        # Only include .py files
        if file_path.suffix != '.py':
            return False
        return True
    
    # Discover all Python files in job directory (recursively)
    # EXCLUDE the main script - it's deployed separately to S3 scripts/ folder
    main_script = f"{job_name}.py"
    job_files = []
    for py_file in job_dir.rglob("*.py"):
        if should_include_file(py_file):
            rel_path = py_file.relative_to(job_dir)
            # Exclude main script - it's deployed separately, not in the zip
            if str(rel_path) != main_script:
                job_files.append(str(rel_path))
            else:
                print(f"   [SKIP] Excluding main script: {main_script} (deployed separately)")
    
    # Verify main script exists (for validation, but don't include in zip)
    main_script_path = job_dir / main_script
    if not main_script_path.exists():
        print(f"   [WARNING] Main script not found: {main_script_path}")
        print(f"   [WARNING] Main script must exist and be deployed separately to S3 scripts/ folder")
    
    # Discover all Python files in shared directory (recursively)
    shared_files = []
    shared_dir = v2_dir / "shared"
    if shared_dir.exists():
        for py_file in shared_dir.rglob("*.py"):
            if should_include_file(py_file):
                rel_path = py_file.relative_to(v2_dir)
                shared_files.append(str(rel_path))
    else:
        print(f"   [WARNING] Shared directory not found: {shared_dir}")
    
    print(f"Creating deployment zip for {job_name}...")
    print(f"   Job directory: {job_dir}")
    print(f"   V2 directory: {v2_dir}")
    print(f"   Output: {zip_path}\n")
    
    # Track what was added
    added_files = []
    missing_files = []
    
    # Create zip file
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Add job files (already discovered, paths relative to job_dir)
        for file_rel_path in sorted(set(job_files)):  # Use set to remove duplicates
            full_path = job_dir / file_rel_path
            if full_path.exists():
                zipf.write(full_path, file_rel_path)
                added_files.append(file_rel_path)
                print(f"   [OK] Added: {file_rel_path}")
            else:
                missing_files.append(file_rel_path)
                print(f"   [MISSING] Missing: {file_rel_path}")
        
        # Add shared files (paths relative to v2_dir)
        for file_rel_path in sorted(set(shared_files)):  # Use set to remove duplicates
            full_path = v2_dir / file_rel_path
            if full_path.exists():
                zipf.write(full_path, file_rel_path)
                added_files.append(file_rel_path)
                print(f"   [OK] Added: {file_rel_path}")
            else:
                missing_files.append(file_rel_path)
                print(f"   [MISSING] Missing: {file_rel_path}")
    
    # Summary
    print(f"\n[SUCCESS] Deployment zip created: {zip_path}")
    print(f"   Size: {zip_path.stat().st_size / 1024:.2f} KB")
    print(f"   Files added: {len(added_files)}")
    
    if missing_files:
        print(f"\n[WARNING] {len(missing_files)} file(s) missing:")
        for f in missing_files:
            print(f"      - {f}")
    
    # Verify structure
    print("\nZip contents:")
    with zipfile.ZipFile(zip_path, 'r') as zipf:
        for name in sorted(zipf.namelist()):
            print(f"   {name}")
    
    # Check for critical files (warn if missing, but don't fail)
    print("\nVerifying critical files:")
    # Note: Main script is NOT in zip - it's deployed separately to S3 scripts/ folder
    critical_files = [
        "shared/config.py",  # Shared config (required)
        "shared/utils/bookmark_utils.py",  # Shared bookmark utils (required)
        "shared/database/redshift_operations.py",  # Shared database ops (required)
    ]
    
    # Optional critical files (job-specific, may not exist for all jobs)
    optional_critical_files = [
        "config.py",  # Job-specific config (optional if using shared only)
        "transformations/__init__.py",  # Transformations module (optional)
    ]
    
    with zipfile.ZipFile(zip_path, 'r') as zipf:
        zip_contents = set(zipf.namelist())
        for critical_file in critical_files:
            if critical_file in zip_contents:
                print(f"   [OK] {critical_file}")
            else:
                print(f"   [WARNING] {critical_file} - MISSING (may cause runtime errors)")
        
        for optional_file in optional_critical_files:
            if optional_file in zip_contents:
                print(f"   [OK] {optional_file}")
            else:
                print(f"   [INFO] {optional_file} - Not found (optional)")
        
        # Verify main script is NOT in zip (it should be deployed separately)
        main_script_in_zip = f"{job_name}.py" in zip_contents
        if main_script_in_zip:
            print(f"   [WARNING] {job_name}.py found in zip - should be deployed separately!")
        else:
            print(f"   [OK] {job_name}.py correctly excluded from zip (deployed separately)")
    
    print("\n" + "="*60)
    print("Next steps:")
    print("   1. Upload zip to S3:")
    print(f"      aws s3 cp {zip_path} s3://aws-glue-assets-442042533707-us-east-2/python-libs/{zip_filename}")
    print("   2. Upload main script to S3:")
    print(f"      aws s3 cp {job_dir}/{job_name}.py s3://aws-glue-assets-442042533707-us-east-2/scripts/{job_name}_v2.py")
    print("   3. Deploy using:")
    print(f"      python deploy.py {job_name}")
    print("      Or use: .\\deploy.ps1 {job_name}")
    print("="*60)
    
    return zip_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create deployment zip for AWS Glue ETL job')
    parser.add_argument('job_name', help='Job name (e.g., HMUObservation, HMUCondition)')
    
    args = parser.parse_args()
    
    try:
        create_deployment_zip(args.job_name)
    except Exception as e:
        print(f"\n[ERROR] Error creating deployment zip: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

