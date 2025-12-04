"""
Validate Python files before deployment
Checks for syntax errors in all Python files that will be deployed
"""
import sys
import ast
import zipfile
from pathlib import Path

# Set UTF-8 encoding for Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def validate_python_file(file_path: Path) -> tuple[bool, str]:
    """Validate a Python file for syntax errors"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            source = f.read()
        ast.parse(source)
        return True, ""
    except SyntaxError as e:
        return False, f"SyntaxError: {e.msg} at line {e.lineno}: {e.text}"
    except Exception as e:
        return False, f"Error: {str(e)}"

def validate_deployment_zip(zip_path: Path) -> tuple[bool, list[str]]:
    """Validate all Python files in a deployment zip"""
    errors = []
    
    if not zip_path.exists():
        return False, [f"Zip file not found: {zip_path}"]
    
    with zipfile.ZipFile(zip_path, 'r') as zipf:
        python_files = [name for name in zipf.namelist() if name.endswith('.py')]
        
        for py_file in python_files:
            try:
                source = zipf.read(py_file).decode('utf-8')
                ast.parse(source)
                print(f"  [OK] {py_file}")
            except SyntaxError as e:
                error_msg = f"  [ERROR] {py_file}: SyntaxError at line {e.lineno}: {e.msg}"
                print(error_msg)
                errors.append(error_msg)
            except Exception as e:
                error_msg = f"  ✗ {py_file}: {str(e)}"
                print(error_msg)
                errors.append(error_msg)
    
    return len(errors) == 0, errors

def validate_job_files(job_name: str) -> tuple[bool, list[str]]:
    """Validate all Python files for a job"""
    v2_dir = Path(__file__).parent
    job_dir = v2_dir / job_name
    
    if not job_dir.exists():
        return False, [f"Job directory not found: {job_dir}"]
    
    errors = []
    python_files = list(job_dir.rglob("*.py"))
    
    print(f"Validating {len(python_files)} Python files in {job_name}...")
    
    for py_file in python_files:
        # Skip __pycache__
        if '__pycache__' in str(py_file):
            continue
        
        is_valid, error_msg = validate_python_file(py_file)
        rel_path = py_file.relative_to(v2_dir.parent)
        
        if is_valid:
            print(f"  [OK] {rel_path}")
        else:
            print(f"  [ERROR] {rel_path}: {error_msg}")
            errors.append(f"{rel_path}: {error_msg}")
    
    # Also validate shared files
    shared_dir = v2_dir / "shared"
    if shared_dir.exists():
        shared_files = list(shared_dir.rglob("*.py"))
        print(f"\nValidating {len(shared_files)} shared Python files...")
        
        for py_file in shared_files:
            if '__pycache__' in str(py_file):
                continue
            
            is_valid, error_msg = validate_python_file(py_file)
            rel_path = py_file.relative_to(v2_dir.parent)
            
            if is_valid:
                print(f"  [OK] {rel_path}")
            else:
                print(f"  [ERROR] {rel_path}: {error_msg}")
                errors.append(f"{rel_path}: {error_msg}")
    
    return len(errors) == 0, errors

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate Python files before deployment')
    parser.add_argument('job_name', help='Job name (e.g., HMUObservation)')
    parser.add_argument('--zip', help='Also validate deployment zip file', action='store_true')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print(f"VALIDATING {args.job_name} BEFORE DEPLOYMENT")
    print("=" * 60)
    print()
    
    # Validate source files
    is_valid, errors = validate_job_files(args.job_name)
    
    # Validate zip if requested
    if args.zip:
        v2_dir = Path(__file__).parent
        zip_path = v2_dir.parent / f"{args.job_name}_v2.zip"
        
        if zip_path.exists():
            print(f"\nValidating deployment zip: {zip_path.name}")
            zip_valid, zip_errors = validate_deployment_zip(zip_path)
            if not zip_valid:
                errors.extend(zip_errors)
        else:
            print(f"\n⚠️  Zip file not found: {zip_path.name}")
            print("   Run create_deployment_zip.py first")
    
    print()
    print("=" * 60)
    if is_valid and len(errors) == 0:
        print("[SUCCESS] ALL VALIDATION CHECKS PASSED")
        print("=" * 60)
        sys.exit(0)
    else:
        print(f"[FAILED] VALIDATION FAILED: {len(errors)} error(s)")
        print("=" * 60)
        for error in errors:
            print(f"  {error}")
        sys.exit(1)

