#!/usr/bin/env python3
"""
Update all Glue jobs to use externalized DDL:
1. Add table schema checking at initialization
2. Remove CREATE TABLE statements from preactions
3. Assume tables exist (created separately via DDL files)
"""

import re
from pathlib import Path

def extract_table_names_from_file(file_path):
    """Extract all table names that this Glue job writes to."""
    with open(file_path, 'r') as f:
        content = f.read()

    table_names = []

    # Find all CREATE TABLE statements
    pattern = r'CREATE TABLE IF NOT EXISTS (?:public\.)?(\w+)'
    matches = re.findall(pattern, content)

    return list(set(matches))  # Remove duplicates

def add_table_check_to_main(content, table_names):
    """Add table existence check to main() function."""

    # Import statement to add at the top
    import_statement = """
# Import table utility functions
import sys
sys.path.append('./glue_utils')
from table_utils import check_all_tables
"""

    # Check if import already exists
    if 'from table_utils import' not in content:
        # Add after other imports (after awsglue imports)
        import_pattern = r'(from awsglue\.job import Job.*?\n)'
        content = re.sub(import_pattern, r'\1' + import_statement, content, count=1)

    # Create table check code
    table_names_str = ', '.join([f'"{name}"' for name in sorted(table_names)])
    table_check_code = f"""
        # Check table existence and log schemas
        table_names = [{table_names_str}]
        check_all_tables(glueContext, table_names, REDSHIFT_CONNECTION, S3_TEMP_DIR)

"""

    # Find main() function and add check after job start logging
    main_pattern = r'(def main\(\):.*?logger\.info\(f"⏰ Job started at:.*?\n)'
    replacement = r'\1' + table_check_code
    content = re.sub(main_pattern, replacement, content, flags=re.DOTALL, count=1)

    return content

def remove_create_table_from_preactions(content):
    """Remove CREATE TABLE IF NOT EXISTS from preactions, keeping only other SQL."""

    # Pattern 1: Remove standalone CREATE TABLE statements from triple-quoted preactions
    # This handles: preactions = """CREATE TABLE..."""
    def clean_preaction(match):
        preaction_content = match.group(1)
        # Remove CREATE TABLE statements but keep other SQL (like DELETE, TRUNCATE)
        cleaned = re.sub(
            r'CREATE TABLE IF NOT EXISTS.*?(?=;|\Z)',
            '',
            preaction_content,
            flags=re.DOTALL
        )
        # Clean up extra semicolons and whitespace
        cleaned = re.sub(r';\s*;', ';', cleaned)
        cleaned = cleaned.strip()
        if cleaned and not cleaned.endswith(';'):
            cleaned += ';'
        return f'preactions = """{cleaned}"""' if cleaned else 'preactions = ""'

    content = re.sub(
        r'preactions\s*=\s*"""(.*?)"""',
        clean_preaction,
        content,
        flags=re.DOTALL
    )

    # Pattern 2: Remove CREATE TABLE function calls from preactions
    # This handles: preactions = create_table_sql() + "; TRUNCATE..."
    content = re.sub(
        r'(\w+_preactions\s*=\s*)create_\w+_sql\(\)\s*\+?\s*["\'];\s*',
        r'\1"',
        content
    )

    # Pattern 3: Handle standalone function calls
    content = re.sub(
        r'(\w+_preactions\s*=\s*)create_\w+_sql\(\)',
        r'\1""',
        content
    )

    return content

def update_glue_job(file_path):
    """Update a single Glue job file."""
    print(f"\n📝 Processing {file_path.name}...")

    with open(file_path, 'r') as f:
        content = f.read()

    # Extract table names
    table_names = extract_table_names_from_file(file_path)

    if not table_names:
        print(f"   ⚠️  No tables found in {file_path.name}")
        return False

    print(f"   Found {len(table_names)} tables: {', '.join(sorted(table_names))}")

    # Add table checking
    content = add_table_check_to_main(content, table_names)

    # Remove CREATE TABLE from preactions
    content = remove_create_table_from_preactions(content)

    # Write updated content
    with open(file_path, 'w') as f:
        f.write(content)

    print(f"   ✅ Updated {file_path.name}")
    return True

def main():
    project_root = Path(__file__).parent.parent

    # Find all Glue job Python files
    glue_files = sorted(project_root.glob('HMU*/HMU*.py'))

    print(f"{'='*80}")
    print(f"🔧 UPDATING GLUE JOBS FOR EXTERNAL DDL")
    print(f"{'='*80}")

    updated_count = 0
    skipped_count = 0

    for glue_file in glue_files:
        if update_glue_job(glue_file):
            updated_count += 1
        else:
            skipped_count += 1

    print(f"\n{'='*80}")
    print(f"✨ UPDATE COMPLETE!")
    print(f"{'='*80}")
    print(f"Total jobs processed: {len(glue_files)}")
    print(f"✅ Updated: {updated_count}")
    print(f"⚠️  Skipped: {skipped_count}")

    print(f"\n📋 Next steps:")
    print(f"  1. Review the updated Glue job files")
    print(f"  2. Deploy DDL files from ./ddl/ to Redshift")
    print(f"  3. Deploy updated Glue jobs")
    print(f"  4. Test jobs to ensure tables are detected correctly")

if __name__ == "__main__":
    main()
