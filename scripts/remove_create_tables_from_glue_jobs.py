#!/usr/bin/env python3
"""
Script to update all Glue jobs to check for table existence instead of creating tables.
This script:
1. Removes CREATE TABLE SQL functions
2. Adds verify_table_exists() and verify_all_required_tables() functions
3. Updates write_to_redshift_versioned() to not accept preactions parameter
4. Updates all calls to write_to_redshift_versioned() to remove SQL parameters
5. Adds table verification call before writes
"""

import re
import os
import sys
from pathlib import Path

# Template for the verification functions
VERIFICATION_FUNCTIONS = '''def verify_table_exists(table_name):
    """Verify that a Redshift table exists, raise error if it doesn't"""
    logger.info(f"Verifying table exists: public.{table_name}")

    try:
        # Try to read from the table to verify it exists
        test_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "redshiftTmpDir": S3_TEMP_DIR,
                "useConnectionProperties": "true",
                "dbtable": f"public.{table_name}",
                "connectionName": REDSHIFT_CONNECTION
            },
            transformation_ctx=f"verify_{table_name}"
        )

        # If we can convert to DF, table exists
        df = test_frame.toDF()
        logger.info(f"✅ Table 'public.{table_name}' exists and is accessible")
        return True

    except Exception as e:
        logger.error(f"❌ Table 'public.{table_name}' does NOT exist or is not accessible")
        logger.error(f"   Error: {str(e)}")
        logger.error(f"   Tables must be created using DDL scripts before running Glue jobs")
        raise RuntimeError(f"Required table 'public.{table_name}' does not exist. Please run DDL scripts first.")

def verify_all_required_tables(required_tables):
    """Verify all required tables exist before processing"""
    logger.info(f"\\n{'='*80}")
    logger.info(f"🔍 VERIFYING REQUIRED TABLES EXIST")
    logger.info("=" * 80)

    all_exist = True
    for table in required_tables:
        try:
            verify_table_exists(table)
        except RuntimeError:
            all_exist = False

    if not all_exist:
        logger.error(f"\\n{'='*80}")
        logger.error("❌ MISSING REQUIRED TABLES")
        logger.error("=" * 80)
        logger.error("Please run the DDL scripts to create all required tables before running this Glue job")
        logger.error("DDL scripts should be located in the ddl/ folder")
        raise RuntimeError("Missing required tables. Cannot proceed with ETL job.")

    logger.info(f"\\n{'='*80}")
    logger.info(f"✅ ALL REQUIRED TABLES VERIFIED")
    logger.info("=" * 80)
    return True

'''

def find_create_table_functions(content):
    """Find all CREATE TABLE function definitions"""
    pattern = r'def create_\w+_sql\(\):.*?""".*?"""'
    matches = list(re.finditer(pattern, content, re.DOTALL))
    return matches

def extract_table_names_from_calls(content):
    """Extract table names from write_to_redshift_versioned calls"""
    pattern = r'write_to_redshift_versioned\([^,]+,\s*"([^"]+)"'
    matches = re.findall(pattern, content)
    return list(set(matches))  # Remove duplicates

def remove_create_table_functions(content):
    """Remove all CREATE TABLE SQL function definitions"""
    # Pattern to match function definitions
    pattern = r'def create_\w+_sql\(\):.*?(?=\n(?:def |class |# |$))'
    content = re.sub(pattern, '', content, flags=re.DOTALL)
    return content

def update_write_function_signature(content):
    """Update write_to_redshift_versioned function to remove preactions parameter"""
    # Update function signature
    old_sig = r'def write_to_redshift_versioned\(dynamic_frame,\s*table_name,\s*id_column,\s*preactions=""\):'
    new_sig = 'def write_to_redshift_versioned(dynamic_frame, table_name, id_column):'
    content = re.sub(old_sig, new_sig, content)

    # Remove the empty record handling for preactions
    old_empty_handling = r'if total_records == 0:.*?return'
    new_empty_handling = '''if total_records == 0:
            logger.info(f"No records to process for {table_name} - skipping")
            return'''
    content = re.sub(old_empty_handling, new_empty_handling, content, flags=re.DOTALL)

    # Update preactions building logic
    old_preactions = r'# Step 4: Build preactions for selective deletion\s+selective_preactions = preactions\s+if entities_to_delete:'
    new_preactions = '''# Step 4: Build preactions for selective deletion
        if entities_to_delete:'''
    content = re.sub(old_preactions, new_preactions, content)

    # Fix the delete clause handling
    old_delete_handling = r'if selective_preactions:\s+selective_preactions = delete_clause \+ " " \+ selective_preactions\s+else:\s+selective_preactions = delete_clause'
    new_delete_handling = '''logger.info(f"Will delete {num_entities} existing entities before inserting updated versions")
        else:
            delete_clause = ""'''
    content = re.sub(old_delete_handling, new_delete_handling, content)

    # Update the write options to conditionally add preactions
    old_write_options = r'glueContext\.write_dynamic_frame\.from_options\(\s+frame=filtered_dynamic_frame,\s+connection_type="redshift",\s+connection_options=\{\s+"redshiftTmpDir": S3_TEMP_DIR,\s+"useConnectionProperties": "true",\s+"dbtable": f"public\.\{table_name\}",\s+"connectionName": REDSHIFT_CONNECTION,\s+"preactions": selective_preactions or ""\s+\},'

    new_write_options = '''connection_options = {
            "redshiftTmpDir": S3_TEMP_DIR,
            "useConnectionProperties": "true",
            "dbtable": f"public.{table_name}",
            "connectionName": REDSHIFT_CONNECTION
        }

        # Only add preactions if we have deletions to perform
        if delete_clause:
            connection_options["preactions"] = delete_clause

        glueContext.write_dynamic_frame.from_options(
            frame=filtered_dynamic_frame,
            connection_type="redshift",
            connection_options=connection_options,'''

    content = re.sub(old_write_options, new_write_options, content, flags=re.DOTALL)

    return content

def update_write_calls(content, table_names):
    """Remove SQL parameters from write_to_redshift_versioned calls"""
    # Pattern to match: write_to_redshift_versioned(frame, "table", "id", sql_var)
    pattern = r'(\w+)_table_sql = create_\w+_sql\(\)\s+write_to_redshift_versioned\(([^,]+),\s*"([^"]+)",\s*"([^"]+)",\s*\1_table_sql\)'
    replacement = r'write_to_redshift_versioned(\2, "\3", "\4")'
    content = re.sub(pattern, replacement, content)

    # Also handle single-line patterns
    pattern2 = r'write_to_redshift_versioned\(([^,]+),\s*"([^"]+)",\s*"([^"]+)",\s*[^)]+\)'
    replacement2 = r'write_to_redshift_versioned(\1, "\2", "\3")'
    content = re.sub(pattern2, replacement2, content)

    return content

def add_table_verification(content, table_names):
    """Add table verification call before writes"""
    # Create the required_tables list
    tables_list = '[\n        "' + '",\n        "'.join(sorted(table_names)) + '"\n    ]'

    # Insert verification call before first write
    pattern = r'(logger\.info\("📝 (?:Creating|Writing) .*?"\))\s+(write_to_redshift_versioned)'

    verification_call = f'''# Verify all required tables exist before processing
        required_tables = {tables_list}
        verify_all_required_tables(required_tables)

        \\1
        \\2'''

    # Only replace the first occurrence
    content = re.sub(pattern, verification_call, content, count=1)

    return content

def update_glue_job(job_path):
    """Update a single Glue job file"""
    print(f"\\nProcessing: {job_path}")

    with open(job_path, 'r') as f:
        content = f.read()

    # Check if already updated
    if 'verify_table_exists' in content:
        print(f"  ⊙ Skipping - already updated")
        return False

    # Extract table names from write calls
    table_names = extract_table_names_from_calls(content)
    if not table_names:
        print(f"  ⊙ Skipping - no tables found")
        return False

    print(f"  Found {len(table_names)} tables: {', '.join(sorted(table_names))}")

    # Remove CREATE TABLE functions
    content = remove_create_table_functions(content)

    # Add verification functions after get_entities_to_delete function or before write_to_redshift_versioned
    insert_pattern = r'(def get_entities_to_delete.*?return entities_to_delete\n\n)'
    if re.search(insert_pattern, content, re.DOTALL):
        content = re.sub(insert_pattern, r'\\1' + VERIFICATION_FUNCTIONS + '\\n', content, flags=re.DOTALL)
    else:
        # Insert before write_to_redshift_versioned function
        insert_pattern2 = r'(def write_to_redshift_versioned)'
        content = re.sub(insert_pattern2, VERIFICATION_FUNCTIONS + '\\n\\1', content)

    # Update write_to_redshift_versioned function
    content = update_write_function_signature(content)

    # Update write calls
    content = update_write_calls(content, table_names)

    # Add table verification before writes
    content = add_table_verification(content, table_names)

    # Update log messages from "Creating" to "Writing"
    content = re.sub(r'logger\.info\("📝 Creating (.*?) table', r'logger.info("📝 Writing \\1 table', content)
    content = re.sub(r'table created and written successfully', 'table written successfully', content)

    # Write back
    with open(job_path, 'w') as f:
        f.write(content)

    print(f"  ✓ Updated successfully")
    return True

def main():
    # Find all HMU* directories except HMUMyJob
    script_dir = Path(__file__).parent.parent
    glue_dirs = [d for d in script_dir.glob("HMU*") if d.is_dir() and d.name != "HMUMyJob" and d.name != "HMUPatient"]

    print(f"Found {len(glue_dirs)} Glue job directories to process (excluding HMUPatient which is already updated)")

    updated_count = 0
    skipped_count = 0

    for job_dir in sorted(glue_dirs):
        py_file = job_dir / f"{job_dir.name}.py"
        if py_file.exists():
            if update_glue_job(str(py_file)):
                updated_count += 1
            else:
                skipped_count += 1

    print(f"\\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"✓ Updated: {updated_count} jobs")
    print(f"⊙ Skipped: {skipped_count} jobs")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()
