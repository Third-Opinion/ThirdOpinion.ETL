#!/usr/bin/env python3
"""
Batch refactor all Glue jobs to use Iceberg format following HMUPatient.py template
"""

import os
import re
import sys
from pathlib import Path

# Iceberg configuration template
ICEBERG_CONFIG = """from datetime import datetime
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, DateType, BooleanType, IntegerType
import json
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
catalog_nm = "glue_catalog"

s3_bucket = "s3://7df690fd40c734f8937daf02f39b2ec3-457560472834-group/datalake/hmu_fhir_data_store_836e877666cebf177ce6370ec1478a92_healthlake_view/"
ahl_database = "hmu_fhir_data_store_836e877666cebf177ce6370ec1478a92_healthlake_view"
tableCatalogId = "457560472834"  # AHL service account
s3_output_bucket = "s3://healthlake-glue-output-2025"


spark = (SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{{catalog_nm}}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{{catalog_nm}}.warehouse", s3_bucket)
    .config(f"spark.sql.catalog.{{catalog_nm}}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{{catalog_nm}}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.glue.lakeformation-enabled", "true")
    .config("spark.sql.catalog.glue_catalog.glue.id", tableCatalogId)
    .getOrCreate())
sc = spark.sparkContext
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Configuration - Updated to use S3/Iceberg instead of Glue Catalog
DATABASE_NAME = ahl_database  # Using AHL Iceberg database
TABLE_NAME = "{table_name}"
REDSHIFT_CONNECTION = "Redshift connection"
S3_TEMP_DIR = "s3://aws-glue-assets-442042533707-us-east-2/temporary/"

# Note: We now read data from S3 using Iceberg catalog instead of Glue Catalog
# This provides better performance and direct access to S3 data
"""

def get_table_name_from_job(job_name):
    """Convert job name to table name"""
    # HMUAllergyIntolerance -> allergyintolerance
    # HMUCarePlan -> careplan
    # HMUDocumentReference -> documentreference
    if job_name.startswith("HMU"):
        table_name = job_name[3:].lower()
        # Handle special cases
        if table_name == "diagnosticreport":
            table_name = "diagnosticreport"
        elif table_name == "documentreference":
            table_name = "documentreference"
        elif table_name == "medicationdispense":
            table_name = "medicationdispense"
        elif table_name == "medicationrequest":
            table_name = "medicationrequest"
        elif table_name == "allergyintolerance":
            table_name = "allergyintolerance"
        elif table_name == "careplan":
            table_name = "careplan"
        return table_name
    return job_name.lower()

def refactor_glue_job(job_dir, job_name):
    """Refactor a single Glue job Python file to use Iceberg"""
    py_file = job_dir / f"{job_name}.py"

    if not py_file.exists():
        print(f"⚠️  Skipping {job_name}: No Python file found")
        return False

    print(f"🔄 Refactoring {job_name}...")

    # Read the original file
    with open(py_file, 'r') as f:
        original_content = f.read()

    # Get table name for this job
    table_name = get_table_name_from_job(job_name)

    # Create new content with Iceberg configuration
    new_content = ICEBERG_CONFIG.format(table_name=table_name)

    # Extract the main transformation logic from original file
    # Look for def transform_ functions and main() function
    transform_functions = []
    main_function = None

    # Simple pattern matching to extract functions
    function_pattern = r'(def (transform_\w+|create_\w+|write_to_redshift|convert_to_json_string|main)\([^)]*\):.*?)(?=\ndef |\nif __name__|$)'
    matches = re.findall(function_pattern, original_content, re.DOTALL)

    for match in matches:
        func_content = match[0]
        func_name = match[1]

        if func_name == "main":
            # Update main function to use Iceberg
            func_content = update_main_function_for_iceberg(func_content, table_name)
        elif func_name == "convert_to_json_string":
            # Skip, already in template
            continue

        if func_name != "convert_to_json_string":
            transform_functions.append(func_content)

    # If no main function found, create a basic one
    if not any("def main(" in func for func in transform_functions):
        transform_functions.append(create_basic_main_function(table_name))

    # Add UDF definition if not in template
    if "convert_to_json_udf" not in new_content:
        new_content += """
def convert_to_json_string(field):
    \"\"\"Convert complex data to JSON strings to avoid nested structures\"\"\"
    if field is None:
        return None
    try:
        if isinstance(field, str):
            return field
        else:
            return json.dumps(field, ensure_ascii=False, default=str)
    except (TypeError, ValueError) as e:
        logger.warning(f"JSON serialization failed for field: {str(e)}")
        return str(field)

# Define UDF globally so it can be used in all transformation functions
convert_to_json_udf = F.udf(convert_to_json_string, StringType())
"""

    # Append transformation functions
    new_content += "\n\n" + "\n\n".join(transform_functions)

    # Add job.commit() at the end
    new_content += """

if __name__ == "__main__":
    main()
    job.commit()"""

    # Write the refactored file
    with open(py_file, 'w') as f:
        f.write(new_content)

    print(f"✅ Refactored {job_name}")
    return True

def update_main_function_for_iceberg(main_func, table_name):
    """Update main function to read from Iceberg"""
    # Replace Glue catalog reading with Iceberg
    main_func = re.sub(
        r'glueContext\.create_dynamic_frame\.from_catalog\([^)]+\)',
        f'spark.table(f"{{catalog_nm}}.{{DATABASE_NAME}}.{{TABLE_NAME}}")',
        main_func
    )

    # Update data reading section
    main_func = re.sub(
        r'# Step 1:.*?(?=# Step 2:|def |$)',
        f'''# Step 1: Read data from S3 using Iceberg catalog
        logger.info("\\n" + "=" * 50)
        logger.info("📥 STEP 1: READING DATA FROM S3 ICEBERG CATALOG")
        logger.info("=" * 50)
        logger.info(f"Database: {{DATABASE_NAME}}")
        logger.info(f"Table: {{TABLE_NAME}}")
        logger.info(f"Catalog: {{catalog_nm}}")

        # Use Iceberg to read data from S3
        table_name_full = f"{{catalog_nm}}.{{DATABASE_NAME}}.{{TABLE_NAME}}"
        logger.info(f"Reading from table: {{table_name_full}}")

        df_raw = spark.table(table_name_full)

        print(f"=== {table_name.upper()} DATA PREVIEW (Iceberg) ===")
        df_raw.show(5, truncate=False)
        print(f"Total records: {{df_raw.count()}}")
        print("Available columns:", df_raw.columns)

        ''',
        main_func,
        flags=re.DOTALL
    )

    # Update logging level references
    main_func = main_func.replace("logger.setLevel(logging.INFO)", "logger.setLevel(logging.DEBUG)")

    return main_func

def create_basic_main_function(table_name):
    """Create a basic main function for jobs without one"""
    return f'''def main():
    \"\"\"Main ETL process\"\"\"
    start_time = datetime.now()

    try:
        logger.info("=" * 80)
        logger.info("🚀 STARTING ENHANCED FHIR {table_name.upper()} ETL PROCESS")
        logger.info("=" * 80)
        logger.info(f"⏰ Job started at: {{start_time.strftime('%Y-%m-%d %H:%M:%S')}}")
        logger.info(f"📊 Source: {{DATABASE_NAME}}.{{TABLE_NAME}}")
        logger.info(f"🎯 Target: Redshift")

        # Step 1: Read data from S3 using Iceberg catalog
        logger.info("\\n" + "=" * 50)
        logger.info("📥 STEP 1: READING DATA FROM S3 ICEBERG CATALOG")
        logger.info("=" * 50)

        table_name_full = f"{{catalog_nm}}.{{DATABASE_NAME}}.{{TABLE_NAME}}"
        logger.info(f"Reading from table: {{table_name_full}}")

        df_raw = spark.table(table_name_full)
        total_records = df_raw.count()
        logger.info(f"📊 Read {{total_records:,}} raw records")

        # Add transformation steps here

        end_time = datetime.now()
        processing_time = end_time - start_time
        logger.info(f"⏰ Job completed at: {{end_time.strftime('%Y-%m-%d %H:%M:%S')}}")
        logger.info(f"⏱️  Total processing time: {{processing_time}}")
        logger.info("✅ ETL JOB COMPLETED SUCCESSFULLY")

    except Exception as e:
        logger.error(f"🚨 Error: {{str(e)}}")
        logger.error(f"🚨 Error type: {{type(e).__name__}}")
        raise'''

def main():
    """Main script execution"""
    print("🚀 Starting batch refactoring of Glue jobs to Iceberg format")

    # List of jobs to refactor (excluding HMUPatient and HMUAllergyIntolerance as they're done)
    jobs_to_refactor = [
        "HMUCarePlan",
        "HMUCondition",
        "HMUDiagnosticReport",
        "HMUDocumentReference",
        "HMUEncounter",
        "HMUMedication",
        "HMUMedicationDispense",
        "HMUMedicationRequest",
        "HMUObservation",
        "HMUPractitioner",
        "HMUProcedure"
    ]

    base_path = Path(".")
    successful = []
    failed = []

    for job_name in jobs_to_refactor:
        job_dir = base_path / job_name
        if not job_dir.exists():
            print(f"❌ Directory not found: {job_dir}")
            failed.append(job_name)
            continue

        if refactor_glue_job(job_dir, job_name):
            successful.append(job_name)
        else:
            failed.append(job_name)

    print("\n" + "=" * 60)
    print("📊 REFACTORING SUMMARY")
    print("=" * 60)

    if successful:
        print(f"✅ Successfully refactored {len(successful)} jobs:")
        for job in successful:
            print(f"   - {job}")

    if failed:
        print(f"❌ Failed to refactor {len(failed)} jobs:")
        for job in failed:
            print(f"   - {job}")

    print("\n✨ Batch refactoring complete!")

    # Note: This is a simplified refactoring script
    print("\n⚠️  Note: Please review each refactored file to ensure:")
    print("   1. Field naming follows snake_case convention")
    print("   2. Resource type is set correctly")
    print("   3. Table-specific transformations are preserved")
    print("   4. All necessary functions are included")

if __name__ == "__main__":
    main()