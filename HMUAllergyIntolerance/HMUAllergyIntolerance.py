# Deployed: 2025-10-09 06:27:50 UTC
from datetime import datetime
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

# Import FHIR version comparison utilities

# Table utility functions (inlined for Glue compatibility)
def check_and_log_table_schema(glueContext, table_name, redshift_connection, s3_temp_dir):
    """Check if a Redshift table exists and log its column information."""
    logger.info("=" * 60)
    logger.info(f"🔍 Checking table: public.{table_name}")
    logger.info("=" * 60)
    try:
        existing_table = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={"redshiftTmpDir": s3_temp_dir, "useConnectionProperties": "true", 
                              "dbtable": f"public.{table_name}", "connectionName": redshift_connection},
            transformation_ctx=f"check_table_{table_name}")
        df = existing_table.toDF()
        logger.info(f"✅ Table 'public.{table_name}' EXISTS")
        logger.info(f"📋 Table Schema:")
        logger.info(f"   {'Column Name':<40} {'Data Type':<20}")
        logger.info(f"   {'-'*40} {'-'*20}")
        for field in df.schema.fields:
            logger.info(f"   {field.name:<40} {str(field.dataType):<20}")
        row_count = df.count()
        logger.info(f"📊 Table Statistics:")
        logger.info(f"   Total columns: {len(df.schema.fields)}")
        logger.info(f"   Total rows: {row_count:,}")
        return True
    except Exception as e:
        logger.warning(f"⚠️  Table 'public.{table_name}' DOES NOT EXIST or cannot be accessed")
        logger.debug(f"   Error details: {str(e)}")
        logger.info(f"   Table will be created on first write operation")
        return False

def check_all_tables(glueContext, table_names, redshift_connection, s3_temp_dir):
    """Check existence and schema for multiple tables."""
    logger.info("=" * 80)
    logger.info(f"🔍 CHECKING REDSHIFT TABLES")
    logger.info("=" * 80)
    logger.info(f"Tables to check: {', '.join(table_names)}")
    table_status = {}
    for table_name in table_names:
        exists = check_and_log_table_schema(glueContext, table_name, redshift_connection, s3_temp_dir)
        table_status[table_name] = exists
    logger.info("=" * 80)
    logger.info(f"📊 TABLE CHECK SUMMARY")
    logger.info("=" * 80)
    existing_count = sum(1 for exists in table_status.values() if exists)
    missing_count = len(table_names) - existing_count
    logger.info(f"Total tables checked: {len(table_names)}")
    logger.info(f"✅ Existing tables: {existing_count}")
    logger.info(f"⚠️  Missing tables: {missing_count}")
    if missing_count > 0:
        missing_tables = [name for name, exists in table_status.items() if not exists]
        logger.info(f"Missing tables (will be created):")
        for table in missing_tables:
            logger.info(f"  - {table}")
    logger.info("=" * 80)
    return table_status

# FHIR version comparison utilities implemented inline below

# Timestamp-based versioning utilities
from pyspark.sql import DataFrame
from typing import Set

# Set up logging to write to stdout (CloudWatch)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Add handler to write logs to stdout so they appear in CloudWatch
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
catalog_nm = "glue_catalog"

s3_bucket = "s3://7df690fd40c734f8937daf02f39b2ec3-457560472834-group/datalake/hmu_fhir_data_store_836e877666cebf177ce6370ec1478a92_healthlake_view/"
ahl_database = "hmu_fhir_data_store_836e877666cebf177ce6370ec1478a92_healthlake_view"
tableCatalogId = "457560472834"  # AHL service account
s3_output_bucket = "s3://healthlake-glue-output-2025"


spark = (SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{catalog_nm}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{catalog_nm}.warehouse", s3_bucket)
    .config(f"spark.sql.catalog.{catalog_nm}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{catalog_nm}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
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
TABLE_NAME = "allergyintolerance"
REDSHIFT_CONNECTION = "Redshift connection"
S3_TEMP_DIR = "s3://aws-glue-assets-442042533707-us-east-2/temporary/"

# Note: We now read data from S3 using Iceberg catalog instead of Glue Catalog
# This provides better performance and direct access to S3 data

def convert_to_json_string(field):
    """Convert complex data to JSON strings to avoid nested structures"""
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

def get_existing_versions_from_redshift(glue_context, table_name: str, primary_key_column: str) -> Set[str]:
    """
    Retrieve existing version timestamps from Redshift to identify records for deletion

    Args:
        glue_context: AWS Glue context
        table_name: Name of the table to query
        primary_key_column: Name of the primary key column (unused but kept for consistency)

    Returns:
        Set of existing meta_last_updated timestamps as strings
    """
    try:
        # Read existing data from Redshift
        existing_df = glue_context.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": f"public.{table_name}",
                "connectionName": REDSHIFT_CONNECTION,
            },
            transformation_ctx=f"read_existing_{table_name}"
        ).toDF()

        # Extract unique meta_last_updated values
        if existing_df.count() > 0 and "meta_last_updated" in existing_df.columns:
            versions = existing_df.select("meta_last_updated").distinct().rdd.map(lambda row: str(row[0]) if row[0] is not None else None).filter(lambda x: x is not None).collect()
            logger.info(f"Found {len(versions)} existing version timestamps in {table_name}")
            return set(versions)
        else:
            logger.info(f"No existing data found in {table_name}")
            return set()

    except Exception as e:
        logger.warning(f"Could not read existing data from {table_name}: {str(e)}")
        return set()

def filter_dataframe_by_version(df: DataFrame, existing_versions: Set[str]) -> DataFrame:
    """
    Filter DataFrame to exclude records with meta_last_updated timestamps that exist in Redshift

    Args:
        df: Source DataFrame
        existing_versions: Set of existing meta_last_updated timestamps

    Returns:
        Filtered DataFrame containing only new/updated records
    """
    # Step 1: Deduplicate incoming data - keep only latest version per entity
    from pyspark.sql.window import Window

    window_spec = Window.partitionBy("allergy_intolerance_id").orderBy(F.col("meta_last_updated").desc())
    df_latest = df.withColumn("row_num", F.row_number().over(window_spec)) \
                  .filter(F.col("row_num") == 1) \
                  .drop("row_num")

    incoming_count = df.count()
    deduplicated_count = df_latest.count()

    if incoming_count > deduplicated_count:
        logger.info(f"Deduplicated incoming data: {incoming_count} → {deduplicated_count} records (kept latest per allergy intolerance)")

    if not existing_versions:
        logger.info("No existing versions found, processing all records")
        return df_latest

    # Step 2: Convert timestamps to string format for comparison
    df_with_version_string = df_latest.withColumn(
        "meta_last_updated_str",
        F.when(F.col("meta_last_updated").isNotNull(),
               F.date_format(F.col("meta_last_updated"), "yyyy-MM-dd HH:mm:ss"))
        .otherwise(F.lit(None))
    )

    # Filter out records that already exist (based on timestamp)
    filtered_df = df_with_version_string.filter(
        (~F.col("meta_last_updated_str").isin(list(existing_versions))) |
        F.col("meta_last_updated_str").isNull()
    ).drop("meta_last_updated_str")

    filtered_count = filtered_df.count()
    logger.info(f"Filtered from {deduplicated_count} to {filtered_count} records ({deduplicated_count - filtered_count} already exist in Redshift)")

    return filtered_df

def get_entities_to_delete(glue_context, table_name: str, current_df: DataFrame, primary_key_column: str) -> Set[str]:
    """
    Identify entities that should be deleted (exist in Redshift but not in current dataset)

    Args:
        glue_context: AWS Glue context
        table_name: Name of the table to check
        current_df: Current DataFrame being processed
        primary_key_column: Name of the primary key column

    Returns:
        Set of primary key values that should be deleted
    """
    try:
        # Read existing data from Redshift
        existing_df = glue_context.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": f"public.{table_name}",
                "connectionName": REDSHIFT_CONNECTION,
            },
            transformation_ctx=f"read_existing_{table_name}_for_deletion"
        ).toDF()

        if existing_df.count() == 0:
            logger.info(f"No existing data in {table_name} to check for deletion")
            return set()

        # Get primary keys from both datasets
        existing_keys = set(existing_df.select(primary_key_column).rdd.map(lambda row: row[0]).collect())
        current_keys = set(current_df.select(primary_key_column).rdd.map(lambda row: row[0]).collect())

        # Find keys that exist in Redshift but not in current data
        keys_to_delete = existing_keys - current_keys

        if keys_to_delete:
            logger.info(f"Found {len(keys_to_delete)} entities to delete from {table_name}")
        else:
            logger.info(f"No entities to delete from {table_name}")

        return keys_to_delete

    except Exception as e:
        logger.warning(f"Could not check for entities to delete in {table_name}: {str(e)}")
        return set()

def transform_main_allergy_intolerance_data(df):
    """Transform the main allergy intolerance data"""
    logger.info("Transforming main allergy intolerance data...")

    # Log available columns for debugging
    available_columns = df.columns
    logger.info(f"Available columns: {available_columns}")

    # Build the select statement dynamically based on available columns
    select_columns = [
        F.col("id").alias("allergy_intolerance_id"),
        F.lit("AllergyIntolerance").alias("resourcetype"),  # Always "AllergyIntolerance" for this resource

        # Clinical Status - following naming convention
        F.when(F.col("clinicalStatus").isNotNull() & F.col("clinicalStatus.coding").isNotNull() & (F.size(F.col("clinicalStatus.coding")) > 0),
               F.col("clinicalStatus.coding")[0].getField("code")
              ).otherwise(None).alias("clinical_status_code"),
        F.when(F.col("clinicalStatus").isNotNull() & F.col("clinicalStatus.coding").isNotNull() & (F.size(F.col("clinicalStatus.coding")) > 0),
               F.col("clinicalStatus.coding")[0].getField("display")
              ).otherwise(None).alias("clinical_status_display"),
        F.when(F.col("clinicalStatus").isNotNull() & F.col("clinicalStatus.coding").isNotNull() & (F.size(F.col("clinicalStatus.coding")) > 0),
               F.col("clinicalStatus.coding")[0].getField("system")
              ).otherwise(None).alias("clinical_status_system"),

        # Verification Status - following naming convention
        F.when(F.col("verificationStatus").isNotNull() & F.col("verificationStatus.coding").isNotNull() & (F.size(F.col("verificationStatus.coding")) > 0),
               F.col("verificationStatus.coding")[0].getField("code")
              ).otherwise(None).alias("verification_status_code"),
        F.when(F.col("verificationStatus").isNotNull() & F.col("verificationStatus.coding").isNotNull() & (F.size(F.col("verificationStatus.coding")) > 0),
               F.col("verificationStatus.coding")[0].getField("display")
              ).otherwise(None).alias("verification_status_display"),
        F.when(F.col("verificationStatus").isNotNull() & F.col("verificationStatus.coding").isNotNull() & (F.size(F.col("verificationStatus.coding")) > 0),
               F.col("verificationStatus.coding")[0].getField("system")
              ).otherwise(None).alias("verification_status_system"),

        # Type and Category
        F.col("type").alias("type"),
        convert_to_json_udf(F.col("category")).alias("category"),

        # Criticality
        F.col("criticality").alias("criticality"),

        # Code - following naming convention
        F.when(F.col("code").isNotNull() & F.col("code.coding").isNotNull() & (F.size(F.col("code.coding")) > 0),
               F.col("code.coding")[0].getField("code")
              ).otherwise(None).alias("code"),
        F.when(F.col("code").isNotNull() & F.col("code.coding").isNotNull() & (F.size(F.col("code.coding")) > 0),
               F.col("code.coding")[0].getField("display")
              ).otherwise(None).alias("code_display"),
        F.when(F.col("code").isNotNull() & F.col("code.coding").isNotNull() & (F.size(F.col("code.coding")) > 0),
               F.col("code.coding")[0].getField("system")
              ).otherwise(None).alias("code_system"),
        F.col("code.text").alias("code_text"),

        # Patient Reference - following naming convention
        F.when(F.col("patient.reference").isNotNull(),
               F.regexp_extract(F.col("patient.reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),

        # Encounter Reference - following naming convention
        F.when(F.col("encounter.reference").isNotNull(),
               F.regexp_extract(F.col("encounter.reference"), r"Encounter/(.+)", 1)
              ).otherwise(None).alias("encounter_id"),

        # Timing fields - following naming convention
        # Handle onsetDateTime with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("onsetDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("onsetDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("onsetDateTime"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("onsetDateTime"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("onsetDateTime"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("onset_datetime"),
        F.col("onsetAge.value").alias("onset_age_value"),
        F.col("onsetAge.unit").alias("onset_age_unit"),
        F.col("onsetPeriod.start").alias("onset_period_start"),
        F.col("onsetPeriod.end").alias("onset_period_end"),

        # Recorded date - following naming convention
        F.coalesce(
            F.to_timestamp(F.col("recordedDate"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("recordedDate"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("recordedDate"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("recordedDate"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("recordedDate"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("recorded_date"),

        # Recorder Reference - following naming convention
        F.when(F.col("recorder.reference").isNotNull(),
               F.regexp_extract(F.col("recorder.reference"), r"Practitioner/(.+)", 1)
              ).otherwise(None).alias("recorder_practitioner_id"),

        # Asserter Reference - following naming convention
        F.when(F.col("asserter.reference").isNotNull(),
               F.regexp_extract(F.col("asserter.reference"), r"Practitioner/(.+)", 1)
              ).otherwise(None).alias("asserter_practitioner_id"),

        # Last occurrence - following naming convention
        F.coalesce(
            F.to_timestamp(F.col("lastOccurrence"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("lastOccurrence"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("lastOccurrence"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("lastOccurrence"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("lastOccurrence"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("last_occurrence"),

        # Note and reactions
        convert_to_json_udf(F.col("note")).alias("note"),
        convert_to_json_udf(F.col("reaction")).alias("reactions"),

        # Meta data handling - following naming convention
        F.coalesce(
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("meta_last_updated"),
        F.col("meta.source").alias("meta_source"),
        convert_to_json_udf(F.col("meta.security")).alias("meta_security"),
        convert_to_json_udf(F.col("meta.tag")).alias("meta_tag"),

        # Extensions
        convert_to_json_udf(F.col("extension")).alias("extensions"),

        # Standard audit fields
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ]

    # Transform main allergy intolerance data using only available columns and flatten complex structures
    main_df = df.select(*select_columns).filter(
        F.col("allergy_intolerance_id").isNotNull()
    )

    return main_df

def transform_allergy_identifiers(df):
    """Transform allergy intolerance identifiers (multiple identifiers per allergy)"""
    logger.info("Transforming allergy intolerance identifiers...")

    # Check if identifier column exists
    if "identifier" not in df.columns:
        logger.warning("identifier column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("allergy_intolerance_id"),
            F.lit("").alias("identifier_use"),
            F.lit("").alias("identifier_type_code"),
            F.lit("").alias("identifier_type_display"),
            F.lit("").alias("identifier_system"),
            F.lit("").alias("identifier_value"),
            F.lit(None).cast(DateType()).alias("identifier_period_start"),
            F.lit(None).cast(DateType()).alias("identifier_period_end")
        ).filter(F.lit(False))

    # First explode the identifier array
    identifiers_df = df.select(
        F.col("id").alias("allergy_intolerance_id"),
        F.explode(F.col("identifier")).alias("identifier_item")
    ).filter(
        F.col("identifier_item").isNotNull()
    )

    # Extract identifier details - following naming convention
    identifiers_final = identifiers_df.select(
        F.col("allergy_intolerance_id"),
        F.col("identifier_item.use").alias("identifier_use"),
        F.when(F.col("identifier_item.type").isNotNull() & F.col("identifier_item.type.coding").isNotNull() & (F.size(F.col("identifier_item.type.coding")) > 0),
               F.col("identifier_item.type.coding")[0].getField("code")
              ).otherwise(None).alias("identifier_type_code"),
        F.when(F.col("identifier_item.type").isNotNull() & F.col("identifier_item.type.coding").isNotNull() & (F.size(F.col("identifier_item.type.coding")) > 0),
               F.col("identifier_item.type.coding")[0].getField("display")
              ).otherwise(None).alias("identifier_type_display"),
        F.col("identifier_item.system").alias("identifier_system"),
        F.col("identifier_item.value").alias("identifier_value"),
        F.to_date(F.col("identifier_item.period.start"), "yyyy-MM-dd").alias("identifier_period_start"),
        F.to_date(F.col("identifier_item.period.end"), "yyyy-MM-dd").alias("identifier_period_end")
    ).filter(
        F.col("identifier_value").isNotNull()
    )

    return identifiers_final

def create_redshift_tables_sql():
    """Generate SQL for creating main allergy intolerance table in Redshift with proper syntax"""
    return """
    -- Main allergy intolerance table
    CREATE TABLE IF NOT EXISTS public.allergy_intolerance (
        allergy_intolerance_id VARCHAR(255) NOT NULL,
        resourcetype VARCHAR(50),
        clinical_status_code VARCHAR(50),
        clinical_status_display VARCHAR(255),
        clinical_status_system VARCHAR(255),
        verification_status_code VARCHAR(50),
        verification_status_display VARCHAR(255),
        verification_status_system VARCHAR(255),
        type VARCHAR(50),
        category TEXT,
        criticality VARCHAR(50),
        code VARCHAR(255),
        code_display VARCHAR(500),
        code_system VARCHAR(255),
        code_text VARCHAR(500),
        patient_id VARCHAR(255),
        encounter_id VARCHAR(255),
        onset_datetime TIMESTAMP,
        onset_age_value DECIMAL(10,2),
        onset_age_unit VARCHAR(20),
        onset_period_start DATE,
        onset_period_end DATE,
        recorded_date TIMESTAMP,
        recorder_practitioner_id VARCHAR(255),
        asserter_practitioner_id VARCHAR(255),
        last_occurrence TIMESTAMP,
        note TEXT,
        reactions TEXT,
        meta_last_updated TIMESTAMP,
        meta_source VARCHAR(255),
        meta_security TEXT,
        meta_tag TEXT,
        extensions TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (allergy_intolerance_id) SORTKEY (allergy_intolerance_id, recorded_date)
    """

def create_allergy_identifiers_table_sql():
    """Generate SQL for creating allergy intolerance identifiers table"""
    return """
    CREATE TABLE IF NOT EXISTS public.allergy_intolerance_identifiers (
        allergy_intolerance_id VARCHAR(255),
        identifier_use VARCHAR(50),
        identifier_type_code VARCHAR(50),
        identifier_type_display VARCHAR(255),
        identifier_system VARCHAR(255),
        identifier_value VARCHAR(255),
        identifier_period_start DATE,
        identifier_period_end DATE
    ) SORTKEY (allergy_intolerance_id, identifier_system)
    """

def write_to_redshift_versioned(dynamic_frame, table_name, primary_key_column, preactions=""):
    """Write DynamicFrame to Redshift using JDBC connection"""
    logger.info(f"Writing {table_name} to Redshift...")

    # Add DELETE to preactions to clear existing data while preserving table structure
    if preactions:
        preactions = f"DELETE FROM public.{table_name}; " + preactions
    else:
        preactions = f"DELETE FROM public.{table_name};"

    try:
        # Validate dynamic frame before writing
        record_count = dynamic_frame.count()
        logger.info(f"Writing {record_count} records to {table_name}")

        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="redshift",
            connection_options={
                "redshiftTmpDir": S3_TEMP_DIR,
                "useConnectionProperties": "true",
                "dbtable": f"public.{table_name}",
                "connectionName": REDSHIFT_CONNECTION,
                "preactions": preactions
            },
            transformation_ctx=f"write_{table_name}_to_redshift"
        )
        logger.info(f"✅ Successfully wrote {record_count} records to {table_name} in Redshift")
    except Exception as e:
        logger.error(f"❌ Failed to write {table_name} to Redshift: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        raise

def main():
    """Main ETL process"""
    start_time = datetime.now()

    try:
        logger.info("=" * 80)
        logger.info("🚀 STARTING ENHANCED FHIR ALLERGY INTOLERANCE ETL PROCESS")
        logger.info("=" * 80)
        logger.info(f"⏰ Job started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Check table existence and log schemas
        table_names = ["allergy_intolerance", "allergy_intolerance_identifiers"]
        check_all_tables(glueContext, table_names, REDSHIFT_CONNECTION, S3_TEMP_DIR)

        logger.info(f"📊 Source: {DATABASE_NAME}.{TABLE_NAME}")
        logger.info(f"🎯 Target: Redshift (2 tables)")
        logger.info("🔄 Process: 7 steps (Read → Transform → Convert → Resolve → Validate → Write)")

        # Step 1: Read data from S3 using Iceberg catalog
        logger.info("\n" + "=" * 50)
        logger.info("📥 STEP 1: READING DATA FROM S3 ICEBERG CATALOG")
        logger.info("=" * 50)
        logger.info(f"Database: {DATABASE_NAME}")
        logger.info(f"Table: {TABLE_NAME}")
        logger.info(f"Catalog: {catalog_nm}")

        # Use Iceberg to read allergy intolerance data from S3
        table_name_full = f"{catalog_nm}.{DATABASE_NAME}.{TABLE_NAME}"
        logger.info(f"Reading from table: {table_name_full}")

        allergy_df_raw = spark.table(table_name_full)

        print("=== ALLERGY INTOLERANCE DATA PREVIEW (Iceberg) ===")
        allergy_df_raw.show(5, truncate=False)  # Show 5 rows without truncating
        print(f"Total records: {allergy_df_raw.count()}")
        print("Available columns:", allergy_df_raw.columns)
        print("Schema:")
        allergy_df_raw.printSchema()

        # TESTING MODE: Sample data for quick testing
        USE_SAMPLE = False  # Set to True for testing with limited data
        SAMPLE_SIZE = 1000

        if USE_SAMPLE:
            logger.info(f"⚠️  TESTING MODE: Sampling {SAMPLE_SIZE} records for quick testing")
            logger.info("⚠️  Set USE_SAMPLE = False for production runs")
            allergy_df = allergy_df_raw.limit(SAMPLE_SIZE)
        else:
            logger.info("✅ Processing full dataset")
            allergy_df = allergy_df_raw

        available_columns = allergy_df_raw.columns
        logger.info(f"📋 Available columns in source: {available_columns}")

        logger.info("✅ Successfully read data using S3 Iceberg Catalog")

        total_records = allergy_df.count()
        logger.info(f"📊 Read {total_records:,} raw allergy intolerance records")

        # Debug: Show sample of raw data and schema
        if total_records > 0:
            logger.info("\n🔍 DATA QUALITY CHECKS:")
            logger.info("Sample of raw allergy intolerance data:")
            allergy_df.show(3, truncate=False)

            # Check for NULL values in key fields using efficient aggregation
            from pyspark.sql.functions import sum as spark_sum, when

            null_check_df = allergy_df.agg(
                spark_sum(when(F.col("id").isNull(), 1).otherwise(0)).alias("id_nulls"),
                spark_sum(when(F.col("clinicalStatus").isNull(), 1).otherwise(0)).alias("clinical_status_nulls"),
                spark_sum(when(F.col("verificationStatus").isNull(), 1).otherwise(0)).alias("verification_status_nulls"),
                spark_sum(when(F.col("patient").isNull(), 1).otherwise(0)).alias("patient_nulls")
            ).collect()[0]

            logger.info("NULL value analysis in key fields:")
            for field, alias in [("id", "id_nulls"), ("clinicalStatus", "clinical_status_nulls"),
                               ("verificationStatus", "verification_status_nulls"), ("patient", "patient_nulls")]:
                null_count = null_check_df[alias] or 0
                percentage = (null_count / total_records) * 100 if total_records > 0 else 0
                logger.info(f"  {field}: {null_count:,} NULLs ({percentage:.1f}%)")
        else:
            logger.error("❌ No raw data found! Check the data source.")
            return

        # Step 2: Transform main allergy intolerance data
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 2: TRANSFORMING MAIN ALLERGY INTOLERANCE DATA")
        logger.info("=" * 50)

        main_allergy_df = transform_main_allergy_intolerance_data(allergy_df)
        main_count = main_allergy_df.count()
        logger.info(f"✅ Transformed {main_count:,} main allergy intolerance records")

        if main_count == 0:
            logger.error("❌ No main allergy intolerance records after transformation! Check filtering criteria.")
            return

        # Debug: Show sample of transformed main data
        logger.info("Sample of transformed main allergy intolerance data:")
        main_allergy_df.show(3, truncate=False)

        # Step 3: Transform multi-valued data (identifiers)
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 3: TRANSFORMING MULTI-VALUED DATA")
        logger.info("=" * 50)

        allergy_identifiers_df = transform_allergy_identifiers(allergy_df)
        identifiers_count = allergy_identifiers_df.count()
        logger.info(f"✅ Transformed {identifiers_count:,} identifier records")

        # Debug: Show samples of multi-valued data if available
        if identifiers_count > 0:
            logger.info("Sample of allergy identifiers data:")
            allergy_identifiers_df.show(3, truncate=False)

        # Step 4: Convert to DynamicFrames and ensure data is flat for Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 4: CONVERTING TO DYNAMICFRAMES")
        logger.info("=" * 50)
        logger.info("Converting to DynamicFrames and ensuring Redshift compatibility...")

        # Convert main allergy DataFrame and ensure flat structure
        main_flat_df = main_allergy_df.select(
            F.col("allergy_intolerance_id").cast(StringType()),
            F.col("resourcetype").cast(StringType()),
            F.col("clinical_status_code").cast(StringType()),
            F.col("clinical_status_display").cast(StringType()),
            F.col("clinical_status_system").cast(StringType()),
            F.col("verification_status_code").cast(StringType()),
            F.col("verification_status_display").cast(StringType()),
            F.col("verification_status_system").cast(StringType()),
            F.col("type").cast(StringType()),
            F.col("category").cast(StringType()),
            F.col("criticality").cast(StringType()),
            F.col("code").cast(StringType()),
            F.col("code_display").cast(StringType()),
            F.col("code_system").cast(StringType()),
            F.col("code_text").cast(StringType()),
            F.col("patient_id").cast(StringType()),
            F.col("encounter_id").cast(StringType()),
            F.col("onset_datetime").cast(TimestampType()),
            F.col("onset_age_value").cast("decimal(10,2)"),
            F.col("onset_age_unit").cast(StringType()),
            F.col("onset_period_start").cast(DateType()),
            F.col("onset_period_end").cast(DateType()),
            F.col("recorded_date").cast(TimestampType()),
            F.col("recorder_practitioner_id").cast(StringType()),
            F.col("asserter_practitioner_id").cast(StringType()),
            F.col("last_occurrence").cast(TimestampType()),
            F.col("note").cast(StringType()),
            F.col("reactions").cast(StringType()),
            F.col("meta_last_updated").cast(TimestampType()),
            F.col("meta_source").cast(StringType()),
            F.col("meta_security").cast(StringType()),
            F.col("meta_tag").cast(StringType()),
            F.col("extensions").cast(StringType()),
            F.col("created_at").cast(TimestampType()),
            F.col("updated_at").cast(TimestampType())
        )

        main_dynamic_frame = DynamicFrame.fromDF(main_flat_df, glueContext, "main_allergy_dynamic_frame")

        # Convert identifiers DataFrame with type casting
        identifiers_flat_df = allergy_identifiers_df.select(
            F.col("allergy_intolerance_id").cast(StringType()),
            F.col("identifier_use").cast(StringType()),
            F.col("identifier_type_code").cast(StringType()),
            F.col("identifier_type_display").cast(StringType()),
            F.col("identifier_system").cast(StringType()),
            F.col("identifier_value").cast(StringType()),
            F.col("identifier_period_start").cast(DateType()),
            F.col("identifier_period_end").cast(DateType())
        )
        identifiers_dynamic_frame = DynamicFrame.fromDF(identifiers_flat_df, glueContext, "identifiers_dynamic_frame")

        # Step 5: Resolve any remaining choice types to ensure Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 5: RESOLVING CHOICE TYPES")
        logger.info("=" * 50)
        logger.info("Resolving choice types for Redshift compatibility with Glue 4 optimizations...")

        main_resolved_frame = main_dynamic_frame.resolveChoice(specs=[("*", "cast:string")])
        identifiers_resolved_frame = identifiers_dynamic_frame.resolveChoice(specs=[("*", "cast:string")])

        # Step 6: Final validation before writing
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 6: FINAL VALIDATION")
        logger.info("=" * 50)
        logger.info("Performing final validation before writing to Redshift...")

        # Validate main allergy intolerance data
        main_final_df = main_resolved_frame.toDF()
        main_final_count = main_final_df.count()
        logger.info(f"Final main allergy intolerance count: {main_final_count}")

        if main_final_count == 0:
            logger.error("No main allergy intolerance records to write to Redshift! Stopping the process.")
            return

        # Validate other tables
        identifiers_final_count = identifiers_resolved_frame.toDF().count()

        logger.info(f"Final counts - Identifiers: {identifiers_final_count}")

        # Debug: Show final sample data being written
        logger.info("Final sample data being written to Redshift (main allergy intolerance):")
        main_final_df.show(3, truncate=False)

        # Step 7: Create tables and write to Redshift
        logger.info("\n" + "=" * 50)
        logger.info("💾 STEP 7: WRITING DATA TO REDSHIFT")
        logger.info("=" * 50)
        logger.info(f"🔗 Using connection: {REDSHIFT_CONNECTION}")
        logger.info(f"📁 S3 temp directory: {S3_TEMP_DIR}")

        # Create all tables individually
        logger.info("📝 Creating main allergy intolerance table...")
        allergy_table_sql = create_redshift_tables_sql()
        write_to_redshift_versioned(main_resolved_frame, "allergy_intolerance", "allergy_intolerance_id", allergy_table_sql)
        logger.info("✅ Main allergy intolerance table created and written successfully")

        logger.info("📝 Creating allergy intolerance identifiers table...")
        identifiers_table_sql = create_allergy_identifiers_table_sql()
        write_to_redshift_versioned(identifiers_resolved_frame, "allergy_intolerance_identifiers", "allergy_intolerance_id", identifiers_table_sql)
        logger.info("✅ Allergy intolerance identifiers table created and written successfully")

        # Calculate processing time
        end_time = datetime.now()
        processing_time = end_time - start_time

        logger.info("\n" + "=" * 80)
        logger.info("🎉 ETL PROCESS COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        logger.info(f"⏰ Job completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"⏱️  Total processing time: {processing_time}")
        logger.info(f"📊 Processing rate: {total_records / processing_time.total_seconds():.2f} records/second")

        logger.info("\n📋 TABLES WRITTEN TO REDSHIFT:")
        logger.info("  ✅ public.allergy_intolerance (main allergy data)")
        logger.info("  ✅ public.allergy_intolerance_identifiers (allergy identifiers)")

        logger.info("\n📊 FINAL ETL STATISTICS:")
        logger.info(f"  📥 Total raw records processed: {total_records:,}")
        logger.info(f"  🏥 Main allergy intolerance records: {main_count:,}")
        logger.info(f"  🆔 Identifier records: {identifiers_count:,}")

        # Calculate data expansion ratio
        total_output_records = main_count + identifiers_count
        expansion_ratio = total_output_records / total_records if total_records > 0 else 0
        logger.info(f"  📈 Data expansion ratio: {expansion_ratio:.2f}x (output records / input records)")

        logger.info("\n" + "=" * 80)

        if USE_SAMPLE:
            logger.info("⚠️  WARNING: THIS WAS A TEST RUN WITH SAMPLED DATA")
            logger.info(f"⚠️  Only {SAMPLE_SIZE} records were processed")
            logger.info("⚠️  Set USE_SAMPLE = False for production runs")
        logger.info("✅ ETL JOB COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)

    except Exception as e:
        end_time = datetime.now()
        processing_time = end_time - start_time
        logger.error("\n" + "=" * 80)
        logger.error("❌ ETL PROCESS FAILED!")
        logger.error("=" * 80)
        logger.error(f"⏰ Job failed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.error(f"⏱️  Processing time before failure: {processing_time}")
        logger.error(f"🚨 Error: {str(e)}")
        logger.error(f"🚨 Error type: {type(e).__name__}")
        logger.error("=" * 80)
        raise

if __name__ == "__main__":
    main()
    job.commit()