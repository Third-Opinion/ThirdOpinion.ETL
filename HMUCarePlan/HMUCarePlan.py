# Deployed: 2025-10-09 04:25:35 UTC
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
from pyspark.sql.types import StringType, TimestampType, DateType, BooleanType, IntegerType, DecimalType
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
TABLE_NAME = "careplan"
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

    window_spec = Window.partitionBy("care_plan_id").orderBy(F.col("meta_last_updated").desc())
    df_latest = df.withColumn("row_num", F.row_number().over(window_spec)) \
                  .filter(F.col("row_num") == 1) \
                  .drop("row_num")

    incoming_count = df.count()
    deduplicated_count = df_latest.count()

    if incoming_count > deduplicated_count:
        logger.info(f"Deduplicated incoming data: {incoming_count} → {deduplicated_count} records (kept latest per care plan)")

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

def extract_id_from_reference(reference_field, resource_type):
    """Extract ID from FHIR reference format"""
    if reference_field:
        # Handle Row/struct format: Row(reference="Patient/123", display="Name")
        if hasattr(reference_field, 'reference'):
            reference = reference_field.reference
            if reference and f"{resource_type}/" in reference:
                return reference.split("/")[-1]
        # Handle dict format: {"reference": "Patient/123", "display": "Name"}
        elif isinstance(reference_field, dict):
            reference = reference_field.get('reference')
            if reference and f"{resource_type}/" in reference:
                return reference.split("/")[-1]
        # Handle string format: "Patient/123"
        elif isinstance(reference_field, str):
            if f"{resource_type}/" in reference_field:
                return reference_field.split("/")[-1]
    return None

extract_patient_id_udf = F.udf(lambda ref: extract_id_from_reference(ref, "Patient"), StringType())
extract_encounter_id_udf = F.udf(lambda ref: extract_id_from_reference(ref, "Encounter"), StringType())
extract_care_team_id_udf = F.udf(lambda ref: extract_id_from_reference(ref, "CareTeam"), StringType())
extract_goal_id_udf = F.udf(lambda ref: extract_id_from_reference(ref, "Goal"), StringType())

def transform_main_care_plan_data(df):
    """Transform the main care plan data"""
    logger.info("Transforming main care plan data...")
    
    select_columns = [
        F.col("id").alias("care_plan_id"),
        F.when(F.col("subject").isNotNull(), 
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        F.col("status").alias("status"),
        F.col("intent").alias("intent"),
        F.col("title").alias("title"),
        # Handle meta.lastUpdated with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("meta_last_updated"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ]
    
    main_df = df.select(*select_columns).filter(
        F.col("care_plan_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return main_df

def transform_care_plan_identifiers(df):
    """Transform care plan identifiers"""
    logger.info("Transforming care plan identifiers...")
    
    if "identifier" not in df.columns:
        logger.warning("identifier column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("care_plan_id"),
            F.lit("").alias("identifier_system"),
            F.lit("").alias("identifier_value")
        ).filter(F.lit(False))
    
    identifiers_df = df.select(
        F.col("id").alias("care_plan_id"),
        F.explode(F.col("identifier")).alias("identifier_item")
    ).filter(
        F.col("identifier_item").isNotNull()
    )
    
    identifiers_final = identifiers_df.select(
        F.col("care_plan_id"),
        F.lit(None).cast(StringType()).alias("identifier_system"),
        F.col("identifier_item.value").alias("identifier_value")
    ).filter(
        F.col("identifier_value").isNotNull()
    )
    
    return identifiers_final

def transform_care_plan_categories(df):
    """Transform care plan categories"""
    logger.info("Transforming care plan categories...")
    
    if "category" not in df.columns:
        logger.warning("category column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("care_plan_id"),
            F.lit("").alias("category_code"),
            F.lit("").alias("category_system"),
            F.lit("").alias("category_display"),
            F.lit("").alias("category_text")
        ).filter(F.lit(False))
    
    categories_df = df.select(
        F.col("id").alias("care_plan_id"),
        F.explode(F.col("category")).alias("category_item")
    ).filter(
        F.col("category_item").isNotNull()
    )
    
    # First check if coding array exists and is not null
    categories_with_coding = categories_df.filter(
        F.col("category_item.coding").isNotNull() &
        (F.size(F.col("category_item.coding")) > 0)
    )
    
    if categories_with_coding.count() == 0:
        logger.warning("No categories with coding found, returning empty DataFrame")
        return categories_df.select(
            F.col("care_plan_id"),
            F.lit("").alias("category_code"),
            F.lit("").alias("category_system"),
            F.lit("").alias("category_display"),
            F.lit("").alias("category_text")
        ).filter(F.lit(False))
    
    categories_exploded = categories_with_coding.select(
        F.col("care_plan_id"),
        F.explode(F.col("category_item.coding")).alias("coding_item"),
        F.lit(None).cast(StringType()).alias("category_text")  # Set to null since text field doesn't exist
    )
    
    categories_final = categories_exploded.select(
        F.col("care_plan_id"),
        F.when(F.col("coding_item.code").isNotNull(), 
               F.col("coding_item.code")).otherwise(None).alias("category_code"),
        F.when(F.col("coding_item.system").isNotNull(), 
               F.col("coding_item.system")).otherwise(None).alias("category_system"),
        F.lit(None).cast(StringType()).alias("category_display"),  # Set to null since display field doesn't exist
        F.col("category_text")
    ).filter(
        F.col("category_code").isNotNull()
    )
    
    return categories_final

def transform_care_plan_care_teams(df):
    """Transform care plan care teams"""
    logger.info("Transforming care plan care teams...")
    
    if "careTeam" not in df.columns:
        logger.warning("careTeam column not found, returning empty DataFrame")
        return df.select(
            F.col("id").alias("care_plan_id"),
            F.lit("").alias("care_team_id")
        ).filter(F.lit(False))
    
    care_teams_df = df.select(
        F.col("id").alias("care_plan_id"),
        F.explode(F.col("careTeam")).alias("care_team_item")
    ).filter(
        F.col("care_team_item").isNotNull() &
        F.col("care_team_item.reference").isNotNull()
    )
    
    care_teams_final = care_teams_df.select(
        F.col("care_plan_id"),
        F.regexp_extract(F.col("care_team_item.reference"), r"CareTeam/(.+)", 1).alias("care_team_id")
    ).filter(F.col("care_team_id") != "")
    
    return care_teams_final

def transform_care_plan_goals(df):
    """Transform care plan goals"""
    logger.info("Transforming care plan goals...")
    
    if "goal" not in df.columns:
        logger.warning("goal column not found, returning empty DataFrame")
        return df.select(
            F.col("id").alias("care_plan_id"),
            F.lit("").alias("goal_id")
        ).filter(F.lit(False))
        
    goals_df = df.select(
        F.col("id").alias("care_plan_id"),
        F.explode(F.col("goal")).alias("goal_item")
    ).filter(
        F.col("goal_item").isNotNull() &
        F.col("goal_item.reference").isNotNull()
    )
    
    goals_final = goals_df.select(
        F.col("care_plan_id"),
        F.regexp_extract(F.col("goal_item.reference"), r"Goal/(.+)", 1).alias("goal_id")
    ).filter(F.col("goal_id") != "")
    
    return goals_final

def create_care_plans_table_sql():
    """Generate SQL for creating main care_plans table in Redshift"""
    return """
    CREATE TABLE IF NOT EXISTS public.care_plans (
        care_plan_id VARCHAR(255) NOT NULL,
        patient_id VARCHAR(255) NOT NULL,
        status VARCHAR(50),
        intent VARCHAR(50),
        title VARCHAR(500),
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id);
    """

def create_care_plan_identifiers_table_sql():
    """Generate SQL for creating care_plan_identifiers table"""
    return """
    CREATE TABLE IF NOT EXISTS public.care_plan_identifiers (
        care_plan_id VARCHAR(255),
        identifier_system VARCHAR(255),
        identifier_value VARCHAR(255)
    ) SORTKEY (care_plan_id, identifier_system);
    """

def create_care_plan_categories_table_sql():
    """Generate SQL for creating care_plan_categories table"""
    return """
    CREATE TABLE IF NOT EXISTS public.care_plan_categories (
        care_plan_id VARCHAR(255),
        category_code VARCHAR(50),
        category_system VARCHAR(255),
        category_display VARCHAR(255),
        category_text VARCHAR(500)
    ) SORTKEY (care_plan_id, category_code);
    """

def create_care_plan_care_teams_table_sql():
    """Generate SQL for creating care_plan_care_teams table"""
    return """
    CREATE TABLE IF NOT EXISTS public.care_plan_care_teams (
        care_plan_id VARCHAR(255),
        care_team_id VARCHAR(255)
    ) SORTKEY (care_plan_id);
    """

def create_care_plan_goals_table_sql():
    """Generate SQL for creating care_plan_goals table"""
    return """
    CREATE TABLE IF NOT EXISTS public.care_plan_goals (
        care_plan_id VARCHAR(255),
        goal_id VARCHAR(255)
    ) SORTKEY (care_plan_id);
    """

def write_to_redshift_versioned(dynamic_frame, table_name, primary_key_column, preactions=""):
    """Write DynamicFrame to Redshift using JDBC connection"""
    logger.info(f"Writing {table_name} to Redshift...")
    logger.info(f"🔧 Preactions SQL for {table_name}:\n{preactions}")
    
    try:
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
        logger.info(f"✅ Successfully wrote {table_name} to Redshift")
    except Exception as e:
        logger.error(f"❌ Failed to write {table_name} to Redshift: {str(e)}")
        raise e

def main():
    """Main ETL process"""
    start_time = datetime.now()
    try:
        logger.info("=" * 80)
        logger.info("🚀 STARTING FHIR CARE PLAN ETL PROCESS")
        logger.info("=" * 80)
        
        # Step 1: Read data from S3 using Iceberg catalog
        logger.info("📥 STEP 1: READING DATA FROM GLUE CATALOG")
        # Use Iceberg to read data from S3
        table_name_full = f"{catalog_nm}.{DATABASE_NAME}.{TABLE_NAME}"
        logger.info(f"Reading from table: {table_name_full}")
        df_raw = spark.table(table_name_full)
        care_plan_df = df_raw

        # TESTING MODE: Sample data for quick testing

        # Set to True to process only a sample of records

        USE_SAMPLE = False  # Set to True for testing with limited data

        SAMPLE_SIZE = 1000

        

        if USE_SAMPLE:

            logger.info(f"⚠️  TESTING MODE: Sampling {SAMPLE_SIZE} records for quick testing")

            logger.info("⚠️  Set USE_SAMPLE = False for production runs")

            care_plan_df = care_plan_df.limit(SAMPLE_SIZE)

        else:

            logger.info("✅ Processing full dataset")

            care_plan_df = care_plan_df

        total_records = care_plan_df.count()
        logger.info(f"📊 Read {total_records:,} raw care plan records")
        if total_records == 0:
            logger.error("❌ No raw data found!")
            return

        # Step 2: Transform main care plan data
        logger.info("🔄 STEP 2: TRANSFORMING MAIN CARE PLAN DATA")
        main_care_plan_df = transform_main_care_plan_data(care_plan_df)
        main_count = main_care_plan_df.count()
        logger.info(f"✅ Transformed {main_count:,} main care plan records")
        if main_count == 0:
            logger.error("❌ No main care plan records after transformation!")
            return

        # Step 3: Transform multi-valued data
        logger.info("🔄 STEP 3: TRANSFORMING MULTI-VALUED DATA")
        identifiers_df = transform_care_plan_identifiers(care_plan_df)
        identifiers_count = identifiers_df.count()
        logger.info(f"✅ Transformed {identifiers_count:,} identifier records")
        
        categories_df = transform_care_plan_categories(care_plan_df)
        categories_count = categories_df.count()
        logger.info(f"✅ Transformed {categories_count:,} category records")

        care_teams_df = transform_care_plan_care_teams(care_plan_df)
        care_teams_count = care_teams_df.count()
        logger.info(f"✅ Transformed {care_teams_count:,} care team records")

        goals_df = transform_care_plan_goals(care_plan_df)
        goals_count = goals_df.count()
        logger.info(f"✅ Transformed {goals_count:,} goal records")

        # Step 4: Convert to DynamicFrames
        logger.info("🔄 STEP 4: CONVERTING TO DYNAMICFRAMES")
        main_dynamic_frame = DynamicFrame.fromDF(main_care_plan_df, glueContext, "main_care_plan_dynamic_frame")
        identifiers_dynamic_frame = DynamicFrame.fromDF(identifiers_df, glueContext, "identifiers_dynamic_frame")
        categories_dynamic_frame = DynamicFrame.fromDF(categories_df, glueContext, "categories_dynamic_frame")
        care_teams_dynamic_frame = DynamicFrame.fromDF(care_teams_df, glueContext, "care_teams_dynamic_frame")
        goals_dynamic_frame = DynamicFrame.fromDF(goals_df, glueContext, "goals_dynamic_frame")

        # Step 5: Resolve choice types
        logger.info("🔄 STEP 5: RESOLVING CHOICE TYPES")
        main_resolved_frame = main_dynamic_frame.resolveChoice(
            specs=[
                ("care_plan_id", "cast:string"),
                ("patient_id", "cast:string"),
                ("status", "cast:string"),
                ("intent", "cast:string"),
                ("title", "cast:string"),
                ("meta_last_updated", "cast:timestamp"),
                ("created_at", "cast:timestamp"),
                ("updated_at", "cast:timestamp")
            ]
        )

        identifiers_resolved_frame = identifiers_dynamic_frame.resolveChoice(
            specs=[
                ("care_plan_id", "cast:string"),
                ("identifier_system", "cast:string"),
                ("identifier_value", "cast:string")
            ]
        )

        categories_resolved_frame = categories_dynamic_frame.resolveChoice(
            specs=[
                ("care_plan_id", "cast:string"),
                ("category_code", "cast:string"),
                ("category_system", "cast:string"),
                ("category_display", "cast:string"),
                ("category_text", "cast:string")
            ]
        )

        care_teams_resolved_frame = care_teams_dynamic_frame.resolveChoice(
            specs=[
                ("care_plan_id", "cast:string"),
                ("care_team_id", "cast:string")
            ]
        )

        goals_resolved_frame = goals_dynamic_frame.resolveChoice(
            specs=[
                ("care_plan_id", "cast:string"),
                ("goal_id", "cast:string")
            ]
        )
        
        # Step 6: Write to Redshift
        logger.info("💾 STEP 6: WRITING DATA TO REDSHIFT")
        write_to_redshift_versioned(main_resolved_frame, "care_plans", "care_plan_id", create_care_plans_table_sql())
        if identifiers_count > 0:
            write_to_redshift_versioned(identifiers_resolved_frame, "care_plan_identifiers", "care_plan_id", create_care_plan_identifiers_table_sql())
        if categories_count > 0:
            write_to_redshift_versioned(categories_resolved_frame, "care_plan_categories", "care_plan_id", create_care_plan_categories_table_sql())
        if care_teams_count > 0:
            write_to_redshift_versioned(care_teams_resolved_frame, "care_plan_care_teams", "care_plan_id", create_care_plan_care_teams_table_sql())
        if goals_count > 0:
            write_to_redshift_versioned(goals_resolved_frame, "care_plan_goals", "care_plan_id", create_care_plan_goals_table_sql())

        end_time = datetime.now()
        logger.info("=" * 80)
        
        if USE_SAMPLE:
            logger.info("⚠️  WARNING: THIS WAS A TEST RUN WITH SAMPLED DATA")
            logger.info(f"⚠️  Only {SAMPLE_SIZE} records were processed")
            logger.info("⚠️  Set USE_SAMPLE = False for production runs")
        logger.info("🎉 ETL PROCESS COMPLETED SUCCESSFULLY!")
        logger.info(f"⏱️  Total processing time: {end_time - start_time}")
        logger.info("=" * 80)

    except Exception as e:
        logger.error("❌ ETL PROCESS FAILED!")
        logger.error(f"🚨 Error: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
    job.commit()
