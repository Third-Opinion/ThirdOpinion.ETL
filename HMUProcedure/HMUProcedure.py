# Deployed: 2025-10-09 04:25:45 UTC
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
# FHIR version comparison utilities implemented inline below

# FHIR version comparison utilities are implemented inline below
# Table utility functions (inlined for Glue compatibility)
def check_and_log_table_schema(glueContext, table_name, redshift_connection, s3_temp_dir):
    """Check if a Redshift table exists and log its column information."""
    logger.info(f"\n{'='*60}")
    logger.info(f"🔍 Checking table: public.{table_name}")
    logger.info("=" * 60)

    try:
        existing_table = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "redshiftTmpDir": s3_temp_dir,
                "useConnectionProperties": "true",
                "dbtable": f"public.{table_name}",
                "connectionName": redshift_connection
            },
            transformation_ctx=f"check_table_{table_name}"
        )

        df = existing_table.toDF()
        logger.info(f"✅ Table 'public.{table_name}' EXISTS")
        logger.info(f"\n📋 Table Schema:")
        logger.info(f"   {'Column Name':<40} {'Data Type':<20}")
        logger.info(f"   {'-'*40} {'-'*20}")

        for field in df.schema.fields:
            logger.info(f"   {field.name:<40} {str(field.dataType):<20}")

        row_count = df.count()
        logger.info(f"\n📊 Table Statistics:")
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
    logger.info(f"\n{'='*80}")
    logger.info(f"🔍 CHECKING REDSHIFT TABLES")
    logger.info("=" * 80)
    logger.info(f"Tables to check: {', '.join(table_names)}")

    table_status = {}
    for table_name in table_names:
        exists = check_and_log_table_schema(glueContext, table_name, redshift_connection, s3_temp_dir)
        table_status[table_name] = exists

    logger.info(f"\n{'='*80}")
    logger.info(f"📊 TABLE CHECK SUMMARY")
    logger.info("=" * 80)

    existing_count = sum(1 for exists in table_status.values() if exists)
    missing_count = len(table_names) - existing_count

    logger.info(f"Total tables checked: {len(table_names)}")
    logger.info(f"✅ Existing tables: {existing_count}")
    logger.info(f"⚠️  Missing tables: {missing_count}")

    if missing_count > 0:
        missing_tables = [name for name, exists in table_status.items() if not exists]
        logger.info(f"\nMissing tables (will be created):")
        for table in missing_tables:
            logger.info(f"  - {table}")

    logger.info(f"{'='*80}\n")
    return table_status


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
TABLE_NAME = "procedure"
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

def get_existing_versions_from_redshift(table_name, id_column):
    """Query Redshift to get existing entity timestamps for comparison"""
    logger.info(f"Fetching existing timestamps from {table_name}...")

    try:
        # First check if table exists by trying to read it directly
        # This prevents malformed query errors when table doesn't exist
        existing_versions_df = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "redshiftTmpDir": S3_TEMP_DIR,
                "useConnectionProperties": "true",
                "dbtable": f"public.{table_name}",
                "connectionName": REDSHIFT_CONNECTION
            },
            transformation_ctx=f"read_existing_versions_{table_name}"
        )

        # Convert to Spark DataFrame for easier processing
        existing_df = existing_versions_df.toDF()

        # Select only the columns we need if table exists
        if id_column in existing_df.columns and 'meta_last_updated' in existing_df.columns:
            existing_df = existing_df.select(id_column, 'meta_last_updated')
        else:
            logger.warning(f"Table {table_name} exists but missing required columns: {id_column} or meta_last_updated")
            return {}

        # Collect as dictionary: {entity_id: timestamp}
        timestamp_map = {}
        if existing_df.count() > 0:
            rows = existing_df.collect()
            for row in rows:
                entity_id = row[id_column]
                timestamp = row['meta_last_updated']
                if entity_id and timestamp:
                    timestamp_map[entity_id] = timestamp

        logger.info(f"Found {len(timestamp_map)} existing entities with timestamps in {table_name}")
        return timestamp_map

    except Exception as e:
        logger.info(f"Table {table_name} does not exist or is empty - treating all records as new")
        logger.debug(f"Details: {str(e)}")
        return {}

def filter_dataframe_by_version(df, existing_versions, id_column):
    """Filter DataFrame based on version comparison"""
    logger.info("Filtering data based on version comparison...")

    # Step 1: Deduplicate incoming data - keep only latest version per entity
    from pyspark.sql.window import Window

    window_spec = Window.partitionBy(id_column).orderBy(F.col("meta_last_updated").desc())
    df_latest = df.withColumn("row_num", F.row_number().over(window_spec)) \
                  .filter(F.col("row_num") == 1) \
                  .drop("row_num")

    incoming_count = df.count()
    deduplicated_count = df_latest.count()

    if incoming_count > deduplicated_count:
        logger.info(f"Deduplicated incoming data: {incoming_count} → {deduplicated_count} records (kept latest per entity)")

    if not existing_versions:
        # No existing data, all records are new
        logger.info(f"No existing versions found - treating all {deduplicated_count} records as new")
        return df_latest, deduplicated_count, 0

    # Step 2: Compare with existing versions
    # Add a column to mark records that need processing
    def needs_processing(entity_id, last_updated):
        """Check if record needs processing based on timestamp comparison"""
        if entity_id is None or last_updated is None:
            return True  # Process records with missing IDs/timestamps

        existing_timestamp = existing_versions.get(entity_id)
        if existing_timestamp is None:
            return True  # New entity

        # Convert timestamps to comparable format if needed
        # If timestamps are already datetime objects, direct comparison works
        if existing_timestamp == last_updated:
            return False  # Same timestamp, skip

        # Process if incoming timestamp is newer than existing
        # Note: This handles the case where timestamps might be different
        # In production, you may want to add tolerance for small time differences
        try:
            return last_updated > existing_timestamp
        except TypeError:
            # If comparison fails (e.g., different types), process the record
            return True

    # Create UDF for timestamp comparison
    from pyspark.sql.types import BooleanType
    needs_processing_udf = F.udf(needs_processing, BooleanType())

    # Add processing flag
    df_with_flag = df_latest.withColumn(
        "needs_processing",
        needs_processing_udf(F.col(id_column), F.col("meta_last_updated"))
    )

    # Split into processing needed and skipped
    to_process_df = df_with_flag.filter(F.col("needs_processing") == True).drop("needs_processing")
    skipped_count = df_with_flag.filter(F.col("needs_processing") == False).count()

    to_process_count = to_process_df.count()
    total_count = df_latest.count()

    logger.info(f"Version comparison results:")
    logger.info(f"  Total incoming records: {total_count}")
    logger.info(f"  Records to process (new/updated): {to_process_count}")
    logger.info(f"  Records to skip (same version): {skipped_count}")

    return to_process_df, to_process_count, skipped_count

def get_entities_to_delete(df, existing_versions, id_column):
    """Get list of entity IDs that need their old versions deleted"""
    logger.info("Identifying entities that need old version cleanup...")

    if not existing_versions:
        return []

    # Get list of entity IDs from incoming data
    incoming_entity_ids = set()
    if df.count() > 0:
        entity_rows = df.select(id_column).distinct().collect()
        incoming_entity_ids = {row[id_column] for row in entity_rows if row[id_column]}

    # Find entities that exist in both incoming data and Redshift
    entities_to_delete = []
    for entity_id in incoming_entity_ids:
        if entity_id in existing_versions:
            entities_to_delete.append(entity_id)

    logger.info(f"Found {len(entities_to_delete)} entities that need old version cleanup")
    return entities_to_delete

def transform_main_procedure_data(df):
    """Transform the main procedure data"""
    logger.info("Transforming main procedure data...")
    
    available_columns = df.columns
    logger.info(f"Available columns: {available_columns}")
    
    select_columns = [
        F.col("id").alias("procedure_id"),
        F.col("resourceType").alias("resource_type"),
        F.col("status").alias("status"),
        F.when(F.col("subject").isNotNull(), 
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        F.when(F.col("code").isNotNull(),
               F.col("code").getField("text")
              ).otherwise(None).alias("code_text"),
        # Handle performedDateTime with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("performedDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("performedDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("performedDateTime"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("performedDateTime"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("performedDateTime"), "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp(F.col("performedDateTime"), "yyyy-MM-dd")
        ).alias("performed_date_time"),
        F.when(F.col("meta").isNotNull(),
               # Handle meta.lastUpdated with multiple possible formats
               F.coalesce(
                   F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
                   F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'"),
                   F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                   F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
                   F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                   F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss")
               )
              ).otherwise(None).alias("meta_last_updated"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ]
    
    main_df = df.select(*select_columns).filter(
        F.col("procedure_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return main_df

def transform_procedure_identifiers(df):
    """Transform procedure identifiers"""
    logger.info("Transforming procedure identifiers...")
    
    if "identifier" not in df.columns:
        logger.warning("identifier column not found")
        return spark.createDataFrame([], df.select(F.col("id").alias("procedure_id")).schema.add("identifier_system", StringType()).add("identifier_value", StringType()))

    identifiers_df = df.select(
        F.col("id").alias("procedure_id"),
        F.explode(F.col("identifier")).alias("identifier_item")
    ).filter(
        F.col("identifier_item").isNotNull()
    )
    
    identifiers_final = identifiers_df.select(
        F.col("procedure_id"),
        F.col("identifier_item.system").alias("identifier_system"),
        F.col("identifier_item.value").alias("identifier_value")
    ).filter(
        F.col("identifier_value").isNotNull()
    )
    
    return identifiers_final

def transform_procedure_code_codings(df):
    """Transform procedure code codings"""
    logger.info("Transforming procedure code codings...")

    if "code" not in df.columns or "coding" not in df.select("code.*").columns:
        logger.warning("code.coding column not found")
        return spark.createDataFrame([], df.select(F.col("id").alias("procedure_id")).schema.add("code_system", StringType()).add("code_code", StringType()).add("code_display", StringType()))

    codings_df = df.select(
        F.col("id").alias("procedure_id"),
        F.explode(F.col("code.coding")).alias("coding_item")
    )
    
    codings_final = codings_df.select(
        F.col("procedure_id"),
        F.col("coding_item.system").alias("code_system"),
        F.col("coding_item.code").alias("code_code"),
        F.col("coding_item.display").alias("code_display")
    ).filter(
        F.col("code_code").isNotNull()
    )
    
    return codings_final

def create_redshift_tables_sql():
    return """
    CREATE TABLE IF NOT EXISTS public.procedures (
        procedure_id VARCHAR(255) NOT NULL,
        resource_type VARCHAR(50),
        status VARCHAR(50),
        patient_id VARCHAR(255) NOT NULL,
        code_text VARCHAR(500),
        performed_date_time TIMESTAMP,
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, performed_date_time);
    """

def create_procedure_identifiers_table_sql():
    return """
    CREATE TABLE IF NOT EXISTS public.procedure_identifiers (
        procedure_id VARCHAR(255),
        identifier_system VARCHAR(255),
        identifier_value VARCHAR(255)
    ) SORTKEY (procedure_id, identifier_system);
    """

def create_procedure_code_codings_table_sql():
    return """
    CREATE TABLE IF NOT EXISTS public.procedure_code_codings (
        procedure_id VARCHAR(255),
        code_system VARCHAR(255),
        code_code VARCHAR(100),
        code_display VARCHAR(500)
    ) SORTKEY (procedure_id, code_system);
    """

def write_to_redshift_versioned(dynamic_frame, table_name, procedure_id, preactions=""):
    logger.info(f"Writing {table_name} to Redshift...")
    logger.info(f"🔧 Preactions SQL for {table_name}:\\n{preactions}")
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
    start_time = datetime.now()
    try:
        logger.info("="*80)
        logger.info("🚀 STARTING FHIR PROCEDURE ETL PROCESS")
        logger.info(f"⏰ Job started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Check table existence and log schemas
        table_names = ["procedure_code_codings", "procedure_identifiers", "procedures"]
        check_all_tables(glueContext, table_names, REDSHIFT_CONNECTION, S3_TEMP_DIR)

        
        # Use Iceberg to read data from S3
        table_name_full = f"{catalog_nm}.{DATABASE_NAME}.{TABLE_NAME}"
        logger.info(f"Reading from table: {table_name_full}")
        df_raw = spark.table(table_name_full)
        
        procedure_df = df_raw

        
        # TESTING MODE: Sample data for quick testing

        
        # Set to True to process only a sample of records

        
        USE_SAMPLE = False  # Set to True for testing with limited data

        
        SAMPLE_SIZE = 1000

        
        

        
        if USE_SAMPLE:

        
            logger.info(f"⚠️  TESTING MODE: Sampling {SAMPLE_SIZE} records for quick testing")

        
            logger.info("⚠️  Set USE_SAMPLE = False for production runs")

        
            procedure_df = procedure_df.limit(SAMPLE_SIZE)

        
        else:

        
            logger.info("✅ Processing full dataset")

        
            procedure_df = procedure_df

        total_records = procedure_df.count()
        logger.info(f"📊 Read {total_records:,} raw procedure records")

        if total_records == 0:
            logger.warning("No records found. Exiting job.")
            job.commit()
            return
        
        main_procedure_df = transform_main_procedure_data(procedure_df)
        main_count = main_procedure_df.count()
        logger.info(f"✅ Transformed {main_count:,} main procedure records")
        
        procedure_identifiers_df = transform_procedure_identifiers(procedure_df)
        identifiers_count = procedure_identifiers_df.count()
        logger.info(f"✅ Transformed {identifiers_count:,} procedure identifier records")
        
        procedure_code_codings_df = transform_procedure_code_codings(procedure_df)
        codings_count = procedure_code_codings_df.count()
        logger.info(f"✅ Transformed {codings_count:,} procedure code coding records")

        main_dynamic_frame = DynamicFrame.fromDF(main_procedure_df, glueContext, "main_procedure_dynamic_frame")
        identifiers_dynamic_frame = DynamicFrame.fromDF(procedure_identifiers_df, glueContext, "identifiers_dynamic_frame")
        codings_dynamic_frame = DynamicFrame.fromDF(procedure_code_codings_df, glueContext, "codings_dynamic_frame")
        
        main_resolved_frame = main_dynamic_frame.resolveChoice(specs=[('procedure_id', 'cast:string')])
        identifiers_resolved_frame = identifiers_dynamic_frame.resolveChoice(specs=[('procedure_id', 'cast:string')])
        codings_resolved_frame = codings_dynamic_frame.resolveChoice(specs=[('procedure_id', 'cast:string')])

        write_to_redshift_versioned(main_resolved_frame, "procedures", "procedure_id", create_redshift_tables_sql())
        write_to_redshift_versioned(identifiers_resolved_frame, "procedure_identifiers", "procedure_id", create_procedure_identifiers_table_sql())
        write_to_redshift_versioned(codings_resolved_frame, "procedure_code_codings", "procedure_id", create_procedure_code_codings_table_sql())
        
        end_time = datetime.now()
        logger.info(f"🎉 ETL PROCESS COMPLETED SUCCESSFULLY in {end_time - start_time}")
        
    except Exception as e:
        logger.error(f"❌ ETL PROCESS FAILED: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
    job.commit()
