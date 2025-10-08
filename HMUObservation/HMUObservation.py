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

def get_bookmark_from_redshift():
    """Get the maximum meta_last_updated timestamp from Redshift observations table
    
    This bookmark represents the latest data already loaded into Redshift.
    We'll only process Iceberg records newer than this timestamp.
    """
    logger.info("Fetching bookmark (max meta_last_updated) from Redshift observations table...")
    
    try:
        # Read the entire observations table (only meta_last_updated column for efficiency)
        # Then find the max in Spark instead of using a Redshift subquery
        bookmark_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "redshiftTmpDir": S3_TEMP_DIR,
                "useConnectionProperties": "true",
                "dbtable": "public.observations",
                "connectionName": REDSHIFT_CONNECTION
            },
            transformation_ctx="read_bookmark"
        )

        # Convert to DataFrame
        bookmark_df = bookmark_frame.toDF()

        if bookmark_df.count() > 0:
            # Select only meta_last_updated column and find max using Spark
            max_timestamp_row = bookmark_df.select(
                F.max(F.col("meta_last_updated")).alias("max_timestamp")
            ).collect()[0]

            max_timestamp = max_timestamp_row['max_timestamp']

            if max_timestamp:
                logger.info(f"🔖 Bookmark found: {max_timestamp}")
                logger.info(f"   Will only process Iceberg records with meta.lastUpdated > {max_timestamp}")
                return max_timestamp
            else:
                logger.info("🔖 No bookmark found (observations table is empty)")
                logger.info("   This is an initial full load - will process all Iceberg records")
                return None
        else:
            logger.info("🔖 No bookmark available - proceeding with full load")
            return None

    except Exception as e:
        logger.info(f"🔖 Could not fetch bookmark (table may not exist): {str(e)}")
        logger.info("   Proceeding with full initial load of all Iceberg records")
        return None

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

    if not existing_versions:
        # No existing data, all records are new
        total_count = df.count()
        logger.info(f"No existing versions found - treating all {total_count} records as new")
        return df, total_count, 0

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
    df_with_flag = df.withColumn(
        "needs_processing",
        needs_processing_udf(F.col(id_column), F.col("meta_last_updated"))
    )

    # Split into processing needed and skipped
    to_process_df = df_with_flag.filter(F.col("needs_processing") == True).drop("needs_processing")
    skipped_count = df_with_flag.filter(F.col("needs_processing") == False).count()

    to_process_count = to_process_df.count()
    total_count = df.count()

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

def deduplicate_observations(df):
    """Deduplicate observations by keeping only the latest occurrence of each observation ID
    
    Uses an optimized groupBy + sortWithinPartitions approach that:
    - Avoids expensive window functions
    - Uses partition-level sorting instead of global sorting
    - Minimizes shuffle operations
    - Handles large datasets efficiently
    """
    logger.info("Deduplicating observations by observation ID...")
    logger.info("Using optimized partition-based deduplication strategy")
    
    # Check if required columns exist
    if "id" not in df.columns:
        logger.warning("id column not found in data, skipping deduplication")
        return df
    
    if "meta" not in df.columns:
        logger.warning("meta column not found in data, skipping deduplication")
        return df
    
    # Get initial count
    logger.info("Counting initial records...")
    initial_count = df.count()
    logger.info(f"Initial observation count: {initial_count:,}")
    
    if initial_count == 0:
        logger.info("No observations to deduplicate")
        return df
    
    # Always perform deduplication to ensure data quality
    logger.info("Proceeding with deduplication to ensure data quality...")
    
    # Handle different possible timestamp formats in meta.lastUpdated
    logger.info("Extracting timestamps for deduplication...")
    timestamp_expr = F.coalesce(
        F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
        F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss"),
        F.lit("1970-01-01 00:00:00").cast(TimestampType())  # Default for NULL timestamps
    )
    
    # Add timestamp column
    df_with_ts = df.withColumn("_dedup_ts", timestamp_expr)
    
    # OPTIMIZED APPROACH: Use SQL-based aggregation to find max timestamp per ID
    # Then use broadcast join to filter
    logger.info("Finding maximum timestamp for each observation ID...")
    
    # Create a temporary view for SQL operations
    df_with_ts.createOrReplaceTempView("observations_with_ts")
    
    # Use SQL to find the max timestamp for each ID and join back
    # This is more efficient than window functions for large datasets
    dedup_sql = """
        SELECT obs.*
        FROM observations_with_ts obs
        INNER JOIN (
            SELECT id, MAX(_dedup_ts) as max_ts
            FROM observations_with_ts
            GROUP BY id
        ) max_records
        ON obs.id = max_records.id AND obs._dedup_ts = max_records.max_ts
    """
    
    logger.info("Executing deduplication query (keeping latest record per ID)...")
    deduplicated_with_ts = spark.sql(dedup_sql)
    
    # In case multiple records have the same max timestamp, use dropDuplicates as final safety
    logger.info("Applying final duplicate removal (for identical timestamps)...")
    deduplicated_df = deduplicated_with_ts.dropDuplicates(["id"]).drop("_dedup_ts")
    
    # Get final count
    logger.info("Counting deduplicated records...")
    final_count = deduplicated_df.count()
    removed_count = initial_count - final_count
    
    logger.info(f"✅ Deduplication completed:")
    logger.info(f"  📊 Initial records: {initial_count:,}")
    logger.info(f"  📊 Final records: {final_count:,}")
    logger.info(f"  🗑️  Removed duplicates: {removed_count:,}")
    logger.info(f"  📈 Deduplication ratio: {(removed_count/initial_count)*100:.1f}%")
    
    return deduplicated_df

def filter_by_bookmark(df, bookmark_timestamp):
    """Filter Iceberg DataFrame to only include records newer than the bookmark
    
    Args:
        df: Source DataFrame from Iceberg
        bookmark_timestamp: Maximum meta_last_updated from Redshift (or None for full load)
    
    Returns:
        Filtered DataFrame with only new/updated records
    """
    if bookmark_timestamp is None:
        logger.info("No bookmark - processing all Iceberg records (full load)")
        return df
    
    logger.info(f"Applying bookmark filter to Iceberg data...")
    logger.info(f"Bookmark threshold: {bookmark_timestamp}")
    
    # Parse meta.lastUpdated timestamp from Iceberg data
    timestamp_expr = F.coalesce(
        F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
        F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss")
    )
    
    # Add timestamp column temporarily for filtering
    df_with_ts = df.withColumn("_filter_ts", timestamp_expr)
    
    # Filter for records newer than bookmark
    filtered_df = df_with_ts.filter(F.col("_filter_ts") > F.lit(bookmark_timestamp)).drop("_filter_ts")
    
    # Count results
    initial_count = df.count()
    filtered_count = filtered_df.count()
    skipped_count = initial_count - filtered_count
    
    logger.info(f"✅ Bookmark filter applied:")
    logger.info(f"  📊 Total records in Iceberg: {initial_count:,}")
    logger.info(f"  📊 New/updated records (after bookmark): {filtered_count:,}")
    logger.info(f"  ⏭️  Records skipped (already in Redshift): {skipped_count:,}")
    logger.info(f"  📈 Filter efficiency: {(skipped_count/initial_count)*100:.1f}% skipped")
    
    return filtered_df

def write_to_redshift_simple(dynamic_frame, table_name, preactions=""):
    """Write DynamicFrame to Redshift without version checking
    
    Used with bookmark pattern - since we filter at source, we can simply append all records.
    For initial loads, uses TRUNCATE to clear existing data.
    """
    logger.info(f"Writing {table_name} to Redshift...")
    
    try:
        logger.info(f"Executing preactions for {table_name}: {preactions[:100] if preactions else 'None'}")
        logger.info(f"Writing to table: public.{table_name}")
        
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="redshift",
            connection_options={
                "redshiftTmpDir": S3_TEMP_DIR,
                "useConnectionProperties": "true",
                "dbtable": f"public.{table_name}",
                "connectionName": REDSHIFT_CONNECTION,
                "preactions": preactions or ""
            },
            transformation_ctx=f"write_{table_name}_to_redshift"
        )
        
        logger.info(f"✅ Successfully wrote {table_name} to Redshift")
        
    except Exception as e:
        logger.error(f"❌ Failed to write {table_name} to Redshift: {str(e)}")
        raise e

def write_to_redshift_versioned(dynamic_frame, table_name, id_column, preactions=""):
    """Version-aware write to Redshift - only processes new/updated entities"""
    logger.info(f"Writing {table_name} to Redshift with version checking...")

    try:
        # Convert dynamic frame to DataFrame for processing
        df = dynamic_frame.toDF()
        total_records = df.count()

        if total_records == 0:
            logger.info(f"No records to process for {table_name}")
            return

        # Step 1: Get existing versions from Redshift
        existing_versions = get_existing_versions_from_redshift(table_name, id_column)

        # Step 2: Filter incoming data based on version comparison
        filtered_df, to_process_count, skipped_count = filter_dataframe_by_version(
            df, existing_versions, id_column
        )

        if to_process_count == 0:
            logger.info(f"✅ All {total_records} records in {table_name} are up to date - no changes needed")
            return

        # Step 3: Get entities that need old version cleanup
        entities_to_delete = get_entities_to_delete(filtered_df, existing_versions, id_column)

        # Step 4: Build preactions for selective deletion
        selective_preactions = preactions
        if entities_to_delete:
            # Create DELETE statements for specific entity IDs
            entity_ids_str = "', '".join(entities_to_delete)
            delete_clause = f"DELETE FROM public.{table_name} WHERE {id_column} IN ('{entity_ids_str}');"

            if selective_preactions:
                selective_preactions = delete_clause + " " + selective_preactions
            else:
                selective_preactions = delete_clause

            logger.info(f"Will delete {len(entities_to_delete)} existing entities before inserting updated versions")

        # Step 5: Convert filtered DataFrame back to DynamicFrame
        filtered_dynamic_frame = DynamicFrame.fromDF(filtered_df, glueContext, f"filtered_{table_name}")

        # Step 6: Write only the new/updated records
        logger.info(f"Writing {to_process_count} new/updated records to {table_name}")

        glueContext.write_dynamic_frame.from_options(
            frame=filtered_dynamic_frame,
            connection_type="redshift",
            connection_options={
                "redshiftTmpDir": S3_TEMP_DIR,
                "useConnectionProperties": "true",
                "dbtable": f"public.{table_name}",
                "connectionName": REDSHIFT_CONNECTION,
                "preactions": selective_preactions or ""
            },
            transformation_ctx=f"write_{table_name}_versioned_to_redshift"
        )

        logger.info(f"✅ Successfully wrote {to_process_count} records to {table_name} in Redshift")
        logger.info(f"📊 Version summary: {to_process_count} processed, {skipped_count} skipped (same version)")

    except Exception as e:
        logger.error(f"❌ Failed to write {table_name} to Redshift with versioning: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        raise

# Set up logging to write to stdout (CloudWatch)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

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
    # Performance optimizations for large dataset processing
    .config("spark.sql.shuffle.partitions", "400")  # Increase from default 200/232
    .config("spark.sql.adaptive.enabled", "true")  # Enable adaptive query execution
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")  # Combine small partitions
    .config("spark.sql.adaptive.skewJoin.enabled", "true")  # Handle data skew
    .config("spark.sql.adaptive.localShuffleReader.enabled", "true")  # Optimize shuffle reads
    # Conservative broadcast settings to prevent hanging
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable auto broadcast to prevent hanging
    .config("spark.sql.broadcastTimeout", "300")  # 5 minutes max for broadcasts
    # Timeout and heartbeat settings
    .config("spark.network.timeout", "600s")  # 10 minutes for network operations
    .config("spark.executor.heartbeatInterval", "30s")  # Heartbeat every 30 seconds
    .config("spark.sql.broadcastExchangeMaxThreadThreshold", "8")  # Limit broadcast threads
    # Memory and execution settings
    .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB max partition size
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")  # Target 128MB partitions
    .getOrCreate())
sc = spark.sparkContext
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Configuration - Updated to use S3/Iceberg instead of Glue Catalog
DATABASE_NAME = ahl_database  # Using AHL Iceberg database
TABLE_NAME = "observation"
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

def extract_id_from_reference(reference_field):
    """Extract ID from FHIR reference format"""
    if reference_field:
        # Handle Row/struct format: Row(reference="Patient/123", display="Name")
        if hasattr(reference_field, 'reference'):
            reference = reference_field.reference
            if reference and "/" in reference:
                return reference.split("/")[-1]
        # Handle dict format: {"reference": "Patient/123", "display": "Name"}
        elif isinstance(reference_field, dict):
            reference = reference_field.get('reference')
            if reference and "/" in reference:
                return reference.split("/")[-1]
        # Handle string format: "Patient/123"
        elif isinstance(reference_field, str):
            if "/" in reference_field:
                return reference_field.split("/")[-1]
    return None

def safe_get_field(df, column_name, field_name=None):
    """Safely get a field from a column, handling cases where column might not exist"""
    try:
        if field_name:
            return F.col(column_name).getField(field_name)
        else:
            return F.col(column_name)
    except:
        return F.lit(None)

def transform_main_observation_data(df):
    """Transform the main observation data"""
    logger.info("Transforming main observation data...")
    
    # Log available columns for debugging
    available_columns = df.columns
    logger.info(f"Available columns: {available_columns}")
    
    # Convert complex data to JSON strings to avoid nested structures
    def convert_to_json_string(field):
        if field is None:
            return None
        try:
            if isinstance(field, str):
                return field
            else:
                return json.dumps(field)
        except:
            return str(field)
    
    convert_to_json_udf = F.udf(convert_to_json_string, StringType())
    
    # Build the select statement dynamically based on available columns
    select_columns = [
        F.col("id").alias("observation_id"),
        F.when(F.col("subject").isNotNull(), 
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        F.when(F.col("encounter").isNotNull(),
               F.regexp_extract(F.col("encounter").getField("reference"), r"Encounter/(.+)", 1)
              ).otherwise(None).alias("encounter_id"),
        F.when(F.col("specimen").isNotNull(),
               F.regexp_extract(F.col("specimen").getField("reference"), r"Specimen/(.+)", 1)
              ).otherwise(None).alias("specimen_id"),
        F.col("status"),
        F.col("code").getField("text").alias("observation_text"),
        F.when(F.col("code").getField("coding").isNotNull() &
               (F.size(F.col("code").getField("coding")) > 0),
               F.col("code").getField("coding")[0].getField("code")
              ).otherwise(None).alias("primary_code"),
        F.when(F.col("code").getField("coding").isNotNull() &
               (F.size(F.col("code").getField("coding")) > 0),
               F.col("code").getField("coding")[0].getField("system")
              ).otherwise(None).alias("primary_system"),
        F.when(F.col("code").getField("coding").isNotNull() &
               (F.size(F.col("code").getField("coding")) > 0),
               F.col("code").getField("coding")[0].getField("display")
              ).otherwise(None).alias("primary_display")
    ]
    
    # Add value fields - handle different value types (using actual schema field names)
    select_columns.extend([
        F.col("valueString").alias("value_string"),
        F.col("valueQuantity").getField("value").alias("value_quantity_value"),
        F.col("valueQuantity").getField("unit").alias("value_quantity_unit"),
        F.col("valueQuantity").getField("system").alias("value_quantity_system"),
        F.col("valueCodeableConcept").getField("text").alias("value_codeable_concept_text"),
        F.col("valueCodeableConcept").getField("coding")[0].getField("code").alias("value_codeable_concept_code"),
        F.col("valueCodeableConcept").getField("coding")[0].getField("system").alias("value_codeable_concept_system"),
        F.col("valueCodeableConcept").getField("coding")[0].getField("display").alias("value_codeable_concept_display"),
        # Handle valueDateTime with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("valueDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("valueDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("valueDateTime"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("valueDateTime"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("valueDateTime"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("value_datetime"),
        F.lit(None).alias("value_boolean"),  # valueboolean field not in schema
    ])
    
    # Add data absent reason (using actual schema field names)
    select_columns.extend([
        F.col("dataAbsentReason").getField("coding")[0].getField("code").alias("data_absent_reason_code"),
        F.col("dataAbsentReason").getField("coding")[0].getField("display").alias("data_absent_reason_display"),
        F.col("dataAbsentReason").getField("coding")[0].getField("system").alias("data_absent_reason_system"),
    ])
    
    # Add temporal information - handle multiple datetime formats
    select_columns.extend([
        # Handle effectiveDateTime with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("effectiveDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),  # With nanoseconds
            F.to_timestamp(F.col("effectiveDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),        # With milliseconds
            F.to_timestamp(F.col("effectiveDateTime"), "yyyy-MM-dd'T'HH:mm:ssXXX"),             # No milliseconds (your format)
            F.to_timestamp(F.col("effectiveDateTime"), "yyyy-MM-dd'T'HH:mm:ss")                 # No timezone
        ).alias("effective_datetime"),
        # Handle effectivePeriod.start with multiple formats
        F.coalesce(
            F.to_timestamp(F.col("effectivePeriod").getField("start"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("effectivePeriod").getField("start"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("effectivePeriod").getField("start"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("effectivePeriod").getField("start"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("effective_period_start"),
        F.lit(None).alias("effective_period_end"),  # end field not in schema
        # Handle issued with multiple formats
        F.coalesce(
            F.to_timestamp(F.col("issued"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("issued"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("issued"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("issued"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("issued"),
    ])
    
    # Add body site and method (fields not in schema)
    select_columns.extend([
        F.lit(None).alias("body_site_code"),    # bodySite field not in schema
        F.lit(None).alias("body_site_system"),  # bodySite field not in schema
        F.lit(None).alias("body_site_display"), # bodySite field not in schema
        F.lit(None).alias("body_site_text"),    # bodySite field not in schema
        F.lit(None).alias("method_code"),       # method field not in schema
        F.lit(None).alias("method_system"),     # method field not in schema
        F.lit(None).alias("method_display"),    # method field not in schema
        F.lit(None).alias("method_text"),       # method field not in schema
    ])
    
    # Add metadata (using actual schema field names)
    select_columns.extend([
        # Handle meta.lastUpdated with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("meta_last_updated"),
        F.lit(None).alias("meta_source"),  # source field not in schema
        F.lit(None).alias("meta_profile"), # profile field not in schema
        convert_to_json_udf(F.col("meta").getField("security")).alias("meta_security"),
        F.lit(None).alias("meta_tag"),     # tag field not in schema
        convert_to_json_udf(F.col("extension")).alias("extensions"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ])
    
    # Transform main observation data using only available columns and flatten complex structures
    main_df = df.select(*select_columns).filter(
        F.col("observation_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return main_df

def transform_observation_categories(df):
    """Transform observation categories (multiple categories per observation)"""
    logger.info("Transforming observation categories...")
    
    # Check if category column exists
    if "category" not in df.columns:
        logger.warning("category column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("category_code"),
            F.lit("").alias("category_system"),
            F.lit("").alias("category_display"),
            F.lit("").alias("category_text")
        ).filter(F.lit(False))
    
    # Explode the category array
    categories_df = df.select(
        F.col("id").alias("observation_id"),
        F.explode(F.col("category")).alias("category_item")
    ).filter(
        F.col("category_item").isNotNull()
    )
    
    # Extract category details and explode the coding array
    # First get the text from category level, then explode coding
    categories_with_text = categories_df.select(
        F.col("observation_id"),
        F.col("category_item.text").alias("category_text"),
        F.col("category_item.coding").alias("coding_array")
    )
    
    # Now explode the coding array
    categories_final = categories_with_text.select(
        F.col("observation_id"),
        F.explode(F.col("coding_array")).alias("coding_item"),
        F.col("category_text")
    ).select(
        F.col("observation_id"),
        F.col("coding_item.code").alias("category_code"),
        F.col("coding_item.system").alias("category_system"),
        F.col("coding_item.display").alias("category_display"),
        F.col("category_text")
    ).filter(
        F.col("category_code").isNotNull()
    )
    
    return categories_final

def transform_observation_interpretations(df):
    """Transform observation interpretations"""
    logger.info("Transforming observation interpretations...")
    
    # Check if interpretation column exists
    if "interpretation" not in df.columns:
        logger.warning("interpretation column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("interpretation_code"),
            F.lit("").alias("interpretation_system"),
            F.lit("").alias("interpretation_display")
        ).filter(F.lit(False))
    
    # Explode the interpretation array
    interpretations_df = df.select(
        F.col("id").alias("observation_id"),
        F.explode(F.col("interpretation")).alias("interpretation_item")
    ).filter(
        F.col("interpretation_item").isNotNull()
    )
    
    # Extract interpretation details and explode the coding array
    # First get the text from interpretation level, then explode coding
    interpretations_with_text = interpretations_df.select(
        F.col("observation_id"),
        F.col("interpretation_item.text").alias("interpretation_text"),
        F.col("interpretation_item.coding").alias("coding_array")
    )
    
    # Now explode the coding array
    interpretations_final = interpretations_with_text.select(
        F.col("observation_id"),
        F.explode(F.col("coding_array")).alias("coding_item"),
        F.col("interpretation_text")
    ).select(
        F.col("observation_id"),
        F.col("coding_item.code").alias("interpretation_code"),
        F.col("coding_item.system").alias("interpretation_system"),
        F.col("coding_item.display").alias("interpretation_display"),
        F.col("interpretation_text")
    ).filter(
        F.col("interpretation_code").isNotNull()
    )
    
    return interpretations_final

def transform_observation_reference_ranges(df):
    """Transform observation reference ranges"""
    logger.info("Transforming observation reference ranges...")
    
    # Check if referenceRange column exists (using actual field name with camelCase)
    if "referenceRange" not in df.columns:
        logger.warning("referenceRange column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit(0.0).alias("range_low_value"),
            F.lit("").alias("range_low_unit"),
            F.lit(0.0).alias("range_high_value"),
            F.lit("").alias("range_high_unit"),
            F.lit("").alias("range_type_code"),
            F.lit("").alias("range_type_system"),
            F.lit("").alias("range_type_display"),
            F.lit("").alias("range_text")
        ).filter(F.lit(False))
    
    # Explode the referenceRange array
    ranges_df = df.select(
        F.col("id").alias("observation_id"),
        F.explode(F.col("referenceRange")).alias("range_item")
    ).filter(
        F.col("range_item").isNotNull()
    )
    
    # Debug: Check the schema of range_item to understand its structure
    if ranges_df.count() > 0:
        logger.info("Reference range item schema:")
        ranges_df.select("range_item").printSchema()
        logger.info("Sample reference range data:")
        ranges_df.select("range_item").show(1, truncate=False)
    
    # Try multiple approaches to extract data based on possible structures
    try:
        # Approach 1: Try the nested structure with low/high as complex types
        ranges_final = ranges_df.select(
            F.col("observation_id"),
            # Try different paths for low value
            F.coalesce(
                F.col("range_item.low.value.double"),
                F.col("range_item.low.value.int"),
                F.col("range_item.low.value"),
                F.col("range_item.low")
            ).cast(DecimalType(15,4)).alias("range_low_value"),
            F.coalesce(
                F.col("range_item.low.unit"),
                F.lit(None)
            ).alias("range_low_unit"),
            # Try different paths for high value
            F.coalesce(
                F.col("range_item.high.value.double"),
                F.col("range_item.high.value.int"),
                F.col("range_item.high.value"),
                F.col("range_item.high")
            ).cast(DecimalType(15,4)).alias("range_high_value"),
            F.coalesce(
                F.col("range_item.high.unit"),
                F.lit(None)
            ).alias("range_high_unit"),
            # Extract type if it exists
            F.coalesce(
                F.when(F.col("range_item.type.coding").isNotNull() & 
                       (F.size(F.col("range_item.type.coding")) > 0),
                       F.col("range_item.type.coding")[0].getField("code")),
                F.col("range_item.type.code"),
                F.col("range_item.type")
            ).alias("range_type_code"),
            F.coalesce(
                F.when(F.col("range_item.type.coding").isNotNull() & 
                       (F.size(F.col("range_item.type.coding")) > 0),
                       F.col("range_item.type.coding")[0].getField("system")),
                F.col("range_item.type.system")
            ).alias("range_type_system"),
            F.coalesce(
                F.when(F.col("range_item.type.coding").isNotNull() & 
                       (F.size(F.col("range_item.type.coding")) > 0),
                       F.col("range_item.type.coding")[0].getField("display")),
                F.col("range_item.type.display"),
                F.col("range_item.type.text")
            ).alias("range_type_display"),
            F.col("range_item.text").alias("range_text")
        )
    except Exception as e:
        logger.warning(f"Could not extract reference ranges using nested structure: {str(e)}")
        # Fallback: Just extract text field if available
        ranges_final = ranges_df.select(
            F.col("observation_id"),
            F.lit(None).cast(DecimalType(15,4)).alias("range_low_value"),
            F.lit(None).alias("range_low_unit"),
            F.lit(None).cast(DecimalType(15,4)).alias("range_high_value"),
            F.lit(None).alias("range_high_unit"),
            F.lit(None).alias("range_type_code"),
            F.lit(None).alias("range_type_system"),
            F.lit(None).alias("range_type_display"),
            F.col("range_item.text").alias("range_text")
        )
    
    # Filter to keep only records with some data
    ranges_final = ranges_final.filter(
        F.col("range_text").isNotNull() | 
        F.col("range_low_value").isNotNull() | 
        F.col("range_high_value").isNotNull()
    )
    
    return ranges_final

def transform_observation_components(df):
    """Transform observation components"""
    logger.info("Transforming observation components...")
    
    # Check if component column exists
    if "component" not in df.columns:
        logger.warning("component column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("component_code"),
            F.lit("").alias("component_system"),
            F.lit("").alias("component_display"),
            F.lit("").alias("component_text"),
            F.lit("").alias("component_value_string"),
            F.lit(0.0).alias("component_value_quantity_value"),
            F.lit("").alias("component_value_quantity_unit"),
            F.lit("").alias("component_value_codeable_concept_code"),
            F.lit("").alias("component_value_codeable_concept_system"),
            F.lit("").alias("component_value_codeable_concept_display"),
            F.lit("").alias("component_data_absent_reason_code"),
            F.lit("").alias("component_data_absent_reason_display")
        ).filter(F.lit(False))
    
    # Explode the component array
    components_df = df.select(
        F.col("id").alias("observation_id"),
        F.explode(F.col("component")).alias("component_item")
    ).filter(
        F.col("component_item").isNotNull()
    )
    
    # Extract component details - handle nested structures properly
    # First explode the code.coding array
    components_with_code = components_df.select(
        F.col("observation_id"),
        F.explode(F.col("component_item.code").getField("coding")).alias("code_coding_item"),
        F.lit(None).alias("component_text"),  # text field not available in component.code structure
        F.lit(None).alias("component_value_string"),  # valueString not available in component structure
        # Handle valueQuantity.value safely - extract decimal value directly
        F.when(F.col("component_item.valueQuantity").isNotNull(),
               F.col("component_item.valueQuantity").getField("value")
              ).otherwise(None).alias("component_value_quantity_value"),
        F.when(F.col("component_item.valueQuantity").isNotNull(),
               F.col("component_item.valueQuantity").getField("unit")
              ).otherwise(None).alias("component_value_quantity_unit"),
        F.col("component_item.valueCodeableConcept").alias("value_codeable_concept"),
        F.col("component_item.dataAbsentReason").alias("data_absent_reason")
    )
    
    # Now extract the code details
    components_final = components_with_code.select(
        F.col("observation_id"),
        F.col("code_coding_item.code").alias("component_code"),
        F.col("code_coding_item.system").alias("component_system"),
        F.col("code_coding_item.display").alias("component_display"),
        F.col("component_text"),
        F.col("component_value_string"),
        F.col("component_value_quantity_value"),
        F.col("component_value_quantity_unit"),
        # Handle valueCodeableConcept - safely get first coding element
        F.when((F.col("value_codeable_concept").getField("coding").isNotNull()) & 
               (F.size(F.col("value_codeable_concept").getField("coding")) > 0),
               F.col("value_codeable_concept").getField("coding")[0].getField("code")
              ).otherwise(None).alias("component_value_codeable_concept_code"),
        F.when((F.col("value_codeable_concept").getField("coding").isNotNull()) & 
               (F.size(F.col("value_codeable_concept").getField("coding")) > 0),
               F.col("value_codeable_concept").getField("coding")[0].getField("system")
              ).otherwise(None).alias("component_value_codeable_concept_system"),
        F.when((F.col("value_codeable_concept").getField("coding").isNotNull()) & 
               (F.size(F.col("value_codeable_concept").getField("coding")) > 0),
               F.col("value_codeable_concept").getField("coding")[0].getField("display")
              ).otherwise(None).alias("component_value_codeable_concept_display"),
        # Handle dataAbsentReason - safely get first coding element
        F.when((F.col("data_absent_reason").getField("coding").isNotNull()) & 
               (F.size(F.col("data_absent_reason").getField("coding")) > 0),
               F.col("data_absent_reason").getField("coding")[0].getField("code")
              ).otherwise(None).alias("component_data_absent_reason_code"),
        F.when((F.col("data_absent_reason").getField("coding").isNotNull()) & 
               (F.size(F.col("data_absent_reason").getField("coding")) > 0),
               F.col("data_absent_reason").getField("coding")[0].getField("display")
              ).otherwise(None).alias("component_data_absent_reason_display")
    ).filter(
        F.col("component_code").isNotNull()
    )
    
    return components_final

def transform_observation_notes(df):
    """Transform observation notes"""
    logger.info("Transforming observation notes...")
    
    # Check if note column exists
    if "note" not in df.columns:
        logger.warning("note column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("note_text"),
            F.lit("").alias("note_author_reference"),
            F.current_timestamp().alias("note_time")
        ).filter(F.lit(False))
    
    # Explode the note array
    notes_df = df.select(
        F.col("id").alias("observation_id"),
        F.explode(F.col("note")).alias("note_item")
    ).filter(
        F.col("note_item").isNotNull()
    )
    
    # Extract note details
    notes_final = notes_df.select(
        F.col("observation_id"),
        F.col("note_item.text").alias("note_text"),
        F.lit(None).alias("note_author_reference"),  # authorReference not available in note structure
        F.lit(None).alias("note_time")  # time not available in note structure
    ).filter(
        F.col("note_text").isNotNull()
    )
    
    return notes_final

def transform_observation_performers(df):
    """Transform observation performers"""
    logger.info("Transforming observation performers...")
    
    # Check if performer column exists
    if "performer" not in df.columns:
        logger.warning("performer column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("performer_type"),
            F.lit("").alias("performer_id")
        ).filter(F.lit(False))
    
    # Explode the performer array
    performers_df = df.select(
        F.col("id").alias("observation_id"),
        F.explode(F.col("performer")).alias("performer_item")
    ).filter(
        F.col("performer_item").isNotNull()
    )
    
    # Extract performer details
    performers_final = performers_df.select(
        F.col("observation_id"),
        F.regexp_extract(F.col("performer_item").getField("reference"), r"([^/]+)/", 1).alias("performer_type"),
        F.regexp_extract(F.col("performer_item").getField("reference"), r"/(.+)", 1).alias("performer_id")
    ).filter(
        F.col("performer_id").isNotNull()
    )
    
    return performers_final

def transform_observation_codes(df):
    """Transform observation codes (multiple codes per observation from code.coding array)"""
    logger.info("Transforming observation codes...")
    
    # Check if code column exists
    if "code" not in df.columns:
        logger.warning("code column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("code_code"),
            F.lit("").alias("code_system"),
            F.lit("").alias("code_display"),
            F.lit("").alias("code_text")
        ).filter(F.lit(False))
    
    # First extract the text from code level
    codes_with_text = df.select(
        F.col("id").alias("observation_id"),
        F.col("code.text").alias("code_text"),
        F.col("code.coding").alias("coding_array")
    ).filter(
        F.col("code").isNotNull()
    )
    
    # Explode the coding array
    codes_df = codes_with_text.select(
        F.col("observation_id"),
        F.col("code_text"),
        F.explode(F.col("coding_array")).alias("coding_item")
    ).filter(
        F.col("coding_item").isNotNull()
    )
    
    # Extract code details
    codes_final = codes_df.select(
        F.col("observation_id"),
        F.col("coding_item.code").alias("code_code"),
        F.col("coding_item.system").alias("code_system"),
        F.col("coding_item.display").alias("code_display"),
        F.col("code_text")
    ).filter(
        F.col("code_code").isNotNull()
    )
    
    return codes_final

def transform_observation_members(df):
    """Transform observation members"""
    logger.info("Transforming observation members...")
    
    # Check if hasmember column exists (using actual CSV field name)
    if "hasmember" not in df.columns:
        logger.warning("hasmember column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("member_observation_id")
        ).filter(F.lit(False))
    
    # Explode the hasmember array
    members_df = df.select(
        F.col("id").alias("observation_id"),
        F.explode(F.col("hasmember")).alias("member_item")
    ).filter(
        F.col("member_item").isNotNull()
    )
    
    # Extract member details
    members_final = members_df.select(
        F.col("observation_id"),
        F.regexp_extract(F.col("member_item").getField("reference"), r"Observation/(.+)", 1).alias("member_observation_id")
    ).filter(
        F.col("member_observation_id").isNotNull()
    )
    
    return members_final

def transform_observation_derived_from(df):
    """Transform observation derived from references"""
    logger.info("Transforming observation derived from...")
    
    # Check if derivedfrom column exists (using actual CSV field name)
    if "derivedfrom" not in df.columns:
        logger.warning("derivedfrom column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("derived_from_reference")
        ).filter(F.lit(False))
    
    # Explode the derivedfrom array
    derived_df = df.select(
        F.col("id").alias("observation_id"),
        F.explode(F.col("derivedfrom")).alias("derived_item")
    ).filter(
        F.col("derived_item").isNotNull()
    )
    
    # Extract derived from details
    derived_final = derived_df.select(
        F.col("observation_id"),
        F.col("derived_item").getField("reference").alias("derived_from_reference")
    ).filter(
        F.col("derived_from_reference").isNotNull()
    )
    
    return derived_final

def create_redshift_tables_sql():
    """Generate SQL for creating main observations table in Redshift with proper syntax"""
    return """
    -- Main observations table
    CREATE TABLE IF NOT EXISTS public.observations (
        observation_id VARCHAR(255) NOT NULL,
        patient_id VARCHAR(255) NOT NULL,
        encounter_id VARCHAR(255),
        specimen_id VARCHAR(255),
        status VARCHAR(50),
        observation_text VARCHAR(500),
        primary_code VARCHAR(50),
        primary_system VARCHAR(255),
        primary_display VARCHAR(255),
        value_string VARCHAR(500),
        value_quantity_value DECIMAL(15,4),
        value_quantity_unit VARCHAR(50),
        value_quantity_system VARCHAR(255),
        value_codeable_concept_code VARCHAR(50),
        value_codeable_concept_system VARCHAR(255),
        value_codeable_concept_display VARCHAR(255),
        value_codeable_concept_text VARCHAR(500),
        value_datetime TIMESTAMP,
        value_boolean BOOLEAN,
        data_absent_reason_code VARCHAR(50),
        data_absent_reason_display VARCHAR(255),
        data_absent_reason_system VARCHAR(255),
        effective_datetime TIMESTAMP,
        effective_period_start TIMESTAMP,
        effective_period_end TIMESTAMP,
        issued TIMESTAMP,
        body_site_code VARCHAR(50),
        body_site_system VARCHAR(255),
        body_site_display VARCHAR(255),
        body_site_text VARCHAR(500),
        method_code VARCHAR(50),
        method_system VARCHAR(255),
        method_display VARCHAR(255),
        method_text VARCHAR(500),
        meta_last_updated TIMESTAMP,
        meta_source VARCHAR(255),
        meta_profile TEXT,
        meta_security TEXT,
        meta_tag TEXT,
        extensions TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, effective_datetime)
    """

def create_observation_categories_table_sql():
    """Generate SQL for creating observation_categories table"""
    return """
    CREATE TABLE IF NOT EXISTS public.observation_categories (
        observation_id VARCHAR(255),
        category_code VARCHAR(50),
        category_system VARCHAR(255),
        category_display VARCHAR(255),
        category_text VARCHAR(500)
    ) SORTKEY (observation_id, category_code)
    """

def create_observation_interpretations_table_sql():
    """Generate SQL for creating observation_interpretations table"""
    return """
    CREATE TABLE IF NOT EXISTS public.observation_interpretations (
        observation_id VARCHAR(255),
        interpretation_code VARCHAR(50),
        interpretation_system VARCHAR(255),
        interpretation_display VARCHAR(255)
    ) SORTKEY (observation_id, interpretation_code)
    """

def create_observation_reference_ranges_table_sql():
    """Generate SQL for creating observation_reference_ranges table"""
    return """
    CREATE TABLE IF NOT EXISTS public.observation_reference_ranges (
        observation_id VARCHAR(255),
        range_low_value DECIMAL(15,4),
        range_low_unit VARCHAR(50),
        range_high_value DECIMAL(15,4),
        range_high_unit VARCHAR(50),
        range_type_code VARCHAR(50),
        range_type_system VARCHAR(255),
        range_type_display VARCHAR(255),
        range_text VARCHAR(500)
    ) SORTKEY (observation_id, range_type_code)
    """

def create_observation_components_table_sql():
    """Generate SQL for creating observation_components table"""
    return """
    CREATE TABLE IF NOT EXISTS public.observation_components (
        observation_id VARCHAR(255),
        component_code VARCHAR(50),
        component_system VARCHAR(255),
        component_display VARCHAR(255),
        component_text VARCHAR(500),
        component_value_string VARCHAR(500),
        component_value_quantity_value DECIMAL(15,4),
        component_value_quantity_unit VARCHAR(50),
        component_value_codeable_concept_code VARCHAR(50),
        component_value_codeable_concept_system VARCHAR(255),
        component_value_codeable_concept_display VARCHAR(255),
        component_data_absent_reason_code VARCHAR(50),
        component_data_absent_reason_display VARCHAR(255)
    ) SORTKEY (observation_id, component_code)
    """

def create_observation_notes_table_sql():
    """Generate SQL for creating observation_notes table"""
    return """
    CREATE TABLE IF NOT EXISTS public.observation_notes (
        observation_id VARCHAR(255),
        note_text TEXT,
        note_author_reference VARCHAR(255),
        note_time TIMESTAMP
    ) SORTKEY (observation_id, note_time)
    """

def create_observation_performers_table_sql():
    """Generate SQL for creating observation_performers table"""
    return """
    CREATE TABLE IF NOT EXISTS public.observation_performers (
        observation_id VARCHAR(255),
        performer_type VARCHAR(50),
        performer_id VARCHAR(255)
    ) SORTKEY (observation_id, performer_type)
    """

def create_observation_members_table_sql():
    """Generate SQL for creating observation_members table"""
    return """
    CREATE TABLE IF NOT EXISTS public.observation_members (
        observation_id VARCHAR(255),
        member_observation_id VARCHAR(255)
    ) SORTKEY (observation_id, member_observation_id)
    """

def create_observation_codes_table_sql():
    """Generate SQL for creating observation_codes table"""
    return """
    CREATE TABLE IF NOT EXISTS public.observation_codes (
        observation_id VARCHAR(255),
        code_code VARCHAR(50),
        code_system VARCHAR(255),
        code_display VARCHAR(255),
        code_text VARCHAR(500)
    ) SORTKEY (observation_id, code_code)
    """

def create_observation_derived_from_table_sql():
    """Generate SQL for creating observation_derived_from table"""
    return """
    CREATE TABLE IF NOT EXISTS public.observation_derived_from (
        observation_id VARCHAR(255),
        derived_from_reference VARCHAR(255)
    ) SORTKEY (observation_id, derived_from_reference)
    """


def main():
    """Main ETL process"""
    start_time = datetime.now()
    try:
        logger.info("=" * 80)
        logger.info("🚀 STARTING ENHANCED FHIR OBSERVATION ETL PROCESS")
        logger.info("=" * 80)
        logger.info(f"⏰ Job started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📊 Source: {DATABASE_NAME}.{TABLE_NAME}")
        logger.info(f"🎯 Target: Redshift (10 tables)")
        logger.info("📋 Reading all available columns from Glue Catalog")
        logger.info("🔄 Process: 9 steps (Read → Bookmark Filter → Deduplicate → Transform → Convert → Resolve → Validate → Write)")
        
        # Step 1: Read data from S3 using Iceberg catalog
        logger.info("\n" + "=" * 50)
        logger.info("📥 STEP 1: READING DATA FROM GLUE CATALOG")
        logger.info("=" * 50)
        logger.info(f"Database: {DATABASE_NAME}")
        logger.info(f"Table: {TABLE_NAME}")
        logger.info("Reading all available columns from Glue Catalog")
        
        # Use the AWS Glue Data Catalog to read observation data (all columns)
        # Use Iceberg to read data from S3
        table_name_full = f"{catalog_nm}.{DATABASE_NAME}.{TABLE_NAME}"
        logger.info(f"Reading from table: {table_name_full}")
        df_raw = spark.table(table_name_full)
        
        # Convert to DataFrame first to check available columns
        observation_df_raw = df_raw
        available_columns = observation_df_raw.columns
        logger.info(f"📋 Available columns in source: {available_columns}")
        
        # TESTING MODE: Sample data for quick testing
        # Comment out or set to False for production runs
        USE_SAMPLE = False  # Set to False for full data processing
        SAMPLE_SIZE = 100000  # 100k records for testing
        
        if USE_SAMPLE:
            logger.info(f"⚠️  TESTING MODE: Sampling {SAMPLE_SIZE} records for quick testing")
            logger.info("⚠️  Set USE_SAMPLE = False for production runs")
            observation_df = observation_df_raw.limit(SAMPLE_SIZE)
        else:
            logger.info(f"✅ Using all {len(available_columns)} available columns")
            observation_df = observation_df_raw
        
        logger.info("✅ Successfully read data using AWS Glue Data Catalog")
        
        total_records = observation_df.count()
        logger.info(f"📊 Read {total_records:,} raw observation records from Iceberg")
        
        # Step 1.5: Apply Bookmark Filter
        logger.info("\n" + "=" * 50)
        logger.info("📌 STEP 1.5: APPLYING BOOKMARK FILTER")
        logger.info("=" * 50)
        logger.info("Checking Redshift for existing data to enable incremental processing...")
        
        bookmark_timestamp = get_bookmark_from_redshift()
        observation_df = filter_by_bookmark(observation_df, bookmark_timestamp)
        
        total_records_after_bookmark = observation_df.count()
        logger.info(f"✅ Bookmark filter applied - {total_records_after_bookmark:,} records to process")
        
        # Step 1.6: Deduplicate observations by observation ID
        # Note: This is still needed to handle duplicates within the incremental data
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 1.6: DEDUPLICATING OBSERVATIONS")
        logger.info("=" * 50)
        logger.info("Removing duplicate observation IDs within incremental data...")
        
        observation_df = deduplicate_observations(observation_df)
        total_records_after_dedup = observation_df.count()
        logger.info(f"✅ Deduplication completed - {total_records_after_dedup:,} unique observations remaining")
        
        # Debug: Show sample of raw data and schema
        if total_records_after_dedup > 0:
            logger.info("\n🔍 DATA QUALITY CHECKS:")
            logger.info("Sample of raw observation data:")
            observation_df.show(3, truncate=False)
            logger.info("Raw data schema:")
            observation_df.printSchema()
            
            # Check for NULL values in key fields
            null_checks = {
                "id": observation_df.filter(F.col("id").isNull()).count(),
                "subject.reference": observation_df.filter(F.col("subject").isNull() | F.col("subject.reference").isNull()).count(),
                "status": observation_df.filter(F.col("status").isNull()).count(),
                "code": observation_df.filter(F.col("code").isNull()).count()
            }
            
            logger.info("NULL value analysis in key fields:")
            for field, null_count in null_checks.items():
                percentage = (null_count / total_records_after_dedup) * 100 if total_records_after_dedup > 0 else 0
                logger.info(f"  {field}: {null_count:,} NULLs ({percentage:.1f}%)")
        else:
            logger.error("❌ No raw data found! Check the data source.")
            return
        
        # Step 2: Transform main observation data
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 2: TRANSFORMING MAIN OBSERVATION DATA")
        logger.info("=" * 50)
        
        main_observation_df = transform_main_observation_data(observation_df)
        main_count = main_observation_df.count()
        logger.info(f"✅ Transformed {main_count:,} main observation records")
        
        if main_count == 0:
            logger.error("❌ No main observation records after transformation! Check filtering criteria.")
            return
        
        # Step 3: Transform multi-valued data (all supporting tables)
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 3: TRANSFORMING MULTI-VALUED DATA")
        logger.info("=" * 50)
        
        observation_codes_df = transform_observation_codes(observation_df)
        codes_count = observation_codes_df.count()
        logger.info(f"✅ Transformed {codes_count:,} observation code records")
        
        observation_categories_df = transform_observation_categories(observation_df)
        categories_count = observation_categories_df.count()
        logger.info(f"✅ Transformed {categories_count:,} observation category records")
        
        observation_interpretations_df = transform_observation_interpretations(observation_df)
        interpretations_count = observation_interpretations_df.count()
        logger.info(f"✅ Transformed {interpretations_count:,} interpretation records")
        
        observation_reference_ranges_df = transform_observation_reference_ranges(observation_df)
        reference_ranges_count = observation_reference_ranges_df.count()
        logger.info(f"✅ Transformed {reference_ranges_count:,} reference range records")
        
        observation_components_df = transform_observation_components(observation_df)
        components_count = observation_components_df.count()
        logger.info(f"✅ Transformed {components_count:,} component records")
        
        observation_notes_df = transform_observation_notes(observation_df)
        notes_count = observation_notes_df.count()
        logger.info(f"✅ Transformed {notes_count:,} note records")
        
        observation_performers_df = transform_observation_performers(observation_df)
        performers_count = observation_performers_df.count()
        logger.info(f"✅ Transformed {performers_count:,} performer records")
        
        observation_members_df = transform_observation_members(observation_df)
        members_count = observation_members_df.count()
        logger.info(f"✅ Transformed {members_count:,} member records")
        
        observation_derived_from_df = transform_observation_derived_from(observation_df)
        derived_from_count = observation_derived_from_df.count()
        logger.info(f"✅ Transformed {derived_from_count:,} derived from records")
        
        # Debug: Show samples of multi-valued data if available
        if codes_count > 0:
            logger.info("Sample of observation codes data:")
            observation_codes_df.show(3, truncate=False)
        
        if categories_count > 0:
            logger.info("Sample of observation categories data:")
            observation_categories_df.show(3, truncate=False)
        
        if interpretations_count > 0:
            logger.info("Sample of observation interpretations data:")
            observation_interpretations_df.show(3, truncate=False)
        
        if reference_ranges_count > 0:
            logger.info("Sample of observation reference ranges data:")
            observation_reference_ranges_df.show(3, truncate=False)
        
        if components_count > 0:
            logger.info("Sample of observation components data:")
            observation_components_df.show(3, truncate=False)
        
        if notes_count > 0:
            logger.info("Sample of observation notes data:")
            observation_notes_df.show(3, truncate=False)
        
        if performers_count > 0:
            logger.info("Sample of observation performers data:")
            observation_performers_df.show(3, truncate=False)
        
        if members_count > 0:
            logger.info("Sample of observation members data:")
            observation_members_df.show(3, truncate=False)
        
        if derived_from_count > 0:
            logger.info("Sample of observation derived from data:")
            observation_derived_from_df.show(3, truncate=False)
        
        # Step 4: Convert to DynamicFrames and ensure data is flat for Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 4: CONVERTING TO DYNAMICFRAMES")
        logger.info("=" * 50)
        logger.info("Converting to DynamicFrames and ensuring Redshift compatibility...")
        
        # Convert main observations DataFrame and ensure flat structure
        main_flat_df = main_observation_df.select(
            F.col("observation_id").cast(StringType()).alias("observation_id"),
            F.col("patient_id").cast(StringType()).alias("patient_id"),
            F.col("encounter_id").cast(StringType()).alias("encounter_id"),
            F.col("specimen_id").cast(StringType()).alias("specimen_id"),
            F.col("status").cast(StringType()).alias("status"),
            F.col("observation_text").cast(StringType()).alias("observation_text"),
            F.col("primary_code").cast(StringType()).alias("primary_code"),
            F.col("primary_system").cast(StringType()).alias("primary_system"),
            F.col("primary_display").cast(StringType()).alias("primary_display"),
            F.col("value_string").cast(StringType()).alias("value_string"),
            F.col("value_quantity_value").cast(DecimalType(15,4)).alias("value_quantity_value"),
            F.col("value_quantity_unit").cast(StringType()).alias("value_quantity_unit"),
            F.col("value_quantity_system").cast(StringType()).alias("value_quantity_system"),
            F.col("value_codeable_concept_code").cast(StringType()).alias("value_codeable_concept_code"),
            F.col("value_codeable_concept_system").cast(StringType()).alias("value_codeable_concept_system"),
            F.col("value_codeable_concept_display").cast(StringType()).alias("value_codeable_concept_display"),
            F.col("value_codeable_concept_text").cast(StringType()).alias("value_codeable_concept_text"),
            F.col("value_datetime").cast(TimestampType()).alias("value_datetime"),
            F.col("value_boolean").cast(BooleanType()).alias("value_boolean"),
            F.col("data_absent_reason_code").cast(StringType()).alias("data_absent_reason_code"),
            F.col("data_absent_reason_display").cast(StringType()).alias("data_absent_reason_display"),
            F.col("data_absent_reason_system").cast(StringType()).alias("data_absent_reason_system"),
            F.col("effective_datetime").cast(TimestampType()).alias("effective_datetime"),
            F.col("effective_period_start").cast(TimestampType()).alias("effective_period_start"),
            F.col("effective_period_end").cast(TimestampType()).alias("effective_period_end"),
            F.col("issued").cast(TimestampType()).alias("issued"),
            F.col("body_site_code").cast(StringType()).alias("body_site_code"),
            F.col("body_site_system").cast(StringType()).alias("body_site_system"),
            F.col("body_site_display").cast(StringType()).alias("body_site_display"),
            F.col("body_site_text").cast(StringType()).alias("body_site_text"),
            F.col("method_code").cast(StringType()).alias("method_code"),
            F.col("method_system").cast(StringType()).alias("method_system"),
            F.col("method_display").cast(StringType()).alias("method_display"),
            F.col("method_text").cast(StringType()).alias("method_text"),
            F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
            F.col("meta_source").cast(StringType()).alias("meta_source"),
            F.col("meta_profile").cast(StringType()).alias("meta_profile"),
            F.col("meta_security").cast(StringType()).alias("meta_security"),
            F.col("meta_tag").cast(StringType()).alias("meta_tag"),
            F.col("extensions").cast(StringType()).alias("extensions"),
            F.col("created_at").cast(TimestampType()).alias("created_at"),
            F.col("updated_at").cast(TimestampType()).alias("updated_at")
        )
        
        main_dynamic_frame = DynamicFrame.fromDF(main_flat_df, glueContext, "main_observation_dynamic_frame")
        
        # Convert other DataFrames with type casting
        codes_flat_df = observation_codes_df.select(
            F.col("observation_id").cast(StringType()).alias("observation_id"),
            F.col("code_code").cast(StringType()).alias("code_code"),
            F.col("code_system").cast(StringType()).alias("code_system"),
            F.col("code_display").cast(StringType()).alias("code_display"),
            F.col("code_text").cast(StringType()).alias("code_text")
        )
        codes_dynamic_frame = DynamicFrame.fromDF(codes_flat_df, glueContext, "codes_dynamic_frame")
        
        categories_flat_df = observation_categories_df.select(
            F.col("observation_id").cast(StringType()).alias("observation_id"),
            F.col("category_code").cast(StringType()).alias("category_code"),
            F.col("category_system").cast(StringType()).alias("category_system"),
            F.col("category_display").cast(StringType()).alias("category_display"),
            F.col("category_text").cast(StringType()).alias("category_text")
        )
        categories_dynamic_frame = DynamicFrame.fromDF(categories_flat_df, glueContext, "categories_dynamic_frame")
        
        interpretations_flat_df = observation_interpretations_df.select(
            F.col("observation_id").cast(StringType()).alias("observation_id"),
            F.col("interpretation_code").cast(StringType()).alias("interpretation_code"),
            F.col("interpretation_system").cast(StringType()).alias("interpretation_system"),
            F.col("interpretation_display").cast(StringType()).alias("interpretation_display")
        )
        interpretations_dynamic_frame = DynamicFrame.fromDF(interpretations_flat_df, glueContext, "interpretations_dynamic_frame")
        
        reference_ranges_flat_df = observation_reference_ranges_df.select(
            F.col("observation_id").cast(StringType()).alias("observation_id"),
            F.col("range_low_value").cast(DecimalType(15,4)).alias("range_low_value"),
            F.col("range_low_unit").cast(StringType()).alias("range_low_unit"),
            F.col("range_high_value").cast(DecimalType(15,4)).alias("range_high_value"),
            F.col("range_high_unit").cast(StringType()).alias("range_high_unit"),
            F.col("range_type_code").cast(StringType()).alias("range_type_code"),
            F.col("range_type_system").cast(StringType()).alias("range_type_system"),
            F.col("range_type_display").cast(StringType()).alias("range_type_display"),
            F.col("range_text").cast(StringType()).alias("range_text")
        )
        reference_ranges_dynamic_frame = DynamicFrame.fromDF(reference_ranges_flat_df, glueContext, "reference_ranges_dynamic_frame")
        
        components_flat_df = observation_components_df.select(
            F.col("observation_id").cast(StringType()).alias("observation_id"),
            F.col("component_code").cast(StringType()).alias("component_code"),
            F.col("component_system").cast(StringType()).alias("component_system"),
            F.col("component_display").cast(StringType()).alias("component_display"),
            F.col("component_text").cast(StringType()).alias("component_text"),
            F.col("component_value_string").cast(StringType()).alias("component_value_string"),
            F.col("component_value_quantity_value").cast(DecimalType(15,4)).alias("component_value_quantity_value"),
            F.col("component_value_quantity_unit").cast(StringType()).alias("component_value_quantity_unit"),
            F.col("component_value_codeable_concept_code").cast(StringType()).alias("component_value_codeable_concept_code"),
            F.col("component_value_codeable_concept_system").cast(StringType()).alias("component_value_codeable_concept_system"),
            F.col("component_value_codeable_concept_display").cast(StringType()).alias("component_value_codeable_concept_display"),
            F.col("component_data_absent_reason_code").cast(StringType()).alias("component_data_absent_reason_code"),
            F.col("component_data_absent_reason_display").cast(StringType()).alias("component_data_absent_reason_display")
        )
        components_dynamic_frame = DynamicFrame.fromDF(components_flat_df, glueContext, "components_dynamic_frame")
        
        notes_flat_df = observation_notes_df.select(
            F.col("observation_id").cast(StringType()).alias("observation_id"),
            F.col("note_text").cast(StringType()).alias("note_text"),
            F.col("note_author_reference").cast(StringType()).alias("note_author_reference"),
            F.col("note_time").cast(TimestampType()).alias("note_time")
        )
        notes_dynamic_frame = DynamicFrame.fromDF(notes_flat_df, glueContext, "notes_dynamic_frame")
        
        performers_flat_df = observation_performers_df.select(
            F.col("observation_id").cast(StringType()).alias("observation_id"),
            F.col("performer_type").cast(StringType()).alias("performer_type"),
            F.col("performer_id").cast(StringType()).alias("performer_id")
        )
        performers_dynamic_frame = DynamicFrame.fromDF(performers_flat_df, glueContext, "performers_dynamic_frame")
        
        members_flat_df = observation_members_df.select(
            F.col("observation_id").cast(StringType()).alias("observation_id"),
            F.col("member_observation_id").cast(StringType()).alias("member_observation_id")
        )
        members_dynamic_frame = DynamicFrame.fromDF(members_flat_df, glueContext, "members_dynamic_frame")
        
        derived_from_flat_df = observation_derived_from_df.select(
            F.col("observation_id").cast(StringType()).alias("observation_id"),
            F.col("derived_from_reference").cast(StringType()).alias("derived_from_reference")
        )
        derived_from_dynamic_frame = DynamicFrame.fromDF(derived_from_flat_df, glueContext, "derived_from_dynamic_frame")
        
        # Step 5: Resolve any remaining choice types to ensure Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 5: RESOLVING CHOICE TYPES")
        logger.info("=" * 50)
        logger.info("Resolving choice types for Redshift compatibility...")
        
        main_resolved_frame = main_dynamic_frame.resolveChoice(
            specs=[
                ("observation_id", "cast:string"),
                ("patient_id", "cast:string"),
                ("encounter_id", "cast:string"),
                ("specimen_id", "cast:string"),
                ("status", "cast:string"),
                ("observation_text", "cast:string"),
                ("primary_code", "cast:string"),
                ("primary_system", "cast:string"),
                ("primary_display", "cast:string"),
                ("value_string", "cast:string"),
                ("value_quantity_value", "cast:decimal"),
                ("value_quantity_unit", "cast:string"),
                ("value_quantity_system", "cast:string"),
                ("value_codeable_concept_code", "cast:string"),
                ("value_codeable_concept_system", "cast:string"),
                ("value_codeable_concept_display", "cast:string"),
                ("value_codeable_concept_text", "cast:string"),
                ("value_datetime", "cast:timestamp"),
                ("value_boolean", "cast:boolean"),
                ("data_absent_reason_code", "cast:string"),
                ("data_absent_reason_display", "cast:string"),
                ("data_absent_reason_system", "cast:string"),
                ("effective_datetime", "cast:timestamp"),
                ("effective_period_start", "cast:timestamp"),
                ("effective_period_end", "cast:timestamp"),
                ("issued", "cast:timestamp"),
                ("body_site_code", "cast:string"),
                ("body_site_system", "cast:string"),
                ("body_site_display", "cast:string"),
                ("body_site_text", "cast:string"),
                ("method_code", "cast:string"),
                ("method_system", "cast:string"),
                ("method_display", "cast:string"),
                ("method_text", "cast:string"),
                ("meta_last_updated", "cast:timestamp"),
                ("meta_source", "cast:string"),
                ("meta_profile", "cast:string"),
                ("meta_security", "cast:string"),
                ("meta_tag", "cast:string"),
                ("extensions", "cast:string"),
                ("created_at", "cast:timestamp"),
                ("updated_at", "cast:timestamp")
            ]
        )
        
        codes_resolved_frame = codes_dynamic_frame.resolveChoice(
            specs=[
                ("observation_id", "cast:string"),
                ("code_code", "cast:string"),
                ("code_system", "cast:string"),
                ("code_display", "cast:string"),
                ("code_text", "cast:string")
            ]
        )
        
        categories_resolved_frame = categories_dynamic_frame.resolveChoice(
            specs=[
                ("observation_id", "cast:string"),
                ("category_code", "cast:string"),
                ("category_system", "cast:string"),
                ("category_display", "cast:string"),
                ("category_text", "cast:string")
            ]
        )
        
        interpretations_resolved_frame = interpretations_dynamic_frame.resolveChoice(
            specs=[
                ("observation_id", "cast:string"),
                ("interpretation_code", "cast:string"),
                ("interpretation_system", "cast:string"),
                ("interpretation_display", "cast:string")
            ]
        )
        
        reference_ranges_resolved_frame = reference_ranges_dynamic_frame.resolveChoice(
            specs=[
                ("observation_id", "cast:string"),
                ("range_low_value", "cast:decimal"),
                ("range_low_unit", "cast:string"),
                ("range_high_value", "cast:decimal"),
                ("range_high_unit", "cast:string"),
                ("range_type_code", "cast:string"),
                ("range_type_system", "cast:string"),
                ("range_type_display", "cast:string"),
                ("range_text", "cast:string")
            ]
        )
        
        components_resolved_frame = components_dynamic_frame.resolveChoice(
            specs=[
                ("observation_id", "cast:string"),
                ("component_code", "cast:string"),
                ("component_system", "cast:string"),
                ("component_display", "cast:string"),
                ("component_text", "cast:string"),
                ("component_value_string", "cast:string"),
                ("component_value_quantity_value", "cast:decimal"),
                ("component_value_quantity_unit", "cast:string"),
                ("component_value_codeable_concept_code", "cast:string"),
                ("component_value_codeable_concept_system", "cast:string"),
                ("component_value_codeable_concept_display", "cast:string"),
                ("component_data_absent_reason_code", "cast:string"),
                ("component_data_absent_reason_display", "cast:string")
            ]
        )
        
        notes_resolved_frame = notes_dynamic_frame.resolveChoice(
            specs=[
                ("observation_id", "cast:string"),
                ("note_text", "cast:string"),
                ("note_author_reference", "cast:string"),
                ("note_time", "cast:timestamp")
            ]
        )
        
        performers_resolved_frame = performers_dynamic_frame.resolveChoice(
            specs=[
                ("observation_id", "cast:string"),
                ("performer_type", "cast:string"),
                ("performer_id", "cast:string")
            ]
        )
        
        members_resolved_frame = members_dynamic_frame.resolveChoice(
            specs=[
                ("observation_id", "cast:string"),
                ("member_observation_id", "cast:string")
            ]
        )
        
        derived_from_resolved_frame = derived_from_dynamic_frame.resolveChoice(
            specs=[
                ("observation_id", "cast:string"),
                ("derived_from_reference", "cast:string")
            ]
        )
        
        # Step 6: Final validation before writing
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 6: FINAL VALIDATION")
        logger.info("=" * 50)
        logger.info("Performing final validation before writing to Redshift...")
        
        # Validate main observations data
        main_final_df = main_resolved_frame.toDF()
        main_final_count = main_final_df.count()
        logger.info(f"Final main observations count: {main_final_count}")
        
        if main_final_count == 0:
            logger.error("No main observation records to write to Redshift! Stopping the process.")
            return
        
        # Validate other tables
        codes_final_count = codes_resolved_frame.toDF().count()
        categories_final_count = categories_resolved_frame.toDF().count()
        interpretations_final_count = interpretations_resolved_frame.toDF().count()
        reference_ranges_final_count = reference_ranges_resolved_frame.toDF().count()
        components_final_count = components_resolved_frame.toDF().count()
        notes_final_count = notes_resolved_frame.toDF().count()
        performers_final_count = performers_resolved_frame.toDF().count()
        members_final_count = members_resolved_frame.toDF().count()
        derived_from_final_count = derived_from_resolved_frame.toDF().count()
        
        logger.info(f"Final counts - Codes: {codes_final_count}, Categories: {categories_final_count}, Interpretations: {interpretations_final_count}, Reference Ranges: {reference_ranges_final_count}, Components: {components_final_count}, Notes: {notes_final_count}, Performers: {performers_final_count}, Members: {members_final_count}, Derived From: {derived_from_final_count}")
        
        # Debug: Show final sample data being written
        logger.info("Final sample data being written to Redshift (main observations):")
        main_final_df.show(3, truncate=False)
        
        # Show sample data for other tables as well
        logger.info("Final sample data for observation codes:")
        codes_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for observation categories:")
        categories_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for observation interpretations:")
        interpretations_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for observation reference ranges:")
        reference_ranges_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for observation components:")
        components_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for observation notes:")
        notes_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for observation performers:")
        performers_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for observation members:")
        members_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for observation derived from:")
        derived_from_resolved_frame.toDF().show(3, truncate=False)
        
        # Step 7: Create tables and write to Redshift
        logger.info("\n" + "=" * 50)
        logger.info("💾 STEP 7: WRITING DATA TO REDSHIFT")
        logger.info("=" * 50)
        logger.info(f"🔗 Using connection: {REDSHIFT_CONNECTION}")
        logger.info(f"📁 S3 temp directory: {S3_TEMP_DIR}")
        
        # Create all tables individually and write data
        # Using simple append mode since bookmark pattern filters data at source
        # For initial load or when bookmark returns no data, we use TRUNCATE to clear tables
        
        # Determine if this is an initial load or incremental
        is_initial_load = (bookmark_timestamp is None)
        
        if is_initial_load:
            logger.info("🔄 Initial load mode - will TRUNCATE existing tables before insert")
        else:
            logger.info("➕ Incremental load mode - will APPEND new records only")
        
        logger.info("📝 Writing main observations table...")
        observations_table_sql = create_redshift_tables_sql()
        if is_initial_load:
            observations_preactions = observations_table_sql + "; TRUNCATE TABLE public.observations;"
        else:
            observations_preactions = observations_table_sql
        write_to_redshift_simple(main_resolved_frame, "observations", observations_preactions)
        logger.info("✅ Main observations table written successfully")
        
        logger.info("📝 Writing observation codes table...")
        codes_table_sql = create_observation_codes_table_sql()
        if is_initial_load:
            codes_preactions = codes_table_sql + "; TRUNCATE TABLE public.observation_codes;"
        else:
            codes_preactions = codes_table_sql
        write_to_redshift_simple(codes_resolved_frame, "observation_codes", codes_preactions)
        logger.info("✅ Observation codes table written successfully")
        
        logger.info("📝 Writing observation categories table...")
        categories_table_sql = create_observation_categories_table_sql()
        if is_initial_load:
            categories_preactions = categories_table_sql + "; TRUNCATE TABLE public.observation_categories;"
        else:
            categories_preactions = categories_table_sql
        write_to_redshift_simple(categories_resolved_frame, "observation_categories", categories_preactions)
        logger.info("✅ Observation categories table written successfully")
        
        logger.info("📝 Writing observation interpretations table...")
        interpretations_table_sql = create_observation_interpretations_table_sql()
        if is_initial_load:
            interpretations_preactions = interpretations_table_sql + "; TRUNCATE TABLE public.observation_interpretations;"
        else:
            interpretations_preactions = interpretations_table_sql
        write_to_redshift_simple(interpretations_resolved_frame, "observation_interpretations", interpretations_preactions)
        logger.info("✅ Observation interpretations table written successfully")
        
        logger.info("📝 Writing observation reference ranges table...")
        reference_ranges_table_sql = create_observation_reference_ranges_table_sql()
        if is_initial_load:
            reference_ranges_preactions = reference_ranges_table_sql + "; TRUNCATE TABLE public.observation_reference_ranges;"
        else:
            reference_ranges_preactions = reference_ranges_table_sql
        write_to_redshift_simple(reference_ranges_resolved_frame, "observation_reference_ranges", reference_ranges_preactions)
        logger.info("✅ Observation reference ranges table written successfully")
        
        logger.info("📝 Writing observation components table...")
        components_table_sql = create_observation_components_table_sql()
        if is_initial_load:
            components_preactions = components_table_sql + "; TRUNCATE TABLE public.observation_components;"
        else:
            components_preactions = components_table_sql
        write_to_redshift_simple(components_resolved_frame, "observation_components", components_preactions)
        logger.info("✅ Observation components table written successfully")
        
        logger.info("📝 Writing observation notes table...")
        notes_table_sql = create_observation_notes_table_sql()
        if is_initial_load:
            notes_preactions = notes_table_sql + "; TRUNCATE TABLE public.observation_notes;"
        else:
            notes_preactions = notes_table_sql
        write_to_redshift_simple(notes_resolved_frame, "observation_notes", notes_preactions)
        logger.info("✅ Observation notes table written successfully")
        
        logger.info("📝 Writing observation performers table...")
        performers_table_sql = create_observation_performers_table_sql()
        if is_initial_load:
            performers_preactions = performers_table_sql + "; TRUNCATE TABLE public.observation_performers;"
        else:
            performers_preactions = performers_table_sql
        write_to_redshift_simple(performers_resolved_frame, "observation_performers", performers_preactions)
        logger.info("✅ Observation performers table written successfully")
        
        logger.info("📝 Writing observation members table...")
        members_table_sql = create_observation_members_table_sql()
        if is_initial_load:
            members_preactions = members_table_sql + "; TRUNCATE TABLE public.observation_members;"
        else:
            members_preactions = members_table_sql
        write_to_redshift_simple(members_resolved_frame, "observation_members", members_preactions)
        logger.info("✅ Observation members table written successfully")
        
        logger.info("📝 Writing observation derived from table...")
        derived_from_table_sql = create_observation_derived_from_table_sql()
        if is_initial_load:
            derived_from_preactions = derived_from_table_sql + "; TRUNCATE TABLE public.observation_derived_from;"
        else:
            derived_from_preactions = derived_from_table_sql
        write_to_redshift_simple(derived_from_resolved_frame, "observation_derived_from", derived_from_preactions)
        logger.info("✅ Observation derived from table written successfully")
        
        # Calculate processing time
        end_time = datetime.now()
        processing_time = end_time - start_time
        
        logger.info("\n" + "=" * 80)
        logger.info("🎉 ETL PROCESS COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        logger.info(f"⏰ Job completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"⏱️  Total processing time: {processing_time}")
        
        logger.info("\n📋 TABLES WRITTEN TO REDSHIFT:")
        logger.info("  ✅ public.observations (main observation data)")
        logger.info("  ✅ public.observation_codes (observation codes)")
        logger.info("  ✅ public.observation_categories (observation categories)")
        logger.info("  ✅ public.observation_interpretations (observation interpretations)")
        logger.info("  ✅ public.observation_reference_ranges (reference ranges)")
        logger.info("  ✅ public.observation_components (observation components)")
        logger.info("  ✅ public.observation_notes (observation notes)")
        logger.info("  ✅ public.observation_performers (observation performers)")
        logger.info("  ✅ public.observation_members (observation members)")
        logger.info("  ✅ public.observation_derived_from (derived from references)")
        
        logger.info("\n📊 FINAL ETL STATISTICS:")
        logger.info(f"  📥 Total raw records in Iceberg: {total_records:,}")
        logger.info(f"  📌 Records after bookmark filter: {total_records_after_bookmark:,}")
        logger.info(f"  ⏭️  Records skipped by bookmark: {total_records - total_records_after_bookmark:,}")
        logger.info(f"  🔄 Records after deduplication: {total_records_after_dedup:,}")
        logger.info(f"  🗑️  Duplicates removed: {total_records_after_bookmark - total_records_after_dedup:,}")
        logger.info(f"  🔬 Main observation records written: {main_count:,}")
        logger.info(f"  🔢 Code records: {codes_count:,}")
        logger.info(f"  🏷️  Category records: {categories_count:,}")
        logger.info(f"  📊 Interpretation records: {interpretations_count:,}")
        logger.info(f"  📏 Reference range records: {reference_ranges_count:,}")
        logger.info(f"  🔧 Component records: {components_count:,}")
        logger.info(f"  📝 Note records: {notes_count:,}")
        logger.info(f"  👥 Performer records: {performers_count:,}")
        logger.info(f"  🔗 Member records: {members_count:,}")
        logger.info(f"  📋 Derived from records: {derived_from_count:,}")
        
        # Calculate data expansion ratio
        total_output_records = main_count + codes_count + categories_count + interpretations_count + reference_ranges_count + components_count + notes_count + performers_count + members_count + derived_from_count
        expansion_ratio = total_output_records / total_records_after_dedup if total_records_after_dedup > 0 else 0
        logger.info(f"  📈 Data expansion ratio: {expansion_ratio:.2f}x (output records / deduplicated input records)")
        
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
        logger.error("=" * 80)
        raise e

if __name__ == "__main__":
    main()
    job.commit()