# Deployed: 2025-10-09 06:01:53 UTC
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

def get_bookmark_from_redshift():
    """Get the maximum meta_last_updated timestamp from Redshift practitioners table

    This bookmark represents the latest data already loaded into Redshift.
    We'll only process Iceberg records newer than this timestamp.
    """
    logger.info("Fetching bookmark (max meta_last_updated) from Redshift practitioners table...")

    try:
        # Read the entire practitioners table (only meta_last_updated column for efficiency)
        # Then find the max in Spark instead of using a Redshift subquery
        bookmark_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "redshiftTmpDir": S3_TEMP_DIR,
                "useConnectionProperties": "true",
                "dbtable": "public.practitioners",
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
                logger.info("🔖 No bookmark found (practitioners table is empty)")
                logger.info("   This is an initial full load - will process all Iceberg records")
                return None
        else:
            logger.info("🔖 No bookmark available - proceeding with full load")
            return None

    except Exception as e:
        logger.info(f"🔖 Could not fetch bookmark (table may not exist): {str(e)}")
        logger.info("   Proceeding with full initial load of all Iceberg records")
        return None

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
        F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'"),
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
            logger.info(f"No records to process for {table_name}, but ensuring table exists")
            # Execute preactions to create table even if no data
            if preactions:
                logger.info(f"Executing preactions to create empty {table_name} table")
                # Need to write at least one record to execute preactions, then delete it
                # Create a dummy record with all nulls
                from pyspark.sql import Row
                schema = df.schema
                null_row = Row(**{field.name: None for field in schema.fields})
                dummy_df = spark.createDataFrame([null_row], schema)
                dummy_dynamic_frame = DynamicFrame.fromDF(dummy_df, glueContext, f"dummy_{table_name}")

                glueContext.write_dynamic_frame.from_options(
                    frame=dummy_dynamic_frame,
                    connection_type="redshift",
                    connection_options={
                        "redshiftTmpDir": S3_TEMP_DIR,
                        "useConnectionProperties": "true",
                        "dbtable": f"public.{table_name}",
                        "connectionName": REDSHIFT_CONNECTION,
                        "preactions": preactions,
                        "postactions": f"DELETE FROM public.{table_name};"  # Remove dummy record
                    },
                    transformation_ctx=f"create_empty_{table_name}"
                )
                logger.info(f"✅ Created empty {table_name} table")
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
            # For large deletions, use a different strategy to avoid "Statement is too large" error
            # Redshift has a 16MB limit on SQL statements
            num_entities = len(entities_to_delete)

            # If we have too many entities (>10000), just truncate the table and re-insert everything
            # This is more efficient than trying to batch large DELETE statements
            if num_entities > 10000:
                logger.info(f"Large deletion ({num_entities} entities) - will truncate table and re-insert all data")
                delete_clause = f"TRUNCATE TABLE public.{table_name};"
                # Convert ALL incoming data back to dynamic frame for full re-insert
                filtered_df = df  # Use all data, not just filtered
                to_process_count = total_records
            else:
                # For smaller deletions, use IN clause
                entity_ids_str = "', '".join(entities_to_delete)
                delete_clause = f"DELETE FROM public.{table_name} WHERE {id_column} IN ('{entity_ids_str}');"

            if selective_preactions:
                selective_preactions = delete_clause + " " + selective_preactions
            else:
                selective_preactions = delete_clause

            logger.info(f"Will delete {num_entities} existing entities before inserting updated versions")

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
logger.setLevel(logging.INFO)

# Add handler to write logs to stdout so they appear in CloudWatch
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# BEGIN NOTE TO AI: do not change the following section
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

# END NOTE TO AI

# Configuration - Updated to use S3/Iceberg instead of Glue Catalog
DATABASE_NAME = ahl_database  # Using AHL Iceberg database
TABLE_NAME = "practitioner"
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

def transform_main_practitioner_data(df):
    """Transform the main practitioner data"""
    logger.info("Transforming main practitioner data...")
    
    available_columns = df.columns
    logger.info(f"Available columns: {available_columns}")
    
    select_columns = [
        F.col("id").alias("practitioner_id"),
        F.col("resourceType").alias("resource_type"),
        F.col("active").alias("active"),
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
        F.col("practitioner_id").isNotNull()
    )
    
    return main_df


def transform_practitioner_names(df):
    """Transform practitioner names"""
    logger.info("Transforming practitioner names...")

    if "name" not in df.columns:
        logger.warning("name column not found")
        return spark.createDataFrame([], df.select(F.col("id").alias("practitioner_id")).schema
                                     .add("text", StringType())
                                     .add("family", StringType())
                                     .add("given", StringType()))

    names_df = df.select(
        F.col("id").alias("practitioner_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode(F.col("name")).alias("name_item")
    )

    names_final = names_df.select(
        F.col("practitioner_id"),
        F.col("meta_last_updated"),
        F.col("name_item.text").alias("text"),
        F.col("name_item.family").alias("family"),
        F.concat_ws(" ", F.col("name_item.given")).alias("given")
    ).filter(
        F.col("text").isNotNull()
    )
    
    return names_final

def transform_practitioner_telecoms(df):
    """Transform practitioner telecoms"""
    logger.info("Transforming practitioner telecoms...")

    if "telecom" not in df.columns:
        logger.warning("telecom column not found")
        return spark.createDataFrame([], df.select(F.col("id").alias("practitioner_id")).schema
                                     .add("system", StringType())
                                     .add("value", StringType()))

    telecoms_df = df.select(
        F.col("id").alias("practitioner_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode(F.col("telecom")).alias("telecom_item")
    )

    telecoms_final = telecoms_df.select(
        F.col("practitioner_id"),
        F.col("meta_last_updated"),
        F.col("telecom_item.system").alias("system"),
        F.col("telecom_item.value").alias("value")
    ).filter(
        F.col("value").isNotNull()
    )
    
    return telecoms_final

def transform_practitioner_addresses(df):
    """Transform practitioner addresses"""
    logger.info("Transforming practitioner addresses...")

    if "address" not in df.columns:
        logger.warning("address column not found")
        return spark.createDataFrame([], df.select(F.col("id").alias("practitioner_id")).schema
                                     .add("line", StringType())
                                     .add("city", StringType())
                                     .add("state", StringType())
                                     .add("postal_code", StringType()))

    addresses_df = df.select(
        F.col("id").alias("practitioner_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode(F.col("address")).alias("address_item")
    )

    addresses_final = addresses_df.select(
        F.col("practitioner_id"),
        F.col("meta_last_updated"),
        F.concat_ws(", ", F.col("address_item.line")).alias("line"),
        F.col("address_item.city").alias("city"),
        F.col("address_item.state").alias("state"),
        F.col("address_item.postalCode").alias("postal_code")
    )
    
    return addresses_final

def create_redshift_tables_sql():
    return """
    CREATE TABLE IF NOT EXISTS public.practitioners (
        practitioner_id VARCHAR(255) NOT NULL DISTKEY,
        resource_type VARCHAR(50),
        active BOOLEAN,
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) SORTKEY (practitioner_id);
    """


def create_practitioner_names_table_sql():
    return """
    CREATE TABLE IF NOT EXISTS public.practitioner_names (
        practitioner_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        text VARCHAR(500),
        family VARCHAR(255),
        given VARCHAR(255)
    ) SORTKEY (practitioner_id, family);
    """

def create_practitioner_telecoms_table_sql():
    return """
    CREATE TABLE IF NOT EXISTS public.practitioner_telecoms (
        practitioner_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        "system" VARCHAR(50),
        value VARCHAR(255)
    ) SORTKEY (practitioner_id, "system");
    """

def create_practitioner_addresses_table_sql():
    return """
    CREATE TABLE IF NOT EXISTS public.practitioner_addresses (
        practitioner_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        line VARCHAR(500),
        city VARCHAR(100),
        state VARCHAR(50),
        postal_code VARCHAR(20)
    ) SORTKEY (practitioner_id, state);
    """

# Duplicate function removed - using the version-aware write_to_redshift_versioned from line 146

def main():
    start_time = datetime.now()
    try:
        logger.info("="*80)
        logger.info("🚀 STARTING FHIR PRACTITIONER ETL PROCESS")
        logger.info(f"⏰ Job started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Check table existence and log schemas
        table_names = ["practitioner_addresses", "practitioner_names", "practitioner_telecoms", "practitioners"]
        check_all_tables(glueContext, table_names, REDSHIFT_CONNECTION, S3_TEMP_DIR)

        
        # Use Iceberg to read data from S3
        table_name_full = f"{catalog_nm}.{DATABASE_NAME}.{TABLE_NAME}"
        logger.info(f"Reading from table: {table_name_full}")
        df_raw = spark.table(table_name_full)
        
        practitioner_df = df_raw

        
        # TESTING MODE: Sample data for quick testing

        
        # Set to True to process only a sample of records

        
        USE_SAMPLE = False  # Set to True for testing with limited data

        
        SAMPLE_SIZE = 1000

        
        

        
        if USE_SAMPLE:

        
            logger.info(f"⚠️  TESTING MODE: Sampling {SAMPLE_SIZE} records for quick testing")

        
            logger.info("⚠️  Set USE_SAMPLE = False for production runs")

        
            practitioner_df = practitioner_df.limit(SAMPLE_SIZE)

        
        else:

        
            logger.info("✅ Processing full dataset")

        
            practitioner_df = practitioner_df

        total_records = practitioner_df.count()
        logger.info(f"📊 Read {total_records:,} raw practitioner records from Iceberg")

        # Step: Apply Bookmark Filter
        logger.info("\n" + "=" * 50)
        logger.info("📌 APPLYING BOOKMARK FILTER")
        logger.info("=" * 50)
        logger.info("Checking Redshift for existing data to enable incremental processing...")

        bookmark_timestamp = get_bookmark_from_redshift()
        practitioner_df = filter_by_bookmark(practitioner_df, bookmark_timestamp)

        total_records_after_bookmark = practitioner_df.count()
        logger.info(f"✅ Bookmark filter applied - {total_records_after_bookmark:,} records to process")

        if total_records_after_bookmark == 0:
            logger.warning("No new records to process after bookmark filter. Exiting job.")
            job.commit()
            return

        main_practitioner_df = transform_main_practitioner_data(practitioner_df)
        main_count = main_practitioner_df.count()
        logger.info(f"✅ Transformed {main_count:,} main practitioner records")
        
        practitioner_names_df = transform_practitioner_names(practitioner_df)
        names_count = practitioner_names_df.count()
        logger.info(f"✅ Transformed {names_count:,} practitioner name records")
        
        practitioner_telecoms_df = transform_practitioner_telecoms(practitioner_df)
        telecoms_count = practitioner_telecoms_df.count()
        logger.info(f"✅ Transformed {telecoms_count:,} practitioner telecom records")
        
        practitioner_addresses_df = transform_practitioner_addresses(practitioner_df)
        addresses_count = practitioner_addresses_df.count()
        logger.info(f"✅ Transformed {addresses_count:,} practitioner address records")

        main_dynamic_frame = DynamicFrame.fromDF(main_practitioner_df, glueContext, "main_practitioner_dynamic_frame")
        names_dynamic_frame = DynamicFrame.fromDF(practitioner_names_df, glueContext, "names_dynamic_frame")
        telecoms_dynamic_frame = DynamicFrame.fromDF(practitioner_telecoms_df, glueContext, "telecoms_dynamic_frame")
        addresses_dynamic_frame = DynamicFrame.fromDF(practitioner_addresses_df, glueContext, "addresses_dynamic_frame")
        
        main_resolved_frame = main_dynamic_frame.resolveChoice(specs=[('practitioner_id', 'cast:string')])
        names_resolved_frame = names_dynamic_frame.resolveChoice(specs=[('practitioner_id', 'cast:string'), ('meta_last_updated', 'cast:timestamp')])
        telecoms_resolved_frame = telecoms_dynamic_frame.resolveChoice(specs=[('practitioner_id', 'cast:string'), ('meta_last_updated', 'cast:timestamp')])
        addresses_resolved_frame = addresses_dynamic_frame.resolveChoice(specs=[('practitioner_id', 'cast:string'), ('meta_last_updated', 'cast:timestamp')])

        # Write data to Redshift using bookmark pattern
        # For initial load (no bookmark), use TRUNCATE to clear tables
        # For incremental load, simply append new records
        logger.info("\n" + "=" * 50)
        logger.info("💾 WRITING DATA TO REDSHIFT")
        logger.info("=" * 50)

        is_initial_load = (bookmark_timestamp is None)

        if is_initial_load:
            logger.info("🔄 Initial load mode - will TRUNCATE existing tables before insert")
        else:
            logger.info("➕ Incremental load mode - will APPEND new records")

        # Prepare preactions with TRUNCATE for initial loads
        if is_initial_load:
            practitioners_preactions = "TRUNCATE TABLE public.practitioners;"
        else:
            practitioners_preactions = ""

        if is_initial_load:
            names_preactions = "TRUNCATE TABLE public.practitioner_names;"
        else:
            names_preactions = ""

        if is_initial_load:
            telecoms_preactions = "TRUNCATE TABLE public.practitioner_telecoms;"
        else:
            telecoms_preactions = ""

        if is_initial_load:
            addresses_preactions = "TRUNCATE TABLE public.practitioner_addresses;"
        else:
            addresses_preactions = ""

        # Write using simple append mode (bookmark pattern handles filtering)
        write_to_redshift_simple(main_resolved_frame, "practitioners", practitioners_preactions)
        write_to_redshift_simple(names_resolved_frame, "practitioner_names", names_preactions)
        write_to_redshift_simple(telecoms_resolved_frame, "practitioner_telecoms", telecoms_preactions)
        write_to_redshift_simple(addresses_resolved_frame, "practitioner_addresses", addresses_preactions)
        
        end_time = datetime.now()
        processing_time = end_time - start_time

        logger.info("\n" + "=" * 80)
        logger.info("🎉 ETL PROCESS COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        logger.info(f"⏰ Job completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"⏱️  Total processing time: {processing_time}")

        logger.info("\n📊 PROCESSING STATISTICS:")
        logger.info(f"  📥 Total records in Iceberg: {total_records:,}")
        logger.info(f"  📌 Records after bookmark filter: {total_records_after_bookmark:,}")
        logger.info(f"  ⏭️  Records skipped by bookmark: {total_records - total_records_after_bookmark:,}")
        if total_records > 0:
            logger.info(f"  📈 Bookmark efficiency: {((total_records - total_records_after_bookmark)/total_records)*100:.1f}% skipped")

        if USE_SAMPLE:
            logger.info("\n⚠️  WARNING: THIS WAS A TEST RUN WITH SAMPLED DATA")
            logger.info(f"⚠️  Only {SAMPLE_SIZE} records were processed")
            logger.info("⚠️  Set USE_SAMPLE = False for production runs")
        
    except Exception as e:
        logger.error(f"❌ ETL PROCESS FAILED: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
    job.commit()
