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
from pyspark.sql.types import StringType, TimestampType, DateType, BooleanType, IntegerType, DecimalType, ArrayType, StructType, StructField
import json
import logging
import re

# FHIR version comparison utilities implemented inline below

# FHIR version comparison utilities are implemented inline below

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
TABLE_NAME = "condition"
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

def extract_patient_id_from_reference(reference_field):
    """Extract patient ID from FHIR reference format"""
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

def extract_encounter_id_from_reference(reference_field):
    """Extract encounter ID from FHIR reference format"""
    if reference_field:
        # Handle Row/struct format: Row(reference="Encounter/123", display="Name")
        if hasattr(reference_field, 'reference'):
            reference = reference_field.reference
            if reference and "/" in reference:
                return reference.split("/")[-1]
        # Handle dict format: {"reference": "Encounter/123", "display": "Name"}
        elif isinstance(reference_field, dict):
            reference = reference_field.get('reference')
            if reference and "/" in reference:
                return reference.split("/")[-1]
        # Handle string format: "Encounter/123"
        elif isinstance(reference_field, str):
            if "/" in reference_field:
                return reference_field.split("/")[-1]
    return None

def extract_practitioner_id_from_reference(reference_field):
    """Extract practitioner ID from FHIR reference format"""
    if reference_field:
        # Handle Row/struct format: Row(reference="Practitioner/123", display="Name")
        if hasattr(reference_field, 'reference'):
            reference = reference_field.reference
            if reference and "/" in reference:
                return reference.split("/")[-1]
        # Handle dict format: {"reference": "Practitioner/123", "display": "Name"}
        elif isinstance(reference_field, dict):
            reference = reference_field.get('reference')
            if reference and "/" in reference:
                return reference.split("/")[-1]
        # Handle string format: "Practitioner/123"
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

def convert_to_json_string(field):
    """Convert complex data to JSON strings to avoid nested structures"""
    if field is None:
        return None
    try:
        if isinstance(field, str):
            return field
        else:
            return json.dumps(field)
    except:
        return str(field)

# Define UDFs globally so they can be used in all transformation functions
convert_to_json_udf = F.udf(convert_to_json_string, StringType())

def transform_main_condition_data(df):
    """Transform the main condition data"""
    logger.info("Transforming main condition data...")
    
    # Log available columns for debugging
    available_columns = df.columns
    logger.info(f"Available columns: {available_columns}")
    
    # Build the select statement dynamically based on available columns
    select_columns = [
        F.col("id").alias("condition_id"),
        F.when(F.col("subject").isNotNull(), 
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        F.when(F.col("encounter").isNotNull(),
               F.regexp_extract(F.col("encounter").getField("reference"), r"Encounter/(.+)", 1)
              ).otherwise(None).alias("encounter_id"),
        # F.lit("Condition").alias("resourcetype"),  # Removed - not needed in Redshift table
    ]
    
    # Add clinical status information
    select_columns.extend([
        F.when(F.col("clinicalStatus").isNotNull() & (F.size(F.col("clinicalStatus").getField("coding")) > 0),
               F.col("clinicalStatus").getField("coding")[0].getField("code")
              ).otherwise(None).alias("clinical_status_code"),
        F.when(F.col("clinicalStatus").isNotNull() & (F.size(F.col("clinicalStatus").getField("coding")) > 0),
               F.col("clinicalStatus").getField("coding")[0].getField("system")
              ).otherwise(None).alias("clinical_status_system"),
        F.lit(None).alias("clinical_status_display"),
    ])
    
    # Add verification status information
    select_columns.extend([
        F.when(F.col("verificationStatus").isNotNull() & (F.size(F.col("verificationStatus").getField("coding")) > 0),
               F.col("verificationStatus").getField("coding")[0].getField("code")
              ).otherwise(None).alias("verification_status_code"),
        F.when(F.col("verificationStatus").isNotNull() & (F.size(F.col("verificationStatus").getField("coding")) > 0),
               F.col("verificationStatus").getField("coding")[0].getField("system")
              ).otherwise(None).alias("verification_status_system"),
        F.lit(None).alias("verification_status_display"),
    ])
    
    # Add condition text information
    select_columns.extend([
        F.col("code").getField("text").alias("condition_text"),
    ])
    
    # Note: All codes (SNOMED, ICD-10, ICD-9, etc.) are stored in the separate condition_codes table
    # The main conditions table only stores the general condition text
    
    # Add severity information (if available) - check if column exists first
    if "severity" in available_columns:
        select_columns.extend([
            F.when(F.col("severity").isNotNull(),
                   F.col("severity").getField("coding")[0].getField("code")
                  ).otherwise(None).alias("severity_code"),
            F.when(F.col("severity").isNotNull(),
                   F.col("severity").getField("coding")[0].getField("system")
                  ).otherwise(None).alias("severity_system"),
            F.lit(None).alias("severity_display"),
        ])
    else:
        # Column doesn't exist, use null values
        select_columns.extend([
            F.lit(None).alias("severity_code"),
            F.lit(None).alias("severity_system"),
            F.lit(None).alias("severity_display"),
        ])
    
    # Add onset information
    select_columns.extend([
        # Handle onsetDateTime with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("onsetDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("onsetDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("onsetDateTime"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("onsetDateTime"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("onsetDateTime"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("onset_datetime"),
        F.lit(None).alias("onset_age_value"),
        F.lit(None).alias("onset_age_unit"),
        F.lit(None).alias("onset_period_start"),
        F.lit(None).alias("onset_period_end"),
        F.lit(None).alias("onset_text"),
    ])
    
    # Add abatement information
    select_columns.extend([
        # Handle abatementDateTime with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("abatementDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("abatementDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("abatementDateTime"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("abatementDateTime"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("abatementDateTime"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("abatement_datetime"),
        F.lit(None).alias("abatement_age_value"),
        F.lit(None).alias("abatement_age_unit"),
        F.lit(None).alias("abatement_period_start"),
        F.lit(None).alias("abatement_period_end"),
        F.lit(None).alias("abatement_text"),
        F.lit(None).alias("abatement_boolean"),
    ])
    
    # Add recorded date and recorder information
    select_columns.extend([
        # Handle recordedDate with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("recordedDate"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("recordedDate"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("recordedDate"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("recordedDate"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("recordedDate"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("recorded_date"),
        F.lit(None).alias("recorder_type"),
        F.lit(None).alias("recorder_id"),
        F.lit(None).alias("asserter_type"),
        F.lit(None).alias("asserter_id"),
    ])
    
    # Add metadata information
    select_columns.extend([
        # Handle meta.lastUpdated with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("meta_last_updated"),
        F.lit(None).alias("meta_source"),
        convert_to_json_udf(F.col("meta").getField("profile")).alias("meta_profile"),
        F.lit(None).alias("meta_security"),
        F.lit(None).alias("meta_tag"),
    ])
    
    # Add timestamps
    select_columns.extend([
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ])
    
    # Transform main condition data using only available columns and flatten complex structures
    main_df = df.select(*select_columns).filter(
        F.col("condition_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return main_df

def transform_condition_categories(df):
    """Transform condition categories (multiple categories per condition)"""
    logger.info("Transforming condition categories...")
    
    # Use Spark's native column operations to handle the nested structure
    # category: array -> element: struct -> coding: array -> element: struct
    
    # First explode the category array
    categories_df = df.select(
        F.col("id").alias("condition_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode(F.col("category")).alias("category_item")
    ).filter(
        F.col("category_item").isNotNull()
    )
    
    # Extract category details and explode the coding array
    categories_final = categories_df.select(
        F.col("condition_id"),
        F.col("meta_last_updated"),
        F.explode(F.col("category_item.coding")).alias("coding_item"),
        F.col("category_item.text").alias("category_text")
    ).select(
        F.col("condition_id"),
        F.col("meta_last_updated"),
        F.col("coding_item.code").alias("category_code"),
        F.col("coding_item.system").alias("category_system"),
        F.lit(None).alias("category_display"),
        F.col("category_text")
    ).filter(
        F.col("category_code").isNotNull()
    )
    
    return categories_final

def transform_condition_notes(df):
    """Transform condition notes"""
    logger.info("Transforming condition notes...")
    
    # Check if note column exists
    if "note" not in df.columns:
        logger.warning("note column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("condition_id"),
            F.lit("").alias("note_text"),
            F.lit(None).alias("note_author_reference"),
            F.lit(None).alias("note_time")
        ).filter(F.lit(False))
    
    # First explode the note array
    notes_df = df.select(
        F.col("id").alias("condition_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode(F.col("note")).alias("note_item")
    ).filter(
        F.col("note_item").isNotNull()
    )
    
    # Extract note details
    notes_final = notes_df.select(
        F.col("condition_id"),
        F.col("meta_last_updated"),
        F.col("note_item.text").alias("note_text"),
        F.col("note_item.authorReference").alias("note_author_reference"),
        F.to_timestamp(F.col("note_item.time"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("note_time")
    ).filter(
        F.col("note_text").isNotNull()
    )
    
    return notes_final

def transform_condition_body_sites(df):
    """Transform condition body sites"""
    logger.info("Transforming condition body sites...")
    
    # Check if bodySite column exists
    if "bodySite" not in df.columns:
        logger.warning("bodysite column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("condition_id"),
            F.lit("").alias("body_site_code"),
            F.lit("").alias("body_site_system"),
            F.lit("").alias("body_site_display"),
            F.lit("").alias("body_site_text")
        ).filter(F.lit(False))
    
    # First explode the bodySite array
    body_sites_df = df.select(
        F.col("id").alias("condition_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode(F.col("bodySite")).alias("body_site_item")
    ).filter(
        F.col("body_site_item").isNotNull()
    )
    
    # Extract body site details and explode the coding array
    body_sites_final = body_sites_df.select(
        F.col("condition_id"),
        F.col("meta_last_updated"),
        F.explode(F.col("body_site_item.coding")).alias("coding_item"),
        F.col("body_site_item.text").alias("body_site_text")
    ).select(
        F.col("condition_id"),
        F.col("meta_last_updated"),
        F.col("coding_item.code").alias("body_site_code"),
        F.col("coding_item.system").alias("body_site_system"),
        F.lit(None).alias("body_site_display"),
        F.col("body_site_text")
    ).filter(
        F.col("body_site_code").isNotNull()
    )
    
    return body_sites_final

def transform_condition_stages(df):
    """Transform condition stages"""
    logger.info("Transforming condition stages...")
    
    # Check if stage column exists
    if "stage" not in df.columns:
        logger.warning("stage column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("condition_id"),
            F.lit("").alias("stage_summary_code"),
            F.lit("").alias("stage_summary_system"),
            F.lit("").alias("stage_summary_display"),
            F.lit("").alias("stage_assessment_code"),
            F.lit("").alias("stage_assessment_system"),
            F.lit("").alias("stage_assessment_display"),
            F.lit("").alias("stage_type_code"),
            F.lit("").alias("stage_type_system"),
            F.lit("").alias("stage_type_display")
        ).filter(F.lit(False))
    
    # First explode the stage array
    stages_df = df.select(
        F.col("id").alias("condition_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode(F.col("stage")).alias("stage_item")
    ).filter(
        F.col("stage_item").isNotNull()
    )
    
    # Extract stage details - handle cases where fields might be References instead of CodeableConcepts
    stages_final = stages_df.select(
        F.col("condition_id"),
        F.col("meta_last_updated"),
        F.lit(None).alias("stage_summary_code"),
        F.lit(None).alias("stage_summary_system"),
        F.lit(None).alias("stage_summary_display"),
        F.when(F.col("stage_item.assessment").isNotNull(),
               F.col("stage_item.assessment").getField("reference")).otherwise(None).alias("stage_assessment_code"),
        F.lit(None).alias("stage_assessment_system"),
        F.lit(None).alias("stage_assessment_display"),
        F.lit(None).alias("stage_type_code"),
        F.lit(None).alias("stage_type_system"),
        F.lit(None).alias("stage_type_display")
    ).filter(
        F.col("stage_assessment_code").isNotNull()
    )
    
    return stages_final

def parse_code_string(code_str):
    """Parse string representation of code object to extract coding array"""
    if code_str is None or code_str == '' or code_str == 'null':
        return None

    try:
        # Handle case where it's already a proper dict/struct
        if isinstance(code_str, dict):
            return code_str

        # Parse string representation
        if not isinstance(code_str, str):
            return None

        # Extract the text field first (if present)
        text_match = re.search(r'text=([^,}]+?)(?:,|$|})', code_str)
        text_value = text_match.group(1).strip() if text_match else None
        if text_value == 'null' or text_value == '_text=null':
            text_value = None

        # Extract all coding entries
        codings = []

        # First try to extract the coding array content
        # Look for coding=[ ... ] pattern
        coding_array_match = re.search(r'coding=\[(.*?)\](?:,\s*text=|$)', code_str, re.DOTALL)

        if coding_array_match:
            coding_array_str = coding_array_match.group(1)

            # Split by '}, {' to separate individual coding objects
            # This handles the case where there are multiple coding objects in the array
            coding_parts = re.split(r'\},\s*\{', coding_array_str)

            for part in coding_parts:
                # Clean up the part (remove leading/trailing braces if present)
                part = part.strip().strip('{}')

                # Extract system, code, and display from each part
                system_match = re.search(r'system=([^,}]+)', part)
                code_match = re.search(r'code=([^,}]+)', part)
                display_match = re.search(r'display=([^,}]+)', part)

                system = system_match.group(1).strip() if system_match else None
                code = code_match.group(1).strip() if code_match else None
                display = display_match.group(1).strip() if display_match else None

                # Clean up values and add to codings list
                if system and system != 'null' and code and code != 'null':
                    # Remove any quotes or extra characters
                    system = system.strip(',').strip()
                    code = code.strip(',').strip()
                    display = display.strip(',').strip() if display and display != 'null' else None

                    codings.append({
                        'system': system,
                        'code': code,
                        'display': display
                    })
        else:
            # Fallback to original logic if array pattern not found
            # Split by '{' to find individual coding entries
            parts = code_str.split('{')
            for part in parts:
                if 'system=' in part and 'code=' in part:
                    # Extract system
                    system_match = re.search(r'system=([^,}]+)', part)
                    system = system_match.group(1) if system_match else None

                    # Extract code
                    code_match = re.search(r'code=([^,}]+)', part)
                    code = code_match.group(1) if code_match else None

                    # Extract display
                    display_match = re.search(r'display=([^,}]+)', part)
                    display = display_match.group(1) if display_match else None

                    # Clean up values
                    if system and system != 'null':
                        if code and code != 'null':
                            # Remove any trailing commas or special chars
                            code = code.strip().rstrip(',')
                            system = system.strip().rstrip(',')
                            if display and display != 'null':
                                display = display.strip().rstrip(',')
                            else:
                                display = None

                            codings.append({
                                'system': system,
                                'code': code,
                                'display': display
                            })

        # Debug logging
        if len(codings) > 1:
            logger.debug(f"Parsed {len(codings)} coding entries from code string")

        if codings:
            return {
                'text': text_value,
                'coding': codings
            }
        else:
            return None

    except Exception as e:
        logger.warning(f"Failed to parse code string: {str(e)[:100]}")
        return None

# Create UDF for parsing code strings
parse_code_string_udf = F.udf(parse_code_string, StructType([
    StructField("text", StringType(), True),
    StructField("coding", ArrayType(
        StructType([
            StructField("system", StringType(), True),
            StructField("code", StringType(), True),
            StructField("display", StringType(), True)
        ])
    ), True)
]))

def transform_condition_codes(df):
    """Transform condition codes (all codes from code.coding array)"""
    logger.info("Transforming condition codes...")

    # Check if code column exists
    if "code" not in df.columns:
        logger.warning("code column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("condition_id"),
            F.lit("").alias("code_code"),
            F.lit("").alias("code_system"),
            F.lit("").alias("code_display"),
            F.lit("").alias("code_text")
        ).filter(F.lit(False))

    # Check the data type of the code column
    code_dtype = str(df.schema["code"].dataType)
    logger.info(f"Code column data type: {code_dtype}")

    # If code column is StringType, we need to parse it
    # Check if it starts with StringType (not just contains it, to avoid matching StructType)
    if code_dtype.startswith("StringType"):
        logger.info("Code column is StringType - using string parsing logic")

        # Parse the string representation into structured data
        parsed_df = df.select(
            F.col("id").alias("condition_id"),
            F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
            parse_code_string_udf(F.col("code")).alias("parsed_code")
        ).filter(
            F.col("parsed_code").isNotNull()
        )

        # Debug: Check how many coding elements are in the parsed array
        logger.info("Checking parsed code array sizes...")
        parsed_df.select(
            F.col("condition_id"),
            F.size(F.col("parsed_code.coding")).alias("coding_array_size")
        ).filter(F.col("coding_array_size") > 1).show(5, truncate=False)

        # Extract text and explode the coding array - this should create one row per code
        codes_df = parsed_df.select(
            F.col("condition_id"),
            F.col("meta_last_updated"),
            F.col("parsed_code.text").alias("code_text"),
            F.explode(F.col("parsed_code.coding")).alias("coding_item")
        ).filter(
            F.col("coding_item").isNotNull()
        )

        # Extract code details from parsed data
        codes_final = codes_df.select(
            F.col("condition_id"),
            F.col("meta_last_updated"),
            F.col("coding_item.code").alias("code_code"),
            F.col("coding_item.system").alias("code_system"),
            F.col("coding_item.display").alias("code_display"),
            F.col("code_text")
        ).filter(
            F.col("code_code").isNotNull()
        )

        # Debug: Check distribution of codes per condition
        logger.info("Codes per condition distribution:")
        codes_per_condition = codes_final.groupBy("condition_id").agg(
            F.count("*").alias("num_codes"),
            F.collect_list("code_system").alias("systems")
        )
        codes_per_condition.filter(F.col("num_codes") > 1).show(5, truncate=False)

    else:
        # Original logic for structured data
        logger.info("Code column is structured - using original logic")

        # First explode the code.coding array - this should create one row per code
        codes_df = df.select(
            F.col("id").alias("condition_id"),
            F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
            F.col("code").getField("text").alias("code_text"),
            F.explode(F.col("code").getField("coding")).alias("coding_item")
        ).filter(
            F.col("coding_item").isNotNull()
        )

        # Extract code details
        codes_final = codes_df.select(
            F.col("condition_id"),
            F.col("meta_last_updated"),
            F.col("coding_item.code").alias("code_code"),
            F.col("coding_item.system").alias("code_system"),
            F.col("coding_item.display").alias("code_display"),
            F.col("code_text")
        ).filter(
            F.col("code_code").isNotNull()
        )

    return codes_final

def transform_condition_evidence(df):
    """Transform condition evidence"""
    logger.info("Transforming condition evidence...")
    
    # Check if evidence column exists
    if "evidence" not in df.columns:
        logger.warning("evidence column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("condition_id"),
            F.lit("").alias("evidence_code"),
            F.lit("").alias("evidence_system"),
            F.lit("").alias("evidence_display"),
            F.lit("").alias("evidence_detail_reference")
        ).filter(F.lit(False))
    
    # First explode the evidence array
    evidence_df = df.select(
        F.col("id").alias("condition_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode(F.col("evidence")).alias("evidence_item")
    ).filter(
        F.col("evidence_item").isNotNull()
    )
    
    # Extract evidence details and explode the coding array
    evidence_final = evidence_df.select(
        F.col("condition_id"),
        F.col("meta_last_updated"),
        F.explode(F.col("evidence_item.code.coding")).alias("coding_item"),
        F.col("evidence_item.detail").alias("evidence_detail")
    ).select(
        F.col("condition_id"),
        F.col("meta_last_updated"),
        F.col("coding_item.code").alias("evidence_code"),
        F.col("coding_item.system").alias("evidence_system"),
        F.lit(None).alias("evidence_display"),
        F.lit(None).alias("evidence_detail_reference")
    ).filter(
        F.col("evidence_code").isNotNull()
    )
    
    return evidence_final

def transform_condition_extensions(df):
    """Transform condition extensions into normalized structure"""
    logger.info("Transforming condition extensions...")

    # Check if extension column exists
    if "extension" not in df.columns:
        logger.warning("extension column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("condition_id"),
            F.lit(None).alias("extension_url"),
            F.lit("simple").alias("extension_type"),
            F.lit("unknown").alias("value_type"),
            F.lit(None).alias("value_string"),
            F.lit(None).alias("value_datetime"),
            F.lit(None).alias("value_reference"),
            F.lit(None).alias("value_code"),
            F.lit(None).cast(BooleanType()).alias("value_boolean"),
            F.lit(None).cast(DecimalType(18,6)).alias("value_decimal"),
            F.lit(None).cast("integer").alias("value_integer"),
            F.lit(None).alias("parent_extension_url"),
            F.lit(0).alias("extension_order"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at")
        ).filter(F.lit(False))

    # Check the data type of the extension column
    ext_dtype = str(df.schema["extension"].dataType)
    logger.info(f"Extension column data type: {ext_dtype}")

    # For StringType extension data, return empty for now
    # The extension data is typically JSON-formatted but parsing it would require
    # complex JSON parsing which may have performance implications
    if ext_dtype.startswith("StringType") or "StringType" in ext_dtype:
        logger.warning("Extension column is StringType - returning empty DataFrame")
        logger.warning("Extension parsing from string representation is not yet implemented")
        return df.select(
            F.col("id").alias("condition_id"),
            F.lit(None).alias("extension_url"),
            F.lit("simple").alias("extension_type"),
            F.lit("unknown").alias("value_type"),
            F.lit(None).alias("value_string"),
            F.lit(None).alias("value_datetime"),
            F.lit(None).alias("value_reference"),
            F.lit(None).alias("value_code"),
            F.lit(None).cast(BooleanType()).alias("value_boolean"),
            F.lit(None).cast(DecimalType(18,6)).alias("value_decimal"),
            F.lit(None).cast("integer").alias("value_integer"),
            F.lit(None).alias("parent_extension_url"),
            F.lit(0).alias("extension_order"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at")
        ).filter(F.lit(False))

    try:
        # Explode the extension array
        extensions_df = df.select(
            F.col("id").alias("condition_id"),
            F.explode(F.col("extension")).alias("ext")
        ).filter(
            F.col("ext").isNotNull()
        )

        # Extract extension details
        # Handle both simple extensions and nested extensions
        extensions_final = extensions_df.select(
            F.col("condition_id"),
            F.col("ext.url").alias("extension_url"),
            F.when(F.col("ext.extension").isNotNull(), F.lit("nested")).otherwise(F.lit("simple")).alias("extension_type"),
            # Determine value type based on which value field is present
            F.when(F.col("ext.valueString").isNotNull(), F.lit("string"))
             .when(F.col("ext.valueDateTime").isNotNull(), F.lit("dateTime"))
             .when(F.col("ext.valueReference").isNotNull(), F.lit("reference"))
             .when(F.col("ext.valueCode").isNotNull(), F.lit("code"))
             .when(F.col("ext.valueBoolean").isNotNull(), F.lit("boolean"))
             .when(F.col("ext.valueDecimal").isNotNull(), F.lit("decimal"))
             .when(F.col("ext.valueInteger").isNotNull(), F.lit("integer"))
             .otherwise(F.lit("unknown")).alias("value_type"),
            F.col("ext.valueString").alias("value_string"),
            F.col("ext.valueDateTime").alias("value_datetime"),
            # Handle valueReference which might be a struct with a reference field
            F.when(F.col("ext.valueReference").isNotNull(),
                   F.col("ext.valueReference.reference")).otherwise(None).alias("value_reference"),
            F.col("ext.valueCode").alias("value_code"),
            F.col("ext.valueBoolean").alias("value_boolean"),
            F.col("ext.valueDecimal").alias("value_decimal"),
            F.col("ext.valueInteger").alias("value_integer"),
            F.lit(None).alias("parent_extension_url"),
            F.lit(0).alias("extension_order"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at")
        )

        return extensions_final

    except Exception as e:
        logger.warning(f"Failed to transform extensions: {str(e)}")
        logger.warning("Returning empty extensions DataFrame")
        return df.select(
            F.col("id").alias("condition_id"),
            F.lit(None).alias("extension_url"),
            F.lit("simple").alias("extension_type"),
            F.lit("unknown").alias("value_type"),
            F.lit(None).alias("value_string"),
            F.lit(None).alias("value_datetime"),
            F.lit(None).alias("value_reference"),
            F.lit(None).alias("value_code"),
            F.lit(None).cast(BooleanType()).alias("value_boolean"),
            F.lit(None).cast(DecimalType(18,6)).alias("value_decimal"),
            F.lit(None).cast("integer").alias("value_integer"),
            F.lit(None).alias("parent_extension_url"),
            F.lit(0).alias("extension_order"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at")
        ).filter(F.lit(False))

def create_redshift_tables_sql():
    """Generate SQL for creating main conditions table in Redshift with proper syntax"""
    return """
    -- Main conditions table
    CREATE TABLE IF NOT EXISTS public.conditions (
        condition_id VARCHAR(255) PRIMARY KEY,
        patient_id VARCHAR(255) NOT NULL,
        encounter_id VARCHAR(255),
        clinical_status_code VARCHAR(50),
        clinical_status_display VARCHAR(255),
        clinical_status_system VARCHAR(255),
        verification_status_code VARCHAR(50),
        verification_status_display VARCHAR(255),
        verification_status_system VARCHAR(255),
        condition_text VARCHAR(500),
        severity_code VARCHAR(50),
        severity_display VARCHAR(255),
        severity_system VARCHAR(255),
        onset_datetime TIMESTAMP,
        onset_age_value DECIMAL(10,2),
        onset_age_unit VARCHAR(20),
        onset_period_start TIMESTAMP,
        onset_period_end TIMESTAMP,
        onset_text VARCHAR(500),
        abatement_datetime TIMESTAMP,
        abatement_age_value DECIMAL(10,2),
        abatement_age_unit VARCHAR(20),
        abatement_period_start TIMESTAMP,
        abatement_period_end TIMESTAMP,
        abatement_text VARCHAR(500),
        abatement_boolean BOOLEAN,
        recorded_date TIMESTAMP,
        recorder_type VARCHAR(50),
        recorder_id VARCHAR(255),
        asserter_type VARCHAR(50),
        asserter_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        meta_source VARCHAR(255),
        meta_profile VARCHAR(MAX),
        meta_security VARCHAR(MAX),
        meta_tag VARCHAR(MAX),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, recorded_date);
    """

def create_condition_categories_table_sql():
    """Generate SQL for creating condition_categories table"""
    return """
    CREATE TABLE IF NOT EXISTS public.condition_categories (
        condition_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        category_code VARCHAR(50),
        category_system VARCHAR(255),
        category_display VARCHAR(255),
        category_text VARCHAR(500)
    ) SORTKEY (condition_id, category_code)
    """

def create_condition_notes_table_sql():
    """Generate SQL for creating condition_notes table"""
    return """
    CREATE TABLE IF NOT EXISTS public.condition_notes (
        condition_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        note_text VARCHAR(MAX),
        note_author_reference VARCHAR(255),
        note_time TIMESTAMP
    ) SORTKEY (condition_id, note_time)
    """

def create_condition_body_sites_table_sql():
    """Generate SQL for creating condition_body_sites table"""
    return """
    CREATE TABLE IF NOT EXISTS public.condition_body_sites (
        condition_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        body_site_code VARCHAR(50),
        body_site_system VARCHAR(255),
        body_site_display VARCHAR(255),
        body_site_text VARCHAR(500)
    ) SORTKEY (condition_id, body_site_code)
    """

def create_condition_stages_table_sql():
    """Generate SQL for creating condition_stages table"""
    return """
    CREATE TABLE IF NOT EXISTS public.condition_stages (
        condition_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        stage_summary_code VARCHAR(50),
        stage_summary_system VARCHAR(255),
        stage_summary_display VARCHAR(255),
        stage_assessment_code VARCHAR(50),
        stage_assessment_system VARCHAR(255),
        stage_assessment_display VARCHAR(255),
        stage_type_code VARCHAR(50),
        stage_type_system VARCHAR(255),
        stage_type_display VARCHAR(255)
    ) SORTKEY (condition_id, stage_summary_code)
    """

def create_condition_codes_table_sql():
    """Generate SQL for creating condition_codes table"""
    return """
    CREATE TABLE IF NOT EXISTS public.condition_codes (
        condition_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        code_code VARCHAR(50),
        code_system VARCHAR(255),
        code_display VARCHAR(255),
        code_text VARCHAR(500)
    ) SORTKEY (condition_id, code_system, code_code)
    """

def create_condition_evidence_table_sql():
    """Generate SQL for creating condition_evidence table"""
    return """
    CREATE TABLE IF NOT EXISTS public.condition_evidence (
        condition_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        evidence_code VARCHAR(50),
        evidence_system VARCHAR(255),
        evidence_display VARCHAR(255),
        evidence_detail_reference VARCHAR(255)
    ) SORTKEY (condition_id, evidence_code)
    """

def create_condition_extensions_table_sql():
    """Generate SQL for creating condition_extensions table"""
    return """
    CREATE TABLE IF NOT EXISTS public.condition_extensions (
        condition_id VARCHAR(255) NOT NULL,
        extension_url VARCHAR(500) NOT NULL,
        extension_type VARCHAR(50) NOT NULL,
        value_type VARCHAR(50),
        value_string VARCHAR(MAX),
        value_datetime TIMESTAMP,
        value_reference VARCHAR(255),
        value_code VARCHAR(100),
        value_boolean BOOLEAN,
        value_decimal DECIMAL(18,6),
        value_integer INTEGER,
        parent_extension_url VARCHAR(500),
        extension_order INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) SORTKEY (condition_id, extension_url, extension_order)
    """



def main():
    """Main ETL process"""
    start_time = datetime.now()
    try:
        logger.info("=" * 80)
        logger.info("🚀 STARTING ENHANCED FHIR CONDITION ETL PROCESS")
        logger.info("=" * 80)
        logger.info(f"⏰ Job started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📊 Source: {DATABASE_NAME}.{TABLE_NAME}")
        logger.info(f"🎯 Target: Redshift (7 tables)")
        logger.info("📋 Reading all available columns from Glue Catalog")
        logger.info("🔄 Process: 7 steps (Read → Transform → Convert → Resolve → Validate → Write)")
        
        # Step 1: Read data from S3 using Iceberg catalog
        logger.info("\n" + "=" * 50)
        logger.info("📥 STEP 1: READING DATA FROM GLUE CATALOG")
        logger.info("=" * 50)
        logger.info(f"Database: {DATABASE_NAME}")
        logger.info(f"Table: {TABLE_NAME}")
        logger.info("Reading all available columns from Glue Catalog")
        
        # Use the AWS Glue Data Catalog to read condition data (all columns)
        # Use Iceberg to read data from S3
        table_name_full = f"{catalog_nm}.{DATABASE_NAME}.{TABLE_NAME}"
        logger.info(f"Reading from table: {table_name_full}")
        df_raw = spark.table(table_name_full)

        # Convert to DataFrame first to check available columns
        condition_df_raw = df_raw

        # TESTING MODE: Sample data for quick testing

        # Set to True to process only a sample of records

        USE_SAMPLE = False  # Set to True for testing with limited data

        SAMPLE_SIZE = 1000

        

        if USE_SAMPLE:

            logger.info(f"⚠️  TESTING MODE: Sampling {SAMPLE_SIZE} records for quick testing")

            logger.info("⚠️  Set USE_SAMPLE = False for production runs")

            condition_df = condition_df_raw.limit(SAMPLE_SIZE)

        else:

            logger.info("✅ Processing full dataset")

            condition_df = condition_df_raw

        available_columns = condition_df_raw.columns
        logger.info(f"📋 Available columns in source: {available_columns}")
        
        # Use all available columns
        logger.info(f"✅ Using all {len(available_columns)} available columns")
        condition_df = condition_df_raw
        
        logger.info("✅ Successfully read data using AWS Glue Data Catalog")
        
        total_records = condition_df.count()
        logger.info(f"📊 Read {total_records:,} raw condition records")
        
        # Debug: Show sample of raw data and schema
        if total_records > 0:
            logger.info("\n🔍 DATA QUALITY CHECKS:")
            logger.info("Sample of raw condition data:")
            condition_df.show(3, truncate=False)
            logger.info("Raw data schema:")
            condition_df.printSchema()
            
            # Check for NULL values in key fields
            null_checks = {
                "id": condition_df.filter(F.col("id").isNull()).count(),
                "subject.reference": condition_df.filter(F.col("subject").isNull() | F.col("subject.reference").isNull()).count(),
                "code": condition_df.filter(F.col("code").isNull()).count(),
                "clinicalStatus": condition_df.filter(F.col("clinicalStatus").isNull()).count()
            }
            
            logger.info("NULL value analysis in key fields:")
            for field, null_count in null_checks.items():
                percentage = (null_count / total_records) * 100 if total_records > 0 else 0
                logger.info(f"  {field}: {null_count:,} NULLs ({percentage:.1f}%)")
        else:
            logger.error("❌ No raw data found! Check the data source.")
            return
        
        # Step 2: Transform main condition data
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 2: TRANSFORMING MAIN CONDITION DATA")
        logger.info("=" * 50)
        
        main_condition_df = transform_main_condition_data(condition_df)
        main_count = main_condition_df.count()
        logger.info(f"✅ Transformed {main_count:,} main condition records")
        
        # Check if extensions column accidentally exists in main DataFrame
        if 'extensions' in main_condition_df.columns:
            logger.warning("⚠️ Found 'extensions' column in main condition DataFrame, dropping it...")
            main_condition_df = main_condition_df.drop('extensions')
            logger.info(f"✅ Dropped extensions column from main DataFrame. Remaining columns: {len(main_condition_df.columns)}")
        
        if main_count == 0:
            logger.error("❌ No main condition records after transformation! Check filtering criteria.")
            return
        
        # Debug: Show actual DataFrame schema
        logger.info("🔍 Main condition DataFrame schema:")
        logger.info(f"Columns: {main_condition_df.columns}")
        main_condition_df.printSchema()
        
        # Debug: Show sample of transformed main data
        logger.info("Sample of transformed main condition data:")
        main_condition_df.show(3, truncate=False)
        
        # Step 3: Transform multi-valued data (all supporting tables)
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 3: TRANSFORMING MULTI-VALUED DATA")
        logger.info("=" * 50)
        
        condition_categories_df = transform_condition_categories(condition_df)
        categories_count = condition_categories_df.count()
        logger.info(f"✅ Transformed {categories_count:,} condition category records")
        
        condition_notes_df = transform_condition_notes(condition_df)
        notes_count = condition_notes_df.count()
        logger.info(f"✅ Transformed {notes_count:,} condition note records")
        
        condition_body_sites_df = transform_condition_body_sites(condition_df)
        body_sites_count = condition_body_sites_df.count()
        logger.info(f"✅ Transformed {body_sites_count:,} condition body site records")
        
        condition_stages_df = transform_condition_stages(condition_df)
        stages_count = condition_stages_df.count()
        logger.info(f"✅ Transformed {stages_count:,} condition stage records")
        
        condition_codes_df = transform_condition_codes(condition_df)
        codes_count = condition_codes_df.count()
        logger.info(f"✅ Transformed {codes_count:,} condition code records")
        
        condition_evidence_df = transform_condition_evidence(condition_df)
        evidence_count = condition_evidence_df.count()
        logger.info(f"✅ Transformed {evidence_count:,} condition evidence records")
        
        condition_extensions_df = transform_condition_extensions(condition_df)
        extensions_count = condition_extensions_df.count()
        logger.info(f"✅ Transformed {extensions_count:,} condition extension records")
        
        # Debug: Show samples of multi-valued data if available
        if categories_count > 0:
            logger.info("Sample of condition categories data:")
            condition_categories_df.show(3, truncate=False)
        
        if notes_count > 0:
            logger.info("Sample of condition notes data:")
            condition_notes_df.show(3, truncate=False)
        
        if body_sites_count > 0:
            logger.info("Sample of condition body sites data:")
            condition_body_sites_df.show(3, truncate=False)
        
        if stages_count > 0:
            logger.info("Sample of condition stages data:")
            condition_stages_df.show(3, truncate=False)
        
        if codes_count > 0:
            logger.info("Sample of condition codes data:")
            condition_codes_df.show(3, truncate=False)
        
        if evidence_count > 0:
            logger.info("Sample of condition evidence data:")
            condition_evidence_df.show(3, truncate=False)
        
        if extensions_count > 0:
            logger.info("Sample of condition extensions data:")
            condition_extensions_df.show(3, truncate=False)
        
        # Step 4: Convert to DynamicFrames and ensure data is flat for Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 4: CONVERTING TO DYNAMICFRAMES")
        logger.info("=" * 50)
        logger.info("Converting to DynamicFrames and ensuring Redshift compatibility...")
        
        # Debug: Show what columns are available in main_condition_df
        logger.info("🔍 Available columns in main_condition_df:")
        logger.info(f"Columns: {main_condition_df.columns}")
        
        # Convert main conditions DataFrame and ensure flat structure
        main_flat_df = main_condition_df.select(
            F.col("condition_id").cast(StringType()).alias("condition_id"),
            F.col("patient_id").cast(StringType()).alias("patient_id"),
            F.col("encounter_id").cast(StringType()).alias("encounter_id"),
            # F.col("resourcetype").cast(StringType()).alias("resourcetype"),  # Removed - not in Redshift table
            F.col("clinical_status_code").cast(StringType()).alias("clinical_status_code"),
            F.col("clinical_status_display").cast(StringType()).alias("clinical_status_display"),
            F.col("clinical_status_system").cast(StringType()).alias("clinical_status_system"),
            F.col("verification_status_code").cast(StringType()).alias("verification_status_code"),
            F.col("verification_status_display").cast(StringType()).alias("verification_status_display"),
            F.col("verification_status_system").cast(StringType()).alias("verification_status_system"),
            F.col("condition_text").cast(StringType()).alias("condition_text"),
            F.col("severity_code").cast(StringType()).alias("severity_code"),
            F.col("severity_display").cast(StringType()).alias("severity_display"),
            F.col("severity_system").cast(StringType()).alias("severity_system"),
            F.col("onset_datetime").cast(TimestampType()).alias("onset_datetime"),
            F.col("onset_age_value").cast(DecimalType(10,2)).alias("onset_age_value"),
            F.col("onset_age_unit").cast(StringType()).alias("onset_age_unit"),
            F.col("onset_period_start").cast(TimestampType()).alias("onset_period_start"),
            F.col("onset_period_end").cast(TimestampType()).alias("onset_period_end"),
            F.col("onset_text").cast(StringType()).alias("onset_text"),
            F.col("abatement_datetime").cast(TimestampType()).alias("abatement_datetime"),
            F.col("abatement_age_value").cast(DecimalType(10,2)).alias("abatement_age_value"),
            F.col("abatement_age_unit").cast(StringType()).alias("abatement_age_unit"),
            F.col("abatement_period_start").cast(TimestampType()).alias("abatement_period_start"),
            F.col("abatement_period_end").cast(TimestampType()).alias("abatement_period_end"),
            F.col("abatement_text").cast(StringType()).alias("abatement_text"),
            F.col("abatement_boolean").cast(BooleanType()).alias("abatement_boolean"),
            F.col("recorded_date").cast(TimestampType()).alias("recorded_date"),
            F.col("recorder_type").cast(StringType()).alias("recorder_type"),
            F.col("recorder_id").cast(StringType()).alias("recorder_id"),
            F.col("asserter_type").cast(StringType()).alias("asserter_type"),
            F.col("asserter_id").cast(StringType()).alias("asserter_id"),
            F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
            F.col("meta_source").cast(StringType()).alias("meta_source"),
            F.col("meta_profile").cast(StringType()).alias("meta_profile"),
            F.col("meta_security").cast(StringType()).alias("meta_security"),
            F.col("meta_tag").cast(StringType()).alias("meta_tag"),
            F.col("created_at").cast(TimestampType()).alias("created_at"),
            F.col("updated_at").cast(TimestampType()).alias("updated_at")
        )
        
        # Debug: Show what columns are in the final main_flat_df
        logger.info("🔍 Final main_flat_df columns:")
        logger.info(f"Columns: {main_flat_df.columns}")
        main_flat_df.printSchema()
        
        # Explicitly drop resourcetype column if it exists
        if "resourcetype" in main_flat_df.columns:
            logger.info("⚠️ Found resourcetype column - dropping it explicitly")
            main_flat_df = main_flat_df.drop("resourcetype")
            logger.info(f"✅ After dropping resourcetype - Columns: {main_flat_df.columns}")
        
        # Also check and drop resourcetype from the original DataFrame if it exists
        if "resourcetype" in main_condition_df.columns:
            logger.info("⚠️ Found resourcetype column in main_condition_df - dropping it explicitly")
            main_condition_df = main_condition_df.drop("resourcetype")
            logger.info(f"✅ After dropping resourcetype from main_condition_df - Columns: {main_condition_df.columns}")
        
        # Final validation: Ensure no unexpected columns that don't exist in Redshift table
        expected_columns = {
            "condition_id", "patient_id", "encounter_id", "clinical_status_code", "clinical_status_display",
            "clinical_status_system", "verification_status_code", "verification_status_display",
            "verification_status_system", "condition_text", "severity_code", "severity_display",
            "severity_system", "onset_datetime", "onset_age_value", "onset_age_unit", "onset_period_start",
            "onset_period_end", "onset_text", "abatement_datetime", "abatement_age_value",
            "abatement_age_unit", "abatement_period_start", "abatement_period_end", "abatement_text",
            "abatement_boolean", "recorded_date", "recorder_type", "recorder_id", "asserter_type",
            "asserter_id", "meta_last_updated", "meta_source", "meta_profile",
            "meta_security", "meta_tag", "created_at", "updated_at"
        }
        
        actual_columns = set(main_flat_df.columns)
        unexpected_columns = actual_columns - expected_columns
        
        logger.info(f"🔍 Column validation - Expected: {len(expected_columns)} columns")
        logger.info(f"🔍 Column validation - Actual: {len(actual_columns)} columns")
        logger.info(f"🔍 Column validation - Actual columns: {sorted(actual_columns)}")
        
        if unexpected_columns:
            logger.warning(f"⚠️ Found unexpected columns that don't exist in Redshift table: {unexpected_columns}")
            logger.info("Dropping unexpected columns...")
            for col in unexpected_columns:
                main_flat_df = main_flat_df.drop(col)
            logger.info(f"✅ After dropping unexpected columns - Columns: {main_flat_df.columns}")
        
        missing_columns = expected_columns - actual_columns
        if missing_columns:
            logger.warning(f"⚠️ Missing expected columns: {missing_columns}")
            # Add missing columns with null values
            for col in missing_columns:
                main_flat_df = main_flat_df.withColumn(col, F.lit(None))
            logger.info(f"✅ Added missing columns - Columns: {main_flat_df.columns}")
        
        # Final check after all modifications
        final_columns = set(main_flat_df.columns)
        logger.info(f"🔍 Final DataFrame columns after validation: {sorted(final_columns)}")
        logger.info(f"🔍 Final DataFrame column count: {len(final_columns)}")
        
        main_dynamic_frame = DynamicFrame.fromDF(main_flat_df, glueContext, "main_condition_dynamic_frame")
        
        # Convert other DataFrames with type casting
        categories_flat_df = condition_categories_df.select(
            F.col("condition_id").cast(StringType()).alias("condition_id"),
            F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
            F.col("category_code").cast(StringType()).alias("category_code"),
            F.col("category_system").cast(StringType()).alias("category_system"),
            F.col("category_display").cast(StringType()).alias("category_display"),
            F.col("category_text").cast(StringType()).alias("category_text")
        )
        categories_dynamic_frame = DynamicFrame.fromDF(categories_flat_df, glueContext, "categories_dynamic_frame")
        
        notes_flat_df = condition_notes_df.select(
            F.col("condition_id").cast(StringType()).alias("condition_id"),
            F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
            F.col("note_text").cast(StringType()).alias("note_text"),
            F.col("note_author_reference").cast(StringType()).alias("note_author_reference"),
            F.col("note_time").cast(TimestampType()).alias("note_time")
        )
        notes_dynamic_frame = DynamicFrame.fromDF(notes_flat_df, glueContext, "notes_dynamic_frame")
        
        body_sites_flat_df = condition_body_sites_df.select(
            F.col("condition_id").cast(StringType()).alias("condition_id"),
            F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
            F.col("body_site_code").cast(StringType()).alias("body_site_code"),
            F.col("body_site_system").cast(StringType()).alias("body_site_system"),
            F.col("body_site_display").cast(StringType()).alias("body_site_display"),
            F.col("body_site_text").cast(StringType()).alias("body_site_text")
        )
        body_sites_dynamic_frame = DynamicFrame.fromDF(body_sites_flat_df, glueContext, "body_sites_dynamic_frame")
        
        stages_flat_df = condition_stages_df.select(
            F.col("condition_id").cast(StringType()).alias("condition_id"),
            F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
            F.col("stage_summary_code").cast(StringType()).alias("stage_summary_code"),
            F.col("stage_summary_system").cast(StringType()).alias("stage_summary_system"),
            F.col("stage_summary_display").cast(StringType()).alias("stage_summary_display"),
            F.col("stage_assessment_code").cast(StringType()).alias("stage_assessment_code"),
            F.col("stage_assessment_system").cast(StringType()).alias("stage_assessment_system"),
            F.col("stage_assessment_display").cast(StringType()).alias("stage_assessment_display"),
            F.col("stage_type_code").cast(StringType()).alias("stage_type_code"),
            F.col("stage_type_system").cast(StringType()).alias("stage_type_system"),
            F.col("stage_type_display").cast(StringType()).alias("stage_type_display")
        )
        stages_dynamic_frame = DynamicFrame.fromDF(stages_flat_df, glueContext, "stages_dynamic_frame")
        
        evidence_flat_df = condition_evidence_df.select(
            F.col("condition_id").cast(StringType()).alias("condition_id"),
            F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
            F.col("evidence_code").cast(StringType()).alias("evidence_code"),
            F.col("evidence_system").cast(StringType()).alias("evidence_system"),
            F.col("evidence_display").cast(StringType()).alias("evidence_display"),
            F.col("evidence_detail_reference").cast(StringType()).alias("evidence_detail_reference")
        )
        codes_flat_df = condition_codes_df.select(
            F.col("condition_id").cast(StringType()).alias("condition_id"),
            F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
            F.col("code_code").cast(StringType()).alias("code_code"),
            F.col("code_system").cast(StringType()).alias("code_system"),
            F.col("code_display").cast(StringType()).alias("code_display"),
            F.col("code_text").cast(StringType()).alias("code_text")
        )
        codes_dynamic_frame = DynamicFrame.fromDF(codes_flat_df, glueContext, "codes_dynamic_frame")
        
        evidence_dynamic_frame = DynamicFrame.fromDF(evidence_flat_df, glueContext, "evidence_dynamic_frame")
        
        extensions_flat_df = condition_extensions_df.select(
            F.col("condition_id").cast(StringType()).alias("condition_id"),
            F.col("extension_url").cast(StringType()).alias("extension_url"),
            F.col("extension_type").cast(StringType()).alias("extension_type"),
            F.col("value_type").cast(StringType()).alias("value_type"),
            F.col("value_string").cast(StringType()).alias("value_string"),
            F.col("value_datetime").cast(TimestampType()).alias("value_datetime"),
            F.col("value_reference").cast(StringType()).alias("value_reference"),
            F.col("value_code").cast(StringType()).alias("value_code"),
            F.col("value_boolean").cast(BooleanType()).alias("value_boolean"),
            F.col("value_decimal").cast(DecimalType(18,6)).alias("value_decimal"),
            F.col("value_integer").cast("integer").alias("value_integer"),
            F.col("parent_extension_url").cast(StringType()).alias("parent_extension_url"),
            F.col("extension_order").cast("integer").alias("extension_order"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at")
        )
        extensions_dynamic_frame = DynamicFrame.fromDF(extensions_flat_df, glueContext, "extensions_dynamic_frame")
        
        # Step 5: Resolve any remaining choice types to ensure Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 5: RESOLVING CHOICE TYPES")
        logger.info("=" * 50)
        logger.info("Resolving choice types for Redshift compatibility...")
        
        main_resolved_frame = main_dynamic_frame.resolveChoice(
            specs=[
                ("condition_id", "cast:string"),
                ("patient_id", "cast:string"),
                ("encounter_id", "cast:string"),
                ("clinical_status_code", "cast:string"),
                ("clinical_status_display", "cast:string"),
                ("clinical_status_system", "cast:string"),
                ("verification_status_code", "cast:string"),
                ("verification_status_display", "cast:string"),
                ("verification_status_system", "cast:string"),
                ("condition_text", "cast:string"),
                ("severity_code", "cast:string"),
                ("severity_display", "cast:string"),
                ("severity_system", "cast:string"),
                ("onset_datetime", "cast:timestamp"),
                ("onset_age_value", "cast:decimal"),
                ("onset_age_unit", "cast:string"),
                ("onset_period_start", "cast:timestamp"),
                ("onset_period_end", "cast:timestamp"),
                ("onset_text", "cast:string"),
                ("abatement_datetime", "cast:timestamp"),
                ("abatement_age_value", "cast:decimal"),
                ("abatement_age_unit", "cast:string"),
                ("abatement_period_start", "cast:timestamp"),
                ("abatement_period_end", "cast:timestamp"),
                ("abatement_text", "cast:string"),
                ("abatement_boolean", "cast:boolean"),
                ("recorded_date", "cast:timestamp"),
                ("recorder_type", "cast:string"),
                ("recorder_id", "cast:string"),
                ("asserter_type", "cast:string"),
                ("asserter_id", "cast:string"),
                ("meta_last_updated", "cast:timestamp"),
                ("meta_source", "cast:string"),
                ("meta_profile", "cast:string"),
                ("meta_security", "cast:string"),
                ("meta_tag", "cast:string"),
                ("created_at", "cast:timestamp"),
                ("updated_at", "cast:timestamp")
            ]
        )
        
        categories_resolved_frame = categories_dynamic_frame.resolveChoice(
            specs=[
                ("condition_id", "cast:string"),
                ("category_code", "cast:string"),
                ("category_system", "cast:string"),
                ("category_display", "cast:string"),
                ("category_text", "cast:string")
            ]
        )
        
        notes_resolved_frame = notes_dynamic_frame.resolveChoice(
            specs=[
                ("condition_id", "cast:string"),
                ("note_text", "cast:string"),
                ("note_author_reference", "cast:string"),
                ("note_time", "cast:timestamp")
            ]
        )
        
        body_sites_resolved_frame = body_sites_dynamic_frame.resolveChoice(
            specs=[
                ("condition_id", "cast:string"),
                ("body_site_code", "cast:string"),
                ("body_site_system", "cast:string"),
                ("body_site_display", "cast:string"),
                ("body_site_text", "cast:string")
            ]
        )
        
        stages_resolved_frame = stages_dynamic_frame.resolveChoice(
            specs=[
                ("condition_id", "cast:string"),
                ("stage_summary_code", "cast:string"),
                ("stage_summary_system", "cast:string"),
                ("stage_summary_display", "cast:string"),
                ("stage_assessment_code", "cast:string"),
                ("stage_assessment_system", "cast:string"),
                ("stage_assessment_display", "cast:string"),
                ("stage_type_code", "cast:string"),
                ("stage_type_system", "cast:string"),
                ("stage_type_display", "cast:string")
            ]
        )
        
        codes_resolved_frame = codes_dynamic_frame.resolveChoice(
            specs=[
                ("condition_id", "cast:string"),
                ("code_code", "cast:string"),
                ("code_system", "cast:string"),
                ("code_display", "cast:string"),
                ("code_text", "cast:string")
            ]
        )
        
        evidence_resolved_frame = evidence_dynamic_frame.resolveChoice(
            specs=[
                ("condition_id", "cast:string"),
                ("evidence_code", "cast:string"),
                ("evidence_system", "cast:string"),
                ("evidence_display", "cast:string"),
                ("evidence_detail_reference", "cast:string")
            ]
        )
        
        extensions_resolved_frame = extensions_dynamic_frame.resolveChoice(
            specs=[
                ("condition_id", "cast:string"),
                ("extension_url", "cast:string"),
                ("extension_type", "cast:string"),
                ("value_type", "cast:string"),
                ("value_string", "cast:string"),
                ("value_datetime", "cast:timestamp"),
                ("value_reference", "cast:string"),
                ("value_code", "cast:string"),
                ("value_boolean", "cast:boolean"),
                ("value_decimal", "cast:decimal"),
                ("value_integer", "cast:int"),
                ("parent_extension_url", "cast:string"),
                ("extension_order", "cast:int"),
                ("created_at", "cast:timestamp"),
                ("updated_at", "cast:timestamp")
            ]
        )
        
        # Step 6: Final validation before writing
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 6: FINAL VALIDATION")
        logger.info("=" * 50)
        logger.info("Performing final validation before writing to Redshift...")
        
        # Validate main conditions data
        main_final_df = main_resolved_frame.toDF()
        main_final_count = main_final_df.count()
        logger.info(f"Final main conditions count: {main_final_count}")
        
        if main_final_count == 0:
            logger.error("No main condition records to write to Redshift! Stopping the process.")
            return
        
        # Validate other tables
        categories_final_count = categories_resolved_frame.toDF().count()
        notes_final_count = notes_resolved_frame.toDF().count()
        body_sites_final_count = body_sites_resolved_frame.toDF().count()
        stages_final_count = stages_resolved_frame.toDF().count()
        codes_final_count = codes_resolved_frame.toDF().count()
        evidence_final_count = evidence_resolved_frame.toDF().count()
        extensions_final_count = extensions_resolved_frame.toDF().count()
        
        logger.info(f"Final counts - Categories: {categories_final_count}, Notes: {notes_final_count}, Body Sites: {body_sites_final_count}, Stages: {stages_final_count}, Codes: {codes_final_count}, Evidence: {evidence_final_count}, Extensions: {extensions_final_count}")
        
        # Debug: Show final sample data being written
        logger.info("Final sample data being written to Redshift (main conditions):")
        main_final_df.show(3, truncate=False)
        
        # Additional validation: Check for any remaining issues
        logger.info("🔍 Final validation before Redshift write:")
        logger.info(f"Final DataFrame columns: {main_final_df.columns}")
        logger.info(f"Final DataFrame schema:")
        main_final_df.printSchema()
        
        # Check for any null values in critical fields
        critical_field_checks = {
            "condition_id": main_final_df.filter(F.col("condition_id").isNull()).count(),
            "patient_id": main_final_df.filter(F.col("patient_id").isNull()).count(),
        }
        
        logger.info("Critical field null checks:")
        for field, null_count in critical_field_checks.items():
            logger.info(f"  {field}: {null_count} NULLs")
        
        if critical_field_checks["condition_id"] > 0:
            logger.error("❌ Found NULL condition_id values - this will cause Redshift write to fail!")
        
        if critical_field_checks["patient_id"] > 0:
            logger.error("❌ Found NULL patient_id values - this will cause Redshift write to fail!")
        
        # Extensions are now stored in separate table, no length check needed
        # Final DataFrame preparation: Ensure proper column ordering and types
        logger.info("🔧 Final DataFrame preparation for Redshift...")
        
        # Reorder columns to match Redshift table schema exactly
        column_order = [
            "condition_id", "patient_id", "encounter_id", "clinical_status_code", "clinical_status_display",
            "clinical_status_system", "verification_status_code", "verification_status_display",
            "verification_status_system", "condition_text", "severity_code", "severity_display",
            "severity_system", "onset_datetime", "onset_age_value", "onset_age_unit", "onset_period_start",
            "onset_period_end", "onset_text", "abatement_datetime", "abatement_age_value",
            "abatement_age_unit", "abatement_period_start", "abatement_period_end", "abatement_text",
            "abatement_boolean", "recorded_date", "recorder_type", "recorder_id", "asserter_type",
            "asserter_id", "meta_last_updated", "meta_source", "meta_profile",
            "meta_security", "meta_tag", "created_at", "updated_at"
        ]
        
        # Select columns in the correct order - only select columns that exist in both DataFrame and column_order
        available_columns = [col for col in column_order if col in main_final_df.columns]
        missing_columns = [col for col in column_order if col not in main_final_df.columns]
        
        if missing_columns:
            logger.warning(f"⚠️ Missing columns in DataFrame: {missing_columns}")
            # Add missing columns with null values
            for col in missing_columns:
                main_final_df = main_final_df.withColumn(col, F.lit(None))
        
        # Select only the expected columns and ensure no extra columns
        logger.info(f"🔍 Before column selection - DataFrame columns: {main_final_df.columns}")
        main_final_df = main_final_df.select(*column_order)
        logger.info(f"✅ After column selection - DataFrame columns: {main_final_df.columns}")
        logger.info(f"✅ Reordered columns to match Redshift schema: {main_final_df.columns}")
        
        # Double-check: ensure no extra columns exist
        if len(main_final_df.columns) != len(column_order):
            logger.error(f"❌ Column count mismatch! DataFrame has {len(main_final_df.columns)} columns, expected {len(column_order)}")
        
        # Verify column names match exactly
        if set(main_final_df.columns) != set(column_order):
            logger.error(f"❌ Column name mismatch!")
            logger.error(f"DataFrame: {set(main_final_df.columns)}")
            logger.error(f"Expected: {set(column_order)}")
            extra = set(main_final_df.columns) - set(column_order)
            missing = set(column_order) - set(main_final_df.columns)
            if extra:
                logger.error(f"Extra columns: {extra}")
            if missing:
                logger.error(f"Missing columns: {missing}")
        
        # Final validation before DynamicFrame conversion
        final_actual_columns = set(main_final_df.columns)
        final_unexpected = final_actual_columns - set(column_order)
        if final_unexpected:
            logger.error(f"❌ CRITICAL: Found unexpected columns after reordering: {final_unexpected}")
            logger.error("This will cause Redshift write to fail!")
        
        # Double-check: ensure we only have the exact columns we expect
        if final_actual_columns != set(column_order):
            logger.error(f"❌ CRITICAL: Column mismatch detected!")
            logger.error(f"Expected: {set(column_order)}")
            logger.error(f"Actual: {final_actual_columns}")
            logger.error("This will cause Redshift write to fail!")
        
        # Final column verification before DynamicFrame conversion
        logger.info("🔍 Final column verification before Redshift write:")
        logger.info(f"DataFrame columns: {sorted(main_final_df.columns)}")
        logger.info(f"Expected table columns: {sorted(column_order)}")
        
        # Ensure DataFrame has exactly the expected columns in the right order
        if set(main_final_df.columns) != set(column_order):
            logger.error(f"❌ Column mismatch detected!")
            logger.error(f"DataFrame has: {set(main_final_df.columns)}")
            logger.error(f"Table expects: {set(column_order)}")
            extra_cols = set(main_final_df.columns) - set(column_order)
            missing_cols = set(column_order) - set(main_final_df.columns)
            if extra_cols:
                logger.error(f"Extra columns in DataFrame: {extra_cols}")
            if missing_cols:
                logger.error(f"Missing columns in DataFrame: {missing_cols}")
        
        # Debug: Check DataFrame columns before DynamicFrame conversion
        logger.info(f"🔍 DataFrame columns before DynamicFrame conversion: {main_final_df.columns}")
        logger.info(f"🔍 DataFrame schema before DynamicFrame conversion:")
        main_final_df.printSchema()
        
        # Ensure no extensions column exists before creating DynamicFrame
        if 'extensions' in main_final_df.columns:
            logger.warning("⚠️ Found 'extensions' column in DataFrame, dropping it...")
            main_final_df = main_final_df.drop('extensions')
            logger.info(f"✅ Dropped extensions column. Remaining columns: {main_final_df.columns}")
        
        # Convert the reordered DataFrame back to DynamicFrame for Redshift write
        main_resolved_frame = DynamicFrame.fromDF(main_final_df, glueContext, "main_condition_final_dynamic_frame")
        
        # Debug: Check DynamicFrame schema after conversion
        logger.info(f"🔍 DynamicFrame schema after conversion:")
        main_resolved_frame.printSchema()
        
        # Step 7: Create tables and write to Redshift
        logger.info("\n" + "=" * 50)
        logger.info("💾 STEP 7: WRITING DATA TO REDSHIFT")
        logger.info("=" * 50)
        logger.info(f"🔗 Using connection: {REDSHIFT_CONNECTION}")
        logger.info(f"📁 S3 temp directory: {S3_TEMP_DIR}")
        
        # Create all tables individually
        # Note: Each write_to_redshift call now includes DROP and CREATE to ensure proper schema
        logger.info("📝 Dropping and recreating main conditions table...")
        conditions_table_sql = create_redshift_tables_sql()
        
        # Final debugging before write
        logger.info("🔍 Final DynamicFrame schema before Redshift write:")
        main_resolved_frame.printSchema()
        final_df = main_resolved_frame.toDF()
        logger.info(f"🔍 DynamicFrame column count: {len(final_df.columns)}")
        logger.info(f"🔍 DynamicFrame columns: {final_df.columns}")
        
        # Check for extensions column specifically
        if 'extensions' in final_df.columns:
            logger.error("❌ CRITICAL: DynamicFrame still contains 'extensions' column!")
            logger.error(f"All columns: {final_df.columns}")
            raise Exception("DynamicFrame contains extensions column that should not exist")
        else:
            logger.info("✅ Confirmed: DynamicFrame does NOT contain 'extensions' column")
        
        write_to_redshift_versioned(main_resolved_frame, "conditions", "condition_id", conditions_table_sql)
        logger.info("✅ Main conditions table dropped, recreated and written successfully")
        
        logger.info("📝 Dropping and recreating condition categories table...")
        categories_table_sql = create_condition_categories_table_sql()
        write_to_redshift_versioned(categories_resolved_frame, "condition_categories", "condition_id", categories_table_sql)
        logger.info("✅ Condition categories table dropped, recreated and written successfully")
        
        logger.info("📝 Dropping and recreating condition notes table...")
        notes_table_sql = create_condition_notes_table_sql()
        write_to_redshift_versioned(notes_resolved_frame, "condition_notes", "condition_id", notes_table_sql)
        logger.info("✅ Condition notes table dropped, recreated and written successfully")
        
        logger.info("📝 Dropping and recreating condition body sites table...")
        body_sites_table_sql = create_condition_body_sites_table_sql()
        write_to_redshift_versioned(body_sites_resolved_frame, "condition_body_sites", "condition_id", body_sites_table_sql)
        logger.info("✅ Condition body sites table dropped, recreated and written successfully")
        
        logger.info("📝 Dropping and recreating condition stages table...")
        stages_table_sql = create_condition_stages_table_sql()
        write_to_redshift_versioned(stages_resolved_frame, "condition_stages", "condition_id", stages_table_sql)
        logger.info("✅ Condition stages table dropped, recreated and written successfully")
        
        logger.info("📝 Dropping and recreating condition codes table...")
        codes_table_sql = create_condition_codes_table_sql()
        write_to_redshift_versioned(codes_resolved_frame, "condition_codes", "condition_id", codes_table_sql)
        logger.info("✅ Condition codes table dropped, recreated and written successfully")
        
        logger.info("📝 Dropping and recreating condition evidence table...")
        evidence_table_sql = create_condition_evidence_table_sql()
        write_to_redshift_versioned(evidence_resolved_frame, "condition_evidence", "condition_id", evidence_table_sql)
        logger.info("✅ Condition evidence table dropped, recreated and written successfully")
        
        logger.info("📝 Dropping and recreating condition extensions table...")
        extensions_table_sql = create_condition_extensions_table_sql()
        write_to_redshift_versioned(extensions_resolved_frame, "condition_extensions", "condition_id", extensions_table_sql)
        logger.info("✅ Condition extensions table dropped, recreated and written successfully")
        
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
        logger.info("  ✅ public.conditions (main condition data)")
        logger.info("  ✅ public.condition_categories (condition classifications)")
        logger.info("  ✅ public.condition_notes (condition notes and annotations)")
        logger.info("  ✅ public.condition_body_sites (anatomical locations)")
        logger.info("  ✅ public.condition_stages (condition stages and grades)")
        logger.info("  ✅ public.condition_codes (all condition codes - SNOMED, ICD-10, etc.)")
        logger.info("  ✅ public.condition_evidence (supporting evidence)")
        logger.info("  ✅ public.condition_extensions (FHIR extensions - normalized)")
        
        logger.info("\n📊 FINAL ETL STATISTICS:")
        logger.info(f"  📥 Total raw records processed: {total_records:,}")
        logger.info(f"  🏥 Main condition records: {main_count:,}")
        logger.info(f"  🏷️  Category records: {categories_count:,}")
        logger.info(f"  📝 Note records: {notes_count:,}")
        logger.info(f"  🦴 Body site records: {body_sites_count:,}")
        logger.info(f"  📊 Stage records: {stages_count:,}")
        logger.info(f"  🏥 Code records: {codes_count:,}")
        logger.info(f"  🔍 Evidence records: {evidence_count:,}")
        logger.info(f"  🔧 Extension records: {extensions_count:,}")
        
        # Calculate data expansion ratio
        total_output_records = main_count + categories_count + notes_count + body_sites_count + stages_count + codes_count + evidence_count + extensions_count
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
        logger.error("=" * 80)
        raise e

if __name__ == "__main__":
    main()
    job.commit()
