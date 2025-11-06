# Deployed: 2025-11-05 15:21:14 UTC
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
TABLE_NAME = "medicationdispense"
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

def transform_main_medication_dispense_data(df):
    """Transform the main medication dispense data"""
    logger.info("Transforming main medication dispense data...")
    
    # Log available columns for debugging
    available_columns = df.columns
    logger.info(f"Available columns: {available_columns}")
    
    # Build the select statement dynamically based on available columns
    select_columns = [
        F.col("id").alias("medication_dispense_id"),
        F.col("resourceType").alias("resource_type"),
        F.col("status").alias("status"),
        
        # Extract patient ID from subject.reference
        F.when(F.col("subject").isNotNull(), 
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        
        # Extract medication ID and display from medicationReference
        F.when(F.col("medicationReference").isNotNull(),
               F.regexp_extract(F.col("medicationReference").getField("reference"), r"Medication/(.+)", 1)
              ).otherwise(None).alias("medication_id"),
        F.when(F.col("medicationReference").isNotNull(),
               F.col("medicationReference").getField("display")
              ).otherwise(None).alias("medication_display"),
        
        # Extract from the 'type' field
        F.when(F.col("type.coding").isNotNull() & (F.size(F.col("type.coding")) > 0),
               F.col("type.coding")[0].getField("system")
              ).otherwise(None).alias("type_system"),
        F.when(F.col("type.coding").isNotNull() & (F.size(F.col("type.coding")) > 0),
               F.col("type.coding")[0].getField("code")
              ).otherwise(None).alias("type_code"),
        F.when(F.col("type.coding").isNotNull() & (F.size(F.col("type.coding")) > 0),
               F.col("type.coding")[0].getField("display")
              ).otherwise(None).alias("type_display"),

        # Extract quantity value, handling nested struct
        F.when(F.col("quantity").isNotNull(),
               F.col("quantity").getField("value")
              ).otherwise(None).alias("quantity_value"),
        
        # Convert whenHandedOver to timestamp
        # Handle whenHandedOver with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("whenHandedOver"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("whenHandedOver"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("whenHandedOver"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("whenHandedOver"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("whenHandedOver"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("when_handed_over"),
        
        # Meta fields
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
    
    # Transform main medication dispense data using only available columns
    main_df = df.select(*select_columns).filter(
        F.col("medication_dispense_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return main_df

def transform_medication_dispense_identifiers(df):
    """Transform medication dispense identifiers (multiple identifiers per dispense)"""
    logger.info("Transforming medication dispense identifiers...")
    
    # Check if identifier column exists
    if "identifier" not in df.columns:
        logger.warning("identifier column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("medication_dispense_id"),
            F.lit(None).cast(StringType()).alias("identifier_system"),
            F.lit("").alias("identifier_value")
        ).filter(F.lit(False))
    
    # First explode the identifier array
    identifiers_df = df.select(
        F.col("id").alias("medication_dispense_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode(F.col("identifier")).alias("identifier_item")
    ).filter(
        F.col("identifier_item").isNotNull()
    )
    
    # Extract identifier details
    identifiers_final = identifiers_df.select(
        F.col("medication_dispense_id"),
        F.col("meta_last_updated"),
        F.lit(None).cast(StringType()).alias("identifier_system"),
        F.col("identifier_item.value").alias("identifier_value")
    ).filter(
        F.col("identifier_value").isNotNull()
    )
    
    return identifiers_final

def transform_medication_dispense_performers(df):
    """Transform medication dispense performers (who dispensed the medication)"""
    logger.info("Transforming medication dispense performers...")
    
    # Check if performer column exists
    if "performer" not in df.columns:
        logger.warning("performer column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("medication_dispense_id"),
            F.lit("").alias("performer_actor_reference")
        ).filter(F.lit(False))
    
    # First explode the performer array
    performers_df = df.select(
        F.col("id").alias("medication_dispense_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode(F.col("performer")).alias("performer_item")
    ).filter(
        F.col("performer_item").isNotNull()
    )
    
    # Extract performer details
    performers_final = performers_df.select(
        F.col("medication_dispense_id"),
        F.col("meta_last_updated"),
        F.when(F.col("performer_item.actor").isNotNull(),
               F.col("performer_item.actor.reference")
              ).otherwise(None).alias("performer_actor_reference")
    ).filter(
        F.col("performer_actor_reference").isNotNull()
    )
    
    return performers_final

def transform_medication_dispense_auth_prescriptions(df):
    """Transform medication dispense authorizing prescriptions"""
    logger.info("Transforming medication dispense authorizing prescriptions...")
    
    # Check if authorizingPrescription column exists
    if "authorizingPrescription" not in df.columns:
        logger.warning("authorizingPrescription column not found, returning empty DataFrame")
        return df.select(
            F.col("id").alias("medication_dispense_id"),
            F.lit(None).cast(StringType()).alias("authorizing_prescription_id")
        ).filter(F.lit(False))
        
    # Explode the array and extract the reference ID
    auth_prescriptions_df = df.select(
        F.col("id").alias("medication_dispense_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode_outer(F.col("authorizingPrescription")).alias("prescription_item")
    ).select(
        F.col("medication_dispense_id"),
        F.col("meta_last_updated"),
        F.when(F.col("prescription_item").isNotNull(),
               F.regexp_extract(F.col("prescription_item.reference"), r"MedicationRequest/(.+)", 1)
              ).otherwise(None).alias("authorizing_prescription_id")
    ).filter(
        F.col("authorizing_prescription_id").isNotNull()
    )

    return auth_prescriptions_df

def transform_medication_dispense_dosage_instructions(df):
    """Transform medication dispense dosage instructions"""
    logger.info("Transforming medication dispense dosage instructions...")
    
    # Check if dosageInstruction column exists
    if "dosageInstruction" not in df.columns:
        logger.warning("dosageInstruction column not found, returning empty DataFrame")
        return df.select(
            F.col("id").alias("medication_dispense_id"),
            F.lit("").alias("dosage_text"),
            F.lit(None).alias("dosage_timing_frequency"),
            F.lit(None).alias("dosage_timing_period"),
            F.lit(None).alias("dosage_timing_period_unit"),
            F.lit(None).alias("dosage_route_code"),
            F.lit(None).alias("dosage_route_system"),
            F.lit(None).alias("dosage_route_display"),
            F.lit(None).alias("dosage_dose_value"),
            F.lit(None).alias("dosage_dose_unit"),
            F.lit(None).alias("dosage_dose_system"),
            F.lit(None).alias("dosage_dose_code")
        ).filter(F.lit(False))
    
    # First explode the dosageInstruction array
    dosage_df = df.select(
        F.col("id").alias("medication_dispense_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode(F.col("dosageInstruction")).alias("dosage_item")
    ).filter(
        F.col("dosage_item").isNotNull()
    )
    
    # Extract dosage instruction details
    dosage_final = dosage_df.select(
        F.col("medication_dispense_id"),
        F.col("meta_last_updated"),
        F.col("dosage_item.text").alias("dosage_text"),
        F.col("dosage_item.timing.repeat.frequency").alias("dosage_timing_frequency"),
        F.col("dosage_item.timing.repeat.period").alias("dosage_timing_period"),
        F.col("dosage_item.timing.repeat.periodUnit").alias("dosage_timing_period_unit"),
        F.when(F.col("dosage_item.route").isNotNull() & (F.size(F.col("dosage_item.route.coding")) > 0),
               F.col("dosage_item.route.coding")[0].getField("code")
              ).otherwise(None).alias("dosage_route_code"),
        F.when(F.col("dosage_item.route").isNotNull() & (F.size(F.col("dosage_item.route.coding")) > 0),
               F.col("dosage_item.route.coding")[0].getField("system")
              ).otherwise(None).alias("dosage_route_system"),
        F.when(F.col("dosage_item.route").isNotNull() & (F.size(F.col("dosage_item.route.coding")) > 0),
               F.col("dosage_item.route.coding")[0].getField("display")
              ).otherwise(None).alias("dosage_route_display"),
        F.when(F.col("dosage_item.doseAndRate").isNotNull() & (F.size(F.col("dosage_item.doseAndRate")) > 0),
               F.col("dosage_item.doseAndRate")[0].getField("doseQuantity").getField("value")
              ).otherwise(None).alias("dosage_dose_value"),
        F.when(F.col("dosage_item.doseAndRate").isNotNull() & (F.size(F.col("dosage_item.doseAndRate")) > 0),
               F.col("dosage_item.doseAndRate")[0].getField("doseQuantity").getField("unit")
              ).otherwise(None).alias("dosage_dose_unit"),
        F.when(F.col("dosage_item.doseAndRate").isNotNull() & (F.size(F.col("dosage_item.doseAndRate")) > 0),
               F.col("dosage_item.doseAndRate")[0].getField("doseQuantity").getField("system")
              ).otherwise(None).alias("dosage_dose_system"),
        F.when(F.col("dosage_item.doseAndRate").isNotNull() & (F.size(F.col("dosage_item.doseAndRate")) > 0),
               F.col("dosage_item.doseAndRate")[0].getField("doseQuantity").getField("code")
              ).otherwise(None).alias("dosage_dose_code")
    ).filter(
        F.col("dosage_text").isNotNull() | F.col("dosage_dose_value").isNotNull()
    )
    
    return dosage_final

def create_redshift_tables_sql():
    """Generate SQL for creating main medication_dispenses table in Redshift"""
    return """
    -- Main medication dispenses table
    CREATE TABLE IF NOT EXISTS public.medication_dispenses (
        medication_dispense_id VARCHAR(255) NOT NULL,
        resource_type VARCHAR(50),
        status VARCHAR(50),
        patient_id VARCHAR(255) NOT NULL,
        medication_id VARCHAR(255),
        medication_display VARCHAR(500),
        type_system VARCHAR(255),
        type_code VARCHAR(50),
        type_display VARCHAR(255),
        quantity_value DECIMAL(10,2),
        when_handed_over TIMESTAMP,
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, when_handed_over);
    """

def create_medication_dispense_identifiers_table_sql():
    """Generate SQL for creating medication_dispense_identifiers table"""
    return """
    CREATE TABLE IF NOT EXISTS public.medication_dispense_identifiers (
        medication_dispense_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        identifier_system VARCHAR(255),
        identifier_value VARCHAR(255)
    ) SORTKEY (medication_dispense_id, identifier_system)
    """

def create_medication_dispense_performers_table_sql():
    """Generate SQL for creating medication_dispense_performers table"""
    return """
    CREATE TABLE IF NOT EXISTS public.medication_dispense_performers (
        medication_dispense_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        performer_actor_reference VARCHAR(255)
    ) SORTKEY (medication_dispense_id, performer_actor_reference)
    """

def create_medication_dispense_auth_prescriptions_table_sql():
    """Generate SQL for creating medication_dispense_auth_prescriptions table"""
    return """
    CREATE TABLE IF NOT EXISTS public.medication_dispense_auth_prescriptions (
        medication_dispense_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        authorizing_prescription_id VARCHAR(255)
    ) SORTKEY (medication_dispense_id)
    """

def create_medication_dispense_dosage_instructions_table_sql():
    """Generate SQL for creating medication_dispense_dosage_instructions table"""
    return """
    CREATE TABLE IF NOT EXISTS public.medication_dispense_dosage_instructions (
        medication_dispense_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        dosage_text VARCHAR(MAX),
        dosage_timing_frequency INTEGER,
        dosage_timing_period INTEGER,
        dosage_timing_period_unit VARCHAR(20),
        dosage_route_code VARCHAR(50),
        dosage_route_system VARCHAR(255),
        dosage_route_display VARCHAR(255),
        dosage_dose_value DECIMAL(10,2),
        dosage_dose_unit VARCHAR(100),
        dosage_dose_system VARCHAR(255),
        dosage_dose_code VARCHAR(50)
    ) SORTKEY (medication_dispense_id)
    """

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

def main():
    """Main ETL process"""
    start_time = datetime.now()
    try:
        logger.info("=" * 80)
        logger.info("🚀 STARTING FHIR MEDICATION DISPENSE ETL PROCESS")
        logger.info("=" * 80)
        logger.info(f"⏰ Job started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Check table existence and log schemas
        table_names = ["medication_dispense_auth_prescriptions", "medication_dispense_dosage_instructions", "medication_dispense_identifiers", "medication_dispense_performers", "medication_dispenses"]
        check_all_tables(glueContext, table_names, REDSHIFT_CONNECTION, S3_TEMP_DIR)

        logger.info(f"📊 Source: {DATABASE_NAME}.{TABLE_NAME}")
        logger.info(f"🎯 Target: Redshift (5 tables)")
        logger.info("📋 Reading all available columns from Glue Catalog")
        logger.info("🔄 Process: 7 steps (Read → Transform → Convert → Resolve → Validate → Write)")
        
        # Step 1: Read data from S3 using Iceberg catalog
        logger.info("\n" + "=" * 50)
        logger.info("📥 STEP 1: READING DATA FROM GLUE CATALOG")
        logger.info("=" * 50)
        logger.info(f"Database: {DATABASE_NAME}")
        logger.info(f"Table: {TABLE_NAME}")
        logger.info("Reading all available columns from Glue Catalog")
        
        # Use the AWS Glue Data Catalog to read medication dispense data (all columns)
        # Use Iceberg to read data from S3
        table_name_full = f"{catalog_nm}.{DATABASE_NAME}.{TABLE_NAME}"
        logger.info(f"Reading from table: {table_name_full}")
        df_raw = spark.table(table_name_full)
        
        # Convert to DataFrame first to check available columns
        medication_dispense_df_raw = df_raw

        # TESTING MODE: Sample data for quick testing

        # Set to True to process only a sample of records

        USE_SAMPLE = False  # Set to True for testing with limited data

        SAMPLE_SIZE = 1000

        

        if USE_SAMPLE:

            logger.info(f"⚠️  TESTING MODE: Sampling {SAMPLE_SIZE} records for quick testing")

            logger.info("⚠️  Set USE_SAMPLE = False for production runs")

            medication_dispense_df = medication_dispense_df_raw.limit(SAMPLE_SIZE)

        else:

            logger.info("✅ Processing full dataset")

            medication_dispense_df = medication_dispense_df_raw

        available_columns = medication_dispense_df_raw.columns
        logger.info(f"📋 Available columns in source: {available_columns}")
        
        # Use all available columns
        logger.info(f"✅ Using all {len(available_columns)} available columns")
        medication_dispense_df = medication_dispense_df_raw
        
        logger.info("✅ Successfully read data using AWS Glue Data Catalog")
        
        total_records = medication_dispense_df.count()
        logger.info(f"📊 Read {total_records:,} raw medication dispense records")
        
        # Debug: Show sample of raw data and schema
        if total_records > 0:
            logger.info("\n🔍 DATA QUALITY CHECKS:")
            logger.info("Sample of raw medication dispense data:")
            medication_dispense_df.show(3, truncate=False)
            logger.info("Raw data schema:")
            medication_dispense_df.printSchema()
            
            # Check for NULL values in key fields
            null_checks = {
                "id": medication_dispense_df.filter(F.col("id").isNull()).count(),
                "subject.reference": medication_dispense_df.filter(F.col("subject").isNull() | F.col("subject.reference").isNull()).count(),
                "medicationReference": medication_dispense_df.filter(F.col("medicationReference").isNull()).count(),
                "status": medication_dispense_df.filter(F.col("status").isNull()).count()
            }
            
            logger.info("NULL value analysis in key fields:")
            for field, null_count in null_checks.items():
                percentage = (null_count / total_records) * 100 if total_records > 0 else 0
                logger.info(f"  {field}: {null_count:,} NULLs ({percentage:.1f}%)")
        else:
            logger.error("❌ No raw data found! Check the data source.")
            return
        
        # Step 2: Transform main medication dispense data
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 2: TRANSFORMING MAIN MEDICATION DISPENSE DATA")
        logger.info("=" * 50)
        
        main_medication_dispense_df = transform_main_medication_dispense_data(medication_dispense_df)
        main_count = main_medication_dispense_df.count()
        logger.info(f"✅ Transformed {main_count:,} main medication dispense records")
        
        if main_count == 0:
            logger.error("❌ No main medication dispense records after transformation! Check filtering criteria.")
            return
        
        # Debug: Show actual DataFrame schema
        logger.info("🔍 Main medication dispense DataFrame schema:")
        logger.info(f"Columns: {main_medication_dispense_df.columns}")
        main_medication_dispense_df.printSchema()
        
        # Debug: Show sample of transformed main data
        logger.info("Sample of transformed main medication dispense data:")
        main_medication_dispense_df.show(3, truncate=False)
        
        # Step 3: Transform multi-valued data (all supporting tables)
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 3: TRANSFORMING MULTI-VALUED DATA")
        logger.info("=" * 50)
        
        medication_dispense_identifiers_df = transform_medication_dispense_identifiers(medication_dispense_df)
        identifiers_count = medication_dispense_identifiers_df.count()
        logger.info(f"✅ Transformed {identifiers_count:,} medication dispense identifier records")
        
        medication_dispense_performers_df = transform_medication_dispense_performers(medication_dispense_df)
        performers_count = medication_dispense_performers_df.count()
        logger.info(f"✅ Transformed {performers_count:,} medication dispense performer records")
        
        medication_dispense_auth_prescriptions_df = transform_medication_dispense_auth_prescriptions(medication_dispense_df)
        auth_prescriptions_count = medication_dispense_auth_prescriptions_df.count()
        logger.info(f"✅ Transformed {auth_prescriptions_count:,} medication dispense authorizing prescription records")
        
        medication_dispense_dosage_df = transform_medication_dispense_dosage_instructions(medication_dispense_df)
        dosage_count = medication_dispense_dosage_df.count()
        logger.info(f"✅ Transformed {dosage_count:,} medication dispense dosage instruction records")
        
        # Debug: Show samples of multi-valued data if available
        if identifiers_count > 0:
            logger.info("Sample of medication dispense identifiers data:")
            medication_dispense_identifiers_df.show(3, truncate=False)
        
        if performers_count > 0:
            logger.info("Sample of medication dispense performers data:")
            medication_dispense_performers_df.show(3, truncate=False)

        if auth_prescriptions_count > 0:
            logger.info("Sample of medication dispense authorizing prescriptions data:")
            medication_dispense_auth_prescriptions_df.show(3, truncate=False)

        if dosage_count > 0:
            logger.info("Sample of medication dispense dosage instructions data:")
            medication_dispense_dosage_df.show(3, truncate=False)
        
        # Step 4: Convert to DynamicFrames and ensure data is flat for Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 4: CONVERTING TO DYNAMICFRAMES")
        logger.info("=" * 50)
        logger.info("Converting to DynamicFrames and ensuring Redshift compatibility...")
        
        # Convert main medication dispenses DataFrame and ensure flat structure
        main_flat_df = main_medication_dispense_df.select(
            F.col("medication_dispense_id").cast(StringType()).alias("medication_dispense_id"),
            F.col("resource_type").cast(StringType()).alias("resource_type"),
            F.col("status").cast(StringType()).alias("status"),
            F.col("patient_id").cast(StringType()).alias("patient_id"),
            F.col("medication_id").cast(StringType()).alias("medication_id"),
            F.col("medication_display").cast(StringType()).alias("medication_display"),
            F.col("type_system").cast(StringType()).alias("type_system"),
            F.col("type_code").cast(StringType()).alias("type_code"),
            F.col("type_display").cast(StringType()).alias("type_display"),
            F.col("quantity_value").cast(DecimalType(10,2)).alias("quantity_value"),
            F.col("when_handed_over").cast(TimestampType()).alias("when_handed_over"),
            F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
            F.col("created_at").cast(TimestampType()).alias("created_at"),
            F.col("updated_at").cast(TimestampType()).alias("updated_at")
        )
        
        main_dynamic_frame = DynamicFrame.fromDF(main_flat_df, glueContext, "main_medication_dispense_dynamic_frame")
        
        # Convert other DataFrames with type casting
        identifiers_flat_df = medication_dispense_identifiers_df.select(
            F.col("medication_dispense_id").cast(StringType()).alias("medication_dispense_id"),
            F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
            F.col("identifier_system").cast(StringType()).alias("identifier_system"),
            F.col("identifier_value").cast(StringType()).alias("identifier_value")
        )
        identifiers_dynamic_frame = DynamicFrame.fromDF(identifiers_flat_df, glueContext, "identifiers_dynamic_frame")
        
        performers_flat_df = medication_dispense_performers_df.select(
            F.col("medication_dispense_id").cast(StringType()).alias("medication_dispense_id"),
            F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
            F.col("performer_actor_reference").cast(StringType()).alias("performer_actor_reference")
        )
        performers_dynamic_frame = DynamicFrame.fromDF(performers_flat_df, glueContext, "performers_dynamic_frame")
        
        auth_prescriptions_flat_df = medication_dispense_auth_prescriptions_df.select(
            F.col("medication_dispense_id").cast(StringType()).alias("medication_dispense_id"),
            F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
            F.col("authorizing_prescription_id").cast(StringType()).alias("authorizing_prescription_id")
        )
        auth_prescriptions_dynamic_frame = DynamicFrame.fromDF(auth_prescriptions_flat_df, glueContext, "auth_prescriptions_dynamic_frame")
        
        dosage_flat_df = medication_dispense_dosage_df.select(
            F.col("medication_dispense_id").cast(StringType()).alias("medication_dispense_id"),
            F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
            F.col("dosage_text").cast(StringType()).alias("dosage_text"),
            F.col("dosage_timing_frequency").cast(IntegerType()).alias("dosage_timing_frequency"),
            F.col("dosage_timing_period").cast(IntegerType()).alias("dosage_timing_period"),
            F.col("dosage_timing_period_unit").cast(StringType()).alias("dosage_timing_period_unit"),
            F.col("dosage_route_code").cast(StringType()).alias("dosage_route_code"),
            F.col("dosage_route_system").cast(StringType()).alias("dosage_route_system"),
            F.col("dosage_route_display").cast(StringType()).alias("dosage_route_display"),
            F.col("dosage_dose_value").cast(DecimalType(10,2)).alias("dosage_dose_value"),
            F.col("dosage_dose_unit").cast(StringType()).alias("dosage_dose_unit"),
            F.col("dosage_dose_system").cast(StringType()).alias("dosage_dose_system"),
            F.col("dosage_dose_code").cast(StringType()).alias("dosage_dose_code")
        )
        dosage_dynamic_frame = DynamicFrame.fromDF(dosage_flat_df, glueContext, "dosage_dynamic_frame")
        
        # Step 5: Resolve any remaining choice types to ensure Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 5: RESOLVING CHOICE TYPES")
        logger.info("=" * 50)
        logger.info("Resolving choice types for Redshift compatibility...")
        
        main_resolved_frame = main_dynamic_frame.resolveChoice(
            specs=[
                ("medication_dispense_id", "cast:string"),
                ("resource_type", "cast:string"),
                ("status", "cast:string"),
                ("patient_id", "cast:string"),
                ("medication_id", "cast:string"),
                ("medication_display", "cast:string"),
                ("type_system", "cast:string"),
                ("type_code", "cast:string"),
                ("type_display", "cast:string"),
                ("quantity_value", "cast:decimal"),
                ("when_handed_over", "cast:timestamp"),
                ("meta_last_updated", "cast:timestamp"),
                ("created_at", "cast:timestamp"),
                ("updated_at", "cast:timestamp")
            ]
        )
        
        identifiers_resolved_frame = identifiers_dynamic_frame.resolveChoice(
            specs=[
                ("medication_dispense_id", "cast:string"),
                ("identifier_system", "cast:string"),
                ("identifier_value", "cast:string")
            ]
        )
        
        performers_resolved_frame = performers_dynamic_frame.resolveChoice(
            specs=[
                ("medication_dispense_id", "cast:string"),
                ("performer_actor_reference", "cast:string")
            ]
        )
        
        auth_prescriptions_resolved_frame = auth_prescriptions_dynamic_frame.resolveChoice(
            specs=[
                ("medication_dispense_id", "cast:string"),
                ("authorizing_prescription_id", "cast:string")
            ]
        )
        
        dosage_resolved_frame = dosage_dynamic_frame.resolveChoice(
            specs=[
                ("medication_dispense_id", "cast:string"),
                ("dosage_text", "cast:string"),
                ("dosage_timing_frequency", "cast:int"),
                ("dosage_timing_period", "cast:int"),
                ("dosage_timing_period_unit", "cast:string"),
                ("dosage_route_code", "cast:string"),
                ("dosage_route_system", "cast:string"),
                ("dosage_route_display", "cast:string"),
                ("dosage_dose_value", "cast:decimal"),
                ("dosage_dose_unit", "cast:string"),
                ("dosage_dose_system", "cast:string"),
                ("dosage_dose_code", "cast:string")
            ]
        )
        
        # Step 6: Final validation before writing
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 6: FINAL VALIDATION")
        logger.info("=" * 50)
        logger.info("Performing final validation before writing to Redshift...")
        
        # Validate main medication dispenses data
        main_final_df = main_resolved_frame.toDF()
        main_final_count = main_final_df.count()
        logger.info(f"Final main medication dispenses count: {main_final_count}")
        
        if main_final_count == 0:
            logger.error("No main medication dispense records to write to Redshift! Stopping the process.")
            return
        
        # Validate other tables
        identifiers_final_count = identifiers_resolved_frame.toDF().count()
        performers_final_count = performers_resolved_frame.toDF().count()
        auth_prescriptions_final_count = auth_prescriptions_resolved_frame.toDF().count()
        dosage_final_count = dosage_resolved_frame.toDF().count()
        
        logger.info(f"Final counts - Identifiers: {identifiers_final_count}, Performers: {performers_final_count}, Auth Prescriptions: {auth_prescriptions_final_count}, Dosage: {dosage_final_count}")
        
        # Debug: Show final sample data being written
        logger.info("Final sample data being written to Redshift (main medication dispenses):")
        main_final_df.show(3, truncate=False)
        
        # Additional validation: Check for any remaining issues
        logger.info("🔍 Final validation before Redshift write:")
        logger.info(f"Final DataFrame columns: {main_final_df.columns}")
        logger.info(f"Final DataFrame schema:")
        main_final_df.printSchema()
        
        # Check for any null values in critical fields
        critical_field_checks = {
            "medication_dispense_id": main_final_df.filter(F.col("medication_dispense_id").isNull()).count(),
            "patient_id": main_final_df.filter(F.col("patient_id").isNull()).count(),
        }
        
        logger.info("Critical field null checks:")
        for field, null_count in critical_field_checks.items():
            logger.info(f"  {field}: {null_count} NULLs")
        
        if critical_field_checks["medication_dispense_id"] > 0:
            logger.error("❌ Found NULL medication_dispense_id values - this will cause Redshift write to fail!")
        
        if critical_field_checks["patient_id"] > 0:
            logger.error("❌ Found NULL patient_id values - this will cause Redshift write to fail!")
        
        # Step 7: Create tables and write to Redshift
        logger.info("\n" + "=" * 50)
        logger.info("💾 STEP 7: WRITING DATA TO REDSHIFT")
        logger.info("=" * 50)
        logger.info(f"🔗 Using connection: {REDSHIFT_CONNECTION}")
        logger.info(f"📁 S3 temp directory: {S3_TEMP_DIR}")
        
        # Create all tables individually
        logger.info("📝 Dropping and recreating main medication dispenses table...")
        medication_dispenses_table_sql = create_redshift_tables_sql()
        write_to_redshift_versioned(main_resolved_frame, "medication_dispenses", "medication_dispense_id", medication_dispenses_table_sql)
        logger.info("✅ Main medication dispenses table dropped, recreated and written successfully")
        
        logger.info("📝 Dropping and recreating medication dispense identifiers table...")
        identifiers_table_sql = create_medication_dispense_identifiers_table_sql()
        write_to_redshift_versioned(identifiers_resolved_frame, "medication_dispense_identifiers", "medication_dispense_id", identifiers_table_sql)
        logger.info("✅ Medication dispense identifiers table dropped, recreated and written successfully")
        
        logger.info("📝 Dropping and recreating medication dispense performers table...")
        performers_table_sql = create_medication_dispense_performers_table_sql()
        write_to_redshift_versioned(performers_resolved_frame, "medication_dispense_performers", "medication_dispense_id", performers_table_sql)
        logger.info("✅ Medication dispense performers table dropped, recreated and written successfully")
        
        logger.info("📝 Dropping and recreating medication dispense authorizing prescriptions table...")
        auth_prescriptions_table_sql = create_medication_dispense_auth_prescriptions_table_sql()
        write_to_redshift_versioned(auth_prescriptions_resolved_frame, "medication_dispense_auth_prescriptions", "medication_dispense_id", auth_prescriptions_table_sql)
        logger.info("✅ Medication dispense authorizing prescriptions table dropped, recreated and written successfully")
        
        logger.info("📝 Dropping and recreating medication dispense dosage instructions table...")
        dosage_table_sql = create_medication_dispense_dosage_instructions_table_sql()
        write_to_redshift_versioned(dosage_resolved_frame, "medication_dispense_dosage_instructions", "medication_dispense_id", dosage_table_sql)
        logger.info("✅ Medication dispense dosage instructions table dropped, recreated and written successfully")
        
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
        logger.info("  ✅ public.medication_dispenses (main medication dispense data)")
        logger.info("  ✅ public.medication_dispense_identifiers (identifier system/value pairs)")
        logger.info("  ✅ public.medication_dispense_performers (performer/dispenser information)")
        logger.info("  ✅ public.medication_dispense_auth_prescriptions (authorizing prescription references)")
        logger.info("  ✅ public.medication_dispense_dosage_instructions (dosage instructions)")
        
        logger.info("\n📊 FINAL ETL STATISTICS:")
        logger.info(f"  📥 Total raw records processed: {total_records:,}")
        logger.info(f"  💊 Main medication dispense records: {main_count:,}")
        logger.info(f"  🏷️  Identifier records: {identifiers_count:,}")
        logger.info(f"  👤 Performer records: {performers_count:,}")
        logger.info(f"  📝 Authorizing Prescription records: {auth_prescriptions_count:,}")
        logger.info(f"  💉 Dosage Instruction records: {dosage_count:,}")
        
        # Calculate data expansion ratio
        total_output_records = main_count + identifiers_count + performers_count + auth_prescriptions_count + dosage_count
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
