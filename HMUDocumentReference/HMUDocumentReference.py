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

# Set up logging to write to stdout (CloudWatch)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Add handler to write logs to stdout so they appear in CloudWatch
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
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
TABLE_NAME = "documentreference"
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
        if hasattr(reference_field, 'reference'):
            reference = reference_field.reference
            if reference and "/" in reference:
                return reference.split("/")[-1]
        elif isinstance(reference_field, dict):
            reference = reference_field.get('reference')
            if reference and "/" in reference:
                return reference.split("/")[-1]
        elif isinstance(reference_field, str):
            if "/" in reference_field:
                return reference_field.split("/")[-1]
    return None

def transform_main_document_reference_data(df):
    """Transform the main document reference data"""
    logger.info("Transforming main document reference data...")
    
    available_columns = df.columns
    logger.info(f"Available columns: {available_columns}")
    
    select_columns = [
        F.col("id").alias("document_reference_id"),
        F.when(F.col("subject").isNotNull(),
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        F.col("status").alias("status"),
        F.when(F.col("type").isNotNull() & (F.size(F.col("type.coding")) > 0),
               F.col("type.coding")[0].getField("code")
              ).otherwise(None).alias("type_code"),
        F.when(F.col("type").isNotNull() & (F.size(F.col("type.coding")) > 0),
               F.col("type.coding")[0].getField("system")
              ).otherwise(None).alias("type_system"),
        F.when(F.col("type").isNotNull() & (F.size(F.col("type.coding")) > 0),
               F.col("type.coding")[0].getField("display")
              ).otherwise(None).alias("type_display"),
        # Add meta-type mapping based on type_code and type_system
        F.when(
            (F.col("type.coding")[0].getField("code") == "11502-2") &
            (F.col("type.coding")[0].getField("system") == "http://loinc.org"),
            "lab"
        ).when(
            (F.col("type.coding")[0].getField("code") == "70006-2") &
            (F.col("type.coding")[0].getField("system") == "http://loinc.org"),
            "medication"
        ).when(
            (F.col("type.coding")[0].getField("code").isin("60591-5", "28570-0", "34117-2", "11488-4",
                                                           "11506-3")) &
            (F.col("type.coding")[0].getField("system") == "http://loinc.org"),
            "history"
        ).when(
            (F.col("type.coding")[0].getField("code") == "78322-5") &
            (F.col("type.coding")[0].getField("system") == "http://loinc.org"),
            "surgery"
        ).when(
            (F.col("type.coding")[0].getField("code") == "68629-5") &
            (F.col("type.coding")[0].getField("system") == "http://loinc.org"),
            "allergy"
        ).when(
            (F.col("type.coding")[0].getField("code") == "18842-5") &
            (F.col("type.coding")[0].getField("system") == "http://loinc.org"),
            "discharge"
        ).when(
            (F.col("type.coding")[0].getField("code") == "18748-4") &
            (F.col("type.coding")[0].getField("system") == "http://loinc.org"),
            "imaging"
        ).when(
            (F.col("type.coding")[0].getField("code") == "87273-9") &
            (F.col("type.coding")[0].getField("system") == "http://loinc.org"),
            "immunization"
        ).when(
            (F.col("type.coding")[0].getField("code") == "11526-1") &
            (F.col("type.coding")[0].getField("system") == "http://loinc.org"),
            "pathology"
        ).when(
            (F.col("type.coding")[0].getField("code") == "75425-9") &
            (F.col("type.coding")[0].getField("system") == "http://loinc.org"),
            "cardiology"
        ).otherwise("unknown").alias("meta_type"),
        # Handle date with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp(F.col("date"), "yyyy-MM-dd")
        ).alias("date"),
        F.when(F.col("custodian").isNotNull(),
               F.regexp_extract(F.col("custodian").getField("reference"), r"Organization/(.+)", 1)
              ).otherwise(None).alias("custodian_id"),
        F.col("description").alias("description"),
        # Handle context.period.start with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("context.period.start"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("context.period.start"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("context.period.start"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("context.period.start"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("context.period.start"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("context_period_start"),
        # Handle context.period.end with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("context.period.end"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("context.period.end"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("context.period.end"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("context.period.end"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("context.period.end"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("context_period_end"),
        # Handle meta.lastUpdated with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("meta_last_updated"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ]
    
    main_df = df.select(*select_columns).filter(
        F.col("document_reference_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return main_df

def transform_document_reference_identifiers(df):
    """Transform document reference identifiers"""
    logger.info("Transforming document reference identifiers...")
    
    if "identifier" not in df.columns:
        logger.warning("identifier column not found, returning empty DataFrame")
        return spark.createDataFrame([], schema="document_reference_id string, meta_last_updated timestamp, identifier_system string, identifier_value string")

    identifiers_df = df.select(
        F.col("id").alias("document_reference_id"),
        F.coalesce(
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("meta_last_updated"),
        F.explode(F.col("identifier")).alias("identifier_item")
    ).filter(
        F.col("identifier_item").isNotNull()
    )

    identifiers_final = identifiers_df.select(
        F.col("document_reference_id"),
        F.col("meta_last_updated"),
        F.col("identifier_item.system").alias("identifier_system"),
        F.col("identifier_item.value").alias("identifier_value")
    ).filter(
        F.col("identifier_value").isNotNull()
    )
    
    return identifiers_final

def transform_document_reference_categories(df):
    """Transform document reference categories"""
    logger.info("Transforming document reference categories...")
    
    if "category" not in df.columns:
        logger.warning("category column not found, returning empty DataFrame")
        return spark.createDataFrame([], schema="document_reference_id string, meta_last_updated timestamp, category_code string, category_system string, category_display string")

    # First explode to get category items, preserving meta_last_updated
    categories_df = df.select(
        F.col("id").alias("document_reference_id"),
        F.coalesce(
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("meta_last_updated"),
        F.explode(F.col("category")).alias("category_item")
    ).filter(
        F.col("category_item").isNotNull()
    )

    # Then explode coding array and extract fields, preserving meta_last_updated
    categories_final = categories_df.select(
        F.col("document_reference_id"),
        F.col("meta_last_updated"),
        F.explode(F.col("category_item.coding")).alias("coding_item")
    ).select(
        F.col("document_reference_id"),
        F.col("meta_last_updated"),
        F.col("coding_item.code").alias("category_code"),
        F.col("coding_item.system").alias("category_system"),
        F.col("coding_item.display").alias("category_display")
    ).filter(
        F.col("category_code").isNotNull()
    )
    
    return categories_final

def transform_document_reference_authors(df):
    """Transform document reference authors"""
    logger.info("Transforming document reference authors...")
    
    if "author" not in df.columns:
        logger.warning("author column not found, returning empty DataFrame")
        return spark.createDataFrame([], schema="document_reference_id string, meta_last_updated timestamp, author_id string")

    authors_df = df.select(
        F.col("id").alias("document_reference_id"),
        F.coalesce(
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("meta_last_updated"),
        F.explode(F.col("author")).alias("author_item")
    ).filter(
        F.col("author_item").isNotNull()
    )

    authors_final = authors_df.select(
        F.col("document_reference_id"),
        F.col("meta_last_updated"),
        F.regexp_extract(F.col("author_item.reference"), r"Practitioner/(.+)", 1).alias("author_id")
    ).filter(
        F.col("author_id") != ""
    )
    
    return authors_final

def transform_document_reference_content(df):
    """Transform document reference content"""
    logger.info("Transforming document reference content...")

    if "content" not in df.columns:
        logger.warning("content column not found, returning empty DataFrame")
        return spark.createDataFrame([], schema="document_reference_id string, meta_last_updated timestamp, attachment_content_type string, attachment_url string")

    # First, get the content array with index, preserving meta_last_updated
    content_df = df.select(
        F.col("id").alias("document_reference_id"),
        F.coalesce(
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("meta_last_updated"),
        F.posexplode(F.col("content")).alias("content_index", "content_item")
    ).filter(
        F.col("content_item").isNotNull()
    )

    # Handle both external URLs and embedded data
    content_final = content_df.select(
        F.col("document_reference_id"),
        F.col("meta_last_updated"),
        F.col("content_item.attachment.contentType").alias("attachment_content_type"),
        # Check if there's a URL field (external reference)
        F.when(
            F.col("content_item.attachment.url").isNotNull(),
            F.col("content_item.attachment.url")
        ).when(
            # If no URL but has data field (embedded document), create reference path
            F.col("content_item.attachment.data").isNotNull(),
            F.concat(F.lit("ref:content["), F.col("content_index"), F.lit("].attachment.data"))
        ).otherwise(None).alias("attachment_url")
    ).filter(
        # Keep records that have either URL or data
        F.col("attachment_url").isNotNull()
    )

    return content_final

def create_redshift_tables_sql():
    """Generate SQL for creating main document references table in Redshift"""
    return """
    DROP TABLE IF EXISTS public.document_references CASCADE;

    CREATE TABLE public.document_references (
        document_reference_id VARCHAR(255) PRIMARY KEY,
        patient_id VARCHAR(255) NOT NULL,
        status VARCHAR(50),
        type_code VARCHAR(50),
        type_system VARCHAR(255),
        type_display VARCHAR(500),
        meta_type VARCHAR(50),
        date TIMESTAMP,
        custodian_id VARCHAR(255),
        description VARCHAR(MAX),
        context_period_start TIMESTAMP,
        context_period_end TIMESTAMP,
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, date);
    """

def create_document_reference_identifiers_table_sql():
    """Generate SQL for creating document_reference_identifiers table"""
    return """
    DROP TABLE IF EXISTS public.document_reference_identifiers CASCADE;
    
    CREATE TABLE public.document_reference_identifiers (
        document_reference_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        identifier_system VARCHAR(255),
        identifier_value VARCHAR(255)
    ) SORTKEY (document_reference_id, identifier_system);
    """

def create_document_reference_categories_table_sql():
    """Generate SQL for creating document_reference_categories table"""
    return """
    DROP TABLE IF EXISTS public.document_reference_categories CASCADE;
    
    CREATE TABLE public.document_reference_categories (
        document_reference_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        category_code VARCHAR(50),
        category_system VARCHAR(255),
        category_display VARCHAR(255)
    ) SORTKEY (document_reference_id, category_code);
    """

def create_document_reference_authors_table_sql():
    """Generate SQL for creating document_reference_authors table"""
    return """
    DROP TABLE IF EXISTS public.document_reference_authors CASCADE;
    
    CREATE TABLE public.document_reference_authors (
        document_reference_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        author_id VARCHAR(255)
    ) SORTKEY (document_reference_id);
    """

def create_document_reference_content_table_sql():
    """Generate SQL for creating document_reference_content table"""
    return """
    DROP TABLE IF EXISTS public.document_reference_content CASCADE;

    CREATE TABLE public.document_reference_content (
        document_reference_id VARCHAR(255),
        meta_last_updated TIMESTAMP,
        attachment_content_type VARCHAR(100),
        attachment_url VARCHAR(MAX)
    ) SORTKEY (document_reference_id);
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

def main():
    """Main ETL process"""
    start_time = datetime.now()
    try:
        logger.info("=" * 80)
        logger.info("🚀 STARTING FHIR DOCUMENTREFERENCE ETL PROCESS")
        logger.info("=" * 80)
        
        # Step 1: Read data from S3 using Iceberg catalog
        # Use Iceberg to read data from S3
        table_name_full = f"{catalog_nm}.{DATABASE_NAME}.{TABLE_NAME}"
        logger.info(f"Reading from table: {table_name_full}")
        df_raw = spark.table(table_name_full)
        
        document_reference_df_raw = df_raw

        
        # TESTING MODE: Sample data for quick testing

        
        # Set to True to process only a sample of records

        
        USE_SAMPLE = False  # Set to True for testing with limited data

        
        SAMPLE_SIZE = 1000

        
        

        
        if USE_SAMPLE:

        
            logger.info(f"⚠️  TESTING MODE: Sampling {SAMPLE_SIZE} records for quick testing")

        
            logger.info("⚠️  Set USE_SAMPLE = False for production runs")

        
            document_reference_df = document_reference_df_raw.limit(SAMPLE_SIZE)

        
        else:

        
            logger.info("✅ Processing full dataset")

        
            document_reference_df = document_reference_df_raw

        total_records = document_reference_df_raw.count()
        logger.info(f"📊 Read {total_records:,} raw document reference records")

        if total_records == 0:
            logger.error("❌ No raw data found! Check the data source.")
            return

        # Step 2: Transform main document reference data
        main_document_reference_df = transform_main_document_reference_data(document_reference_df_raw)
        main_count = main_document_reference_df.count()
        logger.info(f"✅ Transformed {main_count:,} main document reference records")

        if main_count == 0:
            logger.error("❌ No main document reference records after transformation! Check filtering criteria.")
            return
            
        # Step 3: Transform multi-valued data
        doc_ref_identifiers_df = transform_document_reference_identifiers(document_reference_df_raw)
        identifiers_count = doc_ref_identifiers_df.count()
        logger.info(f"✅ Transformed {identifiers_count:,} document reference identifier records")
        
        doc_ref_categories_df = transform_document_reference_categories(document_reference_df_raw)
        categories_count = doc_ref_categories_df.count()
        logger.info(f"✅ Transformed {categories_count:,} document reference category records")
        
        doc_ref_authors_df = transform_document_reference_authors(document_reference_df_raw)
        authors_count = doc_ref_authors_df.count()
        logger.info(f"✅ Transformed {authors_count:,} document reference author records")
        
        doc_ref_content_df = transform_document_reference_content(document_reference_df_raw)
        content_count = doc_ref_content_df.count()
        logger.info(f"✅ Transformed {content_count:,} document reference content records")

        # Step 4: Convert to DynamicFrames
        main_dynamic_frame = DynamicFrame.fromDF(main_document_reference_df, glueContext, "main_doc_ref_dynamic_frame")
        identifiers_dynamic_frame = DynamicFrame.fromDF(doc_ref_identifiers_df, glueContext, "identifiers_dynamic_frame")
        categories_dynamic_frame = DynamicFrame.fromDF(doc_ref_categories_df, glueContext, "categories_dynamic_frame")
        authors_dynamic_frame = DynamicFrame.fromDF(doc_ref_authors_df, glueContext, "authors_dynamic_frame")
        content_dynamic_frame = DynamicFrame.fromDF(doc_ref_content_df, glueContext, "content_dynamic_frame")

        # Step 5: Write to Redshift
        logger.info("\n" + "=" * 50)
        logger.info("💾 STEP 5: WRITING DATA TO REDSHIFT")
        logger.info("=" * 50)
        
        document_references_table_sql = create_redshift_tables_sql()
        write_to_redshift_versioned(main_dynamic_frame, "document_references", "document_reference_id", document_references_table_sql)
        
        identifiers_table_sql = create_document_reference_identifiers_table_sql()
        write_to_redshift_versioned(identifiers_dynamic_frame, "document_reference_identifiers", "document_reference_id", identifiers_table_sql)
        
        categories_table_sql = create_document_reference_categories_table_sql()
        write_to_redshift_versioned(categories_dynamic_frame, "document_reference_categories", "document_reference_id", categories_table_sql)
        
        authors_table_sql = create_document_reference_authors_table_sql()
        write_to_redshift_versioned(authors_dynamic_frame, "document_reference_authors", "document_reference_id", authors_table_sql)
        
        content_table_sql = create_document_reference_content_table_sql()
        write_to_redshift_versioned(content_dynamic_frame, "document_reference_content", "document_reference_id", content_table_sql)

        end_time = datetime.now()
        logger.info("\n" + "=" * 80)
        
        if USE_SAMPLE:
            logger.info("⚠️  WARNING: THIS WAS A TEST RUN WITH SAMPLED DATA")
            logger.info(f"⚠️  Only {SAMPLE_SIZE} records were processed")
            logger.info("⚠️  Set USE_SAMPLE = False for production runs")
        logger.info("🎉 ETL PROCESS COMPLETED SUCCESSFULLY!")
        logger.info(f"⏱️  Total processing time: {end_time - start_time}")
        logger.info("=" * 80)

    except Exception as e:
        logger.error("\n" + "=" * 80)
        logger.error("❌ ETL PROCESS FAILED!")
        logger.error(f"🚨 Error: {str(e)}")
        logger.error("=" * 80)
        raise e

if __name__ == "__main__":
    main()
    job.commit()
