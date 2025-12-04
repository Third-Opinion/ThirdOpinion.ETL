# Deployed: 2025-01-XX (v2 - Refactored)
"""
HMUMedication ETL v2 - Refactored and improved version

Main ETL orchestrator for processing FHIR Medication data from Iceberg to Redshift
"""
from datetime import datetime
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.types import StringType, TimestampType

# Import v2 modules (using absolute imports for Glue compatibility)
try:
    from config import MedicationETLConfig, TableNames
    from shared.config import SparkConfig, DatabaseConfig, ProcessingConfig
    from shared.utils.bookmark_utils import get_bookmark_from_redshift, filter_by_bookmark
    from shared.utils.deduplication_utils import deduplicate_entities
    from transformations.main_medication import transform_main_medication_data, transform_and_enrich_medication_data
    from transformations.child_tables import transform_medication_identifiers
    from shared.database.redshift_operations import (
        write_to_redshift_simple,
        write_to_redshift_versioned
    )
except ImportError as e:
    # If imports fail, log the error and re-raise
    logger = logging.getLogger()
    logger.error(f"Failed to import required modules: {e}")
    logger.error("Make sure --extra-py-files points to the correct zip file with all modules")
    raise

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# BEGIN NOTE TO AI: do not change the following section.
try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
except Exception as e:
    # Fallback for testing
    args = {"JOB_NAME": "HMUMedication"}

# Get configuration from environment/job arguments
config = MedicationETLConfig.from_environment()

# Override with job arguments if provided
try:
    test_mode_args = getResolvedOptions(sys.argv, ["TEST_MODE"])
    config.processing.test_mode = test_mode_args.get("TEST_MODE", "false").lower() in ["true", "1", "yes"]
except:
    pass

try:
    backdate_args = getResolvedOptions(sys.argv, ["BACKDATE_DAYS"])
    config.processing.backdate_days = int(backdate_args.get("BACKDATE_DAYS", "0"))
except:
    pass

try:
    sample_args = getResolvedOptions(sys.argv, ["USE_SAMPLE", "SAMPLE_SIZE"])
    config.processing.use_sample = sample_args.get("USE_SAMPLE", "false").lower() in ["true", "1", "yes"]
    config.processing.sample_size = int(sample_args.get("SAMPLE_SIZE", "100000"))
except:
    pass

# Override enrichment config from job arguments if provided
try:
    enrichment_args = getResolvedOptions(sys.argv, ["ENABLE_MEDICATION_ENRICHMENT", "MEDICATION_ENRICHMENT_MODE", "USE_MEDICATION_LOOKUP_TABLE"])
    if config.enrichment:
        if "ENABLE_MEDICATION_ENRICHMENT" in enrichment_args:
            config.enrichment.enable_enrichment = enrichment_args.get("ENABLE_MEDICATION_ENRICHMENT", "false").lower() in ["true", "1", "yes"]
        if "MEDICATION_ENRICHMENT_MODE" in enrichment_args:
            config.enrichment.enrichment_mode = enrichment_args.get("MEDICATION_ENRICHMENT_MODE", "hybrid")
        if "USE_MEDICATION_LOOKUP_TABLE" in enrichment_args:
            config.enrichment.use_lookup_table = enrichment_args.get("USE_MEDICATION_LOOKUP_TABLE", "true").lower() in ["true", "1", "yes"]
except:
    pass

catalog_nm = config.database.catalog_name

# Initialize Spark with Iceberg support
spark = (SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{catalog_nm}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{catalog_nm}.warehouse", config.database.s3_bucket)
    .config(f"spark.sql.catalog.{catalog_nm}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{catalog_nm}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.glue.lakeformation-enabled", "true")
    .config("spark.sql.catalog.glue_catalog.glue.id", config.database.table_catalog_id)
    # Performance optimizations
    .config("spark.sql.shuffle.partitions", str(config.spark.shuffle_partitions))
    .config("spark.sql.adaptive.enabled", str(config.spark.adaptive_enabled).lower())
    .config("spark.sql.adaptive.coalescePartitions.enabled", str(config.spark.adaptive_coalesce_enabled).lower())
    .config("spark.sql.adaptive.skewJoin.enabled", str(config.spark.adaptive_skew_join_enabled).lower())
    .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
    .config("spark.sql.autoBroadcastJoinThreshold", str(config.spark.auto_broadcast_threshold))
    .config("spark.sql.broadcastTimeout", str(config.spark.broadcast_timeout))
    .config("spark.network.timeout", config.spark.network_timeout)
    .config("spark.executor.heartbeatInterval", config.spark.executor_heartbeat_interval)
    .config("spark.sql.broadcastExchangeMaxThreadThreshold", "8")
    .config("spark.sql.files.maxPartitionBytes", config.spark.max_partition_bytes)
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", config.spark.advisory_partition_size)
    .getOrCreate())

sc = spark.sparkContext
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
# END NOTE TO AI


def convert_to_dynamic_frames(main_df, identifiers_df):
    """Convert DataFrames to DynamicFrames with proper type casting"""
    # Main medications
    main_flat_df = main_df.select(
        F.col("medication_id").cast(StringType()).alias("medication_id"),
        F.col("resource_type").cast(StringType()).alias("resource_type"),
        F.col("code").cast(StringType()).alias("code"),  # SUPER column as JSON string
        F.col("primary_code").cast(StringType()).alias("primary_code"),
        F.col("primary_system").cast(StringType()).alias("primary_system"),
        F.col("primary_text").cast(StringType()).alias("primary_text"),
        F.col("status").cast(StringType()).alias("status"),
        F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
        F.col("created_at").cast(TimestampType()).alias("created_at"),
        F.col("updated_at").cast(TimestampType()).alias("updated_at")
    )
    
    main_dynamic_frame = DynamicFrame.fromDF(main_flat_df, glueContext, "main_medication_dynamic_frame")
    
    # Convert identifiers DataFrame
    identifiers_flat_df = identifiers_df.select(
        F.col("medication_id").cast(StringType()).alias("medication_id"),
        F.col("identifier_system").cast(StringType()).alias("identifier_system"),
        F.col("identifier_value").cast(StringType()).alias("identifier_value")
    )
    identifiers_dynamic_frame = DynamicFrame.fromDF(identifiers_flat_df, glueContext, "identifiers_dynamic_frame")
    
    return main_dynamic_frame, identifiers_dynamic_frame


def main():
    """Main ETL process"""
    start_time = datetime.now()
    try:
        logger.info("=" * 80)
        logger.info("🚀 STARTING ENHANCED FHIR MEDICATION ETL PROCESS (v2)")
        logger.info("=" * 80)
        
        # Display TEST MODE banner if enabled
        if config.processing.test_mode:
            logger.info("")
            logger.info("⚠️" * 40)
            logger.info("⚠️  TEST MODE ENABLED - NO DATA WILL BE WRITTEN TO REDSHIFT  ⚠️")
            logger.info("⚠️" * 40)
            logger.info("")
        
        logger.info(f"⏰ Job started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📊 Source: {config.database.database_name}.{config.database.table_name}")
        logger.info(f"🎯 Target: {'DRY RUN (no writes)' if config.processing.test_mode else 'Redshift (2 tables)'}")
        logger.info("🔄 Process: 7 steps (Read → Filter → Bookmark → Deduplicate → Transform → Convert → Write)")
        
        # Step 1: Read data from Iceberg
        logger.info("\n" + "=" * 50)
        logger.info("📥 STEP 1: READING DATA FROM ICEBERG")
        logger.info("=" * 50)
        table_name_full = f"{catalog_nm}.{config.database.database_name}.{config.database.table_name}"
        logger.info(f"Reading from table: {table_name_full}")
        df_raw = spark.table(table_name_full)
        
        # Apply sampling if configured
        if config.processing.use_sample:
            logger.info(f"⚠️  TESTING MODE: Sampling {config.processing.sample_size} records")
            df_raw = df_raw.limit(config.processing.sample_size)
        
        total_records = df_raw.count()
        logger.info(f"📊 Read {total_records:,} raw medication records from Iceberg")
        
        # Step 2: Apply Bookmark Filter
        logger.info("\n" + "=" * 50)
        logger.info("📌 STEP 2: APPLYING BOOKMARK FILTER")
        logger.info("=" * 50)
        bookmark_timestamp = get_bookmark_from_redshift(glueContext, TableNames.MEDICATIONS, config.database, config.processing)
        medication_df = filter_by_bookmark(df_raw, bookmark_timestamp)
        total_records_after_bookmark = medication_df.count()
        logger.info(f"✅ Bookmark filter applied - {total_records_after_bookmark:,} records to process")
        
        # Step 3: Deduplicate medications
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 3: DEDUPLICATING MEDICATIONS")
        logger.info("=" * 50)
        medication_df = deduplicate_entities(medication_df, "medication")
        total_records_after_dedup = medication_df.count()
        logger.info(f"✅ Deduplication completed - {total_records_after_dedup:,} unique medications remaining")
        
        # Step 3.5: Ensure lookup table exists (if enrichment is enabled)
        enrichment_enabled = config.enrichment is not None and config.enrichment.enable_enrichment
        if enrichment_enabled and config.enrichment.use_lookup_table:
            logger.info("\n" + "=" * 50)
            logger.info("🔧 STEP 3.5: ENSURING LOOKUP TABLE EXISTS")
            logger.info("=" * 50)
            try:
                from utils.lookup_table import ensure_lookup_table_exists
                ensure_lookup_table_exists(glueContext, config.database, spark)
            except Exception as e:
                logger.warning(f"Could not ensure lookup table exists: {str(e)}")
                logger.warning("Enrichment will continue, but lookup table may not be available")
        
        # Step 4: Transform main medication data (with optional enrichment)
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 4: TRANSFORMING MAIN MEDICATION DATA")
        logger.info("=" * 50)
        
        # Log enrichment status
        if enrichment_enabled:
            logger.info("🔍 Medication code enrichment: ENABLED")
            logger.info(f"   Enrichment mode: {config.enrichment.enrichment_mode if config.enrichment else 'N/A'}")
            logger.info(f"   Use lookup table: {config.enrichment.use_lookup_table if config.enrichment else 'N/A'}")
            main_medication_df = transform_and_enrich_medication_data(
                medication_df,
                enable_enrichment=True,
                enrichment_mode=config.enrichment.enrichment_mode if config.enrichment else "hybrid",
                use_lookup_table=config.enrichment.use_lookup_table if config.enrichment else True,
                glue_context=glueContext,
                database_config=config.database
            )
        else:
            logger.info("🔍 Medication code enrichment: DISABLED")
            logger.info("   Set ENABLE_MEDICATION_ENRICHMENT=true to enable")
            main_medication_df = transform_main_medication_data(medication_df)
        
        main_count = main_medication_df.count()
        logger.info(f"✅ Transformed {main_count:,} main medication records")
        
        if main_count == 0:
            logger.error("❌ No main medication records after transformation!")
            return
        
        # Step 5: Transform child tables
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 5: TRANSFORMING CHILD TABLES")
        logger.info("=" * 50)
        
        medication_identifiers_df = transform_medication_identifiers(medication_df)
        identifiers_count = medication_identifiers_df.count()
        logger.info(f"✅ Transformed {identifiers_count:,} identifier records")
        
        # Step 6: Convert to DynamicFrames
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 6: CONVERTING TO DYNAMICFRAMES")
        logger.info("=" * 50)
        main_dynamic_frame, identifiers_dynamic_frame = convert_to_dynamic_frames(
            main_medication_df,
            medication_identifiers_df
        )
        logger.info("✅ Converted all DataFrames to DynamicFrames")
        
        # Resolve choice types for Redshift compatibility
        logger.info("Resolving choice types for Redshift compatibility...")
        main_resolved_frame = main_dynamic_frame.resolveChoice(
            specs=[
                ("medication_id", "cast:string"),
                ("resource_type", "cast:string"),
                ("code", "cast:string"),
                ("primary_code", "cast:string"),
                ("primary_system", "cast:string"),
                ("primary_text", "cast:string"),
                ("status", "cast:string"),
                ("meta_last_updated", "cast:timestamp"),
                ("created_at", "cast:timestamp"),
                ("updated_at", "cast:timestamp")
            ]
        )
        
        identifiers_resolved_frame = identifiers_dynamic_frame.resolveChoice(
            specs=[
                ("medication_id", "cast:string"),
                ("meta_last_updated", "cast:timestamp"),
                ("identifier_system", "cast:string"),
                ("identifier_value", "cast:string")
            ]
        )
        
        # Step 7: Write to Redshift
        logger.info("\n" + "=" * 50)
        logger.info("💾 STEP 7: WRITING DATA TO REDSHIFT")
        logger.info("=" * 50)
        
        is_initial_load = (bookmark_timestamp is None)
        
        # Write main medications table
        logger.info("📝 Writing main medications table...")
        main_preactions = f"TRUNCATE TABLE public.{TableNames.MEDICATIONS};" if is_initial_load else ""
        records_written = write_to_redshift_versioned(
            main_resolved_frame, TableNames.MEDICATIONS, "medication_id",
            glueContext, config.database, config.processing, main_preactions
        )
        
        # Write identifiers table
        if identifiers_count > 0:
            logger.info(f"📝 Writing {TableNames.MEDICATION_IDENTIFIERS}...")
            identifiers_preactions = f"TRUNCATE TABLE public.{TableNames.MEDICATION_IDENTIFIERS};" if is_initial_load else ""
            write_to_redshift_simple(
                identifiers_resolved_frame, TableNames.MEDICATION_IDENTIFIERS, 
                glueContext, config.database, config.processing, identifiers_preactions
            )
        else:
            logger.info(f"⏭️  Skipping {TableNames.MEDICATION_IDENTIFIERS} (no records)")
        
        # Step 8: Summary
        logger.info("\n" + "=" * 50)
        logger.info("✅ STEP 8: ETL PROCESS COMPLETED SUCCESSFULLY")
        logger.info("=" * 50)
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"⏰ Job completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"⏱️  Total duration: {duration}")
        logger.info(f"📊 Records processed:")
        logger.info(f"   Main medications: {records_written:,}")
        logger.info(f"   Identifier records: {identifiers_count:,}")
        
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error("❌ ETL PROCESS FAILED")
        logger.error(f"Error: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        import traceback
        logger.error(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()
    job.commit()

