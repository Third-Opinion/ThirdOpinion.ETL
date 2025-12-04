# Deployed: 2025-01-XX (v2 - Refactored)
"""
HMUMedicationRequest ETL v2 - Refactored and improved version

Main ETL orchestrator for processing FHIR MedicationRequest data from Iceberg to Redshift
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
from pyspark.sql.types import StringType, TimestampType, BooleanType

# Import v2 modules (using absolute imports for Glue compatibility)
try:
    from config import MedicationRequestETLConfig, TableNames
    from shared.config import SparkConfig, DatabaseConfig, ProcessingConfig
    from shared.utils.bookmark_utils import get_bookmark_from_redshift, filter_by_bookmark
    from shared.utils.deduplication_utils import deduplicate_entities
    from transformations.main_medication_request import transform_main_medication_request_data
    from transformations.child_tables import (
        transform_medication_request_identifiers,
        transform_medication_request_notes,
        transform_medication_request_dosage_instructions,
        transform_medication_request_categories
    )
    from shared.database.redshift_operations import (
        write_to_redshift_simple,
        write_to_redshift_versioned
    )
except ImportError as e:
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
    args = {"JOB_NAME": "HMUMedicationRequest"}

config = MedicationRequestETLConfig.from_environment()

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

catalog_nm = config.database.catalog_name

spark = (SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{catalog_nm}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{catalog_nm}.warehouse", config.database.s3_bucket)
    .config(f"spark.sql.catalog.{catalog_nm}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{catalog_nm}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.glue.lakeformation-enabled", "true")
    .config("spark.sql.catalog.glue_catalog.glue.id", config.database.table_catalog_id)
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


def convert_to_dynamic_frames(main_df, identifiers_df, notes_df, dosage_df, categories_df):
    """Convert DataFrames to DynamicFrames with proper type casting"""
    main_flat_df = main_df.select(
        F.col("medication_request_id").cast(StringType()).alias("medication_request_id"),
        F.col("patient_id").cast(StringType()).alias("patient_id"),
        F.col("encounter_id").cast(StringType()).alias("encounter_id"),
        F.col("medication_id").cast(StringType()).alias("medication_id"),
        F.col("medication_display").cast(StringType()).alias("medication_display"),
        F.col("status").cast(StringType()).alias("status"),
        F.col("intent").cast(StringType()).alias("intent"),
        F.col("reported_boolean").cast(BooleanType()).alias("reported_boolean"),
        F.col("authored_on").cast(TimestampType()).alias("authored_on"),
        F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
        F.col("created_at").cast(TimestampType()).alias("created_at"),
        F.col("updated_at").cast(TimestampType()).alias("updated_at")
    )
    main_dynamic_frame = DynamicFrame.fromDF(main_flat_df, glueContext, "main_medication_request_dynamic_frame")
    
    identifiers_flat_df = identifiers_df.select(
        F.col("medication_request_id").cast(StringType()).alias("medication_request_id"),
        F.col("identifier_system").cast(StringType()).alias("identifier_system"),
        F.col("identifier_value").cast(StringType()).alias("identifier_value")
    )
    identifiers_dynamic_frame = DynamicFrame.fromDF(identifiers_flat_df, glueContext, "identifiers_dynamic_frame")
    
    notes_flat_df = notes_df.select(
        F.col("medication_request_id").cast(StringType()).alias("medication_request_id"),
        F.col("note_text").cast(StringType()).alias("note_text")
    )
    notes_dynamic_frame = DynamicFrame.fromDF(notes_flat_df, glueContext, "notes_dynamic_frame")
    
    dosage_flat_df = dosage_df.select(
        F.col("medication_request_id").cast(StringType()).alias("medication_request_id"),
        F.col("dosage_text").cast(StringType()).alias("dosage_text"),
        F.col("dosage_timing_frequency").cast("int").alias("dosage_timing_frequency"),
        F.col("dosage_timing_period").cast("double").alias("dosage_timing_period"),
        F.col("dosage_timing_period_unit").cast(StringType()).alias("dosage_timing_period_unit"),
        F.col("dosage_route_code").cast(StringType()).alias("dosage_route_code"),
        F.col("dosage_route_system").cast(StringType()).alias("dosage_route_system"),
        F.col("dosage_route_display").cast(StringType()).alias("dosage_route_display"),
        F.col("dosage_dose_value").cast("double").alias("dosage_dose_value"),
        F.col("dosage_dose_unit").cast(StringType()).alias("dosage_dose_unit"),
        F.col("dosage_dose_system").cast(StringType()).alias("dosage_dose_system"),
        F.col("dosage_dose_code").cast(StringType()).alias("dosage_dose_code"),
        F.col("dosage_as_needed_boolean").cast(BooleanType()).alias("dosage_as_needed_boolean")
    )
    dosage_dynamic_frame = DynamicFrame.fromDF(dosage_flat_df, glueContext, "dosage_dynamic_frame")
    
    categories_flat_df = categories_df.select(
        F.col("medication_request_id").cast(StringType()).alias("medication_request_id"),
        F.col("category_code").cast(StringType()).alias("category_code"),
        F.col("category_system").cast(StringType()).alias("category_system"),
        F.col("category_display").cast(StringType()).alias("category_display"),
        F.col("category_text").cast(StringType()).alias("category_text")
    )
    categories_dynamic_frame = DynamicFrame.fromDF(categories_flat_df, glueContext, "categories_dynamic_frame")
    
    return (main_dynamic_frame, identifiers_dynamic_frame, notes_dynamic_frame,
            dosage_dynamic_frame, categories_dynamic_frame)


def main():
    """Main ETL process"""
    start_time = datetime.now()
    try:
        logger.info("=" * 80)
        logger.info("🚀 STARTING ENHANCED FHIR MEDICATION REQUEST ETL PROCESS (v2)")
        logger.info("=" * 80)
        
        if config.processing.test_mode:
            logger.info("")
            logger.info("⚠️" * 40)
            logger.info("⚠️  TEST MODE ENABLED - NO DATA WILL BE WRITTEN TO REDSHIFT  ⚠️")
            logger.info("⚠️" * 40)
            logger.info("")
        
        logger.info(f"⏰ Job started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📊 Source: {config.database.database_name}.{config.database.table_name}")
        logger.info(f"🎯 Target: {'DRY RUN (no writes)' if config.processing.test_mode else 'Redshift (5 tables)'}")
        logger.info("🔄 Process: 7 steps (Read → Filter → Bookmark → Deduplicate → Transform → Convert → Write)")
        
        # Step 1: Read data from Iceberg
        logger.info("\n" + "=" * 50)
        logger.info("📥 STEP 1: READING DATA FROM ICEBERG")
        logger.info("=" * 50)
        table_name_full = f"{catalog_nm}.{config.database.database_name}.{config.database.table_name}"
        logger.info(f"Reading from table: {table_name_full}")
        df_raw = spark.table(table_name_full)
        
        if config.processing.use_sample:
            logger.info(f"⚠️  TESTING MODE: Sampling {config.processing.sample_size} records")
            df_raw = df_raw.limit(config.processing.sample_size)
        
        total_records = df_raw.count()
        logger.info(f"📊 Read {total_records:,} raw medication request records from Iceberg")
        
        # Step 2: Apply Bookmark Filter
        logger.info("\n" + "=" * 50)
        logger.info("📌 STEP 2: APPLYING BOOKMARK FILTER")
        logger.info("=" * 50)
        bookmark_timestamp = get_bookmark_from_redshift(glueContext, TableNames.MEDICATION_REQUESTS, config.database, config.processing)
        medication_request_df = filter_by_bookmark(df_raw, bookmark_timestamp)
        total_records_after_bookmark = medication_request_df.count()
        logger.info(f"✅ Bookmark filter applied - {total_records_after_bookmark:,} records to process")
        
        # Step 3: Deduplicate medication requests
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 3: DEDUPLICATING MEDICATION REQUESTS")
        logger.info("=" * 50)
        medication_request_df = deduplicate_entities(medication_request_df, "medicationrequest")
        total_records_after_dedup = medication_request_df.count()
        logger.info(f"✅ Deduplication completed - {total_records_after_dedup:,} unique medication requests remaining")
        
        # Step 4: Transform main medication request data
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 4: TRANSFORMING MAIN MEDICATION REQUEST DATA")
        logger.info("=" * 50)
        main_medication_request_df = transform_main_medication_request_data(medication_request_df)
        main_count = main_medication_request_df.count()
        logger.info(f"✅ Transformed {main_count:,} main medication request records")
        
        if main_count == 0:
            logger.error("❌ No main medication request records after transformation!")
            return
        
        # Step 5: Transform child tables
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 5: TRANSFORMING CHILD TABLES")
        logger.info("=" * 50)
        
        identifiers_df = transform_medication_request_identifiers(medication_request_df)
        notes_df = transform_medication_request_notes(medication_request_df)
        dosage_df = transform_medication_request_dosage_instructions(medication_request_df)
        categories_df = transform_medication_request_categories(medication_request_df)
        
        identifiers_count = identifiers_df.count()
        notes_count = notes_df.count()
        dosage_count = dosage_df.count()
        categories_count = categories_df.count()
        
        logger.info(f"✅ Transformed child tables:")
        logger.info(f"   Identifiers: {identifiers_count:,}, Notes: {notes_count:,}")
        logger.info(f"   Dosage Instructions: {dosage_count:,}, Categories: {categories_count:,}")
        
        # Step 6: Convert to DynamicFrames
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 6: CONVERTING TO DYNAMICFRAMES")
        logger.info("=" * 50)
        (main_dynamic_frame, identifiers_dynamic_frame, notes_dynamic_frame,
         dosage_dynamic_frame, categories_dynamic_frame) = convert_to_dynamic_frames(
            main_medication_request_df, identifiers_df, notes_df, dosage_df, categories_df
        )
        logger.info("✅ Converted all DataFrames to DynamicFrames")
        
        # Step 7: Write to Redshift
        logger.info("\n" + "=" * 50)
        logger.info("💾 STEP 7: WRITING DATA TO REDSHIFT")
        logger.info("=" * 50)
        
        is_initial_load = (bookmark_timestamp is None)
        
        # Write main medication requests table
        logger.info("📝 Writing main medication requests table...")
        main_preactions = f"TRUNCATE TABLE public.{TableNames.MEDICATION_REQUESTS};" if is_initial_load else ""
        records_written = write_to_redshift_versioned(
            main_dynamic_frame, TableNames.MEDICATION_REQUESTS, "medication_request_id",
            glueContext, config.database, config.processing, main_preactions
        )
        
        # Write child tables
        child_tables = [
            (identifiers_dynamic_frame, TableNames.MEDICATION_REQUEST_IDENTIFIERS, identifiers_count),
            (notes_dynamic_frame, TableNames.MEDICATION_REQUEST_NOTES, notes_count),
            (dosage_dynamic_frame, TableNames.MEDICATION_REQUEST_DOSAGE_INSTRUCTIONS, dosage_count),
            (categories_dynamic_frame, TableNames.MEDICATION_REQUEST_CATEGORIES, categories_count),
        ]
        
        for dynamic_frame, table_name, expected_count in child_tables:
            if expected_count > 0:
                logger.info(f"📝 Writing {table_name}...")
                preactions = f"TRUNCATE TABLE public.{table_name};" if is_initial_load else ""
                write_to_redshift_simple(
                    dynamic_frame, table_name, glueContext, config.database, config.processing, preactions
                )
            else:
                logger.info(f"⏭️  Skipping {table_name} (no records)")
        
        # Step 8: Summary
        logger.info("\n" + "=" * 50)
        logger.info("✅ STEP 8: ETL PROCESS COMPLETED SUCCESSFULLY")
        logger.info("=" * 50)
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"⏰ Job completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"⏱️  Total duration: {duration}")
        logger.info(f"📊 Records processed:")
        logger.info(f"   Main medication requests: {records_written:,}")
        logger.info(f"   Child records: {identifiers_count + notes_count + dosage_count + categories_count:,}")
        
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

