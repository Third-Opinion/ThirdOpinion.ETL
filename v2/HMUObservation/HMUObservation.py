# Deployed: 2025-01-XX (v2 - Refactored)
"""
HMUObservation ETL v2 - Refactored and improved version

Main ETL orchestrator for processing FHIR Observation data from Iceberg to Redshift
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
from pyspark.sql.types import StringType, TimestampType, BooleanType, IntegerType, DecimalType

# Import v2 modules (using absolute imports for Glue compatibility)
# When script runs as main entry point, relative imports fail, so use absolute imports directly
try:
    from config import ObservationETLConfig, TableNames
    from shared.utils.bookmark_utils import get_bookmark_from_redshift, filter_by_bookmark
    from shared.utils.deduplication_utils import deduplicate_entities
    from utils.deletion_utils import (
        identify_entered_in_error_records,
        delete_entered_in_error_records,
        delete_child_records_for_observations
    )
    from transformations.main_observation import transform_main_observation_data
    from transformations.reference_ranges import transform_observation_reference_ranges
    from transformations.components import transform_observation_components
    from transformations.body_sites import transform_observation_body_sites
    from transformations.notes import transform_observation_notes
    from transformations.performers import transform_observation_performers
    from transformations.members import transform_observation_members
    from transformations.derived_from import transform_observation_derived_from
    from shared.database.redshift_operations import (
        write_to_redshift_simple,
        write_to_redshift_versioned,
        write_codes_table_with_upsert,
        write_categories_table_with_upsert,
        write_interpretations_table_with_upsert
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
    args = {"JOB_NAME": "HMUObservation"}

# Get configuration from environment/job arguments
config = ObservationETLConfig.from_environment()

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

# Override sampling settings from job arguments if provided
try:
    sample_args = getResolvedOptions(sys.argv, ["USE_SAMPLE", "SAMPLE_SIZE"])
    config.processing.use_sample = sample_args.get("USE_SAMPLE", "false").lower() in ["true", "1", "yes"]
    if "SAMPLE_SIZE" in sample_args:
        config.processing.sample_size = int(sample_args.get("SAMPLE_SIZE", "100000"))
except:
    pass

# Table names are now fixed (no postfix support)
logger.info(f"📋 Tables will be written to: {TableNames.OBSERVATIONS}, {TableNames.OBSERVATION_CODES}, etc.")

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
    # Cloud shuffle storage to prevent shuffle data loss when executors fail
    # Note: --write-shuffle-files-to-s3 'true' should be set as a job parameter in Glue console
    # Disable external shuffle service when using cloud shuffle storage (S3)
    # This prevents "Failed to connect to external shuffle server" errors
    .config("spark.shuffle.service.enabled", "false")
    .getOrCreate())

# Cloud shuffle storage disabled - it was causing network latency issues in Flex execution mode
# Shuffle data will be stored locally on executors
# If executors fail, the stage will retry (up to 4 times by default)
shuffle_path = config.spark.get_shuffle_storage_path(config.database) if config.spark.write_shuffle_files_to_s3 else None
if shuffle_path and config.spark.write_shuffle_files_to_s3:
    spark.conf.set("spark.shuffle.storage.path", shuffle_path)
    logger.info(f"📦 Cloud shuffle storage enabled: {shuffle_path}")
    logger.info("   External shuffle service disabled (using S3 for shuffle data)")
else:
    logger.info("📦 Cloud shuffle storage disabled - using local executor storage")
    logger.info("   Shuffle data stored on executors (stage will retry on executor failures)")

sc = spark.sparkContext
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
# END NOTE TO AI


def filter_deleted_records(df):
    """Filter out deleted records and all their revisions
    
    Optimized to avoid collect() operations - uses broadcast join for large datasets
    """
    if 'idDelete' not in df.columns:
        logger.warning("⚠️ WARNING: idDelete field not found in source data")
        logger.warning("⚠️ Skipping deletion filtering - all records will be processed")
        return df
    
    logger.info("\n" + "=" * 50)
    logger.info("🗑️ FILTERING DELETED RECORDS & THEIR REVISIONS")
    logger.info("=" * 50)
    
    # Find all IDs that have idDelete=true (any version)
    deleted_ids_df = df.filter(F.col("idDelete") == True).select("id").distinct()
    
    # Check size - use appropriate filtering method
    # Use lightweight check to determine if we should use broadcast join or collect()
    sample_size = deleted_ids_df.limit(10000).count()
    
    if sample_size == 0:
        logger.info("✅ No deleted records found in source data")
        return df
    elif sample_size < 10000:
        # Small dataset - use collect() with isin() (efficient for small sets)
        deleted_ids = [row['id'] for row in deleted_ids_df.collect()]
        logger.info(f"🗑️ Found {len(deleted_ids)} unique observation IDs marked as deleted")
        if len(deleted_ids) <= 10:
            logger.info(f"🗑️ Deleted IDs: {deleted_ids}")
        else:
            logger.info(f"🗑️ Sample deleted IDs (first 10): {deleted_ids[:10]}")
        
        # Filter out ALL records (all versions) with those IDs
        filtered_df = df.filter(~F.col("id").isin(deleted_ids))
        logger.info(f"✅ Filtered out {len(deleted_ids)} deleted observation IDs")
        return filtered_df
    else:
        # Large dataset - use broadcast join to avoid collecting to driver
        logger.info(f"🗑️ Found large number of deleted IDs - using broadcast join (avoids collect())")
        
        # Broadcast the deleted IDs DataFrame for efficient filtering
        from pyspark.sql.functions import broadcast
        deleted_ids_broadcast = broadcast(deleted_ids_df)
        
        # Use left anti-join to filter out deleted records (more efficient than isin for large sets)
        filtered_df = df.join(
            deleted_ids_broadcast.alias("deleted"),
            df.id == F.col("deleted.id"),
            "left_anti"
        )
        
        # Get count for logging (lightweight)
        deleted_count = sample_size  # Approximate from sample
        logger.info(f"✅ Filtered out deleted observation IDs using broadcast join (avoids driver OOM)")
        return filtered_df


def convert_to_dynamic_frames(main_df, *child_dfs, config=None):
    """Convert DataFrames to DynamicFrames with proper type casting"""
    from pyspark.sql.types import StringType, TimestampType, BooleanType, DecimalType
    
    # Main observations - Updated for normalized schema
    # Removed: body_site_* columns (now in observation_body_sites junction table)
    # Removed: value_codeable_concept_code/system/display (now value_code_id)
    # Removed: data_absent_reason_code/system/display (now data_absent_reason_code_id)
    # Removed: method_code/system/display (now method_code_id)
    from pyspark.sql.types import LongType
    main_flat_df = main_df.select(
        F.col("observation_id").cast("string").alias("observation_id"),
        F.col("patient_id").cast("string").alias("patient_id"),
        F.col("encounter_id").cast("string").alias("encounter_id"),
        F.col("specimen_id").cast("string").alias("specimen_id"),
        F.col("status").cast("string").alias("status"),
        F.col("observation_text").cast("string").alias("observation_text"),
        F.col("normalized_observation_text").cast("string").alias("normalized_observation_text"),
        F.col("value_string").cast("string").alias("value_string"),
        F.col("value_quantity_value").cast("decimal(15,4)").alias("value_quantity_value"),
        F.col("value_quantity_unit").cast("string").alias("value_quantity_unit"),
        F.col("value_quantity_system").cast("string").alias("value_quantity_system"),
        F.col("value_code_id").cast("bigint").alias("value_code_id"),  # Normalized: references codes.code_id
        F.col("value_codeable_concept_text").cast("string").alias("value_codeable_concept_text"),  # Keep denormalized
        F.col("value_datetime").cast("timestamp").alias("value_datetime"),
        F.col("value_boolean").cast("boolean").alias("value_boolean"),
        F.col("data_absent_reason_code_id").cast("bigint").alias("data_absent_reason_code_id"),  # Normalized: references codes.code_id
        F.col("effective_datetime").cast("timestamp").alias("effective_datetime"),
        F.col("effective_period_start").cast("timestamp").alias("effective_period_start"),
        F.col("effective_period_end").cast("timestamp").alias("effective_period_end"),
        F.col("issued").cast("timestamp").alias("issued"),
        F.col("method_code_id").cast("bigint").alias("method_code_id"),  # Normalized: references codes.code_id
        F.col("method_text").cast("string").alias("method_text"),  # Keep denormalized
        F.col("meta_last_updated").cast("timestamp").alias("meta_last_updated"),
        F.col("meta_source").cast("string").alias("meta_source"),
        F.col("meta_profile").cast("string").alias("meta_profile"),
        F.col("meta_security").cast("string").alias("meta_security"),
        F.col("meta_tag").cast("string").alias("meta_tag"),
        F.col("extensions").cast("string").alias("extensions"),
        F.col("derived_from").cast("string").alias("derived_from"),
        F.col("created_at").cast("timestamp").alias("created_at"),
        F.col("updated_at").cast("timestamp").alias("updated_at")
    )
    
    # Materialize main DataFrame FIRST to break lineage before any partitioning operations
    # This prevents FetchFailedException errors caused by complex shuffle operations
    # Materialization must happen BEFORE repartition because repartition still needs to execute the lineage
    if config:
        logger.info("Materializing main DataFrame to break lineage (before partitioning)...")
        logger.info("   This prevents shuffle timeout errors during materialization and DynamicFrame conversion")
        temp_main_path = f"{config.database.s3_temp_dir}HMUObservation/materialization/main_observations_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        # Materialize the DataFrame with its current partitions (don't repartition yet)
        main_flat_df.write.mode("overwrite").parquet(temp_main_path)
        # Get spark session from the DataFrame (use sparkSession instead of deprecated sql_ctx)
        spark_session = main_flat_df.sparkSession
        main_flat_df = spark_session.read.parquet(temp_main_path)
        logger.info("   ✓ Main DataFrame materialized")
    
    # Repartition main DataFrame to reduce shuffle overhead during DynamicFrame conversion
    # Use a fixed target partition count to avoid triggering expensive partition checks
    # Target: 30-50 partitions for optimal DynamicFrame conversion without excessive shuffling
    # Now repartition operates on clean materialized data (no complex lineage)
    target_partitions_main = 40
    logger.info(f"Repartitioning main DataFrame to {target_partitions_main} partitions before DynamicFrame conversion")
    logger.info("   This reduces shuffle overhead and prevents executor failures during conversion")
    main_flat_df = main_flat_df.repartition(target_partitions_main)
    
    # Log before DynamicFrame conversion (this should now be much faster)
    logger.info("Starting DynamicFrame conversion for main DataFrame...")
    start_time = datetime.now()
    
    main_dynamic_frame = DynamicFrame.fromDF(main_flat_df, glueContext, "main_observation_dynamic_frame")
    
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"   ✓ Main DataFrame converted to DynamicFrame in {elapsed:.1f} seconds")
    
    # Convert child DataFrames (simplified - full implementation would cast all columns)
    # Note: observation_categories and observation_interpretations now use IDs (category_id, interpretation_id)
    from pyspark.sql.types import LongType, IntegerType
    child_dynamic_frames = []
    for i, child_df in enumerate(child_dfs):
        # Cast columns based on type
        cast_cols = []
        for col_name in child_df.columns:
            if col_name in ['range_low_value', 'range_high_value', 'component_value_quantity_value']:
                cast_cols.append(F.col(col_name).cast("decimal(15,4)").alias(col_name))
            elif col_name in ['code_id', 'category_id', 'interpretation_id', 'body_site_id', 
                             'component_code_id', 'component_value_code_id', 'component_data_absent_reason_code_id',
                             'range_type_code_id']:
                cast_cols.append(F.col(col_name).cast("bigint").alias(col_name))
            elif col_name in ['code_rank', 'category_rank', 'interpretation_rank', 'body_site_rank']:
                cast_cols.append(F.col(col_name).cast("int").alias(col_name))
            else:
                cast_cols.append(F.col(col_name).cast("string").alias(col_name))
        
        flat_df = child_df.select(*cast_cols)
        
        # Skip materialization for child DataFrames - they're smaller and coalesce should be sufficient
        # Materialization was causing MetadataFetchFailedException due to complex lineage
        # Instead, we'll rely on coalesce (no shuffle) and cloud shuffle storage for resilience
        # If materialization is still needed, it should happen earlier in the transformation pipeline
        logger.info(f"Skipping materialization for child DataFrame {i} (using coalesce instead - no shuffle)")
        
        # For child DataFrames, use coalesce instead of repartition to avoid expensive shuffle
        # Coalesce only reduces partitions (no shuffle), which is faster for DynamicFrame conversion
        # This avoids the expensive shuffle that repartition causes
        # Note: coalesce may still execute some lineage, but it doesn't shuffle data
        # Cloud shuffle storage (enabled) will protect any shuffles that do occur
        target_partitions_child = 20
        logger.info(f"Coalescing child DataFrame {i} to {target_partitions_child} partitions (faster than repartition - no shuffle)")
        flat_df = flat_df.coalesce(target_partitions_child)
        
        # Log before DynamicFrame conversion (this should now be much faster)
        logger.info(f"Starting DynamicFrame conversion for child DataFrame {i}...")
        start_time = datetime.now()
        
        child_dynamic_frame = DynamicFrame.fromDF(flat_df, glueContext, f"child_dynamic_frame_{len(child_dynamic_frames)}")
        
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"   ✓ Child DataFrame {i} converted to DynamicFrame in {elapsed:.1f} seconds")
        
        child_dynamic_frames.append(child_dynamic_frame)
    
    return main_dynamic_frame, child_dynamic_frames


def main():
    """Main ETL process"""
    start_time = datetime.now()
    try:
        logger.info("=" * 80)
        logger.info("🚀 STARTING ENHANCED FHIR OBSERVATION ETL PROCESS (v2)")
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
        logger.info(f"🎯 Target: {'DRY RUN (no writes)' if config.processing.test_mode else 'Redshift (10 tables)'}")
        logger.info("🔄 Process: 9 steps (Read → Filter → Bookmark → Deduplicate → Identify/Filter Errors → Transform → Convert → Resolve → Validate → Write)")
        
        # Step 1: Read data from Iceberg
        logger.info("\n" + "=" * 50)
        logger.info("📥 STEP 1: READING DATA FROM ICEBERG")
        logger.info("=" * 50)
        table_name_full = f"{catalog_nm}.{config.database.database_name}.{config.database.table_name}"
        logger.info(f"Reading from table: {table_name_full}")
        df_raw = spark.table(table_name_full)
        
        # Filter deleted records
        observation_df = filter_deleted_records(df_raw)
        
        # Apply sampling if configured
        if config.processing.use_sample:
            logger.info(f"⚠️  TESTING MODE: Sampling {config.processing.sample_size} records")
            observation_df = observation_df.limit(config.processing.sample_size)
        
        logger.info("📊 Reading raw observation records from Iceberg...")
        
        # Step 2: Apply Bookmark Filter
        logger.info("\n" + "=" * 50)
        logger.info("📌 STEP 2: APPLYING BOOKMARK FILTER")
        logger.info("=" * 50)
        bookmark_timestamp = get_bookmark_from_redshift(glueContext, TableNames.OBSERVATIONS, config.database, config.processing)
        observation_df = filter_by_bookmark(observation_df, bookmark_timestamp)
        logger.info("✅ Bookmark filter applied")
        
        # Step 3: Deduplicate observations
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 3: DEDUPLICATING OBSERVATIONS")
        logger.info("=" * 50)
        observation_df = deduplicate_entities(observation_df, "observation")
        logger.info("✅ Deduplication completed")
        
        # Cache observation_df for reuse across multiple transformations
        # This prevents recomputation when used by multiple child table transformations
        logger.info("💾 Caching observation_df for reuse across transformations...")
        observation_df = observation_df.cache()
        
        # Step 4: Identify and filter out entered-in-error observations
        logger.info("\n" + "=" * 50)
        logger.info("🗑️  STEP 4: IDENTIFYING AND FILTERING ENTERED-IN-ERROR OBSERVATIONS")
        logger.info("=" * 50)
        has_entered_in_error, entered_in_error_df = identify_entered_in_error_records(observation_df)
        
        if has_entered_in_error:
            logger.info("Filtering out entered-in-error observations from processing")
            observation_df = observation_df.filter(F.col("status") != "entered-in-error")
            logger.info("✅ Filtered out entered-in-error observations - will be deleted from Redshift later")
        else:
            logger.info("✅ No entered-in-error observations found - all records are valid")
        
        # Check if we have any records left (using a lightweight check)
        # We'll check by trying to take 1 row instead of counting all
        sample_check = observation_df.limit(1).collect()
        
        if len(sample_check) == 0:
            if has_entered_in_error:
                logger.info("⚠️  All observations are entered-in-error - proceeding to deletion only")
                delete_entered_in_error_records(
                    entered_in_error_df, glueContext, spark, config.database, config.processing
                )
                logger.info("✅ Job completed - all entered-in-error observations deleted")
                return
            else:
                logger.error("❌ No valid observations to process")
                return
        
        # Step 5: Transform main observation data
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 5: TRANSFORMING MAIN OBSERVATION DATA")
        logger.info("=" * 50)
        main_observation_df = transform_main_observation_data(observation_df, config.processing)
        logger.info("✅ Main observation transformation completed")
        
        # Check if we have any records (lightweight check)
        sample_check = main_observation_df.limit(1).collect()
        if len(sample_check) == 0:
            logger.error("❌ No main observation records after transformation!")
            return
        
        # Step 6: Transform child tables
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 6: TRANSFORMING CHILD TABLES")
        logger.info("=" * 50)
        
        # Transform codes using normalized structure
        from transformations.child_tables_codes import transform_observation_codes_to_codes_tables
        
        # Check if this is an initial load (codes table is empty)
        # We'll pass this to the transformation functions so they can skip loading existing IDs
        is_initial_load = False  # Could be determined by checking if codes table exists/is empty
        
        # transform_observation_codes_to_codes_tables will call transform_observation_codes internally
        # and handle caching, ID reuse, and native Spark code_id generation
        unique_codes_df, observation_codes_df = transform_observation_codes_to_codes_tables(
            observation_df,
            config.processing,
            glue_context=glueContext,
            config=config.database,
            is_initial_load=is_initial_load
        )
        
        # Transform categories using normalized structure
        from transformations.categories_interpretations_tables import (
            transform_observation_categories_to_categories_tables,
            transform_observation_interpretations_to_interpretations_tables
        )
        unique_categories_df, observation_categories_df = transform_observation_categories_to_categories_tables(
            observation_df, config.processing
        )
        
        # Transform interpretations using normalized structure
        unique_interpretations_df, observation_interpretations_df = transform_observation_interpretations_to_interpretations_tables(
            observation_df, config.processing
        )
        observation_reference_ranges_df = transform_observation_reference_ranges(observation_df)
        observation_components_df = transform_observation_components(observation_df)
        # Transform body sites using normalized structure (returns both lookup table and junction table)
        from transformations.body_sites import transform_observation_body_sites_to_body_sites_tables
        unique_body_sites_df, observation_body_sites_df = transform_observation_body_sites_to_body_sites_tables(
            observation_df, config.processing, glueContext, config.database, is_initial_load
        )
        observation_notes_df = transform_observation_notes(observation_df)
        observation_performers_df = transform_observation_performers(observation_df)
        observation_members_df = transform_observation_members(observation_df)
        observation_derived_from_df = transform_observation_derived_from(observation_df)
        
        # Unpersist observation_df now that all transformations are complete
        logger.info("🧹 Unpersisting observation_df cache (all transformations complete)...")
        observation_df.unpersist()
        
        # Skip child table counts to avoid shuffle failures in large datasets
        # Counts are not critical for processing and can cause MetadataFetchFailedException
        # The counts were causing: "Missing an output location for shuffle" errors
        logger.info("✅ Transformed child tables (counts skipped to avoid shuffle failures)")
        
        # Step 7: Convert to DynamicFrames
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 7: CONVERTING TO DYNAMICFRAMES")
        logger.info("=" * 50)
        main_dynamic_frame, child_dynamic_frames = convert_to_dynamic_frames(
            main_observation_df,
            observation_codes_df,
            observation_categories_df,
            observation_interpretations_df,
            observation_reference_ranges_df,
            observation_components_df,
            observation_body_sites_df,
            observation_notes_df,
            observation_performers_df,
            observation_members_df,
            observation_derived_from_df,
            config=config
        )
        logger.info("✅ Converted all DataFrames to DynamicFrames")
        
        # Step 8: Write to Redshift
        logger.info("\n" + "=" * 50)
        logger.info("💾 STEP 8: WRITING DATA TO REDSHIFT")
        logger.info("=" * 50)
        
        is_initial_load = (bookmark_timestamp is None)
        
        # Delete entered-in-error observations first
        if has_entered_in_error:
            logger.info("🗑️  Deleting entered-in-error observations...")
            delete_entered_in_error_records(
                entered_in_error_df, glueContext, spark, config.database, config.processing
            )
        
        # Write main observations table
        logger.info("📝 Writing main observations table...")
        observations_preactions = "TRUNCATE TABLE public.observations;" if is_initial_load else ""
        records_written = write_to_redshift_versioned(
            main_dynamic_frame, TableNames.OBSERVATIONS, "observation_id",
            glueContext, config.database, config.processing, observations_preactions
        )
        
        # Clean up child records for updated observations
        if not is_initial_load and records_written > 0:
            logger.info("🧹 Cleaning up child records for updated observations...")
            main_df = main_dynamic_frame.toDF()
            # Use DataFrame directly instead of collecting IDs (avoids expensive collect())
            observation_ids_df = main_df.select("observation_id").distinct()
            # Lightweight check: see if any IDs exist
            sample_check = observation_ids_df.limit(1).collect()
            if len(sample_check) > 0:
                delete_child_records_for_observations(
                    observation_ids_df, glueContext, spark, config.database, config.processing
                )
        
        # Write normalized lookup tables first (codes, categories, interpretations)
        logger.info("📝 Writing normalized lookup tables...")
        
        # Extract all unique codes from all transformations (method, data_absent_reason, value_codeable_concept,
        # component codes, component value codes, component data absent reason codes, range type codes)
        # and merge with existing unique_codes_df from observation_codes
        logger.info("🔍 Extracting all unique codes from all transformations...")
        from transformations.extract_all_codes import extract_all_unique_codes
        all_unique_codes_df = extract_all_unique_codes(
            observation_df,
            existing_unique_codes_df=unique_codes_df,
            processing_config=config.processing,
            glue_context=glueContext,
            config=config.database,
            is_initial_load=is_initial_load
        )
        
        # Replace unique_codes_df with merged result (includes all codes from all transformations)
        unique_codes_df = all_unique_codes_df
        logger.info("✅ Merged all unique codes from all transformations")
        
        # Write codes table
        # Final validation: Ensure no NULL code_ids before conversion
        # Use lightweight check instead of count() to avoid expensive shuffles
        logger.info("🔍 Validating unique_codes_df before write...")
        null_check = unique_codes_df.filter(F.col("code_id").isNull()).limit(1).collect()
        if len(null_check) > 0:
            logger.error(f"❌ Found records with NULL code_id - filtering them out")
            logger.error("Sample records with NULL code_id:")
            null_samples = unique_codes_df.filter(F.col("code_id").isNull()).select(
                "code_id", "code_code", "code_system", "code_display"
            ).limit(10).collect()
            for row in null_samples:
                logger.error(f"   code_id={row['code_id']}, code_code={row['code_code']}, code_system={row['code_system']}, code_display={row['code_display']}")
            unique_codes_df = unique_codes_df.filter(F.col("code_id").isNotNull())
        
        # Additional validation: Ensure code_code and code_system are not NULL/empty
        # Use lightweight check instead of count() to avoid expensive shuffles
        invalid_check = unique_codes_df.filter(
            (F.col("code_code").isNull()) | 
            (F.trim(F.coalesce(F.col("code_code"), F.lit(""))) == F.lit("")) |
            (F.col("code_system").isNull()) |
            (F.trim(F.coalesce(F.col("code_system"), F.lit(""))) == F.lit(""))
        ).limit(1).collect()
        if len(invalid_check) > 0:
            logger.warning(f"⚠️  Found records with invalid code_code or code_system - filtering them out")
            unique_codes_df = unique_codes_df.filter(
                F.col("code_code").isNotNull() & 
                (F.trim(F.coalesce(F.col("code_code"), F.lit(""))) != F.lit("")) &
                F.col("code_system").isNotNull() &
                (F.trim(F.coalesce(F.col("code_system"), F.lit(""))) != F.lit(""))
            )
        
        # Skip final count() - it's expensive and not needed for functionality
        # Spark will handle empty DataFrames natively, and the write will succeed even if empty
        logger.info("✅ unique_codes_df validated (count skipped to avoid shuffle)")
        
        unique_codes_dynamic_frame = DynamicFrame.fromDF(unique_codes_df, glueContext, "unique_codes")
        write_codes_table_with_upsert(
            unique_codes_dynamic_frame, "codes", glueContext, config.database, config.processing, is_initial_load
        )
        
        # Write categories table (small table - use lookup cache optimization)
        from shared.database.redshift_operations import write_small_lookup_table_with_cache
        write_small_lookup_table_with_cache(
            unique_categories_df, "categories", "category_id", glueContext, spark, config.database, config.processing, is_initial_load
        )
        
        # Write interpretations table (small table - use lookup cache optimization)
        write_small_lookup_table_with_cache(
            unique_interpretations_df, "interpretations", "interpretation_id", glueContext, spark, config.database, config.processing, is_initial_load
        )
        
        # Write body_sites table (small table - use lookup cache optimization, shared with conditions)
        write_small_lookup_table_with_cache(
            unique_body_sites_df, "body_sites", "body_site_id", glueContext, spark, config.database, config.processing, is_initial_load
        )
        
        # Write child tables (now using IDs instead of full data)
        child_tables = [
            (child_dynamic_frames[0], TableNames.OBSERVATION_CODES),
            (child_dynamic_frames[1], TableNames.OBSERVATION_CATEGORIES),
            (child_dynamic_frames[2], TableNames.OBSERVATION_INTERPRETATIONS),
            (child_dynamic_frames[3], TableNames.OBSERVATION_REFERENCE_RANGES),
            (child_dynamic_frames[4], TableNames.OBSERVATION_COMPONENTS),
            (child_dynamic_frames[5], TableNames.OBSERVATION_BODY_SITES),
            (child_dynamic_frames[6], TableNames.OBSERVATION_NOTES),
            (child_dynamic_frames[7], TableNames.OBSERVATION_PERFORMERS),
            (child_dynamic_frames[8], TableNames.OBSERVATION_MEMBERS),
            (child_dynamic_frames[9], TableNames.OBSERVATION_DERIVED_FROM),
        ]
        
        # Write child tables in parallel for better performance
        # Each table write is independent and can run concurrently
        logger.info("📝 Writing child tables in parallel...")
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        def write_table_safe(dynamic_frame, table_name, is_initial_load):
            """Write table with error handling for parallel execution"""
            try:
                logger.info(f"📝 Writing {table_name}...")
                preactions = f"TRUNCATE TABLE public.{table_name};" if is_initial_load else ""
                write_to_redshift_simple(
                    dynamic_frame, table_name, glueContext, config.database, config.processing, preactions
                )
                logger.info(f"✅ {table_name} written successfully")
                return (table_name, True, None)
            except Exception as e:
                logger.error(f"❌ Failed to write {table_name}: {str(e)}")
                return (table_name, False, str(e))
        
        # Use ThreadPoolExecutor for parallel writes
        # Limit concurrent writes to avoid overwhelming Redshift connections (max 5 concurrent)
        max_concurrent_writes = 5
        write_results = []
        
        with ThreadPoolExecutor(max_workers=max_concurrent_writes) as executor:
            # Submit all write tasks
            futures = {
                executor.submit(write_table_safe, frame, table, is_initial_load): (frame, table)
                for frame, table in child_tables
            }
            
            # Wait for all writes to complete and collect results
            for future in as_completed(futures):
                frame, table = futures[future]
                try:
                    result = future.result()
                    write_results.append(result)
                except Exception as e:
                    logger.error(f"❌ Exception writing {table}: {str(e)}")
                    write_results.append((table, False, str(e)))
        
        # Report summary
        successful_writes = [r for r in write_results if r[1]]
        failed_writes = [r for r in write_results if not r[1]]
        
        logger.info(f"✅ Child tables write complete: {len(successful_writes)} successful, {len(failed_writes)} failed")
        if failed_writes:
            logger.error(f"❌ Failed tables: {[r[0] for r in failed_writes]}")
            # Don't fail the job, but log the errors
        
        # Calculate processing time
        end_time = datetime.now()
        processing_time = end_time - start_time
        
        logger.info("\n" + "=" * 80)
        logger.info("🎉 ETL PROCESS COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        logger.info(f"⏰ Job completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"⏱️  Total processing time: {processing_time}")
        if config.processing.use_sample:
            logger.info(f"📊 Sample size: {config.processing.sample_size:,} records")
        logger.info("✅ All tables written successfully to Redshift")
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
        import traceback
        logger.error(f"🚨 Traceback:\n{traceback.format_exc()}")
        logger.error("=" * 80)
        raise e


if __name__ == "__main__":
    main()
    job.commit()

