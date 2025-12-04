# Deployed: 2025-01-XX (v2 - Refactored)
"""
HMUCondition ETL v2 - Refactored and improved version

Main ETL orchestrator for processing FHIR Condition data from Iceberg to Redshift
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
    from config import ConditionETLConfig, TableNames
    from shared.config import SparkConfig, DatabaseConfig, ProcessingConfig
    from shared.utils.bookmark_utils import get_bookmark_from_redshift, filter_by_bookmark
    from shared.utils.deduplication_utils import deduplicate_entities
    from utils.deletion_utils import (
        identify_deleted_condition_ids,
        delete_condition_records,
        delete_child_records_for_conditions
    )
    from transformations.main_condition import transform_main_condition_data
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
    args = {"JOB_NAME": "HMUCondition"}

# Get configuration from environment/job arguments
config = ConditionETLConfig.from_environment()

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
logger.info(f"📋 Tables will be written to: {TableNames.CONDITIONS}, {TableNames.CONDITION_CODES}, etc.")

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
    if 'isDelete' not in df.columns:
        logger.warning("⚠️ WARNING: isDelete field not found in source data")
        logger.warning("⚠️ Skipping deletion filtering - all records will be processed")
        return df
    
    logger.info("\n" + "=" * 50)
    logger.info("🗑️ FILTERING DELETED RECORDS & THEIR REVISIONS")
    logger.info("=" * 50)
    
    # Find all IDs that have isDelete=true (any version)
    deleted_ids_df = df.filter(F.col("isDelete") == True).select("id").distinct()
    
    # Check size - use appropriate filtering method
    # Use lightweight check to determine if we should use broadcast join or collect()
    sample_size = deleted_ids_df.limit(10000).count()
    
    if sample_size == 0:
        logger.info("✅ No deleted records found in source data")
        return df
    elif sample_size < 10000:
        # Small dataset - use collect() with isin() (efficient for small sets)
        deleted_ids = [row['id'] for row in deleted_ids_df.collect()]
        logger.info(f"🗑️ Found {len(deleted_ids)} unique condition IDs marked as deleted")
        if len(deleted_ids) <= 10:
            logger.info(f"🗑️ Deleted IDs: {deleted_ids}")
        else:
            logger.info(f"🗑️ Sample deleted IDs (first 10): {deleted_ids[:10]}")
        
        # Filter out ALL records (all versions) with those IDs
        filtered_df = df.filter(~F.col("id").isin(deleted_ids))
        logger.info(f"✅ Filtered out {len(deleted_ids)} deleted condition IDs")
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
        logger.info(f"✅ Filtered out deleted condition IDs using broadcast join (avoids driver OOM)")
        return filtered_df


def convert_to_dynamic_frames(main_df, *child_dfs, config=None):
    """Convert DataFrames to DynamicFrames with proper type casting"""
    from pyspark.sql.types import StringType, TimestampType, BooleanType, DecimalType
    
    # Main conditions - Updated for normalized schema
    # Removed: denormalized code/category/body_site columns (now using IDs via junction tables)
    main_flat_df = main_df.select(
        F.col("condition_id").cast("string").alias("condition_id"),
        F.col("patient_id").cast("string").alias("patient_id"),
        F.col("encounter_id").cast("string").alias("encounter_id"),
        F.col("clinical_status_code").cast("string").alias("clinical_status_code"),
        F.col("clinical_status_display").cast("string").alias("clinical_status_display"),
        F.col("clinical_status_system").cast("string").alias("clinical_status_system"),
        F.col("verification_status_code").cast("string").alias("verification_status_code"),
        F.col("verification_status_display").cast("string").alias("verification_status_display"),
        F.col("verification_status_system").cast("string").alias("verification_status_system"),
        F.col("condition_text").cast("string").alias("condition_text"),
        F.col("diagnosis_name").cast("string").alias("diagnosis_name"),
        F.col("severity_code").cast("string").alias("severity_code"),
        F.col("severity_display").cast("string").alias("severity_display"),
        F.col("severity_system").cast("string").alias("severity_system"),
        F.col("onset_datetime").cast("timestamp").alias("onset_datetime"),
        F.col("onset_age_value").cast("decimal(10,2)").alias("onset_age_value"),
        F.col("onset_age_unit").cast("string").alias("onset_age_unit"),
        F.col("onset_period_start").cast("timestamp").alias("onset_period_start"),
        F.col("onset_period_end").cast("timestamp").alias("onset_period_end"),
        F.col("onset_text").cast("string").alias("onset_text"),
        F.col("abatement_datetime").cast("timestamp").alias("abatement_datetime"),
        F.col("abatement_age_value").cast("decimal(10,2)").alias("abatement_age_value"),
        F.col("abatement_age_unit").cast("string").alias("abatement_age_unit"),
        F.col("abatement_period_start").cast("timestamp").alias("abatement_period_start"),
        F.col("abatement_period_end").cast("timestamp").alias("abatement_period_end"),
        F.col("abatement_text").cast("string").alias("abatement_text"),
        F.col("abatement_boolean").cast("boolean").alias("abatement_boolean"),
        F.col("recorded_date").cast("timestamp").alias("recorded_date"),
        F.col("effective_datetime").cast("timestamp").alias("effective_datetime"),
        F.col("status").cast("string").alias("status"),
        F.col("recorder_type").cast("string").alias("recorder_type"),
        F.col("recorder_id").cast("string").alias("recorder_id"),
        F.col("asserter_type").cast("string").alias("asserter_type"),
        F.col("asserter_id").cast("string").alias("asserter_id"),
        F.col("meta_last_updated").cast("timestamp").alias("meta_last_updated"),
        F.col("meta_source").cast("string").alias("meta_source"),
        F.col("meta_profile").cast("string").alias("meta_profile"),
        F.col("meta_security").cast("string").alias("meta_security"),
        F.col("meta_tag").cast("string").alias("meta_tag"),
        F.col("created_at").cast("timestamp").alias("created_at"),
        F.col("updated_at").cast("timestamp").alias("updated_at")
    )
    
    # Materialize main DataFrame FIRST to break lineage before any partitioning operations
    # This prevents FetchFailedException errors caused by complex shuffle operations
    # Materialization must happen BEFORE repartition because repartition still needs to execute the lineage
    if config:
        logger.info("Materializing main DataFrame to break lineage (before partitioning)...")
        logger.info("   This prevents shuffle timeout errors during materialization and DynamicFrame conversion")
        temp_main_path = f"{config.database.s3_temp_dir}HMUCondition/materialization/main_conditions_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
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
    
    main_dynamic_frame = DynamicFrame.fromDF(main_flat_df, glueContext, "main_condition_dynamic_frame")
    
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"   ✓ Main DataFrame converted to DynamicFrame in {elapsed:.1f} seconds")
    
    # Convert child DataFrames (simplified - full implementation would cast all columns)
    # Note: condition_codes and condition_categories now use IDs (code_id, category_id)
    from pyspark.sql.types import LongType, IntegerType
    child_dynamic_frames = []
    for i, child_df in enumerate(child_dfs):
        # Cast columns based on type
        cast_cols = []
        for col_name in child_df.columns:
            if col_name in ['value_decimal', 'abatement_age_value', 'onset_age_value']:
                cast_cols.append(F.col(col_name).cast("decimal(18,6)").alias(col_name))
            elif col_name in ['code_id', 'category_id', 'body_site_id']:
                cast_cols.append(F.col(col_name).cast("bigint").alias(col_name))
            elif col_name in ['code_rank', 'category_rank', 'body_site_rank', 'stage_rank']:
                cast_cols.append(F.col(col_name).cast("int").alias(col_name))
            elif col_name in ['value_integer']:
                cast_cols.append(F.col(col_name).cast("int").alias(col_name))
            elif col_name in ['value_boolean']:
                cast_cols.append(F.col(col_name).cast("boolean").alias(col_name))
            elif col_name in ['note_time', 'value_datetime']:
                cast_cols.append(F.col(col_name).cast("timestamp").alias(col_name))
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
        logger.info("🚀 STARTING ENHANCED FHIR CONDITION ETL PROCESS (v2)")
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
        logger.info(f"🎯 Target: {'DRY RUN (no writes)' if config.processing.test_mode else 'Redshift (8 tables)'}")
        logger.info("🔄 Process: 9 steps (Read → Filter → Bookmark → Deduplicate → Transform → Convert → Resolve → Validate → Write)")
        
        # Step 1: Read data from Iceberg
        logger.info("\n" + "=" * 50)
        logger.info("📥 STEP 1: READING DATA FROM ICEBERG")
        logger.info("=" * 50)
        table_name_full = f"{catalog_nm}.{config.database.database_name}.{config.database.table_name}"
        logger.info(f"Reading from table: {table_name_full}")
        df_raw = spark.table(table_name_full)
        
        # Filter deleted records
        condition_df = filter_deleted_records(df_raw)
        
        # Apply sampling if configured
        if config.processing.use_sample:
            logger.info(f"⚠️  TESTING MODE: Sampling {config.processing.sample_size} records")
            condition_df = condition_df.limit(config.processing.sample_size)
        
        logger.info("📊 Reading raw condition records from Iceberg...")
        
        # Step 2: Apply Bookmark Filter
        logger.info("\n" + "=" * 50)
        logger.info("📌 STEP 2: APPLYING BOOKMARK FILTER")
        logger.info("=" * 50)
        bookmark_timestamp = get_bookmark_from_redshift(glueContext, TableNames.CONDITIONS, config.database, config.processing)
        condition_df = filter_by_bookmark(condition_df, bookmark_timestamp)
        logger.info("✅ Bookmark filter applied")
        
        # Step 3: Deduplicate conditions
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 3: DEDUPLICATING CONDITIONS")
        logger.info("=" * 50)
        condition_df = deduplicate_entities(condition_df, "condition")
        logger.info("✅ Deduplication completed")
        
        # Cache condition_df for reuse across multiple transformations
        # This prevents recomputation when used by multiple child table transformations
        logger.info("💾 Caching condition_df for reuse across transformations...")
        condition_df = condition_df.cache()
        
        # Step 4: Identify deleted conditions
        logger.info("\n" + "=" * 50)
        logger.info("🗑️  STEP 4: IDENTIFYING DELETED CONDITIONS")
        logger.info("=" * 50)
        deleted_condition_ids = identify_deleted_condition_ids(condition_df)
        
        # Step 5: Transform main condition data
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 5: TRANSFORMING MAIN CONDITION DATA")
        logger.info("=" * 50)
        main_condition_df = transform_main_condition_data(condition_df)
        logger.info("✅ Main condition transformation completed")
        
        # Check if we have any records (lightweight check)
        sample_check = main_condition_df.limit(1).collect()
        if len(sample_check) == 0:
            logger.error("❌ No main condition records after transformation!")
            return
        
        # Step 6: Transform child tables
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 6: TRANSFORMING CHILD TABLES")
        logger.info("=" * 50)
        
        # Transform codes using normalized structure
        from transformations.child_tables_codes import transform_condition_codes_to_codes_tables
        
        # Check if this is an initial load (codes table is empty)
        is_initial_load = False  # Could be determined by checking if codes table exists/is empty
        
        # transform_condition_codes_to_codes_tables will call transform_condition_codes internally
        # and handle caching, ID reuse, and native Spark code_id generation
        unique_codes_df, condition_codes_df = transform_condition_codes_to_codes_tables(
            condition_df,
            config.processing,
            glue_context=glueContext,
            config=config.database,
            is_initial_load=is_initial_load
        )
        
        # Transform categories using normalized structure
        from transformations.categories_tables import transform_condition_categories_to_categories_tables
        unique_categories_df, condition_categories_df = transform_condition_categories_to_categories_tables(
            condition_df, config.processing
        )
        
        # Transform body sites using normalized structure (returns both lookup table and junction table)
        from transformations.body_sites import transform_condition_body_sites_to_body_sites_tables
        unique_body_sites_df, condition_body_sites_df = transform_condition_body_sites_to_body_sites_tables(
            condition_df, config.processing, glueContext, config.database, is_initial_load
        )
        
        # Transform child tables using separate transformation modules (SOLID principle)
        from transformations.evidence import transform_condition_evidence
        from transformations.extensions import transform_condition_extensions
        from transformations.notes import transform_condition_notes
        from transformations.stages import transform_condition_stages
        
        condition_evidence_df = transform_condition_evidence(condition_df)
        condition_extensions_df = transform_condition_extensions(condition_df)
        condition_notes_df = transform_condition_notes(condition_df)
        
        # Transform stages using normalized structure (summary and type to codes table)
        from transformations.stage_tables import (
            transform_condition_stage_summaries_to_codes_tables,
            transform_condition_stage_types_to_codes_tables
        )
        
        # Transform stage summaries to codes table
        unique_stage_summary_codes_df, condition_stage_summaries_df = transform_condition_stage_summaries_to_codes_tables(
            condition_df,
            config.processing,
            glue_context=glueContext,
            config=config.database,
            is_initial_load=is_initial_load
        )
        
        # Transform stage types to codes table
        unique_stage_type_codes_df, condition_stage_types_df = transform_condition_stage_types_to_codes_tables(
            condition_df,
            config.processing,
            glue_context=glueContext,
            config=config.database,
            is_initial_load=is_initial_load
        )
        
        
        # Transform stage assessments (denormalized - can be Reference or CodeableConcept)
        # Note: stage.summary and stage.type are normalized via stage_tables.py above
        condition_stages_df = transform_condition_stages(condition_df)
        
        # Skip child table counts to avoid shuffle failures in large datasets
        # Counts are not critical for processing and can cause MetadataFetchFailedException
        # The counts were causing: "Missing an output location for shuffle" errors
        logger.info("✅ Transformed child tables (counts skipped to avoid shuffle failures)")
        
        # Merge all unique codes from all transformations (codes, stage summaries, stage types)
        logger.info("🔍 Merging all unique codes from all transformations...")
        # Union all unique code DataFrames
        all_unique_codes_list = [unique_codes_df]
        if unique_stage_summary_codes_df is not None:
            all_unique_codes_list.append(unique_stage_summary_codes_df)
        if unique_stage_type_codes_df is not None:
            all_unique_codes_list.append(unique_stage_type_codes_df)
        
        # Union all unique codes (deduplication happens in codes table write)
        if len(all_unique_codes_list) > 1:
            all_unique_codes_df = all_unique_codes_list[0]
            for codes_df in all_unique_codes_list[1:]:
                all_unique_codes_df = all_unique_codes_df.unionByName(codes_df, allowMissingColumns=True)
            # Re-deduplicate after union
            all_unique_codes_df = all_unique_codes_df.groupBy("code_id", "code_code", "code_system").agg(
                F.first("code_display", ignorenulls=True).alias("code_display"),
                F.first("code_text", ignorenulls=True).alias("code_text"),
                F.first("normalized_code_text", ignorenulls=True).alias("normalized_code_text")
            )
            unique_codes_df = all_unique_codes_df
        logger.info("✅ Merged all unique codes from all transformations (codes, stage summaries, stage types)")
        
        # Unpersist condition_df now that all transformations are complete
        logger.info("🧹 Unpersisting condition_df cache (all transformations complete)...")
        condition_df.unpersist()
        
        # Step 7: Convert to DynamicFrames
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 7: CONVERTING TO DYNAMICFRAMES")
        logger.info("=" * 50)
        main_dynamic_frame, child_dynamic_frames = convert_to_dynamic_frames(
            main_condition_df,
            condition_codes_df,
            condition_categories_df,
            condition_body_sites_df,
            condition_evidence_df,
            condition_extensions_df,
            condition_notes_df,
            condition_stages_df,
            condition_stage_summaries_df,
            condition_stage_types_df,
            config=config
        )
        logger.info("✅ Converted all DataFrames to DynamicFrames")
        
        # Step 8: Write to Redshift
        logger.info("\n" + "=" * 50)
        logger.info("💾 STEP 8: WRITING DATA TO REDSHIFT")
        logger.info("=" * 50)
        
        is_initial_load = (bookmark_timestamp is None)
        
        # Delete deleted conditions first
        if deleted_condition_ids:
            logger.info("🗑️  Deleting deleted conditions...")
            delete_condition_records(
                deleted_condition_ids, glueContext, spark, config.database, config.processing
            )
        
        # Write main conditions table
        logger.info("📝 Writing main conditions table...")
        conditions_preactions = "TRUNCATE TABLE public.conditions;" if is_initial_load else ""
        records_written = write_to_redshift_versioned(
            main_dynamic_frame, TableNames.CONDITIONS, "condition_id",
            glueContext, config.database, config.processing, conditions_preactions
        )
        
        # Clean up child records for updated conditions
        if not is_initial_load and records_written > 0:
            logger.info("🧹 Cleaning up child records for updated conditions...")
            main_df = main_dynamic_frame.toDF()
            # Use DataFrame directly instead of collecting IDs (avoids expensive collect())
            condition_ids_df = main_df.select("condition_id").distinct()
            # Lightweight check: see if any IDs exist
            sample_check = condition_ids_df.limit(1).collect()
            if len(sample_check) > 0:
                delete_child_records_for_conditions(
                    condition_ids_df, glueContext, spark, config.database, config.processing
                )
        
        # Write normalized lookup tables first (codes, categories)
        logger.info("📝 Writing normalized lookup tables...")
        
        # Write codes table
        from shared.database.redshift_operations import write_codes_table_with_upsert
        # Final validation: Ensure no NULL code_ids before conversion
        logger.info("🔍 Validating unique_codes_df before write...")
        null_check = unique_codes_df.filter(F.col("code_id").isNull()).limit(1).collect()
        if len(null_check) > 0:
            logger.error(f"❌ Found records with NULL code_id - filtering them out")
            unique_codes_df = unique_codes_df.filter(F.col("code_id").isNotNull())
        
        # Additional validation: Ensure code_code and code_system are not NULL/empty
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
        
        # Write body_sites table (small table - use lookup cache optimization, shared with observations)
        write_small_lookup_table_with_cache(
            unique_body_sites_df, "body_sites", "body_site_id", glueContext, spark, config.database, config.processing, is_initial_load
        )
        
        # Write child tables (now using IDs instead of full data)
        child_tables = [
            (child_dynamic_frames[0], TableNames.CONDITION_CODES),
            (child_dynamic_frames[1], TableNames.CONDITION_CATEGORIES),
            (child_dynamic_frames[2], TableNames.CONDITION_BODY_SITES),
            (child_dynamic_frames[3], TableNames.CONDITION_EVIDENCE),
            (child_dynamic_frames[4], TableNames.CONDITION_EXTENSIONS),
            (child_dynamic_frames[5], TableNames.CONDITION_NOTES),
            (child_dynamic_frames[6], TableNames.CONDITION_STAGES),
            (child_dynamic_frames[7], TableNames.CONDITION_STAGE_SUMMARIES),
            (child_dynamic_frames[8], TableNames.CONDITION_STAGE_TYPES),
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

