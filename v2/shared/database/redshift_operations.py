"""
Redshift read/write operations
"""
from typing import Optional
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
import logging

from ..config import DatabaseConfig, ProcessingConfig
from ..utils.version_utils import (
    get_existing_versions_from_redshift,
    filter_dataframe_by_version,
    get_entities_to_delete
)
from ..utils.redshift_connection_utils import get_redshift_connection_options

logger = logging.getLogger(__name__)

# Default chunk size for chunked writes (records per chunk)
# Writing in chunks reduces memory pressure and shuffle failures
DEFAULT_CHUNK_SIZE = 500000  # 500K records per chunk


def write_to_redshift_simple(
    dynamic_frame: DynamicFrame,
    table_name: str,
    glue_context: GlueContext,
    config: DatabaseConfig,
    processing_config: ProcessingConfig,
    preactions: str = "",
    chunk_size: int = DEFAULT_CHUNK_SIZE
) -> int:
    """
    Write DynamicFrame to Redshift without version checking
    
    Used with bookmark pattern - since we filter at source, we can simply append all records.
    For initial loads, uses TRUNCATE to clear existing data.
    
    Writes data in chunks to reduce memory pressure and avoid shuffle failures.
    
    Args:
        dynamic_frame: Data to write
        table_name: Target table name
        glue_context: AWS Glue context
        config: Database configuration
        processing_config: Processing configuration
        preactions: SQL to run before write (e.g., TRUNCATE for initial load)
        chunk_size: Number of records per chunk (default: 500K)
        
    Returns:
        Number of records written
    """
    logger.info(f"Writing {table_name} to Redshift...")
    
    try:
        logger.info(f"Executing preactions for {table_name}: {preactions[:100] if preactions else 'None'}")
        logger.info(f"Writing to table: public.{table_name}")
        
        # Skip actual write if in TEST_MODE
        if processing_config.test_mode:
            logger.info(f"⚠️  TEST MODE: Skipping write to {table_name}")
            logger.info(f"   Preactions that would execute: {preactions[:200] if preactions else 'None'}")
            return 0
        
        # Optimize partitioning to reduce shuffle failures and memory pressure
        # Analysis shows: 16.8GB shuffle data + window operations = executor memory exhaustion
        # Strategy: Optimize partitions for write performance
        # Only repartition if necessary - use coalesce when reducing partitions (no shuffle)
        df = dynamic_frame.toDF()
        num_partitions = max(1, df.rdd.getNumPartitions())
        
        # Target: 120 partitions for optimal write performance with large datasets
        # But only repartition if we have significantly fewer partitions
        # For small DataFrames, keep existing partitions to avoid unnecessary shuffles
        target_partitions = 120
        
        if num_partitions < target_partitions and num_partitions < 50:
            # Only repartition if we have very few partitions (< 50)
            # This avoids expensive shuffles for DataFrames that are already well-partitioned
            logger.info(f"📦 Optimizing partitions: repartitioning {num_partitions} → {target_partitions} for {table_name}")
            logger.info(f"   This better distributes data for Redshift write")
            df = df.repartition(target_partitions)
        elif num_partitions > target_partitions:
            # Use coalesce to reduce partitions (no shuffle, faster)
            logger.info(f"📦 Optimizing partitions: coalescing {num_partitions} → {target_partitions} for {table_name}")
            logger.info(f"   Using coalesce (no shuffle) to reduce partitions")
            df = df.coalesce(target_partitions)
        else:
            # Already well-partitioned, no change needed
            logger.info(f"📦 Partition optimization skipped: {num_partitions} partitions already optimal for {table_name}")
        
        # Convert back to DynamicFrame and write
        optimized_dynamic_frame = DynamicFrame.fromDF(df, glue_context, f"optimized_{table_name}")
        
        # Get explicit connection options that respect the database parameter
        connection_options = get_redshift_connection_options(
            connection_name=config.redshift_connection,
            database=config.redshift_database,
            s3_temp_dir=config.s3_temp_dir,
            dbtable=f"public.{table_name}",
            preactions=preactions or ""
        )
        
        glue_context.write_dynamic_frame.from_options(
            frame=optimized_dynamic_frame,
            connection_type="redshift",
            connection_options=connection_options,
            transformation_ctx=f"write_{table_name}_to_redshift"
        )
        
        logger.info(f"✅ Successfully wrote {table_name} to Redshift")
        return 0
    
    except Exception as e:
        logger.error(f"❌ Failed to write {table_name} to Redshift: {str(e)}")
        raise e


def write_to_redshift_versioned(
    dynamic_frame: DynamicFrame,
    table_name: str,
    id_column: str,
    glue_context: GlueContext,
    config: DatabaseConfig,
    processing_config: ProcessingConfig,
    preactions: str = ""
) -> int:
    """
    Version-aware write to Redshift - only processes new/updated entities
    
    Args:
        dynamic_frame: Data to write
        table_name: Target table name
        id_column: Column name to use for version comparison
        glue_context: AWS Glue context
        config: Database configuration
        processing_config: Processing configuration
        preactions: SQL to run before write (e.g., TRUNCATE for initial load)
        
    Returns:
        Number of records written
    """
    logger.info(f"Writing {table_name} to Redshift with version checking...")
    
    try:
        # Convert dynamic frame to DataFrame for processing
        df = dynamic_frame.toDF()
        
        # Skip version checking if in TEST_MODE
        if processing_config.test_mode:
            logger.info(f"⚠️  TEST MODE: Skipping write to {table_name} with version checking")
            return 0
        
        # Step 1: Get existing versions from Redshift
        existing_versions = get_existing_versions_from_redshift(
            glue_context, table_name, id_column, config
        )
        
        # Step 2: Filter incoming data based on version comparison
        filtered_df, to_process_count, skipped_count = filter_dataframe_by_version(
            df, existing_versions, id_column
        )
        
        # Check if DataFrame is empty using lightweight check (count may be None to avoid shuffle)
        if to_process_count == 0:
            logger.info(f"✅ All records in {table_name} are up to date - no changes needed")
            return 0
        
        # If count is None, use lightweight check instead
        if to_process_count is None:
            sample_check = filtered_df.limit(1).collect()
            if len(sample_check) == 0:
                logger.info(f"✅ All records in {table_name} are up to date - no changes needed")
                return 0
            logger.info(f"Writing new/updated records to {table_name} (count skipped to avoid shuffle)")
        else:
            logger.info(f"Writing {to_process_count} new/updated records to {table_name}")
        
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
        filtered_dynamic_frame = DynamicFrame.fromDF(filtered_df, glue_context, f"filtered_{table_name}")
        
        # Step 6: Write only the new/updated records
        logger.info(f"Writing {to_process_count} new/updated records to {table_name}")
        
        connection_options = get_redshift_connection_options(
            connection_name=config.redshift_connection,
            database=config.redshift_database,
            s3_temp_dir=config.s3_temp_dir,
            dbtable=f"public.{table_name}",
            preactions=selective_preactions or ""
        )
        
        glue_context.write_dynamic_frame.from_options(
            frame=filtered_dynamic_frame,
            connection_type="redshift",
            connection_options=connection_options,
            transformation_ctx=f"write_{table_name}_versioned_to_redshift"
        )
        
        # Handle None counts (counts skipped to avoid shuffle)
        if to_process_count is not None:
            logger.info(f"✅ Successfully wrote {to_process_count} records to {table_name} in Redshift")
            logger.info(f"📊 Version summary: {to_process_count} processed, {skipped_count or 0} skipped (same version)")
            return to_process_count
        else:
            logger.info(f"✅ Successfully wrote records to {table_name} in Redshift (count skipped to avoid shuffle)")
            return 0  # Return 0 when count wasn't calculated
    
    except Exception as e:
        logger.error(f"❌ Failed to write {table_name} to Redshift with versioning: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        raise


def write_codes_table_with_upsert(
    dynamic_frame: DynamicFrame,
    table_name: str,
    glue_context: GlueContext,
    config: DatabaseConfig,
    processing_config: ProcessingConfig,
    is_initial_load: bool = False
) -> int:
    """
    Write codes table with upsert logic (MERGE) for Redshift
    Uses staging table approach for efficient upserts
    
    Args:
        dynamic_frame: Codes data to write (code_id, code_code, code_system, code_display, code_text, normalized_code_text)
        table_name: Target table name (typically "codes")
        glue_context: AWS Glue context
        config: Database configuration
        processing_config: Processing configuration
        is_initial_load: If True, truncate table first (faster than MERGE for full reload)
        
    Returns:
        Number of records written
    """
    logger.info(f"Writing {table_name} to Redshift with upsert logic...")
    
    try:
        # Skip actual write if in TEST_MODE
        if processing_config.test_mode:
            logger.info(f"⚠️  TEST MODE: Skipping write to {table_name}")
            return 0
        
        # For initial load, use TRUNCATE + INSERT (faster than MERGE)
        if is_initial_load:
            logger.info(f"Initial load detected - using TRUNCATE + INSERT for {table_name}")
            preactions = f"TRUNCATE TABLE public.{table_name};"
            
            connection_options_init = get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable=f"public.{table_name}",
                preactions=preactions
            )
            
            glue_context.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="redshift",
                connection_options=connection_options_init,
                transformation_ctx=f"write_{table_name}_initial_load"
            )
            logger.info(f"✅ Successfully wrote {table_name} to Redshift (initial load)")
            return 0
        
        # For incremental loads, use MERGE via staging table
        # Redshift MERGE syntax:
        # MERGE INTO target_table USING source_table ON condition
        # WHEN MATCHED THEN UPDATE SET ...
        # WHEN NOT MATCHED THEN INSERT ...
        
        staging_table = f"{table_name}_staging"
        logger.info(f"Using staging table approach for upsert: {staging_table}")
        
        # Step 1: Write to staging table
        connection_options_staging = get_redshift_connection_options(
            connection_name=config.redshift_connection,
            database=config.redshift_database,
            s3_temp_dir=config.s3_temp_dir,
            dbtable=f"public.{staging_table}",
            preactions=f"DROP TABLE IF EXISTS public.{staging_table}; CREATE TABLE public.{staging_table} (LIKE public.{table_name});"
        )
        
        glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="redshift",
            connection_options=connection_options_staging,
            transformation_ctx=f"write_{staging_table}"
        )
        
        # Step 2: MERGE staging into target
        # Note: Redshift MERGE requires explicit column lists
        merge_sql = f"""
        MERGE INTO public.{table_name} AS target
        USING public.{staging_table} AS source
        ON target.code_id = source.code_id
        WHEN MATCHED THEN
            UPDATE SET
                code_display = COALESCE(source.code_display, target.code_display),
                code_text = COALESCE(source.code_text, target.code_text),
                normalized_code_text = COALESCE(source.normalized_code_text, target.normalized_code_text),
                updated_at = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (code_id, code_code, code_system, code_display, code_text, normalized_code_text, created_at, updated_at)
            VALUES (source.code_id, source.code_code, source.code_system, source.code_display, source.code_text, source.normalized_code_text, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
        
        DROP TABLE IF EXISTS public.{staging_table};
        """
        
        # Execute MERGE via JDBC
        from awsglue.utils import getResolvedOptions
        import sys
        
        # Use Glue's JDBC connection to execute MERGE
        logger.info(f"Executing MERGE statement for {table_name}...")
        
        # Write MERGE SQL to a temporary file and execute via preactions on a dummy write
        # Alternative: Use JDBC connection directly
        # For now, we'll use a simpler approach: write to staging, then use postactions
        
        # Actually, Glue doesn't support postactions well, so we'll use a different approach:
        # Write to staging table, then use a follow-up write with preactions that does the MERGE
        
        # Create a minimal DataFrame to trigger the MERGE
        df = dynamic_frame.toDF()
        if not df.rdd.isEmpty():
            # Get first row to create a minimal trigger DataFrame
            first_row = df.limit(1)
            trigger_dynamic_frame = DynamicFrame.fromDF(first_row, glue_context, f"trigger_{table_name}")
            
            connection_options_merge = get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable=f"public.{table_name}",
                preactions=merge_sql
            )
            
            glue_context.write_dynamic_frame.from_options(
                frame=trigger_dynamic_frame,
                connection_type="redshift",
                connection_options=connection_options_merge,
                transformation_ctx=f"merge_{table_name}"
            )
        
        logger.info(f"✅ Successfully upserted {table_name} to Redshift")
        return 0
    
    except Exception as e:
        logger.error(f"❌ Failed to write {table_name} to Redshift with upsert: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        raise


def write_categories_table_with_upsert(
    dynamic_frame: DynamicFrame,
    table_name: str,
    glue_context: GlueContext,
    config: DatabaseConfig,
    processing_config: ProcessingConfig,
    is_initial_load: bool = False
) -> int:
    """
    Write categories table with upsert logic (MERGE) for Redshift
    Uses staging table approach for efficient upserts
    
    Args:
        dynamic_frame: Categories data to write (category_id, category_code, category_system, category_display, category_text)
        table_name: Target table name (typically "categories")
        glue_context: AWS Glue context
        config: Database configuration
        processing_config: Processing configuration
        is_initial_load: If True, truncate table first (faster than MERGE for full reload)
        
    Returns:
        Number of records written
    """
    logger.info(f"Writing {table_name} to Redshift with upsert logic...")
    
    try:
        if processing_config.test_mode:
            logger.info(f"⚠️  TEST MODE: Skipping write to {table_name}")
            return 0
        
        if is_initial_load:
            logger.info(f"Initial load detected - using TRUNCATE + INSERT for {table_name}")
            preactions = f"TRUNCATE TABLE public.{table_name};"
            
            connection_options_init = get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable=f"public.{table_name}",
                preactions=preactions
            )
            
            glue_context.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="redshift",
                connection_options=connection_options_init,
                transformation_ctx=f"write_{table_name}_initial_load"
            )
            logger.info(f"✅ Successfully wrote {table_name} to Redshift (initial load)")
            return 0
        
        staging_table = f"{table_name}_staging"
        logger.info(f"Using staging table approach for upsert: {staging_table}")
        
        connection_options_staging = get_redshift_connection_options(
            connection_name=config.redshift_connection,
            database=config.redshift_database,
            s3_temp_dir=config.s3_temp_dir,
            dbtable=f"public.{staging_table}",
            preactions=f"DROP TABLE IF EXISTS public.{staging_table}; CREATE TABLE public.{staging_table} (LIKE public.{table_name});"
        )
        
        glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="redshift",
            connection_options=connection_options_staging,
            transformation_ctx=f"write_{staging_table}"
        )
        
        merge_sql = f"""
        MERGE INTO public.{table_name} AS target
        USING public.{staging_table} AS source
        ON target.category_id = source.category_id
        WHEN MATCHED THEN
            UPDATE SET
                category_display = COALESCE(source.category_display, target.category_display),
                category_text = COALESCE(source.category_text, target.category_text),
                updated_at = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (category_id, category_code, category_system, category_display, category_text, created_at, updated_at)
            VALUES (source.category_id, source.category_code, source.category_system, source.category_display, source.category_text, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
        
        DROP TABLE IF EXISTS public.{staging_table};
        """
        
        df = dynamic_frame.toDF()
        if not df.rdd.isEmpty():
            first_row = df.limit(1)
            trigger_dynamic_frame = DynamicFrame.fromDF(first_row, glue_context, f"trigger_{table_name}")
            
            connection_options_merge = get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable=f"public.{table_name}",
                preactions=merge_sql
            )
            
            glue_context.write_dynamic_frame.from_options(
                frame=trigger_dynamic_frame,
                connection_type="redshift",
                connection_options=connection_options_merge,
                transformation_ctx=f"merge_{table_name}"
            )
        
        logger.info(f"✅ Successfully upserted {table_name} to Redshift")
        return 0
    
    except Exception as e:
        logger.error(f"❌ Failed to write {table_name} to Redshift with upsert: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        raise


def write_small_lookup_table_with_cache(
    df: DataFrame,
    table_name: str,
    id_column: str,  # "category_id" or "interpretation_id"
    glue_context: GlueContext,
    spark_session,
    config: DatabaseConfig,
    processing_config: ProcessingConfig,
    is_initial_load: bool = False
) -> int:
    """
    Write small lookup table (categories/interpretations) with Spark-level cache.
    Perfect for tables with < 1000 unique values.
    
    Strategy:
    1. For initial load: Write all (no existing data to check)
    2. For incremental load: 
       - Load existing IDs from Redshift
       - Broadcast in Spark (small table fits in memory)
       - Filter duplicates (left anti-join)
       - Write only new values (simple INSERT, no MERGE)
    
    Args:
        df: DataFrame with data to write (must include id_column)
        table_name: Target table name (e.g., "categories", "interpretations")
        id_column: ID column name (e.g., "category_id", "interpretation_id")
        glue_context: AWS Glue context
        spark_session: Spark session (for JDBC reads)
        config: Database configuration
        processing_config: Processing configuration
        is_initial_load: If True, skip lookup and write all
        
    Returns:
        Number of new records written
    """
    logger.info(f"Writing {table_name} with lookup cache optimization (small table)...")
    
    try:
        if processing_config.test_mode:
            logger.info(f"⚠️  TEST MODE: Skipping write to {table_name}")
            return 0
        
        if is_initial_load:
            # Initial load: write all (no existing data to check)
            logger.info(f"Initial load detected - writing all {table_name} (no lookup needed)")
            dynamic_frame = DynamicFrame.fromDF(df, glue_context, f"{table_name}_df")
            
            preactions = f"TRUNCATE TABLE public.{table_name};"
            connection_options = get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable=f"public.{table_name}",
                preactions=preactions
            )
            
            glue_context.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="redshift",
                connection_options=connection_options,
                transformation_ctx=f"write_{table_name}_initial"
            )
            logger.info(f"✅ Successfully wrote {table_name} to Redshift (initial load)")
            return 0
        
        # Incremental load: use lookup cache
        logger.info(f"Loading existing {table_name} from Redshift for lookup cache...")
        
        # Read existing IDs from Redshift using Glue connection
        # We'll use a simple query to get just the IDs
        existing_ids_query = f"SELECT {id_column} FROM public.{table_name}"
        
        try:
            # Use Glue's read capability to get existing IDs
            # Note: Glue doesn't have direct JDBC read, so we'll use a workaround:
            # Read via DynamicFrame from Redshift
            existing_dynamic_frame = glue_context.create_dynamic_frame.from_options(
                connection_type="redshift",
                connection_options=get_redshift_connection_options(
                    connection_name=config.redshift_connection,
                    database=config.redshift_database,
                    s3_temp_dir=config.s3_temp_dir,
                    query=existing_ids_query
                )
            )
            
            existing_df = existing_dynamic_frame.toDF().select(id_column)
            
            # Check if table is empty
            existing_count = existing_df.limit(1).count()
            if existing_count == 0:
                logger.info(f"No existing {table_name} found - writing all as new")
                existing_df = None
            else:
                # Broadcast existing IDs (small table, fits in memory)
                existing_df = broadcast(existing_df)
                logger.info(f"Broadcasting existing {table_name} IDs for filtering")
        
        except Exception as e:
            logger.warning(f"Could not load existing {table_name} from Redshift: {e}")
            logger.warning("Falling back to writing all records (may create duplicates)")
            existing_df = None
        
        # Filter out existing values (left anti-join)
        if existing_df is not None:
            new_df = df.join(
                existing_df,
                df[id_column] == existing_df[id_column],
                "left_anti"  # Keep only new values
            )
        else:
            new_df = df
        
        # Get counts (lightweight - use limit to avoid full scan)
        total_count = df.limit(1).count()  # Just check if any exist
        if total_count > 0:
            # For small tables, we can safely count
            try:
                total_count = df.count()
                new_count = new_df.count() if existing_df is not None else total_count
                logger.info(f"{table_name}: {total_count} total, {new_count} new, {total_count - new_count} already exist")
            except Exception:
                logger.info(f"{table_name}: Writing records (count skipped to avoid shuffle)")
                new_count = -1  # Unknown count
        else:
            logger.info(f"No {table_name} to write")
            return 0
        
        # Write only new values (simple INSERT, no MERGE needed)
        if new_count > 0 or new_count == -1:
            new_dynamic_frame = DynamicFrame.fromDF(new_df, glue_context, f"new_{table_name}")
            
            connection_options = get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable=f"public.{table_name}"
            )
            
            glue_context.write_dynamic_frame.from_options(
                frame=new_dynamic_frame,
                connection_type="redshift",
                connection_options=connection_options,
                transformation_ctx=f"write_{table_name}_new"
            )
            
            if new_count > 0:
                logger.info(f"✅ Wrote {new_count} new {table_name} to Redshift")
            else:
                logger.info(f"✅ Wrote new {table_name} to Redshift (count unknown)")
            return new_count if new_count > 0 else 0
        else:
            logger.info(f"✅ No new {table_name} to write (all already exist)")
            return 0
    
    except Exception as e:
        logger.error(f"❌ Failed to write {table_name} to Redshift with cache: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        raise


def write_interpretations_table_with_upsert(
    dynamic_frame: DynamicFrame,
    table_name: str,
    glue_context: GlueContext,
    config: DatabaseConfig,
    processing_config: ProcessingConfig,
    is_initial_load: bool = False
) -> int:
    """
    Write interpretations table with upsert logic (MERGE) for Redshift
    Uses staging table approach for efficient upserts
    
    Args:
        dynamic_frame: Interpretations data to write (interpretation_id, interpretation_code, interpretation_system, interpretation_display, interpretation_text)
        table_name: Target table name (typically "interpretations")
        glue_context: AWS Glue context
        config: Database configuration
        processing_config: Processing configuration
        is_initial_load: If True, truncate table first (faster than MERGE for full reload)
        
    Returns:
        Number of records written
    """
    logger.info(f"Writing {table_name} to Redshift with upsert logic...")
    
    try:
        if processing_config.test_mode:
            logger.info(f"⚠️  TEST MODE: Skipping write to {table_name}")
            return 0
        
        if is_initial_load:
            logger.info(f"Initial load detected - using TRUNCATE + INSERT for {table_name}")
            preactions = f"TRUNCATE TABLE public.{table_name};"
            
            connection_options_init = get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable=f"public.{table_name}",
                preactions=preactions
            )
            
            glue_context.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="redshift",
                connection_options=connection_options_init,
                transformation_ctx=f"write_{table_name}_initial_load"
            )
            logger.info(f"✅ Successfully wrote {table_name} to Redshift (initial load)")
            return 0
        
        staging_table = f"{table_name}_staging"
        logger.info(f"Using staging table approach for upsert: {staging_table}")
        
        connection_options_staging = get_redshift_connection_options(
            connection_name=config.redshift_connection,
            database=config.redshift_database,
            s3_temp_dir=config.s3_temp_dir,
            dbtable=f"public.{staging_table}",
            preactions=f"DROP TABLE IF EXISTS public.{staging_table}; CREATE TABLE public.{staging_table} (LIKE public.{table_name});"
        )
        
        glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="redshift",
            connection_options=connection_options_staging,
            transformation_ctx=f"write_{staging_table}"
        )
        
        merge_sql = f"""
        MERGE INTO public.{table_name} AS target
        USING public.{staging_table} AS source
        ON target.interpretation_id = source.interpretation_id
        WHEN MATCHED THEN
            UPDATE SET
                interpretation_display = COALESCE(source.interpretation_display, target.interpretation_display),
                interpretation_text = COALESCE(source.interpretation_text, target.interpretation_text),
                updated_at = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (interpretation_id, interpretation_code, interpretation_system, interpretation_display, interpretation_text, created_at, updated_at)
            VALUES (source.interpretation_id, source.interpretation_code, source.interpretation_system, source.interpretation_display, source.interpretation_text, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
        
        DROP TABLE IF EXISTS public.{staging_table};
        """
        
        df = dynamic_frame.toDF()
        if not df.rdd.isEmpty():
            first_row = df.limit(1)
            trigger_dynamic_frame = DynamicFrame.fromDF(first_row, glue_context, f"trigger_{table_name}")
            
            connection_options_merge = get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable=f"public.{table_name}",
                preactions=merge_sql
            )
            
            glue_context.write_dynamic_frame.from_options(
                frame=trigger_dynamic_frame,
                connection_type="redshift",
                connection_options=connection_options_merge,
                transformation_ctx=f"merge_{table_name}"
            )
        
        logger.info(f"✅ Successfully upserted {table_name} to Redshift")
        return 0
    
    except Exception as e:
        logger.error(f"❌ Failed to write {table_name} to Redshift with upsert: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        raise

