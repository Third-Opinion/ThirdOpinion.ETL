"""
Deletion utilities for observations
"""
from typing import List
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
import logging

from shared.config import DatabaseConfig, ProcessingConfig
from shared.utils.redshift_connection_utils import get_redshift_connection_options
from config import TableNames

logger = logging.getLogger(__name__)


def identify_entered_in_error_records(df: DataFrame) -> tuple[bool, DataFrame]:
    """
    Identify observations with 'entered-in-error' status (lazy evaluation)
    
    This function performs a lightweight check without materializing all IDs.
    The actual ID collection is deferred until deletion time.
    
    Args:
        df: Source DataFrame from Iceberg with deduplicated observations
        
    Returns:
        Tuple of (has_entered_in_error: bool, entered_in_error_df: DataFrame)
        - has_entered_in_error: True if any entered-in-error records exist
        - entered_in_error_df: Filtered DataFrame (lazy, not materialized)
    """
    logger.info("Checking for observations with 'entered-in-error' status...")
    
    # Check if status column exists
    if "status" not in df.columns:
        logger.warning("status column not found in data, skipping entered-in-error check")
        return False, None
    
    if "id" not in df.columns:
        logger.warning("id column not found in data, skipping entered-in-error check")
        return False, None
    
    # Filter for entered-in-error status (lazy - no execution yet)
    entered_in_error_df = df.filter(F.col("status") == "entered-in-error")
    
    # Lightweight check: just see if any exist (only processes 1 row)
    sample_check = entered_in_error_df.limit(1).collect()
    
    if len(sample_check) == 0:
        logger.info("✅ No observations with 'entered-in-error' status found")
        return False, None
    
    # We found some - log that we'll handle them, but don't collect IDs yet
    logger.info("⚠️  Found observations with 'entered-in-error' status")
    logger.info("   These will be filtered from processing and deleted from Redshift")
    logger.info("   (ID collection deferred until deletion to improve performance)")
    
    return True, entered_in_error_df


def delete_entered_in_error_records(
    entered_in_error_df: DataFrame,
    glue_context: GlueContext,
    spark: SparkSession,
    config: DatabaseConfig,
    processing_config: ProcessingConfig
) -> None:
    """
    Delete observations with 'entered-in-error' status from all Redshift tables
    
    This function removes erroneous observations from all observation-related tables.
    Uses a temporary table approach that writes the DataFrame directly to Redshift,
    avoiding expensive .collect() operations that would materialize all IDs.
    
    Args:
        entered_in_error_df: DataFrame containing entered-in-error observations (lazy)
        glue_context: AWS Glue context
        spark: Spark session
        config: Database configuration
        processing_config: Processing configuration
    """
    if entered_in_error_df is None:
        logger.info("No entered-in-error observations to delete")
        return
    
    # Skip actual deletion in TEST_MODE
    if processing_config.test_mode:
        logger.info("⚠️  TEST MODE: Skipping deletion of entered-in-error observations")
        return
    
    logger.info("🗑️  Deleting entered-in-error observations from all tables...")
    logger.info("   Using DataFrame-to-temp-table method (avoids expensive ID collection)")
    
    # Use temp table method directly with DataFrame - no ID collection needed!
    _delete_using_dataframe_temp_table(entered_in_error_df, glue_context, spark, config, processing_config)


def delete_child_records_for_observations(
    observation_ids_df: DataFrame,
    glue_context: GlueContext,
    spark: SparkSession,
    config: DatabaseConfig,
    processing_config: ProcessingConfig
) -> None:
    """
    Delete child table records for specific observation IDs (not the observations themselves)
    
    This is used when updating observations to remove old child records before inserting new ones.
    Does NOT delete from the main observations table.
    
    Args:
        observation_ids_df: DataFrame with observation_id column (lazy, not materialized)
        glue_context: AWS Glue context
        spark: Spark session
        config: Database configuration
        processing_config: Processing configuration
    """
    logger.info("Deleting child records for updated observations...")
    logger.info("   Using DataFrame-to-temp-table method (avoids expensive ID collection)")
    
    # Use temp table method directly with DataFrame - no ID collection needed!
    _delete_child_records_using_dataframe_temp_table(
        observation_ids_df, glue_context, spark, config, processing_config
    )


def _delete_child_records_using_dataframe_temp_table(
    observation_ids_df: DataFrame,
    glue_context: GlueContext,
    spark: SparkSession,
    config: DatabaseConfig,
    processing_config: ProcessingConfig
) -> None:
    """
    Delete child records using temporary table JOIN - writes DataFrame directly to temp table
    
    This approach avoids expensive .collect() operations by writing the DataFrame
    directly to Redshift, then using JOIN-based DELETE statements.
    """
    # Skip actual deletion in TEST_MODE
    if processing_config.test_mode:
        logger.info("⚠️  TEST MODE: Skipping child record deletion")
        return
    
    temp_table_name = None
    try:
        # Extract just the observation_id column (already distinct from caller)
        ids_df = observation_ids_df.select(F.col("observation_id").alias("observation_id"))
        ids_dynamic_frame = DynamicFrame.fromDF(ids_df, glue_context, "ids_for_child_delete")
        
        logger.info("   Writing observation IDs directly to temp table (no ID collection)")
        
        # Temporary table name
        temp_table_name = f"tmp_delete_child_records_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"   Using temporary table: {temp_table_name}")
        
        # Step 1: Create temp table and load IDs directly from DataFrame
        create_table_sql = f"""
        DROP TABLE IF EXISTS public.{temp_table_name};
        CREATE TABLE public.{temp_table_name} (observation_id VARCHAR(255));
        """
        
        connection_options_temp = get_redshift_connection_options(
            connection_name=config.redshift_connection,
            database=config.redshift_database,
            s3_temp_dir=config.s3_temp_dir,
            dbtable=f"public.{temp_table_name}",
            preactions=create_table_sql
        )
        
        glue_context.write_dynamic_frame.from_options(
            frame=ids_dynamic_frame,
            connection_type="redshift",
            connection_options=connection_options_temp,
            transformation_ctx="write_ids_to_temp_table_child_delete"
        )
        
        logger.info("   ✓ Loaded IDs into temp table (written directly from DataFrame)")
        
        # Step 2: Execute DELETE statements for child tables ONLY (not main observations table)
        child_tables = TableNames.child_tables()
        delete_statements = [
            f"DELETE FROM public.{table} USING public.{temp_table_name} WHERE {table}.observation_id = {temp_table_name}.observation_id;"
            for table in child_tables
        ]
        # Clean up temp table
        delete_statements.append(f"DROP TABLE IF EXISTS public.{temp_table_name};")
        
        combined_delete_sql = " ".join(delete_statements)
        
        logger.info(f"   Executing child table deletions across {len(child_tables)} tables using temp table JOIN...")
        
        # Execute DELETE statements
        empty_df = spark.createDataFrame([], "observation_id STRING")
        empty_dynamic_frame = DynamicFrame.fromDF(empty_df, glue_context, "empty_frame_for_child_delete")
        
        glue_context.write_dynamic_frame.from_options(
            frame=empty_dynamic_frame,
            connection_type="redshift",
            connection_options=get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable=f"public.{TableNames.OBSERVATIONS}",
                preactions=combined_delete_sql
            ),
            transformation_ctx="delete_child_records_temp_table"
        )
        
        logger.info("✅ Successfully deleted child records for updated observations")
        logger.info(f"   Deleted from {len(child_tables)} child tables using temp table method")
        logger.info(f"   Temp table {temp_table_name} dropped")
    
    except Exception as e:
        logger.error(f"❌ Failed to delete child records via temp table: {str(e)}")
        logger.error(f"   Error type: {type(e).__name__}")
        logger.warning(f"   Continuing with job execution - manual cleanup may be required")
        # Try to clean up temp table if it exists
        if temp_table_name:
            try:
                cleanup_sql = f"DROP TABLE IF EXISTS public.{temp_table_name};"
                logger.info(f"   Attempting to clean up temp table: {temp_table_name}")
                empty_df = spark.createDataFrame([], "observation_id STRING")
                empty_dynamic_frame = DynamicFrame.fromDF(empty_df, glue_context, "cleanup_frame")
                glue_context.write_dynamic_frame.from_options(
                    frame=empty_dynamic_frame,
                    connection_type="redshift",
                    connection_options=get_redshift_connection_options(
                        connection_name=config.redshift_connection,
                        database=config.redshift_database,
                        s3_temp_dir=config.s3_temp_dir,
                        dbtable=f"public.{TableNames.OBSERVATIONS}",
                        preactions=cleanup_sql
                    ),
                    transformation_ctx="cleanup_temp_table"
                )
                logger.info(f"   ✓ Temp table cleaned up")
            except:
                logger.warning(f"   Could not clean up temp table {temp_table_name} - may need manual cleanup")


def _delete_child_records_in_clause(
    observation_ids_list: List[str],
    glue_context: GlueContext,
    spark: SparkSession,
    config: DatabaseConfig,
    processing_config: ProcessingConfig
) -> None:
    """Delete child records only using IN clause"""
    # Create comma-separated list of IDs for SQL IN clause
    escaped_ids = ["'" + str(obs_id).replace("'", "''") + "'" for obs_id in observation_ids_list]
    ids_str = ", ".join(escaped_ids)
    
    # Build DELETE statements for child tables ONLY (not main observations table)
    child_tables = TableNames.child_tables()
    delete_statements = [
        f"DELETE FROM public.{table} WHERE observation_id IN ({ids_str});"
        for table in child_tables
    ]
    
    # Combine all DELETE statements
    combined_delete_sql = " ".join(delete_statements)
    
    logger.info(f"   Executing child table deletions using IN clause...")
    logger.info(f"   SQL preview (first 200 chars): {combined_delete_sql[:200]}...")
    
    # Skip actual deletion in TEST_MODE
    if processing_config.test_mode:
        logger.info(f"⚠️  TEST MODE: Skipping child record deletion for {len(observation_ids_list)} observation(s)")
        logger.info(f"   IDs that would have child records deleted: {observation_ids_list[:10]}{'...' if len(observation_ids_list) > 10 else ''}")
        return
    
    try:
        # Execute the DELETE statements using a dummy write with preactions
        empty_df = spark.createDataFrame([], "observation_id STRING")
        empty_dynamic_frame = DynamicFrame.fromDF(empty_df, glue_context, "empty_frame_for_child_delete")
        
        connection_options = get_redshift_connection_options(
            connection_name=config.redshift_connection,
            database=config.redshift_database,
            s3_temp_dir=config.s3_temp_dir,
            dbtable=f"public.{TableNames.OBSERVATIONS}",
            preactions=combined_delete_sql
        )
        
        glue_context.write_dynamic_frame.from_options(
            frame=empty_dynamic_frame,
            connection_type="redshift",
            connection_options=connection_options,
            transformation_ctx="delete_child_records_preaction"
        )
        
        logger.info(f"   ✓ Successfully deleted child records for {len(observation_ids_list)} observation(s)")
    
    except Exception as e:
        logger.error(f"   ✗ Failed to delete child records: {str(e)}")
        logger.error(f"   Error type: {type(e).__name__}")
        raise


def _delete_child_records_temp_table(
    observation_ids_list: List[str],
    glue_context: GlueContext,
    spark: SparkSession,
    config: DatabaseConfig,
    processing_config: ProcessingConfig
) -> None:
    """Delete child records only using temporary table JOIN"""
    # Skip actual deletion in TEST_MODE
    if processing_config.test_mode:
        logger.info(f"⚠️  TEST MODE: Skipping child record deletion for {len(observation_ids_list)} observation(s) using temp table method")
        logger.info(f"   IDs that would have child records deleted: {observation_ids_list[:10]}{'...' if len(observation_ids_list) > 10 else ''}")
        return
    
    temp_table_name = None
    try:
        # Create DataFrame with observation IDs
        ids_data = [(obs_id,) for obs_id in observation_ids_list]
        ids_df = spark.createDataFrame(ids_data, ["observation_id"])
        ids_dynamic_frame = DynamicFrame.fromDF(ids_df, glue_context, "ids_for_child_delete")
        
        logger.info(f"   Created DataFrame with {len(observation_ids_list)} IDs")
        
        # Temporary table name
        temp_table_name = f"tmp_delete_child_records_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"   Using temporary table: {temp_table_name}")
        
        # Step 1: Create temp table and load IDs
        create_table_sql = f"""
        DROP TABLE IF EXISTS public.{temp_table_name};
        CREATE TABLE public.{temp_table_name} (observation_id VARCHAR(255));
        """
        
        connection_options_temp = get_redshift_connection_options(
            connection_name=config.redshift_connection,
            database=config.redshift_database,
            s3_temp_dir=config.s3_temp_dir,
            dbtable=f"public.{temp_table_name}",
            preactions=create_table_sql
        )
        
        glue_context.write_dynamic_frame.from_options(
            frame=ids_dynamic_frame,
            connection_type="redshift",
            connection_options=connection_options_temp,
            transformation_ctx="write_ids_to_temp_table_child_delete"
        )
        
        logger.info(f"   ✓ Loaded {len(observation_ids_list)} IDs into temp table")
        
        # Step 2: Execute DELETE statements for child tables ONLY
        child_tables = TableNames.child_tables()
        delete_statements = [
            f"DELETE FROM public.{table} USING public.{temp_table_name} WHERE {table}.observation_id = {temp_table_name}.observation_id;"
            for table in child_tables
        ]
        # Clean up temp table
        delete_statements.append(f"DROP TABLE IF EXISTS public.{temp_table_name};")
        
        combined_delete_sql = " ".join(delete_statements)
        
        logger.info(f"   Executing child table deletions using temp table JOIN...")
        
        # Execute DELETE statements
        empty_df = spark.createDataFrame([], "observation_id STRING")
        empty_dynamic_frame = DynamicFrame.fromDF(empty_df, glue_context, "empty_frame_for_child_delete")
        
        glue_context.write_dynamic_frame.from_options(
            frame=empty_dynamic_frame,
            connection_type="redshift",
            connection_options=get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable=f"public.{TableNames.OBSERVATIONS}",
                preactions=combined_delete_sql
            ),
            transformation_ctx="delete_child_records_temp_table"
        )
        
        logger.info(f"   ✓ Successfully deleted child records for {len(observation_ids_list)} observation(s)")
        logger.info(f"   ✓ Temp table cleaned up")
    
    except Exception as e:
        logger.error(f"   ✗ Failed to delete child records via temp table: {str(e)}")
        logger.error(f"   Error type: {type(e).__name__}")
        raise


def _delete_using_in_clause(
    observation_ids_list: List[str],
    glue_context: GlueContext,
    spark: SparkSession,
    config: DatabaseConfig,
    processing_config: ProcessingConfig
) -> None:
    """Delete using IN clause - suitable for small batches (<100 IDs)"""
    # Create comma-separated list of IDs for SQL IN clause
    # Escape single quotes in IDs and wrap in quotes
    escaped_ids = ["'" + str(obs_id).replace("'", "''") + "'" for obs_id in observation_ids_list]
    ids_str = ", ".join(escaped_ids)
    
    # Build DELETE statements for all observation-related tables
    # Order matters: delete from child tables first to avoid foreign key issues
    child_tables = TableNames.child_tables()
    delete_statements = [
        f"DELETE FROM public.{table} WHERE observation_id IN ({ids_str});"
        for table in child_tables
    ]
    # Main table last
    delete_statements.append(f"DELETE FROM public.{TableNames.OBSERVATIONS} WHERE observation_id IN ({ids_str});")
    
    # Combine all DELETE statements
    combined_delete_sql = " ".join(delete_statements)
    
    logger.info(f"   Executing deletions across {len(TableNames.all_tables())} tables using IN clause...")
    logger.info(f"   SQL preview (first 200 chars): {combined_delete_sql[:200]}...")
    
    # Skip actual deletion in TEST_MODE
    if processing_config.test_mode:
        logger.info(f"⚠️  TEST MODE: Skipping deletion of {len(observation_ids_list)} observation(s)")
        logger.info(f"   IDs that would be deleted: {observation_ids_list[:10]}{'...' if len(observation_ids_list) > 10 else ''}")
        return
    
    try:
        # Execute the DELETE statements using a dummy write with preactions
        empty_df = spark.createDataFrame([], "observation_id STRING")
        empty_dynamic_frame = DynamicFrame.fromDF(empty_df, glue_context, "empty_frame_for_delete")
        
        glue_context.write_dynamic_frame.from_options(
            frame=empty_dynamic_frame,
            connection_type="redshift",
            connection_options=get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable=f"public.{TableNames.OBSERVATIONS}",
                preactions=combined_delete_sql
            ),
            transformation_ctx="delete_entered_in_error_in_clause"
        )
        
        logger.info(f"✅ Successfully deleted {len(observation_ids_list)} entered-in-error observation(s)")
        logger.info(f"   Deleted from {len(TableNames.all_tables())} tables: observations + {len(TableNames.child_tables())} child tables")
    
    except Exception as e:
        logger.error(f"❌ Failed to delete entered-in-error observations: {str(e)}")
        logger.error(f"   Error type: {type(e).__name__}")
        logger.warning(f"   Continuing with job execution - manual cleanup may be required")


def _delete_using_temp_table(
    observation_ids_list: List[str],
    glue_context: GlueContext,
    spark: SparkSession,
    config: DatabaseConfig,
    processing_config: ProcessingConfig
) -> None:
    """
    Delete using temporary table JOIN - suitable for large batches (>=100 IDs)
    
    This approach:
    1. Creates a temp table with IDs to delete
    2. Loads IDs into temp table via Glue write
    3. Uses DELETE ... USING temp table JOIN (efficient for large datasets)
    4. Drops temp table
    
    Avoids Redshift's 16 MB SQL statement limit and 32,768 parameter limit.
    """
    # Skip actual deletion in TEST_MODE
    if processing_config.test_mode:
        logger.info(f"⚠️  TEST MODE: Skipping deletion of {len(observation_ids_list)} observation(s) using temp table method")
        logger.info(f"   IDs that would be deleted: {observation_ids_list[:10]}{'...' if len(observation_ids_list) > 10 else ''}")
        return
    
    temp_table_name = None
    try:
        # Create DataFrame with observation IDs to delete
        ids_data = [(obs_id,) for obs_id in observation_ids_list]
        ids_df = spark.createDataFrame(ids_data, ["observation_id"])
        ids_dynamic_frame = DynamicFrame.fromDF(ids_df, glue_context, "ids_to_delete")
        
        logger.info(f"   Created DataFrame with {len(observation_ids_list)} IDs to delete")
        
        # Temporary table name (use timestamp to ensure uniqueness)
        temp_table_name = f"tmp_delete_entered_in_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"   Using temporary table: {temp_table_name}")
        
        # Step 1: Create regular table (not TEMP) and load IDs
        # Note: We can't use TEMP TABLE because Glue's COPY operation happens in a different session
        # Regular table will be dropped after use
        create_table_sql = f"""
        DROP TABLE IF EXISTS public.{temp_table_name};
        CREATE TABLE public.{temp_table_name} (observation_id VARCHAR(255));
        """
        
        connection_options_temp = get_redshift_connection_options(
            connection_name=config.redshift_connection,
            database=config.redshift_database,
            s3_temp_dir=config.s3_temp_dir,
            dbtable=f"public.{temp_table_name}",
            preactions=create_table_sql
        )
        
        glue_context.write_dynamic_frame.from_options(
            frame=ids_dynamic_frame,
            connection_type="redshift",
            connection_options=connection_options_temp,
            transformation_ctx="write_ids_to_temp_table"
        )
        
        logger.info(f"   ✓ Loaded {len(observation_ids_list)} IDs into temp table")
        
        # Step 2: Execute DELETE statements using USING clause with temp table JOIN
        # This is much more efficient than large IN clauses
        child_tables = TableNames.child_tables()
        delete_statements = [
            f"DELETE FROM public.{table} USING public.{temp_table_name} WHERE {table}.observation_id = {temp_table_name}.observation_id;"
            for table in child_tables
        ]
        # Main table last
        delete_statements.append(
            f"DELETE FROM public.{TableNames.OBSERVATIONS} USING public.{temp_table_name} WHERE {TableNames.OBSERVATIONS}.observation_id = {temp_table_name}.observation_id;"
        )
        # Clean up temp table
        delete_statements.append(f"DROP TABLE IF EXISTS public.{temp_table_name};")
        
        combined_delete_sql = " ".join(delete_statements)
        
        logger.info(f"   Executing deletions across {len(TableNames.all_tables())} tables using temp table JOIN...")
        logger.info(f"   SQL preview (first 200 chars): {combined_delete_sql[:200]}...")
        
        # Execute DELETE statements using preactions on a dummy write
        empty_df = spark.createDataFrame([], "observation_id STRING")
        empty_dynamic_frame = DynamicFrame.fromDF(empty_df, glue_context, "empty_frame_for_delete")
        
        glue_context.write_dynamic_frame.from_options(
            frame=empty_dynamic_frame,
            connection_type="redshift",
            connection_options=get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable=f"public.{TableNames.OBSERVATIONS}",
                preactions=combined_delete_sql
            ),
            transformation_ctx="delete_entered_in_error_temp_table"
        )
        
        logger.info(f"✅ Successfully deleted {len(observation_ids_list)} entered-in-error observation(s)")
        logger.info(f"   Deleted from {len(TableNames.all_tables())} tables using temp table method")
        logger.info(f"   Temp table {temp_table_name} dropped")
    
    except Exception as e:
        logger.error(f"❌ Failed to delete entered-in-error observations using temp table: {str(e)}")
        logger.error(f"   Error type: {type(e).__name__}")
        logger.warning(f"   Continuing with job execution - manual cleanup may be required")
        # Try to clean up temp table if it exists
        if temp_table_name:
            try:
                cleanup_sql = f"DROP TABLE IF EXISTS public.{temp_table_name};"
                logger.info(f"   Attempting to clean up temp table: {temp_table_name}")
                empty_df = spark.createDataFrame([], "observation_id STRING")
                empty_dynamic_frame = DynamicFrame.fromDF(empty_df, glue_context, "cleanup_frame")
                glue_context.write_dynamic_frame.from_options(
                    frame=empty_dynamic_frame,
                    connection_type="redshift",
                    connection_options=get_redshift_connection_options(
                        connection_name=config.redshift_connection,
                        database=config.redshift_database,
                        s3_temp_dir=config.s3_temp_dir,
                        dbtable=f"public.{TableNames.OBSERVATIONS}",
                        preactions=cleanup_sql
                    ),
                    transformation_ctx="cleanup_temp_table"
                )
                logger.info(f"   ✓ Temp table cleaned up")
            except:
                logger.warning(f"   Could not clean up temp table {temp_table_name} - may need manual cleanup")


def _delete_using_dataframe_temp_table(
    entered_in_error_df: DataFrame,
    glue_context: GlueContext,
    spark: SparkSession,
    config: DatabaseConfig,
    processing_config: ProcessingConfig
) -> None:
    """
    Delete using temporary table JOIN - writes DataFrame directly to temp table
    
    This approach avoids expensive .collect() operations by writing the DataFrame
    directly to Redshift, then using JOIN-based DELETE statements.
    
    Steps:
    1. Extract just the 'id' column from the DataFrame
    2. Write directly to temp table (no ID collection needed)
    3. Use DELETE ... USING temp table JOIN (efficient for large datasets)
    4. Drop temp table
    
    This is much faster than collecting IDs first because:
    - No materialization of IDs to driver
    - No distinct() shuffle operation
    - Direct DataFrame-to-Redshift write (optimized)
    """
    temp_table_name = None
    try:
        # Extract just the ID column and rename it
        ids_df = entered_in_error_df.select(F.col("id").alias("observation_id")).distinct()
        ids_dynamic_frame = DynamicFrame.fromDF(ids_df, glue_context, "ids_to_delete")
        
        logger.info("   Writing entered-in-error IDs directly to temp table (no ID collection)")
        
        # Temporary table name (use timestamp to ensure uniqueness)
        temp_table_name = f"tmp_delete_entered_in_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"   Using temporary table: {temp_table_name}")
        
        # Step 1: Create regular table (not TEMP) and load IDs directly from DataFrame
        # Note: We can't use TEMP TABLE because Glue's COPY operation happens in a different session
        # Regular table will be dropped after use
        create_table_sql = f"""
        DROP TABLE IF EXISTS public.{temp_table_name};
        CREATE TABLE public.{temp_table_name} (observation_id VARCHAR(255));
        """
        
        connection_options_temp = get_redshift_connection_options(
            connection_name=config.redshift_connection,
            database=config.redshift_database,
            s3_temp_dir=config.s3_temp_dir,
            dbtable=f"public.{temp_table_name}",
            preactions=create_table_sql
        )
        
        glue_context.write_dynamic_frame.from_options(
            frame=ids_dynamic_frame,
            connection_type="redshift",
            connection_options=connection_options_temp,
            transformation_ctx="write_ids_to_temp_table"
        )
        
        logger.info("   ✓ Loaded IDs into temp table (written directly from DataFrame)")
        
        # Step 2: Execute DELETE statements using USING clause with temp table JOIN
        # This is much more efficient than large IN clauses
        child_tables = TableNames.child_tables()
        delete_statements = [
            f"DELETE FROM public.{table} USING public.{temp_table_name} WHERE {table}.observation_id = {temp_table_name}.observation_id;"
            for table in child_tables
        ]
        # Main table last
        delete_statements.append(
            f"DELETE FROM public.{TableNames.OBSERVATIONS} USING public.{temp_table_name} WHERE {TableNames.OBSERVATIONS}.observation_id = {temp_table_name}.observation_id;"
        )
        # Clean up temp table
        delete_statements.append(f"DROP TABLE IF EXISTS public.{temp_table_name};")
        
        combined_delete_sql = " ".join(delete_statements)
        
        logger.info(f"   Executing deletions across {len(TableNames.all_tables())} tables using temp table JOIN...")
        logger.info(f"   SQL preview (first 200 chars): {combined_delete_sql[:200]}...")
        
        # Execute DELETE statements using preactions on a dummy write
        empty_df = spark.createDataFrame([], "observation_id STRING")
        empty_dynamic_frame = DynamicFrame.fromDF(empty_df, glue_context, "empty_frame_for_delete")
        
        glue_context.write_dynamic_frame.from_options(
            frame=empty_dynamic_frame,
            connection_type="redshift",
            connection_options=get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable=f"public.{TableNames.OBSERVATIONS}",
                preactions=combined_delete_sql
            ),
            transformation_ctx="delete_entered_in_error_temp_table"
        )
        
        logger.info("✅ Successfully deleted entered-in-error observations")
        logger.info(f"   Deleted from {len(TableNames.all_tables())} tables using temp table method")
        logger.info(f"   Temp table {temp_table_name} dropped")
    
    except Exception as e:
        logger.error(f"❌ Failed to delete entered-in-error observations using temp table: {str(e)}")
        logger.error(f"   Error type: {type(e).__name__}")
        logger.warning(f"   Continuing with job execution - manual cleanup may be required")
        # Try to clean up temp table if it exists
        if temp_table_name:
            try:
                cleanup_sql = f"DROP TABLE IF EXISTS public.{temp_table_name};"
                logger.info(f"   Attempting to clean up temp table: {temp_table_name}")
                empty_df = spark.createDataFrame([], "observation_id STRING")
                empty_dynamic_frame = DynamicFrame.fromDF(empty_df, glue_context, "cleanup_frame")
                glue_context.write_dynamic_frame.from_options(
                    frame=empty_dynamic_frame,
                    connection_type="redshift",
                    connection_options=get_redshift_connection_options(
                        connection_name=config.redshift_connection,
                        database=config.redshift_database,
                        s3_temp_dir=config.s3_temp_dir,
                        dbtable=f"public.{TableNames.OBSERVATIONS}",
                        preactions=cleanup_sql
                    ),
                    transformation_ctx="cleanup_temp_table"
                )
                logger.info(f"   ✓ Temp table cleaned up")
            except:
                logger.warning(f"   Could not clean up temp table {temp_table_name} - may need manual cleanup")

