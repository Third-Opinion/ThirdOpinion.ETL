"""
Deletion utilities for conditions
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


def identify_deleted_condition_ids(df: DataFrame) -> List[str]:
    """
    Identify condition IDs that are marked as deleted (isDelete=true)
    
    Args:
        df: Source DataFrame from Iceberg with deduplicated conditions
        
    Returns:
        List of condition_ids that are deleted
    """
    logger.info("Identifying conditions marked as deleted (isDelete=true)...")
    
    # Check if isDelete column exists
    if "isDelete" not in df.columns:
        logger.warning("isDelete column not found in data, skipping deletion check")
        return []
    
    if "id" not in df.columns:
        logger.warning("id column not found in data, skipping deletion check")
        return []
    
    # Filter for deleted conditions
    deleted_df = df.filter(F.col("isDelete") == True)
    
    # Count records
    deleted_count = deleted_df.count()
    
    if deleted_count == 0:
        logger.info("✅ No conditions marked as deleted found")
        return []
    
    # Extract condition IDs
    condition_ids = [row['id'] for row in deleted_df.select('id').distinct().collect()]
    
    logger.info(f"⚠️  Found {len(condition_ids)} condition(s) marked as deleted")
    logger.info(f"   These conditions will be deleted from all Redshift tables")
    
    # Log sample IDs for audit purposes (max 10)
    if len(condition_ids) <= 10:
        logger.info(f"   Condition IDs to delete: {condition_ids}")
    else:
        logger.info(f"   Sample Condition IDs to delete (first 10): {condition_ids[:10]}")
    
    return condition_ids


def delete_condition_records(
    condition_ids_list: List[str],
    glue_context: GlueContext,
    spark: SparkSession,
    config: DatabaseConfig,
    processing_config: ProcessingConfig
) -> None:
    """
    Delete conditions from all Redshift tables
    
    This function removes deleted conditions from all condition-related tables.
    Uses a temporary table approach to handle large numbers of IDs efficiently
    and avoid Redshift's 16 MB SQL statement size limit.
    
    Args:
        condition_ids_list: List of condition IDs to delete
        glue_context: AWS Glue context
        spark: Spark session
        config: Database configuration
        processing_config: Processing configuration
    """
    if not condition_ids_list:
        logger.info("No deleted conditions to remove")
        return
    
    logger.info(f"🗑️  Deleting {len(condition_ids_list)} condition(s) from all tables...")
    
    # Determine deletion strategy based on number of IDs
    if len(condition_ids_list) < processing_config.batch_size:
        logger.info(f"   Using IN clause method (small batch: {len(condition_ids_list)} IDs)")
        _delete_using_in_clause(condition_ids_list, glue_context, spark, config, processing_config)
    else:
        logger.info(f"   Using temporary table method (large batch: {len(condition_ids_list)} IDs)")
        _delete_using_temp_table(condition_ids_list, glue_context, spark, config, processing_config)


def delete_child_records_for_conditions(
    condition_ids_list: List[str],
    glue_context: GlueContext,
    spark: SparkSession,
    config: DatabaseConfig,
    processing_config: ProcessingConfig
) -> None:
    """
    Delete child table records for specific condition IDs (not the conditions themselves)
    
    This is used when updating conditions to remove old child records before inserting new ones.
    Does NOT delete from the main conditions table.
    
    Args:
        condition_ids_list: List of condition_id values to clean up child records for
        glue_context: AWS Glue context
        spark: Spark session
        config: Database configuration
        processing_config: Processing configuration
    """
    logger.info(f"Deleting child records for {len(condition_ids_list)} condition(s)...")
    
    # Use small batch or large batch method based on size
    if len(condition_ids_list) < processing_config.batch_size:
        logger.info(f"   Using IN clause method (small batch: {len(condition_ids_list)} IDs)")
        _delete_child_records_in_clause(condition_ids_list, glue_context, spark, config, processing_config)
    else:
        logger.info(f"   Using temporary table method (large batch: {len(condition_ids_list)} IDs)")
        _delete_child_records_temp_table(condition_ids_list, glue_context, spark, config, processing_config)


def _delete_child_records_in_clause(
    condition_ids_list: List[str],
    glue_context: GlueContext,
    spark: SparkSession,
    config: DatabaseConfig,
    processing_config: ProcessingConfig
) -> None:
    """Delete child records only using IN clause"""
    # Create comma-separated list of IDs for SQL IN clause
    escaped_ids = ["'" + str(cond_id).replace("'", "''") + "'" for cond_id in condition_ids_list]
    ids_str = ", ".join(escaped_ids)
    
    # Build DELETE statements for child tables ONLY (not main conditions table)
    child_tables = TableNames.child_tables()
    delete_statements = [
        f"DELETE FROM public.{table} WHERE condition_id IN ({ids_str});"
        for table in child_tables
    ]
    
    # Combine all DELETE statements
    combined_delete_sql = " ".join(delete_statements)
    
    logger.info(f"   Executing child table deletions using IN clause...")
    logger.info(f"   SQL preview (first 200 chars): {combined_delete_sql[:200]}...")
    
    # Skip actual deletion in TEST_MODE
    if processing_config.test_mode:
        logger.info(f"⚠️  TEST MODE: Skipping child record deletion for {len(condition_ids_list)} condition(s)")
        logger.info(f"   IDs that would have child records deleted: {condition_ids_list[:10]}{'...' if len(condition_ids_list) > 10 else ''}")
        return
    
    try:
        # Execute the DELETE statements using a dummy write with preactions
        empty_df = spark.createDataFrame([], "condition_id STRING")
        empty_dynamic_frame = DynamicFrame.fromDF(empty_df, glue_context, "empty_frame_for_child_delete")
        
        glue_context.write_dynamic_frame.from_options(
            frame=empty_dynamic_frame,
            connection_type="redshift",
            connection_options=get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable=f"public.{TableNames.CONDITIONS}",
                preactions=combined_delete_sql
            ),
            transformation_ctx="delete_child_records_preaction"
        )
        
        logger.info(f"   ✓ Successfully deleted child records for {len(condition_ids_list)} condition(s)")
    
    except Exception as e:
        logger.error(f"   ✗ Failed to delete child records: {str(e)}")
        logger.error(f"   Error type: {type(e).__name__}")
        raise


def _delete_child_records_temp_table(
    condition_ids_list: List[str],
    glue_context: GlueContext,
    spark: SparkSession,
    config: DatabaseConfig,
    processing_config: ProcessingConfig
) -> None:
    """Delete child records only using temporary table JOIN"""
    # Skip actual deletion in TEST_MODE
    if processing_config.test_mode:
        logger.info(f"⚠️  TEST MODE: Skipping child record deletion for {len(condition_ids_list)} condition(s) using temp table method")
        logger.info(f"   IDs that would have child records deleted: {condition_ids_list[:10]}{'...' if len(condition_ids_list) > 10 else ''}")
        return
    
    temp_table_name = None
    try:
        # Create DataFrame with condition IDs
        ids_data = [(cond_id,) for cond_id in condition_ids_list]
        ids_df = spark.createDataFrame(ids_data, ["condition_id"])
        ids_dynamic_frame = DynamicFrame.fromDF(ids_df, glue_context, "ids_for_child_delete")
        
        logger.info(f"   Created DataFrame with {len(condition_ids_list)} IDs")
        
        # Temporary table name
        temp_table_name = f"tmp_delete_child_records_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"   Using temporary table: {temp_table_name}")
        
        # Step 1: Create temp table and load IDs
        create_table_sql = f"""
        DROP TABLE IF EXISTS public.{temp_table_name};
        CREATE TABLE public.{temp_table_name} (condition_id VARCHAR(255));
        """
        
        glue_context.write_dynamic_frame.from_options(
            frame=ids_dynamic_frame,
            connection_type="redshift",
            connection_options=get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable=f"public.{temp_table_name}",
                preactions=create_table_sql
            ),
            transformation_ctx="write_ids_to_temp_table_child_delete"
        )
        
        logger.info(f"   ✓ Loaded {len(condition_ids_list)} IDs into temp table")
        
        # Step 2: Execute DELETE statements for child tables ONLY
        child_tables = TableNames.child_tables()
        delete_statements = [
            f"DELETE FROM public.{table} USING public.{temp_table_name} WHERE {table}.condition_id = {temp_table_name}.condition_id;"
            for table in child_tables
        ]
        # Clean up temp table
        delete_statements.append(f"DROP TABLE IF EXISTS public.{temp_table_name};")
        
        combined_delete_sql = " ".join(delete_statements)
        
        logger.info(f"   Executing child table deletions using temp table JOIN...")
        
        # Execute DELETE statements
        empty_df = spark.createDataFrame([], "condition_id STRING")
        empty_dynamic_frame = DynamicFrame.fromDF(empty_df, glue_context, "empty_frame_for_child_delete")
        
        glue_context.write_dynamic_frame.from_options(
            frame=empty_dynamic_frame,
            connection_type="redshift",
            connection_options=get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable=f"public.{TableNames.CONDITIONS}",
                preactions=combined_delete_sql
            ),
            transformation_ctx="delete_child_records_temp_table"
        )
        
        logger.info(f"   ✓ Successfully deleted child records for {len(condition_ids_list)} condition(s)")
        logger.info(f"   ✓ Temp table cleaned up")
    
    except Exception as e:
        logger.error(f"   ✗ Failed to delete child records via temp table: {str(e)}")
        logger.error(f"   Error type: {type(e).__name__}")
        raise


def _delete_using_in_clause(
    condition_ids_list: List[str],
    glue_context: GlueContext,
    spark: SparkSession,
    config: DatabaseConfig,
    processing_config: ProcessingConfig
) -> None:
    """Delete using IN clause - suitable for small batches (<100 IDs)"""
    # Create comma-separated list of IDs for SQL IN clause
    escaped_ids = ["'" + str(cond_id).replace("'", "''") + "'" for cond_id in condition_ids_list]
    ids_str = ", ".join(escaped_ids)
    
    # Build DELETE statements for all condition-related tables
    # Order matters: delete from child tables first to avoid foreign key issues
    child_tables = TableNames.child_tables()
    delete_statements = [
        f"DELETE FROM public.{table} WHERE condition_id IN ({ids_str});"
        for table in child_tables
    ]
    # Main table last
    delete_statements.append(f"DELETE FROM public.{TableNames.CONDITIONS} WHERE condition_id IN ({ids_str});")
    
    # Combine all DELETE statements
    combined_delete_sql = " ".join(delete_statements)
    
    logger.info(f"   Executing deletions across {len(TableNames.all_tables())} tables using IN clause...")
    logger.info(f"   SQL preview (first 200 chars): {combined_delete_sql[:200]}...")
    
    # Skip actual deletion in TEST_MODE
    if processing_config.test_mode:
        logger.info(f"⚠️  TEST MODE: Skipping deletion of {len(condition_ids_list)} condition(s)")
        logger.info(f"   IDs that would be deleted: {condition_ids_list[:10]}{'...' if len(condition_ids_list) > 10 else ''}")
        return
    
    try:
        # Execute the DELETE statements using a dummy write with preactions
        empty_df = spark.createDataFrame([], "condition_id STRING")
        empty_dynamic_frame = DynamicFrame.fromDF(empty_df, glue_context, "empty_frame_for_delete")
        
        glue_context.write_dynamic_frame.from_options(
            frame=empty_dynamic_frame,
            connection_type="redshift",
            connection_options=get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable=f"public.{TableNames.CONDITIONS}",
                preactions=combined_delete_sql
            ),
            transformation_ctx="delete_conditions_in_clause"
        )
        
        logger.info(f"✅ Successfully deleted {len(condition_ids_list)} condition(s)")
        logger.info(f"   Deleted from {len(TableNames.all_tables())} tables: conditions + {len(TableNames.child_tables())} child tables")
    
    except Exception as e:
        logger.error(f"❌ Failed to delete conditions: {str(e)}")
        logger.error(f"   Error type: {type(e).__name__}")
        logger.warning(f"   Continuing with job execution - manual cleanup may be required")


def _delete_using_temp_table(
    condition_ids_list: List[str],
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
        logger.info(f"⚠️  TEST MODE: Skipping deletion of {len(condition_ids_list)} condition(s) using temp table method")
        logger.info(f"   IDs that would be deleted: {condition_ids_list[:10]}{'...' if len(condition_ids_list) > 10 else ''}")
        return
    
    temp_table_name = None
    try:
        # Create DataFrame with condition IDs to delete
        ids_data = [(cond_id,) for cond_id in condition_ids_list]
        ids_df = spark.createDataFrame(ids_data, ["condition_id"])
        ids_dynamic_frame = DynamicFrame.fromDF(ids_df, glue_context, "ids_to_delete")
        
        logger.info(f"   Created DataFrame with {len(condition_ids_list)} IDs to delete")
        
        # Temporary table name (use timestamp to ensure uniqueness)
        temp_table_name = f"tmp_delete_conditions_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"   Using temporary table: {temp_table_name}")
        
        # Step 1: Create regular table (not TEMP) and load IDs
        create_table_sql = f"""
        DROP TABLE IF EXISTS public.{temp_table_name};
        CREATE TABLE public.{temp_table_name} (condition_id VARCHAR(255));
        """
        
        glue_context.write_dynamic_frame.from_options(
            frame=ids_dynamic_frame,
            connection_type="redshift",
            connection_options=get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable=f"public.{temp_table_name}",
                preactions=create_table_sql
            ),
            transformation_ctx="write_ids_to_temp_table"
        )
        
        logger.info(f"   ✓ Loaded {len(condition_ids_list)} IDs into temp table")
        
        # Step 2: Execute DELETE statements using USING clause with temp table JOIN
        child_tables = TableNames.child_tables()
        delete_statements = [
            f"DELETE FROM public.{table} USING public.{temp_table_name} WHERE {table}.condition_id = {temp_table_name}.condition_id;"
            for table in child_tables
        ]
        # Main table last
        delete_statements.append(
            f"DELETE FROM public.{TableNames.CONDITIONS} USING public.{temp_table_name} WHERE {TableNames.CONDITIONS}.condition_id = {temp_table_name}.condition_id;"
        )
        # Clean up temp table
        delete_statements.append(f"DROP TABLE IF EXISTS public.{temp_table_name};")
        
        combined_delete_sql = " ".join(delete_statements)
        
        logger.info(f"   Executing deletions across {len(TableNames.all_tables())} tables using temp table JOIN...")
        
        # Execute DELETE statements using preactions on a dummy write
        empty_df = spark.createDataFrame([], "condition_id STRING")
        empty_dynamic_frame = DynamicFrame.fromDF(empty_df, glue_context, "empty_frame_for_delete")
        
        glue_context.write_dynamic_frame.from_options(
            frame=empty_dynamic_frame,
            connection_type="redshift",
            connection_options=get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable=f"public.{TableNames.CONDITIONS}",
                preactions=combined_delete_sql
            ),
            transformation_ctx="delete_conditions_temp_table"
        )
        
        logger.info(f"✅ Successfully deleted {len(condition_ids_list)} condition(s)")
        logger.info(f"   Deleted from {len(TableNames.all_tables())} tables using temp table method")
        logger.info(f"   Temp table {temp_table_name} dropped")
    
    except Exception as e:
        logger.error(f"❌ Failed to delete conditions using temp table: {str(e)}")
        logger.error(f"   Error type: {type(e).__name__}")
        logger.warning(f"   Continuing with job execution - manual cleanup may be required")
        # Try to clean up temp table if it exists
        if temp_table_name:
            try:
                cleanup_sql = f"DROP TABLE IF EXISTS public.{temp_table_name};"
                logger.info(f"   Attempting to clean up temp table: {temp_table_name}")
                empty_df = spark.createDataFrame([], "condition_id STRING")
                empty_dynamic_frame = DynamicFrame.fromDF(empty_df, glue_context, "cleanup_frame")
                glue_context.write_dynamic_frame.from_options(
                    frame=empty_dynamic_frame,
                    connection_type="redshift",
                    connection_options=get_redshift_connection_options(
                        connection_name=config.redshift_connection,
                        database=config.redshift_database,
                        s3_temp_dir=config.s3_temp_dir,
                        dbtable=f"public.{TableNames.CONDITIONS}",
                        preactions=cleanup_sql
                    ),
                    transformation_ctx="cleanup_temp_table"
                )
                logger.info(f"   ✓ Temp table cleaned up")
            except:
                logger.warning(f"   Could not clean up temp table {temp_table_name} - may need manual cleanup")

