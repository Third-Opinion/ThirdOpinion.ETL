"""
Bookmark utilities for incremental processing
"""
from datetime import datetime, timedelta
from typing import Optional
from awsglue.context import GlueContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

from ..config import DatabaseConfig, ProcessingConfig
from .redshift_connection_utils import get_redshift_connection_options

logger = logging.getLogger(__name__)


def get_bookmark_from_redshift(
    glue_context: GlueContext,
    main_table_name: str,
    config: DatabaseConfig,
    processing_config: ProcessingConfig
) -> Optional[datetime]:
    """
    Get the maximum meta_last_updated timestamp from Redshift main table
    
    This bookmark represents the latest data already loaded into Redshift.
    We'll only process Iceberg records newer than this timestamp.
    
    If BACKDATE_DAYS is specified and a bookmark exists, the bookmark will be
    decremented by that many days to reprocess historical data.
    
    Args:
        glue_context: AWS Glue context
        main_table_name: Name of the main table (e.g., "observations", "conditions")
        config: Database configuration
        processing_config: Processing configuration
        
    Returns:
        Bookmark timestamp or None if no bookmark exists
    """
    logger.info(f"Fetching bookmark (max meta_last_updated) from Redshift {main_table_name} table...")
    
    # In TEST_MODE, skip Redshift read and process all records
    if processing_config.test_mode:
        logger.info("⚠️  TEST MODE: Skipping Redshift bookmark check - will process all records")
        return None
    
    try:
        # Use raw SQL query to get the maximum timestamp
        # The Glue connection is now configured to use "test" database as default
        logger.info(f"Fetching bookmark from database: {config.redshift_database}")
        logger.info(f"Using Glue connection '{config.redshift_connection}' (configured for '{config.redshift_database}' database)")
        
        bookmark_query = f"SELECT MAX(meta_last_updated) as max_timestamp FROM public.{main_table_name}"
        
        # Use connection properties - the connection is configured with the correct database
        connection_options = get_redshift_connection_options(
            connection_name=config.redshift_connection,
            database=config.redshift_database,
            s3_temp_dir=config.s3_temp_dir,
            query=bookmark_query
        )
        
        bookmark_frame = glue_context.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options=connection_options,
            transformation_ctx="read_bookmark"
        )
        
        # Convert to DataFrame and get the value
        bookmark_df = bookmark_frame.toDF()
        bookmark_rows = bookmark_df.collect()
        
        if len(bookmark_rows) > 0:
            max_timestamp = bookmark_rows[0]['max_timestamp']
            
            # Log what we found for debugging
            logger.info(f"Bookmark query returned: {max_timestamp} (type: {type(max_timestamp).__name__})")
            
            if max_timestamp:
                logger.info(f"✅ Bookmark found: {max_timestamp}")
                logger.info(f"⚠️  WARNING: If test.{main_table_name} is empty, this bookmark may be from a different database")
                logger.info(f"   The Glue connection may be using its default database instead of '{config.redshift_database}'")
                
                # Apply backdate if specified (only when bookmark exists)
                if processing_config.backdate_days > 0:
                    # Convert timestamp to datetime if it's not already
                    if isinstance(max_timestamp, str):
                        max_timestamp = datetime.fromisoformat(max_timestamp.replace('Z', '+00:00'))
                    
                    original_bookmark = max_timestamp
                    max_timestamp = max_timestamp - timedelta(days=processing_config.backdate_days)
                    
                    logger.info(f"📅 BACKDATE MODE: Rewinding bookmark by {processing_config.backdate_days} days")
                    logger.info(f"   Original bookmark: {original_bookmark}")
                    logger.info(f"   Adjusted bookmark: {max_timestamp}")
                    logger.info(f"   Will reprocess data from {max_timestamp} onwards")
                
                logger.info(f"Will only process Iceberg records with meta.lastUpdated > {max_timestamp}")
                logger.info(f"⚠️  WARNING: Bookmark found, but if test.{main_table_name} is empty, the query may have hit a different database")
                logger.info(f"   Verify that the Glue connection 'Redshift connection' is configured to use '{config.redshift_database}' database")
                return max_timestamp
            else:
                logger.info(f"No bookmark found ({main_table_name} table is empty or max_timestamp is NULL)")
                logger.info("This is an initial full load - will process all Iceberg records")
                if processing_config.backdate_days > 0:
                    logger.info(f"⚠️  BACKDATE_DAYS={processing_config.backdate_days} specified but no bookmark exists - ignoring backdate")
                return None
        else:
            logger.info("No bookmark available - proceeding with full load")
            if processing_config.backdate_days > 0:
                logger.info(f"⚠️  BACKDATE_DAYS={processing_config.backdate_days} specified but no bookmark exists - ignoring backdate")
            return None
    
    except Exception as e:
        error_str = str(e)
        
        # Check if this is a network/connectivity issue (VPC without Secrets Manager endpoint)
        if "Network is unreachable" in error_str or "secretsmanager" in error_str.lower():
            logger.warning("⚠️  Could not connect to AWS Secrets Manager to fetch Redshift credentials")
            logger.warning("   This may occur if:")
            logger.warning("   1. The Glue job VPC doesn't have a VPC endpoint for Secrets Manager")
            logger.warning("   2. The Glue job VPC doesn't have internet access")
            logger.warning("   3. This is the first run and the table doesn't exist yet")
            logger.info("   Proceeding with full initial load of all Iceberg records")
        elif "table" in error_str.lower() and ("does not exist" in error_str.lower() or "not found" in error_str.lower()):
            logger.info("✅ Table does not exist yet - this is the first run")
            logger.info("   Proceeding with full initial load of all Iceberg records")
        else:
            logger.warning(f"⚠️  Could not fetch bookmark: {error_str}")
            logger.info("   Proceeding with full initial load of all Iceberg records")
        
        if processing_config.backdate_days > 0:
            logger.info(f"⚠️  BACKDATE_DAYS={processing_config.backdate_days} specified but no bookmark exists - ignoring backdate")
        return None


def filter_by_bookmark(df: DataFrame, bookmark_timestamp: Optional[datetime]) -> DataFrame:
    """
    Filter Iceberg DataFrame to only include records newer than the bookmark
    
    Args:
        df: Source DataFrame from Iceberg
        bookmark_timestamp: Maximum meta_last_updated from Redshift (or None for full load)
    
    Returns:
        Filtered DataFrame with only new/updated records
    """
    if bookmark_timestamp is None:
        logger.info("No bookmark - processing all Iceberg records (full load)")
        return df
    
    logger.info(f"Applying bookmark filter to Iceberg data...")
    logger.info(f"Bookmark threshold: {bookmark_timestamp}")
    
    # Parse meta.lastUpdated timestamp from Iceberg data
    from .timestamp_utils import create_timestamp_parser
    
    timestamp_expr = create_timestamp_parser(F.col("meta").getField("lastUpdated"))
    
    # Add timestamp column temporarily for filtering
    df_with_ts = df.withColumn("_filter_ts", timestamp_expr)
    
    # Filter for records newer than bookmark
    filtered_df = df_with_ts.filter(F.col("_filter_ts") > F.lit(bookmark_timestamp)).drop("_filter_ts")
    
    # Count results
    initial_count = df.count()
    filtered_count = filtered_df.count()
    skipped_count = initial_count - filtered_count
    
    logger.info(f"✅ Bookmark filter applied:")
    logger.info(f"  📊 Total records in Iceberg: {initial_count:,}")
    logger.info(f"  📊 New/updated records (after bookmark): {filtered_count:,}")
    logger.info(f"  ⏭️  Records skipped (already in Redshift): {skipped_count:,}")
    if initial_count > 0:
        logger.info(f"  📈 Filter efficiency: {(skipped_count/initial_count)*100:.1f}% skipped")
    
    return filtered_df

