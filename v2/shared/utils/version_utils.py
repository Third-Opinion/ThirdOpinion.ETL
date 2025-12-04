"""
Version comparison utilities for FHIR entity versioning
"""
from typing import Dict, List, Tuple, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
from awsglue.context import GlueContext
import logging

from ..config import DatabaseConfig
from .redshift_connection_utils import get_redshift_connection_options

logger = logging.getLogger(__name__)


def get_existing_versions_from_redshift(
    glue_context: GlueContext,
    table_name: str,
    id_column: str,
    config: DatabaseConfig
) -> Dict[str, any]:
    """
    Query Redshift to get existing entity timestamps for comparison
    
    Args:
        glue_context: AWS Glue context
        table_name: Name of the table to query
        id_column: Column name for entity ID
        config: Database configuration
        
    Returns:
        Dictionary mapping entity_id to timestamp
    """
    logger.info(f"Fetching existing timestamps from {table_name}...")
    
    try:
        # First check if table exists by trying to read it directly
        # This prevents malformed query errors when table doesn't exist
        connection_options = get_redshift_connection_options(
            connection_name=config.redshift_connection,
            database=config.redshift_database,
            s3_temp_dir=config.s3_temp_dir,
            dbtable=f"public.{table_name}"
        )
        
        existing_versions_df = glue_context.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options=connection_options,
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


def filter_dataframe_by_version(
    df: DataFrame,
    existing_versions: Dict[str, any],
    id_column: str
) -> Tuple[DataFrame, int, int]:
    """
    Filter DataFrame based on version comparison
    
    Args:
        df: Input DataFrame
        existing_versions: Dictionary of existing entity_id -> timestamp mappings
        id_column: Column name for entity ID
        
    Returns:
        Tuple of (filtered_df, to_process_count, skipped_count)
    """
    logger.info("Filtering data based on version comparison...")
    
    if not existing_versions:
        # No existing data, all records are new
        # Skip count() to avoid expensive shuffle - estimate from partitions instead
        # For logging, use lightweight check
        sample_check = df.limit(1).collect()
        if len(sample_check) > 0:
            logger.info(f"No existing versions found - treating all records as new (count skipped to avoid shuffle)")
        else:
            logger.info("No existing versions found - DataFrame is empty")
        # Return None for counts to indicate they weren't calculated (avoids shuffle)
        return df, None, 0
    
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
    needs_processing_udf = F.udf(needs_processing, BooleanType())
    
    # Add processing flag
    df_with_flag = df.withColumn(
        "needs_processing",
        needs_processing_udf(F.col(id_column), F.col("meta_last_updated"))
    )
    
    # Split into processing needed and skipped
    to_process_df = df_with_flag.filter(F.col("needs_processing") == True).drop("needs_processing")
    
    # Skip expensive count() operations - use lightweight checks instead
    # Counts are not needed for functionality, only for logging
    to_process_check = to_process_df.limit(1).collect()
    skipped_check = df_with_flag.filter(F.col("needs_processing") == False).limit(1).collect()
    
    logger.info(f"Version comparison results:")
    logger.info(f"  Records to process (new/updated): {'Yes' if len(to_process_check) > 0 else 'No'} (count skipped to avoid shuffle)")
    logger.info(f"  Records to skip (same version): {'Yes' if len(skipped_check) > 0 else 'No'} (count skipped to avoid shuffle)")
    
    # Return None for counts to indicate they weren't calculated (avoids expensive shuffles)
    to_process_count = None
    skipped_count = None
    
    return to_process_df, to_process_count, skipped_count


def get_entities_to_delete(
    df: DataFrame,
    existing_versions: Dict[str, any],
    id_column: str
) -> List[str]:
    """
    Get list of entity IDs that need their old versions deleted
    
    Args:
        df: Input DataFrame with new/updated entities
        existing_versions: Dictionary of existing entity_id -> timestamp mappings
        id_column: Column name for entity ID
        
    Returns:
        List of entity IDs that need cleanup
    """
    logger.info("Identifying entities that need old version cleanup...")
    
    if not existing_versions:
        return []
    
    # Get list of entity IDs from incoming data
    # Use lightweight check instead of count() to avoid expensive shuffle
    incoming_entity_ids = set()
    sample_check = df.limit(1).collect()
    if len(sample_check) > 0:
        # Collect distinct IDs - this is necessary for the logic, but limit the collect
        # For very large datasets, consider using a different approach
        entity_rows = df.select(id_column).distinct().collect()
        incoming_entity_ids = {row[id_column] for row in entity_rows if row[id_column]}
    
    # Find entities that exist in both incoming data and Redshift
    entities_to_delete = []
    for entity_id in incoming_entity_ids:
        if entity_id in existing_versions:
            entities_to_delete.append(entity_id)
    
    logger.info(f"Found {len(entities_to_delete)} entities that need old version cleanup")
    return entities_to_delete

