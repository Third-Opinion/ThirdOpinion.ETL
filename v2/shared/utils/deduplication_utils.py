"""
Deduplication utilities for FHIR entities
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
import logging

from .timestamp_utils import create_timestamp_parser

logger = logging.getLogger(__name__)


def deduplicate_entities(df: DataFrame, entity_type: str = "entity") -> DataFrame:
    """
    Deduplicate entities by keeping only the latest occurrence of each entity ID
    
    Uses an optimized window function approach that:
    - Uses row_number() window function for efficient deduplication
    - Minimizes shuffle operations
    - Handles large datasets efficiently
    
    Args:
        df: Source DataFrame with entities
        entity_type: Type of entity (e.g., "observation", "condition") for logging
        
    Returns:
        Deduplicated DataFrame with one record per entity ID
    """
    logger.info(f"Deduplicating {entity_type}s by {entity_type} ID...")
    logger.info("Using optimized window function-based deduplication strategy")
    
    # Check if required columns exist
    if "id" not in df.columns:
        logger.warning("id column not found in data, skipping deduplication")
        return df
    
    if "meta" not in df.columns:
        logger.warning("meta column not found in data, skipping deduplication")
        return df
    
    # Check if DataFrame is empty using lightweight check (avoids expensive shuffle)
    logger.info("Checking if deduplication is needed...")
    sample_check = df.limit(1).collect()
    if len(sample_check) == 0:
        logger.info(f"No {entity_type}s to deduplicate")
        return df
    
    # Skip initial count() to avoid expensive shuffle - we know there's data from the check above
    logger.info(f"Proceeding with deduplication (initial count skipped to avoid shuffle)")
    
    # Always perform deduplication to ensure data quality
    logger.info("Proceeding with deduplication to ensure data quality...")
    
    # Handle different possible timestamp formats in meta.lastUpdated
    logger.info("Extracting timestamps for deduplication...")
    timestamp_expr = create_timestamp_parser(F.col("meta").getField("lastUpdated"))
    
    # Add timestamp column
    df_with_ts = df.withColumn("_dedup_ts", timestamp_expr)
    
    # OPTIMIZED APPROACH: Repartition before window operation to reduce shuffle memory pressure
    # Window operations can cause large shuffles - repartitioning first helps distribute the load
    logger.info(f"Repartitioning data before window operation to reduce shuffle overhead...")
    num_partitions = max(1, df_with_ts.rdd.getNumPartitions())
    
    # Repartition to reasonable number (50-100) before window operation
    # This reduces shuffle data volume during the window function
    if num_partitions > 200 or num_partitions < 50:
        target_partitions = 75
        logger.info(f"Repartitioning from {num_partitions} to {target_partitions} partitions before window operation")
        df_with_ts = df_with_ts.repartition(target_partitions)
    
    # OPTIMIZED APPROACH: Use window function for efficient deduplication
    logger.info(f"Finding maximum timestamp for each {entity_type} ID using window function...")
    
    # Define window partitioned by id, ordered by timestamp descending
    window_spec = Window.partitionBy("id").orderBy(F.col("_dedup_ts").desc())
    
    # Add row number - row 1 will be the latest record for each ID
    df_with_rownum = df_with_ts.withColumn("_row_num", F.row_number().over(window_spec))
    
    # Filter to keep only row 1 (latest record per ID)
    logger.info("Executing deduplication (keeping latest record per ID)...")
    deduplicated_with_ts = df_with_rownum.filter(F.col("_row_num") == 1).drop("_row_num")
    
    # In case multiple records have the same max timestamp, use dropDuplicates as final safety
    logger.info("Applying final duplicate removal (for identical timestamps)...")
    deduplicated_df = deduplicated_with_ts.dropDuplicates(["id"]).drop("_dedup_ts")
    
    # Skip final count() to avoid expensive shuffle
    # The deduplication is complete regardless of the count
    logger.info("✅ Deduplication completed (counts skipped to avoid shuffle)")
    logger.info(f"  📊 Deduplication applied - keeping latest record per {entity_type} ID")
    
    return deduplicated_df

