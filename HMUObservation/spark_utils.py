"""
Spark utility functions for optimizing DataFrame operations in AWS Glue jobs.
These utilities help prevent Spark context shutdowns and improve performance.
"""

import time
import logging
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import gc

logger = logging.getLogger(__name__)


def safe_count(df: DataFrame, operation_name: str = "count", max_retries: int = 3,
               repartition_before: bool = True, num_partitions: int = 200) -> int:
    """
    Safely count DataFrame rows with retry logic and optional repartitioning.

    Args:
        df: The DataFrame to count
        operation_name: Name of the operation for logging
        max_retries: Maximum number of retry attempts
        repartition_before: Whether to repartition before counting
        num_partitions: Number of partitions if repartitioning

    Returns:
        The count of rows in the DataFrame
    """
    for attempt in range(max_retries):
        try:
            # Optionally repartition to distribute data evenly
            if repartition_before and attempt > 0:
                logger.info(f"Repartitioning DataFrame to {num_partitions} partitions before {operation_name}")
                df = df.repartition(num_partitions)

            # Perform the count
            count = df.count()
            logger.info(f"✅ {operation_name} successful: {count:,} records")
            return count

        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"⚠️ {operation_name} failed on attempt {attempt + 1}, retrying... Error: {str(e)}")
                time.sleep(5 * (attempt + 1))  # Exponential backoff

                # Try to recover by triggering garbage collection
                gc.collect()
            else:
                logger.error(f"❌ {operation_name} failed after {max_retries} attempts")
                raise

    return 0


def batch_count_operations(dataframes_dict: dict, repartition_before: bool = True) -> dict:
    """
    Perform multiple count operations in a more efficient batch manner.

    Args:
        dataframes_dict: Dictionary of {name: DataFrame} to count
        repartition_before: Whether to repartition before counting

    Returns:
        Dictionary of {name: count} results
    """
    results = {}

    for name, df in dataframes_dict.items():
        if df is not None:
            try:
                count = safe_count(df, f"Counting {name}", repartition_before=repartition_before)
                results[name] = count
            except Exception as e:
                logger.error(f"Failed to count {name}: {str(e)}")
                results[name] = -1
        else:
            results[name] = 0

    return results


def optimize_dataframe_for_counting(df: DataFrame, cache: bool = True,
                                   repartition: bool = True, num_partitions: int = 200) -> DataFrame:
    """
    Optimize a DataFrame for multiple count operations.

    Args:
        df: The DataFrame to optimize
        cache: Whether to cache the DataFrame
        repartition: Whether to repartition the DataFrame
        num_partitions: Number of partitions if repartitioning

    Returns:
        The optimized DataFrame
    """
    # Repartition for better parallelism
    if repartition:
        current_partitions = df.rdd.getNumPartitions()
        logger.info(f"Current partitions: {current_partitions}, repartitioning to {num_partitions}")
        df = df.repartition(num_partitions)

    # Cache if multiple operations will be performed
    if cache:
        logger.info("Caching DataFrame for repeated operations")
        df = df.cache()

        # Force cache by triggering an action
        _ = df.first()

    return df


def checkpoint_dataframe(df: DataFrame, spark: SparkSession, checkpoint_dir: str = None) -> DataFrame:
    """
    Checkpoint a DataFrame to break the lineage and prevent stack overflow.

    Args:
        df: The DataFrame to checkpoint
        spark: The SparkSession
        checkpoint_dir: Directory for checkpointing (uses temp dir if not specified)

    Returns:
        The checkpointed DataFrame
    """
    if checkpoint_dir is None:
        checkpoint_dir = "s3://aws-glue-assets-442042533707-us-east-2/checkpoints/"

    # Set checkpoint directory if not already set
    if spark.sparkContext.getCheckpointDir() is None:
        spark.sparkContext.setCheckpointDir(checkpoint_dir)

    logger.info(f"Checkpointing DataFrame to {checkpoint_dir}")
    return df.checkpoint()


def monitor_spark_context(spark: SparkSession) -> bool:
    """
    Check if Spark context is still active.

    Args:
        spark: The SparkSession

    Returns:
        True if context is active, False otherwise
    """
    try:
        is_stopped = spark.sparkContext._jsc.sc().isStopped()
        if is_stopped:
            logger.error("⚠️ Spark context has been stopped!")
            return False
        return True
    except Exception as e:
        logger.error(f"Error checking Spark context status: {str(e)}")
        return False


def log_memory_usage(spark: SparkSession, operation_name: str = ""):
    """
    Log current memory usage and executor status.

    Args:
        spark: The SparkSession
        operation_name: Name of the current operation for context
    """
    try:
        status = spark.sparkContext.statusTracker()

        # Get executor information
        executor_infos = status.getExecutorInfos()
        active_executors = len([e for e in executor_infos if e.totalCores > 0])

        logger.info(f"📊 Memory Status for {operation_name}:")
        logger.info(f"  Active Executors: {active_executors}")

        # Get stage information
        active_stages = status.getActiveStageIds()
        logger.info(f"  Active Stages: {len(active_stages)}")

    except Exception as e:
        logger.warning(f"Could not log memory usage: {str(e)}")


def release_dataframe_resources(df: DataFrame, unpersist: bool = True):
    """
    Release resources associated with a DataFrame.

    Args:
        df: The DataFrame to release
        unpersist: Whether to unpersist cached DataFrames
    """
    try:
        if unpersist and df.is_cached:
            logger.info("Unpersisting cached DataFrame")
            df.unpersist()

        # Trigger garbage collection
        gc.collect()

    except Exception as e:
        logger.warning(f"Error releasing DataFrame resources: {str(e)}")


def split_dataframe_for_processing(df: DataFrame, batch_size: int = 100000) -> list:
    """
    Split a large DataFrame into smaller batches for processing.

    Args:
        df: The DataFrame to split
        batch_size: Size of each batch

    Returns:
        List of DataFrames
    """
    total_count = df.count()
    num_batches = (total_count // batch_size) + (1 if total_count % batch_size > 0 else 0)

    logger.info(f"Splitting DataFrame into {num_batches} batches of {batch_size:,} records")

    # Add row numbers for splitting
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    window = Window.orderBy(F.monotonically_increasing_id())
    df_with_row_num = df.withColumn("row_num", F.row_number().over(window))

    batches = []
    for i in range(num_batches):
        start = i * batch_size + 1
        end = min((i + 1) * batch_size, total_count)

        batch_df = df_with_row_num.filter(
            (F.col("row_num") >= start) & (F.col("row_num") <= end)
        ).drop("row_num")

        batches.append(batch_df)
        logger.info(f"  Batch {i + 1}: Records {start:,} to {end:,}")

    return batches


def configure_spark_for_large_shuffle(spark: SparkSession):
    """
    Configure Spark session for large shuffle operations.

    Args:
        spark: The SparkSession to configure
    """
    conf_updates = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.shuffle.partitions": "400",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.shuffle.compress": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
        "spark.network.timeout": "800s",
        "spark.executor.heartbeatInterval": "60s"
    }

    for key, value in conf_updates.items():
        spark.conf.set(key, value)
        logger.info(f"Set {key} = {value}")

    logger.info("✅ Spark configured for large shuffle operations")