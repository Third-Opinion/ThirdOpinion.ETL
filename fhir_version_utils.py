# Deployed: 2025-10-09 06:01:41 UTC
#!/usr/bin/env python3
"""
FHIR Version Comparison Utilities

This module provides shared utilities for comparing FHIR entity versions and
writing data to Redshift with version-aware processing.

Functions:
- get_existing_versions_from_redshift(): Query Redshift for current versions
- filter_dataframe_by_version(): Compare incoming data with existing versions
- get_entities_to_delete(): Identify entities needing old version cleanup
- write_to_redshift_versioned(): Version-aware write to Redshift
"""

import logging
from typing import Dict, Set

# Import PySpark components with fallback for local development
try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    PYSPARK_AVAILABLE = True
except ImportError:
    # PySpark not available in local environment, define stubs for syntax checking
    DataFrame = object
    F = object
    PYSPARK_AVAILABLE = False

# Set up logging
logger = logging.getLogger(__name__)


def get_existing_versions_from_redshift(table_name: str, id_column: str, glueContext) -> Dict[str, str]:
    """
    Query Redshift to get existing entity ID to timestamp mappings.

    Args:
        table_name: Name of the table to query
        id_column: Name of the primary key column
        glueContext: Glue context for database operations

    Returns:
        Dictionary mapping entity IDs to their current last updated timestamps
    """
    logger.info(f"Fetching existing timestamps from {table_name}...")

    try:
        # Query to get current entity timestamps from Redshift
        query = f"SELECT {id_column}, meta_last_updated FROM public.{table_name}"

        # Create dynamic frame to execute the query
        existing_df = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "redshiftTmpDir": "s3://aws-glue-temporary-442042533707-us-east-2/",
                "useConnectionProperties": "true",
                "connectionName": "redshift-default-connection",
                "query": query
            }
        ).toDF()

        # Convert to dictionary for fast lookups
        existing_timestamps = {}
        if existing_df.count() > 0:
            rows = existing_df.collect()
            for row in rows:
                entity_id = row[id_column]
                timestamp = row["meta_last_updated"]
                if entity_id and timestamp:
                    existing_timestamps[entity_id] = timestamp

        logger.info(f"Found {len(existing_timestamps)} existing entities with timestamps")
        return existing_timestamps

    except Exception as e:
        logger.warning(f"Could not fetch existing versions from {table_name}: {e}")
        return {}


def filter_dataframe_by_version(df: DataFrame, id_column: str, existing_timestamps: Dict[str, str]) -> DataFrame:
    """
    Filter dataframe to only include records that need processing based on timestamp comparison.

    Args:
        df: Incoming DataFrame
        id_column: Name of the primary key column
        existing_timestamps: Dictionary of entity ID to current timestamp mappings

    Returns:
        Filtered DataFrame containing only records that need processing
    """
    def needs_processing(entity_id, timestamp):
        """Check if record needs processing based on timestamp comparison"""
        if entity_id is None or timestamp is None:
            return True  # Process records with missing IDs/timestamps

        existing_timestamp = existing_timestamps.get(entity_id)
        if existing_timestamp is None:
            return True  # New entity

        # Convert timestamps to comparable format if needed
        try:
            if str(existing_timestamp) == str(timestamp):
                return False  # Same timestamp, skip
        except:
            pass  # If comparison fails, process the record

        return True  # Different timestamp or comparison failed, process

    # Create UDF for version comparison
    try:
        from pyspark.sql.types import BooleanType
        from pyspark.sql.functions import udf
        needs_processing_udf = udf(needs_processing, BooleanType())
    except ImportError:
        # Fallback for local development
        def needs_processing_udf(*args):
            return True

    # Filter dataframe based on timestamp comparison
    filtered_df = df.filter(
        needs_processing_udf(F.col(id_column), F.col("meta_last_updated"))
    )

    total_count = df.count()
    filtered_count = filtered_df.count()
    skipped_count = total_count - filtered_count

    logger.info(f"Version comparison results:")
    logger.info(f"  Total incoming records: {total_count}")
    logger.info(f"  Records to process (new/updated): {filtered_count}")
    logger.info(f"  Records to skip (same version): {skipped_count}")

    return filtered_df


def get_entities_to_delete(df: DataFrame, id_column: str) -> Set[str]:
    """
    Get set of entity IDs that need their old versions deleted.

    Args:
        df: DataFrame of entities being processed
        id_column: Name of the primary key column

    Returns:
        Set of entity IDs that need old version cleanup
    """
    if df.count() == 0:
        return set()

    # Get unique entity IDs from the dataframe
    entity_ids = df.select(id_column).distinct().rdd.map(lambda row: row[0]).collect()
    entity_ids_set = set(filter(None, entity_ids))  # Remove None values

    logger.info(f"Identified {len(entity_ids_set)} entities needing version cleanup")
    return entity_ids_set


def write_to_redshift_versioned(dynamic_frame, table_name: str, id_column: str, preactions: str = ""):
    """
    Write DynamicFrame to Redshift with version-aware processing.

    Args:
        dynamic_frame: Glue DynamicFrame to write
        table_name: Target table name
        id_column: Primary key column name
        preactions: SQL statements to execute before insert
    """
    logger.info(f"Writing {table_name} to Redshift with version awareness...")

    # Get the GlueContext from the dynamic frame
    glueContext = dynamic_frame.glue_ctx

    # Convert to DataFrame for version processing
    df = dynamic_frame.toDF()

    if df.count() == 0:
        logger.info(f"No data to process for {table_name}")
        return

    # Get existing timestamps from Redshift
    existing_timestamps = get_existing_versions_from_redshift(table_name, id_column, glueContext)

    # Filter to only records that need processing
    filtered_df = filter_dataframe_by_version(df, id_column, existing_timestamps)

    if filtered_df.count() == 0:
        logger.info(f"No new or updated records for {table_name}, skipping write")
        return

    # Get entities that need old version cleanup
    entities_to_delete = get_entities_to_delete(filtered_df, id_column)

    # Build deletion SQL for old versions
    delete_sql = ""
    if entities_to_delete:
        entity_list = "', '".join(entities_to_delete)
        delete_sql = f"DELETE FROM public.{table_name} WHERE {id_column} IN ('{entity_list}');"
        logger.info(f"Will delete old versions for {len(entities_to_delete)} entities")

    # Combine deletion with any other preactions
    full_preactions = delete_sql + (" " + preactions if preactions else "")

    # Convert back to DynamicFrame for writing
    filtered_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database="temp",
        table_name="temp_table",
        push_down_predicate=""
    )
    # Actually create from the filtered DataFrame
    filtered_dynamic_frame = glueContext.create_dynamic_frame_from_rdd(
        filtered_df.rdd, "filtered_data"
    )

    # Write to Redshift with version-aware preactions
    try:
        glueContext.write_dynamic_frame.from_options(
            frame=filtered_dynamic_frame,
            connection_type="redshift",
            connection_options={
                "redshiftTmpDir": "s3://aws-glue-temporary-442042533707-us-east-2/",
                "useConnectionProperties": "true",
                "connectionName": "redshift-default-connection",
                "dbtable": f"public.{table_name}",
                "preactions": full_preactions
            }
        )

        processed_count = filtered_df.count()
        logger.info(f"✅ Successfully wrote {processed_count} records to {table_name}")
        logger.info(f"📊 Timestamp summary: {processed_count} processed, {len(existing_timestamps) - len(entities_to_delete)} skipped (same timestamp)")

    except Exception as e:
        logger.error(f"❌ Failed to write to {table_name}: {e}")
        raise