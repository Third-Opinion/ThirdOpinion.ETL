"""
Stage transformation functions for conditions
Uses codes table for storage efficiency (normalized structure)
Normalizes stage.summary and stage.type to codes table
Keeps stage.assessment denormalized (can be Reference or CodeableConcept)
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
import logging
from typing import Optional, Tuple

from shared.config import ProcessingConfig
from utils.transformation_helpers import extract_patient_id
from .codes_transformation import (
    transform_unique_codes,
    transform_condition_codes_with_code_id
)

logger = logging.getLogger(__name__)


def transform_condition_stage_summaries(
    df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None
) -> DataFrame:
    """
    Transform condition stage summaries (stage.summary field)
    Returns codes with all details for extraction to codes table
    """
    logger.info("Transforming condition stage summaries...")
    
    if "stage" not in df.columns:
        logger.warning("stage column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("condition_id"),
            F.lit("").alias("patient_id"),
            F.lit("").alias("code_code"),
            F.lit("").alias("code_system"),
            F.lit("").alias("code_display"),
            F.lit(0).cast("int").alias("array_position")
        ).filter(F.lit(False))
    
    # Explode stage array and extract summaries
    stage_summaries_df = df.select(
        F.col("id").alias("condition_id"),
        extract_patient_id(df).alias("patient_id"),
        F.posexplode(F.col("stage")).alias("stage_rank", "stage_item")
    ).filter(
        F.col("stage_item").isNotNull() &
        F.col("stage_item.summary").isNotNull() &
        F.col("stage_item.summary.coding").isNotNull() &
        (F.size(F.col("stage_item.summary.coding")) > 0)
    )
    
    # Extract summary coding details
    summaries_final = stage_summaries_df.select(
        F.col("condition_id"),
        F.col("patient_id"),
        F.regexp_replace(F.col("stage_item.summary.coding")[0].getField("code"), '^"|"$', '').alias("code_code"),
        F.col("stage_item.summary.coding")[0].getField("system").alias("code_system"),
        F.substring(F.col("stage_item.summary.coding")[0].getField("display"), 1, 2000).alias("code_display"),
        F.col("stage_rank").cast("int").alias("array_position")
    ).filter(
        F.col("code_code").isNotNull() & F.col("patient_id").isNotNull()
    )
    
    return summaries_final


def transform_condition_stage_types(
    df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None
) -> DataFrame:
    """
    Transform condition stage types (stage.type field)
    Returns codes with all details for extraction to codes table
    """
    logger.info("Transforming condition stage types...")
    
    if "stage" not in df.columns:
        logger.warning("stage column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("condition_id"),
            F.lit("").alias("patient_id"),
            F.lit("").alias("code_code"),
            F.lit("").alias("code_system"),
            F.lit("").alias("code_display"),
            F.lit(0).cast("int").alias("array_position")
        ).filter(F.lit(False))
    
    # Explode stage array and extract types
    stage_types_df = df.select(
        F.col("id").alias("condition_id"),
        extract_patient_id(df).alias("patient_id"),
        F.posexplode(F.col("stage")).alias("stage_rank", "stage_item")
    ).filter(
        F.col("stage_item").isNotNull() &
        F.col("stage_item.type").isNotNull() &
        F.col("stage_item.type.coding").isNotNull() &
        (F.size(F.col("stage_item.type.coding")) > 0)
    )
    
    # Extract type coding details
    types_final = stage_types_df.select(
        F.col("condition_id"),
        F.col("patient_id"),
        F.regexp_replace(F.col("stage_item.type.coding")[0].getField("code"), '^"|"$', '').alias("code_code"),
        F.col("stage_item.type.coding")[0].getField("system").alias("code_system"),
        F.substring(F.col("stage_item.type.coding")[0].getField("display"), 1, 2000).alias("code_display"),
        F.col("stage_rank").cast("int").alias("array_position")
    ).filter(
        F.col("code_code").isNotNull() & F.col("patient_id").isNotNull()
    )
    
    return types_final


def transform_condition_stage_summaries_to_codes_tables(
    df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None,
    glue_context=None,
    config=None,
    is_initial_load: bool = False
) -> Tuple[DataFrame, DataFrame]:
    """
    Transform condition stage summaries using codes table approach
    
    Returns both:
    1. unique_codes_df: DataFrame for codes table (unique stage summary codes with code_id)
    2. condition_stage_summaries_df: DataFrame for condition_stage_summaries table (references via code_id)
    """
    logger.info("Transforming condition stage summaries to codes table structure...")
    
    # Step 1: Transform to get summaries with all details
    summaries_df = transform_condition_stage_summaries(df, processing_config)
    
    # Check if we have any data (lightweight check)
    sample_check = summaries_df.limit(1).collect()
    if len(sample_check) == 0:
        logger.info("No stage summaries found - returning empty DataFrames")
        # Return empty DataFrames with correct schema
        empty_codes_schema = StructType([
            StructField("code_id", LongType(), False),
            StructField("code_code", StringType(), False),
            StructField("code_system", StringType(), False),
            StructField("code_display", StringType(), True),
            StructField("code_text", StringType(), True),
            StructField("normalized_code_text", StringType(), True)
        ])
        empty_junction_schema = StructType([
            StructField("condition_id", StringType(), False),
            StructField("patient_id", StringType(), False),
            StructField("code_id", LongType(), False),
            StructField("stage_rank", IntegerType(), True)
        ])
        return (
            df.sparkSession.createDataFrame([], empty_codes_schema),
            df.sparkSession.createDataFrame([], empty_junction_schema)
        )
    
    # Cache summaries_df to avoid re-computation
    logger.info("Caching summaries_df to avoid re-computation...")
    summaries_df.cache()
    
    try:
        # Step 2: Extract unique codes for codes table (with ID reuse and native Spark generation)
        unique_codes_df = transform_unique_codes(
            summaries_df, 
            processing_config,
            glue_context=glue_context,
            config=config,
            is_initial_load=is_initial_load
        )
        
        # Step 3: Transform to condition_stage_summaries with code_id references
        condition_stage_summaries_df = transform_condition_codes_with_code_id(
            summaries_df, 
            processing_config,
            glue_context=glue_context,
            config=config,
            is_initial_load=is_initial_load
        )
        
        # Rename columns to match table structure
        condition_stage_summaries_df = condition_stage_summaries_df.select(
            F.col("condition_id"),
            F.col("patient_id"),
            F.col("code_id"),
            F.col("code_rank").alias("stage_rank")
        )
        
        logger.info("✅ Condition stage summaries transformation completed")
        logger.info("   - Counts skipped to avoid shuffle failures")
        
        return unique_codes_df, condition_stage_summaries_df
    finally:
        # Always unpersist to free memory
        summaries_df.unpersist()
        logger.info("Uncached summaries_df to free memory")


def transform_condition_stage_types_to_codes_tables(
    df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None,
    glue_context=None,
    config=None,
    is_initial_load: bool = False
) -> Tuple[DataFrame, DataFrame]:
    """
    Transform condition stage types using codes table approach
    
    Returns both:
    1. unique_codes_df: DataFrame for codes table (unique stage type codes with code_id)
    2. condition_stage_types_df: DataFrame for condition_stage_types table (references via code_id)
    """
    logger.info("Transforming condition stage types to codes table structure...")
    
    # Step 1: Transform to get types with all details
    types_df = transform_condition_stage_types(df, processing_config)
    
    # Check if we have any data (lightweight check)
    sample_check = types_df.limit(1).collect()
    if len(sample_check) == 0:
        logger.info("No stage types found - returning empty DataFrames")
        # Return empty DataFrames with correct schema
        empty_codes_schema = StructType([
            StructField("code_id", LongType(), False),
            StructField("code_code", StringType(), False),
            StructField("code_system", StringType(), False),
            StructField("code_display", StringType(), True),
            StructField("code_text", StringType(), True),
            StructField("normalized_code_text", StringType(), True)
        ])
        empty_junction_schema = StructType([
            StructField("condition_id", StringType(), False),
            StructField("patient_id", StringType(), False),
            StructField("code_id", LongType(), False),
            StructField("stage_rank", IntegerType(), True)
        ])
        return (
            df.sparkSession.createDataFrame([], empty_codes_schema),
            df.sparkSession.createDataFrame([], empty_junction_schema)
        )
    
    # Cache types_df to avoid re-computation
    logger.info("Caching types_df to avoid re-computation...")
    types_df.cache()
    
    try:
        # Step 2: Extract unique codes for codes table (with ID reuse and native Spark generation)
        unique_codes_df = transform_unique_codes(
            types_df, 
            processing_config,
            glue_context=glue_context,
            config=config,
            is_initial_load=is_initial_load
        )
        
        # Step 3: Transform to condition_stage_types with code_id references
        condition_stage_types_df = transform_condition_codes_with_code_id(
            types_df, 
            processing_config,
            glue_context=glue_context,
            config=config,
            is_initial_load=is_initial_load
        )
        
        # Rename columns to match table structure
        condition_stage_types_df = condition_stage_types_df.select(
            F.col("condition_id"),
            F.col("patient_id"),
            F.col("code_id"),
            F.col("code_rank").alias("stage_rank")
        )
        
        logger.info("✅ Condition stage types transformation completed")
        logger.info("   - Counts skipped to avoid shuffle failures")
        
        return unique_codes_df, condition_stage_types_df
    finally:
        # Always unpersist to free memory
        types_df.unpersist()
        logger.info("Uncached types_df to free memory")

