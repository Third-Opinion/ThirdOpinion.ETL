"""
Code transformation functions for conditions
Uses codes table for storage efficiency (normalized structure)
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import logging
from typing import Optional, Tuple

from shared.config import ProcessingConfig
from utils.transformation_helpers import extract_patient_id
from .codes_transformation import (
    transform_unique_codes,
    transform_condition_codes_with_code_id
)

# Export the main function
__all__ = ['transform_condition_codes_to_codes_tables']

logger = logging.getLogger(__name__)


def transform_condition_codes(
    df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None
) -> DataFrame:
    """
    Transform condition codes (multiple codes per condition from code.coding array)
    
    Returns codes with all details (code_code, code_system, code_display, code_text).
    This is used internally to extract unique codes and create condition_codes with code_id references.
    """
    logger.info("Transforming condition codes...")
    
    if "code" not in df.columns:
        logger.warning("code column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("condition_id"),
            F.lit("").alias("patient_id"),
            F.lit("").alias("code_code"),
            F.lit("").alias("code_system"),
            F.lit("").alias("code_display"),
            F.lit("").alias("code_text")
        ).filter(F.lit(False))
    
    # First extract patient_id, condition_id and text from code level
    codes_with_text = df.select(
        F.col("id").alias("condition_id"),
        extract_patient_id(df).alias("patient_id"),
        F.col("code.text").alias("code_text"),
        F.col("code.coding").alias("coding_array")
    ).filter(F.col("code").isNotNull())
    
    # Use posexplode to capture array position (avoids expensive window function later)
    # Only explode if coding_array is not null and has elements
    codes_df = codes_with_text.filter(
        F.col("coding_array").isNotNull() & 
        (F.size(F.col("coding_array")) > 0)
    ).select(
        F.col("condition_id"),
        F.col("patient_id"),
        F.col("code_text"),
        F.posexplode(F.col("coding_array")).alias("array_position", "coding_item")
    ).filter(F.col("coding_item").isNotNull())
    
    # Extract code details (preserve array_position for ranking)
    codes_final = codes_df.select(
        F.col("condition_id"),
        F.col("patient_id"),
        F.regexp_replace(F.col("coding_item.code"), '^"|"$', '').alias("code_code"),
        F.col("coding_item.system").alias("code_system"),
        F.substring(F.col("coding_item.display"), 1, 2000).alias("code_display"),
        F.col("code_text"),
        F.col("array_position")  # Preserve position for ranking (0-based)
        # code_id will be generated later using native Spark (no UDF)
    ).filter(
        F.col("code_code").isNotNull() & F.col("patient_id").isNotNull()
    )
    
    return codes_final


def transform_condition_codes_to_codes_tables(
    df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None,
    glue_context=None,
    config=None,
    is_initial_load: bool = False
) -> Tuple[DataFrame, DataFrame]:
    """
    Transform condition codes using codes table approach
    
    Returns both:
    1. unique_codes_df: DataFrame for codes table (unique codes with code_id)
    2. condition_codes_df: DataFrame for condition_codes table (references via code_id)
    
    Args:
        df: Source condition DataFrame
        processing_config: Processing configuration
        
    Returns:
        Tuple of (unique_codes_df, condition_codes_df)
    """
    logger.info("Transforming condition codes to codes table structure...")
    
    # Step 1: Transform to get codes with all details (includes code_id generation)
    codes_df = transform_condition_codes(df, processing_config)
    
    # Cache codes_df to avoid re-computation
    logger.info("Caching codes_df to avoid re-computation...")
    codes_df.cache()
    
    try:
        # Step 2: Extract unique codes for codes table (with ID reuse and native Spark generation)
        unique_codes_df = transform_unique_codes(
            codes_df, 
            processing_config,
            glue_context=glue_context,
            config=config,
            is_initial_load=is_initial_load
        )
        
        # Step 3: Transform to condition_codes with code_id references (with ID reuse and native Spark generation)
        condition_codes_df = transform_condition_codes_with_code_id(
            codes_df, 
            processing_config,
            glue_context=glue_context,
            config=config,
            is_initial_load=is_initial_load
        )
        
        logger.info("✅ Condition codes transformation completed")
        logger.info("   - Counts skipped to avoid shuffle failures")
        
        return unique_codes_df, condition_codes_df
    finally:
        # Always unpersist to free memory
        codes_df.unpersist()
        logger.info("Uncached codes_df to free memory")

