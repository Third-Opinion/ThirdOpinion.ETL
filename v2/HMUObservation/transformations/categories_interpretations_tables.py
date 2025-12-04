"""
Category and interpretation transformation functions for observations
Uses categories and interpretations tables for storage efficiency (normalized structure)
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging
from typing import Optional, Tuple

from shared.config import ProcessingConfig
from utils.transformation_helpers import extract_patient_id
from .categories_interpretations_transformation import (
    transform_unique_categories,
    transform_observation_categories_with_category_id,
    transform_unique_interpretations,
    transform_observation_interpretations_with_interpretation_id
)

logger = logging.getLogger(__name__)


def transform_observation_categories(df: DataFrame) -> DataFrame:
    """Transform observation categories (multiple categories per observation)"""
    logger.info("Transforming observation categories...")
    
    if "category" not in df.columns:
        logger.warning("category column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("patient_id"),
            F.lit("").alias("category_code"),
            F.lit("").alias("category_system"),
            F.lit("").alias("category_display"),
            F.lit("").alias("category_text")
        ).filter(F.lit(False))
    
    # Explode the category array
    categories_df = df.select(
        F.col("id").alias("observation_id"),
        extract_patient_id(df).alias("patient_id"),
        F.explode(F.col("category")).alias("category_item")
    ).filter(F.col("category_item").isNotNull())
    
    # Extract category details and explode the coding array
    categories_with_text = categories_df.select(
        F.col("observation_id"),
        F.col("patient_id"),
        F.col("category_item.text").alias("category_text"),
        F.col("category_item.coding").alias("coding_array")
    )
    
    # Use posexplode to capture array position (avoids expensive window function later)
    categories_final = categories_with_text.select(
        F.col("observation_id"),
        F.col("patient_id"),
        F.col("category_text"),
        F.posexplode(F.col("coding_array")).alias("array_position", "coding_item")
    ).select(
        F.col("observation_id"),
        F.col("patient_id"),
        F.regexp_replace(F.col("coding_item.code"), '^"|"$', '').alias("category_code"),
        F.col("coding_item.system").alias("category_system"),
        F.col("coding_item.display").alias("category_display"),
        F.col("category_text"),
        F.col("array_position")  # Preserve position for ranking (0-based)
    ).filter(
        F.col("category_code").isNotNull() & F.col("patient_id").isNotNull()
    )
    
    return categories_final


def transform_observation_interpretations(df: DataFrame) -> DataFrame:
    """Transform observation interpretations"""
    logger.info("Transforming observation interpretations...")
    
    if "interpretation" not in df.columns:
        logger.warning("interpretation column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("patient_id"),
            F.lit("").alias("interpretation_code"),
            F.lit("").alias("interpretation_system"),
            F.lit("").alias("interpretation_display")
        ).filter(F.lit(False))
    
    # Explode the interpretation array
    interpretations_df = df.select(
        F.col("id").alias("observation_id"),
        extract_patient_id(df).alias("patient_id"),
        F.explode(F.col("interpretation")).alias("interpretation_item")
    ).filter(F.col("interpretation_item").isNotNull())
    
    # Extract interpretation details and explode the coding array
    interpretations_with_text = interpretations_df.select(
        F.col("observation_id"),
        F.col("patient_id"),
        F.col("interpretation_item.text").alias("interpretation_text"),
        F.col("interpretation_item.coding").alias("coding_array")
    )
    
    # Use posexplode to capture array position (avoids expensive window function later)
    interpretations_final = interpretations_with_text.select(
        F.col("observation_id"),
        F.col("patient_id"),
        F.col("interpretation_text"),
        F.posexplode(F.col("coding_array")).alias("array_position", "coding_item")
    ).select(
        F.col("observation_id"),
        F.col("patient_id"),
        F.regexp_replace(F.col("coding_item.code"), '^"|"$', '').alias("interpretation_code"),
        F.col("coding_item.system").alias("interpretation_system"),
        F.col("coding_item.display").alias("interpretation_display"),
        F.col("interpretation_text"),
        F.col("array_position")  # Preserve position for ranking (0-based)
    ).filter(
        F.col("interpretation_code").isNotNull() & F.col("patient_id").isNotNull()
    )
    
    return interpretations_final


def transform_observation_categories_to_categories_tables(
    df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None
) -> Tuple[DataFrame, DataFrame]:
    """
    Transform observation categories using categories table approach
    
    Returns both:
    1. unique_categories_df: DataFrame for categories table (unique categories with category_id)
    2. observation_categories_df: DataFrame for observation_categories table (references via category_id)
    
    Args:
        df: Source observation DataFrame
        processing_config: Processing configuration
        
    Returns:
        Tuple of (unique_categories_df, observation_categories_df)
    """
    logger.info("Transforming observation categories to categories table structure...")
    
    # Step 1: Transform to get categories with all details
    categories_df = transform_observation_categories(df)
    
    # Step 2: Extract unique categories for categories table
    unique_categories_df = transform_unique_categories(categories_df, processing_config)
    
    # Step 3: Transform to observation_categories with category_id references
    observation_categories_df = transform_observation_categories_with_category_id(categories_df, processing_config)
    
    logger.info("✅ Observation categories transformation completed")
    logger.info("   - Counts skipped to avoid shuffle failures")
    
    return unique_categories_df, observation_categories_df


def transform_observation_interpretations_to_interpretations_tables(
    df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None
) -> Tuple[DataFrame, DataFrame]:
    """
    Transform observation interpretations using interpretations table approach
    
    Returns both:
    1. unique_interpretations_df: DataFrame for interpretations table (unique interpretations with interpretation_id)
    2. observation_interpretations_df: DataFrame for observation_interpretations table (references via interpretation_id)
    
    Args:
        df: Source observation DataFrame
        processing_config: Processing configuration
        
    Returns:
        Tuple of (unique_interpretations_df, observation_interpretations_df)
    """
    logger.info("Transforming observation interpretations to interpretations table structure...")
    
    # Step 1: Transform to get interpretations with all details
    interpretations_df = transform_observation_interpretations(df)
    
    # Step 2: Extract unique interpretations for interpretations table
    unique_interpretations_df = transform_unique_interpretations(interpretations_df, processing_config)
    
    # Step 3: Transform to observation_interpretations with interpretation_id references
    observation_interpretations_df = transform_observation_interpretations_with_interpretation_id(interpretations_df, processing_config)
    
    logger.info("✅ Observation interpretations transformation completed")
    logger.info("   - Counts skipped to avoid shuffle failures")
    
    return unique_interpretations_df, observation_interpretations_df

