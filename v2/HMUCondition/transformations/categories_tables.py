"""
Category transformation functions for conditions
Uses categories table for storage efficiency (normalized structure)
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
import logging
from typing import Optional, Tuple
import hashlib

from shared.config import ProcessingConfig
from utils.transformation_helpers import extract_patient_id

logger = logging.getLogger(__name__)


def generate_category_id_native(category_code_col, category_system_col):
    """
    Generate deterministic category_id using native Spark functions (no UDF)
    Uses same hash approach as codes
    """
    category_code_clean = F.trim(F.coalesce(category_code_col, F.lit("")))
    category_system_clean = F.trim(F.coalesce(category_system_col, F.lit("")))
    
    hash_key = F.when(
        (category_code_clean != F.lit("")) & (category_system_clean != F.lit("")),
        F.concat(category_system_clean, F.lit("|"), category_code_clean)
    ).otherwise(
        F.concat(F.lit("INVALID|"), F.coalesce(category_code_clean, F.lit("NULL")), F.lit("|"), F.coalesce(category_system_clean, F.lit("NULL")))
    )
    
    md5_hash = F.md5(hash_key)
    max_bigint_value = 9223372036854775807
    category_id = (F.abs(F.hash(md5_hash)).cast("bigint") % F.lit(max_bigint_value))
    category_id = F.coalesce(category_id, F.lit(1))
    
    return category_id


def transform_condition_categories(df: DataFrame) -> DataFrame:
    """Transform condition categories (multiple categories per condition)"""
    logger.info("Transforming condition categories...")
    
    if "category" not in df.columns:
        logger.warning("category column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("condition_id"),
            F.lit("").alias("patient_id"),
            F.lit("").alias("category_code"),
            F.lit("").alias("category_system"),
            F.lit("").alias("category_display"),
            F.lit("").alias("category_text")
        ).filter(F.lit(False))
    
    # Explode the category array
    categories_df = df.select(
        F.col("id").alias("condition_id"),
        extract_patient_id(df).alias("patient_id"),
        F.explode(F.col("category")).alias("category_item")
    ).filter(F.col("category_item").isNotNull())
    
    # Extract category details and explode the coding array
    categories_with_text = categories_df.select(
        F.col("condition_id"),
        F.col("patient_id"),
        F.col("category_item.text").alias("category_text"),
        F.col("category_item.coding").alias("coding_array")
    )
    
    # Use posexplode to capture array position (avoids expensive window function later)
    categories_final = categories_with_text.select(
        F.col("condition_id"),
        F.col("patient_id"),
        F.col("category_text"),
        F.posexplode(F.col("coding_array")).alias("array_position", "coding_item")
    ).select(
        F.col("condition_id"),
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


def transform_unique_categories(
    categories_df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None
) -> DataFrame:
    """
    Extract unique categories from condition categories DataFrame
    Creates categories table data with deterministic category_id
    """
    logger.info("Extracting unique categories for categories table...")
    
    if categories_df is None:
        logger.warning("No categories data provided, returning empty DataFrame")
        return categories_df.sparkSession.createDataFrame([], StructType([
            StructField("category_id", LongType(), False),
            StructField("category_code", StringType(), False),
            StructField("category_system", StringType(), False),
            StructField("category_display", StringType(), True),
            StructField("category_text", StringType(), True)
        ]))
    
    # Generate category_id using native Spark
    categories_with_id = categories_df.withColumn(
        "category_id",
        generate_category_id_native(F.col("category_code"), F.col("category_system"))
    ).filter(F.col("category_id").isNotNull())
    
    # Truncate category_text to 500 chars
    categories_cleaned = categories_with_id.withColumn(
        "category_text_truncated",
        F.when(
            F.col("category_text").isNotNull(),
            F.substring(
                F.regexp_replace(
                    F.regexp_replace(F.col("category_text"), "\t", " "),
                    "\n", " "
                ),
                1, 500
            )
        ).otherwise(None)
    )
    
    # Select unique categories
    unique_categories = categories_cleaned.select(
        F.col("category_id"),
        F.col("category_code"),
        F.col("category_system"),
        F.col("category_display"),
        F.col("category_text_truncated").alias("category_text")
    ).groupBy("category_id", "category_code", "category_system").agg(
        F.first(F.col("category_display"), ignorenulls=True).alias("category_display"),
        F.first(F.col("category_text"), ignorenulls=True).alias("category_text")
    ).select(
        F.col("category_id"),
        F.col("category_code"),
        F.col("category_system"),
        F.col("category_display"),
        F.col("category_text")
    )
    
    logger.info("✅ Extracted unique categories (count skipped to avoid shuffle failures)")
    
    return unique_categories


def transform_condition_categories_with_category_id(
    categories_df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None
) -> DataFrame:
    """
    Transform condition categories to use category_id references
    """
    logger.info("Transforming condition categories to use category_id references...")
    
    if categories_df is None:
        logger.warning("No categories data provided, returning empty DataFrame")
        return categories_df.sparkSession.createDataFrame([], StructType([
            StructField("condition_id", StringType(), False),
            StructField("patient_id", StringType(), False),
            StructField("category_id", LongType(), False),
            StructField("category_rank", IntegerType(), True)
        ]))
    
    # Generate category_id using native Spark
    condition_categories_normalized = categories_df.withColumn(
        "category_id",
        generate_category_id_native(F.col("category_code"), F.col("category_system"))
    ).filter(
        F.col("category_id").isNotNull() & 
        F.col("condition_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    # Use array position from posexplode
    if "array_position" in condition_categories_normalized.columns:
        condition_categories_final = condition_categories_normalized.select(
            F.col("condition_id"),
            F.col("patient_id"),
            F.col("category_id"),
            (F.col("array_position") + 1).cast("int").alias("category_rank")
        )
    else:
        condition_categories_final = condition_categories_normalized.select(
            F.col("condition_id"),
            F.col("patient_id"),
            F.col("category_id"),
            F.lit(None).cast("int").alias("category_rank")
        )
    
    logger.info("✅ Transformed condition-category relationships (count skipped to avoid shuffle failures)")
    
    return condition_categories_final


def transform_condition_categories_to_categories_tables(
    df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None
) -> Tuple[DataFrame, DataFrame]:
    """
    Transform condition categories using categories table approach
    
    Returns both:
    1. unique_categories_df: DataFrame for categories table (unique categories with category_id)
    2. condition_categories_df: DataFrame for condition_categories table (references via category_id)
    """
    logger.info("Transforming condition categories to categories table structure...")
    
    # Step 1: Transform to get categories with all details
    categories_df = transform_condition_categories(df)
    
    # Step 2: Extract unique categories for categories table
    unique_categories_df = transform_unique_categories(categories_df, processing_config)
    
    # Step 3: Transform to condition_categories with category_id references
    condition_categories_df = transform_condition_categories_with_category_id(categories_df, processing_config)
    
    logger.info("✅ Condition categories transformation completed")
    logger.info("   - Counts skipped to avoid shuffle failures")
    
    return unique_categories_df, condition_categories_df

