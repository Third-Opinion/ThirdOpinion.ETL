"""
Category and interpretation transformation functions for normalized tables
Redshift-compatible implementation using hash-based IDs
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
import logging
from typing import Optional, Tuple
import hashlib

from shared.config import ProcessingConfig

logger = logging.getLogger(__name__)


def generate_category_id(category_code: str, category_system: str) -> int:
    """
    Generate deterministic category_id from category_code + category_system
    Uses MD5 hash converted to 64-bit integer (first 8 bytes)
    
    Args:
        category_code: The category code (e.g., "vital-signs")
        category_system: The category system (e.g., "http://terminology.hl7.org/CodeSystem/observation-category")
        
    Returns:
        Deterministic BIGINT category_id (always positive)
    """
    if not category_code or not category_system:
        return None
    
    category_code = str(category_code).strip() if category_code else ""
    category_system = str(category_system).strip() if category_system else ""
    
    if not category_code or not category_system:
        return None
    
    hash_key = f"{category_system}|{category_code}"
    hash_bytes = hashlib.md5(hash_key.encode('utf-8')).digest()[:8]
    code_id = int.from_bytes(hash_bytes, byteorder='big', signed=False)
    max_bigint = 0x7FFFFFFFFFFFFFFF
    code_id = code_id % (max_bigint + 1)
    
    return code_id


def generate_interpretation_id(interpretation_code: str, interpretation_system: str) -> int:
    """
    Generate deterministic interpretation_id from interpretation_code + interpretation_system
    Uses MD5 hash converted to 64-bit integer (first 8 bytes)
    
    Args:
        interpretation_code: The interpretation code (e.g., "N")
        interpretation_system: The interpretation system (e.g., "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation")
        
    Returns:
        Deterministic BIGINT interpretation_id (always positive)
    """
    if not interpretation_code or not interpretation_system:
        return None
    
    interpretation_code = str(interpretation_code).strip() if interpretation_code else ""
    interpretation_system = str(interpretation_system).strip() if interpretation_system else ""
    
    if not interpretation_code or not interpretation_system:
        return None
    
    hash_key = f"{interpretation_system}|{interpretation_code}"
    hash_bytes = hashlib.md5(hash_key.encode('utf-8')).digest()[:8]
    code_id = int.from_bytes(hash_bytes, byteorder='big', signed=False)
    max_bigint = 0x7FFFFFFFFFFFFFFF
    code_id = code_id % (max_bigint + 1)
    
    return code_id


def transform_unique_categories(
    categories_df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None
) -> DataFrame:
    """
    Extract unique categories from observation categories DataFrame
    Creates categories table data with deterministic category_id
    
    Args:
        categories_df: DataFrame with category_code, category_system, category_display, category_text
        processing_config: Processing configuration
        
    Returns:
        DataFrame with unique categories: category_id, category_code, category_system, 
        category_display, category_text (truncated to 500)
    """
    logger.info("Extracting unique categories for categories table...")
    
    # Lightweight check: use limit(1) instead of rdd.isEmpty() to avoid full scan
    if categories_df is None:
        logger.warning("No categories data provided, returning empty DataFrame")
        return categories_df.sparkSession.createDataFrame([], StructType([
            StructField("category_id", LongType(), False),
            StructField("category_code", StringType(), False),
            StructField("category_system", StringType(), False),
            StructField("category_display", StringType(), True),
            StructField("category_text", StringType(), True)
        ]))
    
    def generate_category_id_udf(category_code, category_system):
        """UDF wrapper for category_id generation"""
        try:
            return generate_category_id(category_code, category_system)
        except Exception as e:
            logger.warning(f"Error generating category_id for {category_code}/{category_system}: {e}")
            return None
    
    category_id_udf = F.udf(generate_category_id_udf, LongType())
    
    # Add category_id to each category record
    categories_with_id = categories_df.withColumn(
        "category_id",
        category_id_udf(F.col("category_code"), F.col("category_system"))
    ).filter(F.col("category_id").isNotNull())
    
    # Truncate category_text to 500 chars (was 65535)
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
    
    # Select unique categories (same category_code + category_system = same category_id)
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


def transform_observation_categories_with_category_id(
    categories_df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None
) -> DataFrame:
    """
    Transform observation categories to use category_id references
    
    Args:
        categories_df: DataFrame with observation_id, patient_id, category_code, category_system, 
                     category_display, category_text
        processing_config: Processing configuration
        
    Returns:
        DataFrame with observation_id, patient_id, category_id, category_rank
    """
    logger.info("Transforming observation categories to use category_id references...")
    
    # Lightweight check: use limit(1) instead of rdd.isEmpty() to avoid full scan
    if categories_df is None:
        logger.warning("No categories data provided, returning empty DataFrame")
        return categories_df.sparkSession.createDataFrame([], StructType([
            StructField("observation_id", StringType(), False),
            StructField("patient_id", StringType(), False),
            StructField("category_id", LongType(), False),
            StructField("category_rank", IntegerType(), True)
        ]))
    
    # Lightweight check: only process 1 row to see if DataFrame is empty
    sample_check = categories_df.limit(1).collect()
    if len(sample_check) == 0:
        logger.warning("No categories data provided, returning empty DataFrame")
    
    def generate_category_id_udf(category_code, category_system):
        """UDF wrapper for category_id generation"""
        try:
            return generate_category_id(category_code, category_system)
        except Exception as e:
            logger.warning(f"Error generating category_id for {category_code}/{category_system}: {e}")
            return None
    
    category_id_udf = F.udf(generate_category_id_udf, LongType())
    
    # Add category_id
    observation_categories_normalized = categories_df.withColumn(
        "category_id",
        category_id_udf(F.col("category_code"), F.col("category_system"))
    ).filter(
        F.col("category_id").isNotNull() & 
        F.col("observation_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    # Use array position from posexplode (avoids expensive window function)
    # array_position is 0-based, convert to 1-based rank
    if "array_position" in observation_categories_normalized.columns:
        observation_categories_final = observation_categories_normalized.select(
            F.col("observation_id"),
            F.col("patient_id"),
            F.col("category_id"),
            (F.col("array_position") + 1).cast("int").alias("category_rank")  # Convert 0-based to 1-based
        )
    else:
        # Fallback: No array position available - set rank to NULL (optional field)
        logger.warning("array_position not found in categories_df - setting category_rank to NULL")
        observation_categories_final = observation_categories_normalized.select(
            F.col("observation_id"),
            F.col("patient_id"),
            F.col("category_id"),
            F.lit(None).cast("int").alias("category_rank")
        )
    
    logger.info("✅ Transformed observation-category relationships (count skipped to avoid shuffle failures)")
    
    return observation_categories_final


def transform_unique_interpretations(
    interpretations_df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None
) -> DataFrame:
    """
    Extract unique interpretations from observation interpretations DataFrame
    Creates interpretations table data with deterministic interpretation_id
    
    Args:
        interpretations_df: DataFrame with interpretation_code, interpretation_system, 
                           interpretation_display, interpretation_text
        processing_config: Processing configuration
        
    Returns:
        DataFrame with unique interpretations: interpretation_id, interpretation_code, 
        interpretation_system, interpretation_display, interpretation_text (truncated to 100)
    """
    logger.info("Extracting unique interpretations for interpretations table...")
    
    # Lightweight check: use limit(1) instead of rdd.isEmpty() to avoid full scan
    if interpretations_df is None:
        logger.warning("No interpretations data provided, returning empty DataFrame")
        return interpretations_df.sparkSession.createDataFrame([], StructType([
            StructField("interpretation_id", LongType(), False),
            StructField("interpretation_code", StringType(), False),
            StructField("interpretation_system", StringType(), False),
            StructField("interpretation_display", StringType(), True),
            StructField("interpretation_text", StringType(), True)
        ]))
    
    def generate_interpretation_id_udf(interpretation_code, interpretation_system):
        """UDF wrapper for interpretation_id generation"""
        try:
            return generate_interpretation_id(interpretation_code, interpretation_system)
        except Exception as e:
            logger.warning(f"Error generating interpretation_id for {interpretation_code}/{interpretation_system}: {e}")
            return None
    
    interpretation_id_udf = F.udf(generate_interpretation_id_udf, LongType())
    
    # Add interpretation_id to each interpretation record
    interpretations_with_id = interpretations_df.withColumn(
        "interpretation_id",
        interpretation_id_udf(F.col("interpretation_code"), F.col("interpretation_system"))
    ).filter(F.col("interpretation_id").isNotNull())
    
    # Truncate interpretation_text to 100 chars (was 65535, but usually short)
    interpretations_cleaned = interpretations_with_id.withColumn(
        "interpretation_text_truncated",
        F.when(
            F.col("interpretation_text").isNotNull(),
            F.substring(
                F.regexp_replace(
                    F.regexp_replace(F.col("interpretation_text"), "\t", " "),
                    "\n", " "
                ),
                1, 100
            )
        ).otherwise(None)
    )
    
    # Select unique interpretations (same interpretation_code + interpretation_system = same interpretation_id)
    # Group by code+system, but keep all text variations (they're few and short)
    unique_interpretations = interpretations_cleaned.select(
        F.col("interpretation_id"),
        F.col("interpretation_code"),
        F.col("interpretation_system"),
        F.col("interpretation_display"),
        F.col("interpretation_text_truncated").alias("interpretation_text")
    ).groupBy("interpretation_id", "interpretation_code", "interpretation_system").agg(
        F.first(F.col("interpretation_display"), ignorenulls=True).alias("interpretation_display"),
        F.first(F.col("interpretation_text"), ignorenulls=True).alias("interpretation_text")
    ).select(
        F.col("interpretation_id"),
        F.col("interpretation_code"),
        F.col("interpretation_system"),
        F.col("interpretation_display"),
        F.col("interpretation_text")
    )
    
    logger.info("✅ Extracted unique interpretations (count skipped to avoid shuffle failures)")
    
    return unique_interpretations


def transform_observation_interpretations_with_interpretation_id(
    interpretations_df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None
) -> DataFrame:
    """
    Transform observation interpretations to use interpretation_id references
    
    Args:
        interpretations_df: DataFrame with observation_id, patient_id, interpretation_code, 
                           interpretation_system, interpretation_display, interpretation_text
        processing_config: Processing configuration
        
    Returns:
        DataFrame with observation_id, patient_id, interpretation_id, interpretation_rank
    """
    logger.info("Transforming observation interpretations to use interpretation_id references...")
    
    # Lightweight check: use limit(1) instead of rdd.isEmpty() to avoid full scan
    if interpretations_df is None:
        logger.warning("No interpretations data provided, returning empty DataFrame")
        return interpretations_df.sparkSession.createDataFrame([], StructType([
            StructField("observation_id", StringType(), False),
            StructField("patient_id", StringType(), False),
            StructField("interpretation_id", LongType(), False),
            StructField("interpretation_rank", IntegerType(), True)
        ]))
    
    # Lightweight check: only process 1 row to see if DataFrame is empty
    sample_check = interpretations_df.limit(1).collect()
    if len(sample_check) == 0:
        logger.warning("No interpretations data provided, returning empty DataFrame")
    
    def generate_interpretation_id_udf(interpretation_code, interpretation_system):
        """UDF wrapper for interpretation_id generation"""
        try:
            return generate_interpretation_id(interpretation_code, interpretation_system)
        except Exception as e:
            logger.warning(f"Error generating interpretation_id for {interpretation_code}/{interpretation_system}: {e}")
            return None
    
    interpretation_id_udf = F.udf(generate_interpretation_id_udf, LongType())
    
    # Add interpretation_id
    observation_interpretations_normalized = interpretations_df.withColumn(
        "interpretation_id",
        interpretation_id_udf(F.col("interpretation_code"), F.col("interpretation_system"))
    ).filter(
        F.col("interpretation_id").isNotNull() & 
        F.col("observation_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    # Use array position from posexplode (avoids expensive window function)
    # array_position is 0-based, convert to 1-based rank
    if "array_position" in observation_interpretations_normalized.columns:
        observation_interpretations_final = observation_interpretations_normalized.select(
            F.col("observation_id"),
            F.col("patient_id"),
            F.col("interpretation_id"),
            (F.col("array_position") + 1).cast("int").alias("interpretation_rank")  # Convert 0-based to 1-based
        )
    else:
        # Fallback: No array position available - set rank to NULL (optional field)
        logger.warning("array_position not found in interpretations_df - setting interpretation_rank to NULL")
        observation_interpretations_final = observation_interpretations_normalized.select(
            F.col("observation_id"),
            F.col("patient_id"),
            F.col("interpretation_id"),
            F.lit(None).cast("int").alias("interpretation_rank")
        )
    
    logger.info("✅ Transformed observation-interpretation relationships (count skipped to avoid shuffle failures)")
    
    return observation_interpretations_final

