"""
Extract all unique codes from all condition transformations
Extracts codes from: condition codes
Uses Spark native functions, avoids shuffles
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Optional
import logging

from transformations.codes_transformation import generate_code_id_native
from shared.config import ProcessingConfig, DatabaseConfig
from awsglue.context import GlueContext

logger = logging.getLogger(__name__)


def extract_all_unique_codes(
    condition_df: DataFrame,
    existing_unique_codes_df: Optional[DataFrame] = None,
    processing_config: Optional[ProcessingConfig] = None,
    glue_context: Optional[GlueContext] = None,
    config: Optional[DatabaseConfig] = None,
    is_initial_load: bool = False
) -> DataFrame:
    """
    Extract all unique codes from all condition transformations and merge with existing codes.
    
    Extracts codes from:
    - Condition codes
    
    Args:
        condition_df: Source condition DataFrame (for extracting code details)
        existing_unique_codes_df: Existing unique codes DataFrame from condition_codes (to merge with)
        processing_config: Processing configuration
        glue_context: AWS Glue context (for loading existing code_ids)
        config: Database configuration
        is_initial_load: If True, skip loading existing IDs
        
    Returns:
        DataFrame with unique codes: code_id, code_code, code_system, code_display, code_text, normalized_code_text
    """
    logger.info("Extracting all unique codes from all transformations...")
    
    all_codes_list = []
    
    # 1. Extract codes from condition codes
    logger.info("Extracting codes from condition codes...")
    condition_codes = condition_df.select(
        F.col("code.coding").alias("coding_array"),
        F.col("code.text").alias("code_text")
    ).filter(
        F.col("code.coding").isNotNull() &
        (F.size(F.col("code.coding")) > 0)
    ).select(
        F.explode(F.col("coding_array")).alias("coding_item"),
        F.col("code_text")
    ).select(
        F.col("coding_item.code").alias("code_code"),
        F.col("coding_item.system").alias("code_system"),
        F.col("coding_item.display").alias("code_display"),
        F.col("code_text")
    ).filter(
        F.col("code_code").isNotNull() &
        (F.trim(F.coalesce(F.col("code_code"), F.lit(""))) != F.lit("")) &
        F.col("code_system").isNotNull() &
        (F.trim(F.coalesce(F.col("code_system"), F.lit(""))) != F.lit(""))
    )
    
    all_codes_list.append(condition_codes)
    
    # Union all code DataFrames
    if all_codes_list:
        all_codes = all_codes_list[0]
        for codes_df in all_codes_list[1:]:
            all_codes = all_codes.unionByName(codes_df, allowMissingColumns=True)
        
        # Generate code_id for all codes
        all_codes_with_id = all_codes.withColumn(
            "code_id",
            generate_code_id_native(F.col("code_code"), F.col("code_system"))
        ).withColumn(
            "normalized_code_text",
            F.lit(None).cast("string")  # Conditions don't use normalized_code_text
        )
        
        # Extract unique codes
        unique_all_codes = all_codes_with_id.select(
            F.col("code_id"),
            F.col("code_code"),
            F.col("code_system"),
            F.col("code_display"),
            F.col("code_text"),
            F.col("normalized_code_text")
        ).groupBy("code_id", "code_code", "code_system").agg(
            F.first("code_display", ignorenulls=True).alias("code_display"),
            F.first("code_text", ignorenulls=True).alias("code_text"),
            F.first("normalized_code_text", ignorenulls=True).alias("normalized_code_text")
        ).filter(
            F.col("code_id").isNotNull() &
            F.col("code_code").isNotNull() &
            (F.trim(F.coalesce(F.col("code_code"), F.lit(""))) != F.lit("")) &
            F.col("code_system").isNotNull() &
            (F.trim(F.coalesce(F.col("code_system"), F.lit(""))) != F.lit(""))
        )
        
        # Merge with existing codes if provided
        if existing_unique_codes_df is not None:
            logger.info("Merging with existing unique codes...")
            # Union with existing codes (deduplication by code_id happens in groupBy above)
            unique_all_codes = existing_unique_codes_df.unionByName(unique_all_codes, allowMissingColumns=True)
            # Re-deduplicate after union
            unique_all_codes = unique_all_codes.groupBy("code_id", "code_code", "code_system").agg(
                F.first("code_display", ignorenulls=True).alias("code_display"),
                F.first("code_text", ignorenulls=True).alias("code_text"),
                F.first("normalized_code_text", ignorenulls=True).alias("normalized_code_text")
            )
        
        logger.info("✅ Extracted and merged all unique codes")
        return unique_all_codes
    else:
        # No codes found - return existing or empty
        if existing_unique_codes_df is not None:
            return existing_unique_codes_df
        else:
            return condition_df.sparkSession.createDataFrame([], schema=None)

