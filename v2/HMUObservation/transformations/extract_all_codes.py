"""
Extract all unique codes from all observation transformations
Extracts codes from: main observation (method, data_absent_reason, value_codeable_concept),
                     components (component_code, component_value_code, component_data_absent_reason_code),
                     reference ranges (range_type_code)
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
    observation_df: DataFrame,
    existing_unique_codes_df: Optional[DataFrame] = None,
    processing_config: Optional[ProcessingConfig] = None,
    glue_context: Optional[GlueContext] = None,
    config: Optional[DatabaseConfig] = None,
    is_initial_load: bool = False
) -> DataFrame:
    """
    Extract all unique codes from all observation transformations and merge with existing codes.
    
    Extracts codes from:
    - Main observation: method, data_absent_reason, value_codeable_concept
    - Components: component_code, component_value_code, component_data_absent_reason_code
    - Reference ranges: range_type_code
    
    Args:
        observation_df: Source observation DataFrame (for extracting code details)
        existing_unique_codes_df: Existing unique codes DataFrame from observation_codes (to merge with)
        processing_config: Processing configuration
        glue_context: AWS Glue context (for loading existing code_ids)
        config: Database configuration
        is_initial_load: If True, skip loading existing IDs
        
    Returns:
        DataFrame with unique codes: code_id, code_code, code_system, code_display, code_text, normalized_code_text
    """
    logger.info("Extracting all unique codes from all transformations...")
    
    all_codes_list = []
    
    # 1. Extract codes from main observation: method, data_absent_reason, value_codeable_concept
    logger.info("Extracting codes from main observation (method, data_absent_reason, value_codeable_concept)...")
    
    # Method codes
    method_codes = observation_df.select(
        F.col("method.coding").alias("coding_array"),
        F.col("method.text").alias("code_text")
    ).filter(
        F.col("method.coding").isNotNull() &
        (F.size(F.col("method.coding")) > 0)
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
    
    # Data absent reason codes
    data_absent_codes = observation_df.select(
        F.col("dataAbsentReason.coding").alias("coding_array"),
        F.col("dataAbsentReason.text").alias("code_text")
    ).filter(
        F.col("dataAbsentReason.coding").isNotNull() &
        (F.size(F.col("dataAbsentReason.coding")) > 0)
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
    
    # Value codeable concept codes
    value_codes = observation_df.select(
        F.col("valueCodeableConcept.coding").alias("coding_array"),
        F.col("valueCodeableConcept.text").alias("code_text")
    ).filter(
        F.col("valueCodeableConcept.coding").isNotNull() &
        (F.size(F.col("valueCodeableConcept.coding")) > 0)
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
    
    # 2. Extract codes from components: component_code, component_value_code, component_data_absent_reason_code
    logger.info("Extracting codes from components (component_code, component_value_code, component_data_absent_reason_code)...")
    
    # Component codes
    component_codes = observation_df.select(
        F.explode(F.col("component")).alias("component_item")
    ).filter(
        F.col("component_item.code.coding").isNotNull() &
        (F.size(F.col("component_item.code.coding")) > 0)
    ).select(
        F.explode(F.col("component_item.code.coding")).alias("coding_item"),
        F.col("component_item.code.text").alias("code_text")
    ).select(
        F.regexp_replace(F.col("coding_item.code"), '^"|"$', '').alias("code_code"),
        F.col("coding_item.system").alias("code_system"),
        F.col("coding_item.display").alias("code_display"),
        F.col("code_text")
    ).filter(
        F.col("code_code").isNotNull() &
        (F.trim(F.coalesce(F.col("code_code"), F.lit(""))) != F.lit("")) &
        F.col("code_system").isNotNull() &
        (F.trim(F.coalesce(F.col("code_system"), F.lit(""))) != F.lit(""))
    )
    
    # Component value codes
    component_value_codes = observation_df.select(
        F.explode(F.col("component")).alias("component_item")
    ).filter(
        F.col("component_item.valueCodeableConcept.coding").isNotNull() &
        (F.size(F.col("component_item.valueCodeableConcept.coding")) > 0)
    ).select(
        F.explode(F.col("component_item.valueCodeableConcept.coding")).alias("coding_item"),
        F.col("component_item.valueCodeableConcept.text").alias("code_text")
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
    
    # Component data absent reason codes
    component_data_absent_codes = observation_df.select(
        F.explode(F.col("component")).alias("component_item")
    ).filter(
        F.col("component_item.dataAbsentReason.coding").isNotNull() &
        (F.size(F.col("component_item.dataAbsentReason.coding")) > 0)
    ).select(
        F.explode(F.col("component_item.dataAbsentReason.coding")).alias("coding_item"),
        F.col("component_item.dataAbsentReason.text").alias("code_text")
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
    
    # 3. Extract codes from reference ranges: range_type_code
    logger.info("Extracting codes from reference ranges (range_type_code)...")
    
    # Range type codes - handle complex nested structure
    range_type_codes = observation_df.select(
        F.explode(F.col("referenceRange")).alias("range_item")
    ).filter(
        F.col("range_item.type.coding").isNotNull() &
        (F.size(F.col("range_item.type.coding")) > 0)
    ).select(
        F.explode(F.col("range_item.type.coding")).alias("coding_item"),
        # Extract text from type field
        # Resource: Observation.referenceRange[].type
        # Note: _text is a struct (FHIR extension/modifier element with id/extension metadata), 
        #       not a string, so we cannot use it. The actual text value is in .text field.
        F.col("range_item.type.text").alias("code_text")
    ).select(
        F.col("coding_item.code").alias("code_code"),
        F.col("coding_item.system").alias("code_system"),
        F.col("coding_item.display").alias("code_display"),
        # Use display as fallback for text if text is null (common pattern in FHIR)
        F.coalesce(
            F.col("code_text"),
            F.col("coding_item.display")
        ).alias("code_text")
    ).filter(
        F.col("code_code").isNotNull() &
        (F.trim(F.coalesce(F.col("code_code"), F.lit(""))) != F.lit("")) &
        F.col("code_system").isNotNull() &
        (F.trim(F.coalesce(F.col("code_system"), F.lit(""))) != F.lit(""))
    )
    
    # Union all code DataFrames (Spark native - no shuffle for union)
    # Collect all non-empty DataFrames
    all_codes_list = []
    for codes_df in [method_codes, data_absent_codes, value_codes, component_codes, 
                     component_value_codes, component_data_absent_codes, range_type_codes]:
        if codes_df is not None:
            all_codes_list.append(codes_df)
    
    # Filter out empty DataFrames before union (avoid errors)
    non_empty_codes = all_codes_list
    
    if len(non_empty_codes) == 0:
        logger.warning("No codes found in any transformation - returning empty DataFrame")
        from pyspark.sql.types import StructType, StructField, StringType, LongType
        empty_schema = StructType([
            StructField("code_id", LongType(), False),
            StructField("code_code", StringType(), False),
            StructField("code_system", StringType(), False),
            StructField("code_display", StringType(), True),
            StructField("code_text", StringType(), True),
            StructField("normalized_code_text", StringType(), True)
        ])
        return observation_df.sparkSession.createDataFrame([], empty_schema)
    
    # Union all (Spark native - efficient, no shuffle for union of compatible schemas)
    # Start with first DataFrame, then union others
    all_codes_union = non_empty_codes[0]
    for codes_df in non_empty_codes[1:]:
        # Use unionByName to handle schema differences gracefully
        all_codes_union = all_codes_union.unionByName(codes_df, allowMissingColumns=True)
    
    # Generate code_id using native Spark (deterministic hash)
    all_codes_with_id = all_codes_union.withColumn(
        "code_id",
        generate_code_id_native(
            F.col("code_code"),
            F.col("code_system")
        )
    ).filter(
        F.col("code_id").isNotNull() &
        F.col("code_code").isNotNull() &
        (F.trim(F.coalesce(F.col("code_code"), F.lit(""))) != F.lit("")) &
        F.col("code_system").isNotNull() &
        (F.trim(F.coalesce(F.col("code_system"), F.lit(""))) != F.lit(""))
    )
    
    # Extract unique codes by grouping by code_id (take first values for display/text)
    # Use groupBy + first() - this is efficient for small-medium cardinality
    unique_codes = all_codes_with_id.groupBy("code_id", "code_code", "code_system").agg(
        F.first("code_display").alias("code_display"),
        F.first("code_text").alias("code_text")
    ).select(
        F.col("code_id"),
        F.col("code_code"),
        F.col("code_system"),
        F.col("code_display"),
        F.col("code_text"),
        F.lit(None).cast("string").alias("normalized_code_text")  # Will be populated by code enrichment if enabled
    )
    
    # Truncate code_text to 1000 chars (per codes table schema)
    unique_codes = unique_codes.withColumn(
        "code_text",
        F.when(
            F.col("code_text").isNotNull(),
            F.substring(
                F.regexp_replace(
                    F.regexp_replace(F.col("code_text"), "\t", " "),
                    "\n", " "
                ),
                1, 1000
            )
        ).otherwise(None)
    )
    
    # Merge with existing unique_codes_df from observation_codes (if provided)
    if existing_unique_codes_df is not None:
        logger.info("Merging with existing unique codes from observation_codes...")
        # Union with existing codes (Spark native - efficient)
        # Use unionByName to handle schema differences gracefully
        unique_codes = unique_codes.unionByName(
            existing_unique_codes_df.select(
                F.col("code_id"),
                F.col("code_code"),
                F.col("code_system"),
                F.col("code_display"),
                F.col("code_text"),
                F.col("normalized_code_text")
            ),
            allowMissingColumns=True
        )
        
        # Deduplicate by code_id (groupBy + first - efficient for small-medium cardinality)
        unique_codes = unique_codes.groupBy("code_id", "code_code", "code_system").agg(
            F.first("code_display").alias("code_display"),
            F.first("code_text").alias("code_text"),
            F.first("normalized_code_text").alias("normalized_code_text")
        ).select(
            F.col("code_id"),
            F.col("code_code"),
            F.col("code_system"),
            F.col("code_display"),
            F.col("code_text"),
            F.col("normalized_code_text")
        )
        
        logger.info("✅ Merged with existing unique codes from observation_codes")
    
    logger.info("✅ Extracted all unique codes from all transformations")
    
    return unique_codes

