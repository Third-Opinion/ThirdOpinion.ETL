"""
Code transformation functions for observations
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
    transform_observation_codes_with_code_id
)

# Export the main function
__all__ = ['transform_observation_codes_to_codes_tables']

logger = logging.getLogger(__name__)


def transform_observation_codes(
    df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None
) -> DataFrame:
    """
    Transform observation codes (multiple codes per observation from code.coding array)
    Includes code enrichment for missing codes.
    
    Returns codes with all details (code_code, code_system, code_display, code_text, normalized_code_text).
    This is used internally to extract unique codes and create observation_codes with code_id references.
    """
    logger.info("Transforming observation codes...")
    
    if "code" not in df.columns:
        logger.warning("code column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("patient_id"),
            F.lit("").alias("code_code"),
            F.lit("").alias("code_system"),
            F.lit("").alias("code_display"),
            F.lit("").alias("code_text"),
            F.lit("").alias("normalized_code_text")
        ).filter(F.lit(False))
    
    # Get config values
    enable_enrichment = processing_config.enable_code_enrichment if processing_config else True
    enrichment_mode = processing_config.code_enrichment_mode if processing_config else "hybrid"
    
    # First extract patient_id, observation_id and text from code level
    codes_with_text = df.select(
        F.col("id").alias("observation_id"),
        extract_patient_id(df).alias("patient_id"),
        F.col("code.text").alias("code_text"),
        F.col("code.coding").alias("coding_array")
    ).filter(F.col("code").isNotNull())
    
    # OPTIMIZATION: Split records into two groups to reduce enrichment volume
    # Only enrich records that actually need it (empty/null coding_array)
    # This reduces enrichment from ~14M records to ~5.6M records (~60% reduction)
    codes_with_coding = codes_with_text.filter(
        (F.col("coding_array").isNotNull()) & 
        (F.size(F.col("coding_array")) > 0)
    )
    
    codes_without_coding = codes_with_text.filter(
        (F.col("coding_array").isNull()) | 
        (F.size(F.col("coding_array")) == 0)
    )
    
    logger.info("Split records for optimized enrichment:")
    logger.info("   - Records with codes: will skip enrichment (normalize existing codes only)")
    logger.info("   - Records without codes: will be enriched (add missing codes)")
    
    # Process records WITH codes: Just normalize existing codes (no enrichment needed)
    normalize_schema = ArrayType(StructType([
        StructField("code", StringType(), True),
        StructField("system", StringType(), True),
        StructField("display", StringType(), True),
        StructField("text", StringType(), True)
    ]))
    
    # For records WITH codes, we should also populate normalized_code_text
    # by looking up existing codes in the enrichment mapping table
    # This ensures normalized_code_text is available for ALL codes, not just enriched ones
    codes_with_coding_normalized = codes_with_coding.withColumn(
        "normalized_existing_codes",
        F.transform(
            F.col("coding_array"),
            lambda x: F.struct(
                F.coalesce(x.getField("code"), F.lit("")).alias("code"),
                F.coalesce(x.getField("system"), F.lit("")).alias("system"),
                F.coalesce(x.getField("display"), F.lit("")).alias("display"),
                F.lit(None).cast("string").alias("text")
            )
        )
    ).withColumn(
        "coding_array",
        F.col("normalized_existing_codes")
    )
    
    # Try to get normalized_code_text from existing codes using lookup
    # This will be populated later when we process the codes, but for now set to None
    # The normalized_code_text will be populated in transform_unique_codes by looking up
    # the code_code + code_system in the enrichment mapping
    codes_with_coding_normalized = codes_with_coding_normalized.withColumn(
        "normalized_code_text",
        F.lit(None).cast("string")  # Will be populated from code lookup later
    )
    
    # Process records WITHOUT codes: Enrich them
    codes_without_coding_enriched = codes_without_coding
    
    # Enrich codes if enabled and coding array is empty/null
    # OPTIMIZED: Use native Spark functions instead of UDFs for better performance
    if enable_enrichment:
        try:
            from utils.code_enrichment_native import enrich_codes_native
            
            # Get SparkSession from DataFrame
            spark_session = codes_without_coding_enriched.sparkSession
            
            # Use native Spark-based enrichment (no UDFs) - only on records that need it
            codes_without_coding_enriched = enrich_codes_native(
                codes_without_coding_enriched,
                spark_session,
                enrichment_mode=enrichment_mode
            )
            
            logger.info("✅ Code enrichment applied using native Spark functions (no UDFs) - only on records without codes")
            
        except ImportError as e:
            logger.warning(f"Could not import code_enrichment_native: {str(e)} - falling back to UDF-based enrichment")
            # Fallback to UDF-based enrichment if native version not available
            try:
                from utils.code_enrichment import enrich_observation_code, get_normalized_text_for_code
                
                # Create UDF for enrichment (fallback)
                def enrich_code_udf_wrapper(code_text, coding_array):
                    """Wrapper for enrichment UDF (fallback)"""
                    if code_text is None:
                        return ([], None)
                    # coding_array is already null/empty for this group, so just enrich
                    codes_to_add, normalized_text = enrich_observation_code(
                        code_text, 
                        [],
                        enable_enrichment=True,
                        enrichment_mode=enrichment_mode
                    )
                    return (codes_to_add, normalized_text)
                
                enrich_schema = StructType([
                    StructField("codes", ArrayType(StructType([
                        StructField("code", StringType(), True),
                        StructField("system", StringType(), True),
                        StructField("display", StringType(), True),
                        StructField("text", StringType(), True)
                    ])), True),
                    StructField("normalized_text", StringType(), True)
                ])
                
                enrich_udf = F.udf(enrich_code_udf_wrapper, enrich_schema)
                
                codes_enriched = codes_without_coding_enriched.withColumn(
                    "enrichment_result",
                    enrich_udf(F.col("code_text"), F.col("coding_array"))
                )
                
                codes_with_enriched = codes_enriched.withColumn(
                    "final_coding_array",
                    F.when(
                        (F.col("enrichment_result.codes").isNotNull()) & 
                        (F.size(F.col("enrichment_result.codes")) > 0),
                        F.col("enrichment_result.codes")
                    ).otherwise(F.array().cast(normalize_schema))
                ).withColumn(
                    "normalized_code_text",
                    F.col("enrichment_result.normalized_text")
                )
                
                codes_without_coding_enriched = codes_with_enriched.select(
                    F.col("observation_id"),
                    F.col("patient_id"),
                    F.col("code_text"),
                    F.col("final_coding_array").alias("coding_array"),
                    F.col("normalized_code_text")
                )
                
                logger.info("✅ Code enrichment applied using UDF (fallback) - only on records without codes")
            except Exception as e2:
                logger.warning(f"Error during code enrichment (both native and UDF failed): {str(e2)} - continuing without enrichment")
                codes_without_coding_enriched = codes_without_coding_enriched.withColumn("normalized_code_text", F.lit(None))
                codes_without_coding_enriched = codes_without_coding_enriched.withColumn("coding_array", F.array().cast(normalize_schema))
        except Exception as e:
            logger.warning(f"Error during native code enrichment: {str(e)} - continuing without enrichment")
            codes_without_coding_enriched = codes_without_coding_enriched.withColumn("normalized_code_text", F.lit(None))
            codes_without_coding_enriched = codes_without_coding_enriched.withColumn("coding_array", F.array().cast(normalize_schema))
    else:
        # Enrichment disabled - records without codes get empty array
        codes_without_coding_enriched = codes_without_coding_enriched.withColumn("normalized_code_text", F.lit(None))
        codes_without_coding_enriched = codes_without_coding_enriched.withColumn("coding_array", F.array().cast(normalize_schema))
    
    # Union both groups back together
    # Ensure both DataFrames have the same columns in the same order
    codes_with_coding_final = codes_with_coding_normalized.select(
        F.col("observation_id"),
        F.col("patient_id"),
        F.col("code_text"),
        F.col("coding_array"),
        F.col("normalized_code_text")
    )
    
    codes_without_coding_final = codes_without_coding_enriched.select(
        F.col("observation_id"),
        F.col("patient_id"),
        F.col("code_text"),
        F.col("coding_array"),
        F.col("normalized_code_text")
    )
    
    # Union both groups
    codes_with_text = codes_with_coding_final.unionByName(codes_without_coding_final, allowMissingColumns=False)
    
    logger.info("✅ Unioned enriched and non-enriched records (ready for explode)")
    
    # Use posexplode to capture array position (avoids expensive window function later)
    # Only explode if coding_array is not null and has elements
    codes_df = codes_with_text.filter(
        F.col("coding_array").isNotNull() & 
        (F.size(F.col("coding_array")) > 0)
    ).select(
        F.col("observation_id"),
        F.col("patient_id"),
        F.col("code_text"),
        F.col("normalized_code_text"),
        F.posexplode(F.col("coding_array")).alias("array_position", "coding_item")
    ).filter(F.col("coding_item").isNotNull())
    
    # CRITICAL: Materialize RIGHT after explode, before any UDFs or complex operations
    # This breaks the lineage at the simplest point (just after explode)
    # If we wait until after UDFs, the materialization write itself will trigger shuffles that fail
    # Note: We need to pass database config for s3_temp_dir, but we only have processing_config here
    # For now, skip materialization here - it should be done in the main function where we have full config
    # The key is to materialize BEFORE any UDFs or complex operations
    logger.info("codes_df exploded (will be materialized in main function before UDFs if needed)")
    
    # Extract code details (preserve array_position for ranking)
    # Note: code_id will be generated later in transform_unique_codes/transform_observation_codes_with_code_id
    # using native Spark functions (no UDF) and reusing existing IDs from Redshift
    try:
        from .codes_transformation import generate_code_id_native
        
        codes_final = codes_df.select(
            F.col("observation_id"),
            F.col("patient_id"),
            F.regexp_replace(F.col("coding_item.code"), '^"|"$', '').alias("code_code"),
            F.col("coding_item.system").alias("code_system"),
            F.substring(F.col("coding_item.display"), 1, 2000).alias("code_display"),  # Truncate to 2000 chars
            F.col("code_text"),
            F.col("normalized_code_text"),
            F.col("array_position")  # Preserve position for ranking (0-based)
            # code_id will be generated later using native Spark (no UDF)
        ).filter(
            F.col("code_code").isNotNull() & F.col("patient_id").isNotNull()
        )
    except Exception as e:
        logger.warning(f"Could not generate code_id in transform_observation_codes: {e} - will be generated later")
        codes_final = codes_df.select(
            F.col("observation_id"),
            F.col("patient_id"),
            F.regexp_replace(F.col("coding_item.code"), '^"|"$', '').alias("code_code"),
            F.col("coding_item.system").alias("code_system"),
            F.substring(F.col("coding_item.display"), 1, 2000).alias("code_display"),  # Truncate to 2000 chars
            F.col("code_text"),
            F.col("normalized_code_text"),
            F.col("array_position")  # Preserve position for ranking (0-based)
        ).filter(
            F.col("code_code").isNotNull() & F.col("patient_id").isNotNull()
        )
    
    return codes_final


def transform_observation_codes_to_codes_tables(
    df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None,
    glue_context=None,
    config=None,
    is_initial_load: bool = False
) -> Tuple[DataFrame, DataFrame]:
    """
    Transform observation codes using codes table approach
    
    Returns both:
    1. unique_codes_df: DataFrame for codes table (unique codes with code_id)
    2. observation_codes_df: DataFrame for observation_codes table (references via code_id)
    
    Args:
        df: Source observation DataFrame
        processing_config: Processing configuration
        
    Returns:
        Tuple of (unique_codes_df, observation_codes_df)
    """
    logger.info("Transforming observation codes to codes table structure...")
    
    # Step 1: Transform to get codes with all details (includes code_id generation)
    codes_df = transform_observation_codes(df, processing_config)
    
    # CRITICAL: Materialize codes_df RIGHT after explode, before any UDFs
    # This breaks the lineage at the simplest point (just after explode + basic selects)
    # Materialization must happen BEFORE caching or UDFs to avoid shuffle failures
    # We need database config for s3_temp_dir, but we only have processing_config here
    # So we'll need to pass database config or materialize in the main function
    # For now, cache it and materialize will happen in main function if database config is available
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
        
        # Step 3: Transform to observation_codes with code_id references (with ID reuse and native Spark generation)
        observation_codes_df = transform_observation_codes_with_code_id(
            codes_df, 
            processing_config,
            glue_context=glue_context,
            config=config,
            is_initial_load=is_initial_load
        )
        
        # Step 4: Materialize observation_codes_df to break lineage before DynamicFrame conversion
        # This prevents MetadataFetchFailedException during materialization in convert_to_dynamic_frames
        # Materialize early to avoid shuffle failures from complex lineage (code enrichment, explode, UDFs)
        # Note: We need database config for s3_temp_dir, but we only have processing_config here
        # So we'll skip early materialization and let convert_to_dynamic_frames handle it
        # (The materialization in convert_to_dynamic_frames should work if we materialize BEFORE any operations)
        logger.info("observation_codes_df ready (will be materialized in convert_to_dynamic_frames if needed)")
        
        logger.info("✅ Observation codes transformation completed")
        logger.info("   - Counts skipped to avoid shuffle failures")
        
        return unique_codes_df, observation_codes_df
    finally:
        # Always unpersist to free memory
        codes_df.unpersist()
        logger.info("Uncached codes_df to free memory")

