"""
Body sites transformation for observations
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, ArrayType
from typing import Tuple, Optional
import logging

from utils.transformation_helpers import extract_patient_id
from transformations.codes_transformation import generate_code_id_native
from shared.config import ProcessingConfig, DatabaseConfig
from awsglue.context import GlueContext

logger = logging.getLogger(__name__)


def transform_observation_body_sites_to_body_sites_tables(
    df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None,
    glue_context: Optional[GlueContext] = None,
    config: Optional[DatabaseConfig] = None,
    is_initial_load: bool = False
) -> Tuple[DataFrame, DataFrame]:
    """
    Transform observation body sites using body_sites table approach
    
    Returns both:
    1. unique_body_sites_df: DataFrame for body_sites table (unique body sites with body_site_id)
    2. observation_body_sites_df: DataFrame for observation_body_sites table (references via body_site_id)
    
    Args:
        df: Source observation DataFrame
        processing_config: Processing configuration
        glue_context: AWS Glue context (for loading existing body_site_ids)
        config: Database configuration
        is_initial_load: If True, skip loading existing IDs
        
    Returns:
        Tuple of (unique_body_sites_df, observation_body_sites_df)
    """
    logger.info("Transforming observation body sites to body_sites table structure...")
    
    if "bodySite" not in df.columns:
        logger.warning("bodySite column not found in data, returning empty DataFrames")
        empty_schema_junction = StructType([
            StructField("observation_id", StringType(), False),
            StructField("patient_id", StringType(), False),
            StructField("body_site_id", LongType(), False),
            StructField("body_site_rank", IntegerType(), True)
        ])
        empty_schema_lookup = StructType([
            StructField("body_site_id", LongType(), False),
            StructField("body_site_code", StringType(), False),
            StructField("body_site_system", StringType(), False),
            StructField("body_site_display", StringType(), True),
            StructField("body_site_text", StringType(), True)
        ])
        return (
            df.sparkSession.createDataFrame([], empty_schema_lookup),
            df.sparkSession.createDataFrame([], empty_schema_junction)
        )
    
    # Handle bodySite - it can be either a single struct (CodeableConcept) or an array
    # Check if bodySite is an array or struct type
    body_site_field = df.schema["bodySite"]
    is_array_type = isinstance(body_site_field.dataType, ArrayType)
    
    if is_array_type:
        # bodySite is already an array - use posexplode directly
        logger.info("bodySite is an array type - using posexplode")
        body_sites_df = df.select(
            F.col("id").alias("observation_id"),
            extract_patient_id(df).alias("patient_id"),
            F.posexplode(F.col("bodySite")).alias("body_site_rank", "body_site_item")
        ).filter(
            F.col("body_site_item").isNotNull()
        )
    else:
        # bodySite is a single struct - wrap it in an array first, then explode
        logger.info("bodySite is a struct type - wrapping in array before exploding")
        body_sites_df = df.select(
            F.col("id").alias("observation_id"),
            extract_patient_id(df).alias("patient_id"),
            F.posexplode(F.array(F.col("bodySite"))).alias("body_site_rank", "body_site_item")
        ).filter(
            F.col("body_site_item").isNotNull()
        )
    
    # Extract body site details and explode the coding array
    body_sites_with_coding = body_sites_df.select(
        F.col("observation_id"),
        F.col("patient_id"),
        F.col("body_site_rank"),
        F.explode_outer(F.col("body_site_item.coding")).alias("coding_item"),
        F.col("body_site_item.text").alias("body_site_text")
    ).filter(
        F.col("coding_item").isNotNull()
    )
    
    # Extract code fields for body_site_id generation
    body_site_code_col = F.col("coding_item.code")
    body_site_system_col = F.col("coding_item.system")
    body_site_display_col = F.col("coding_item.display")
    
    # Generate body_site_id using native Spark (same hash function as codes)
    body_sites_with_id = body_sites_with_coding.select(
        F.col("observation_id"),
        F.col("patient_id"),
        F.col("body_site_rank").cast("int").alias("body_site_rank"),
        # Generate body_site_id using same hash function as codes (native Spark)
        F.when(
            body_site_code_col.isNotNull() & body_site_system_col.isNotNull() &
            (F.trim(F.coalesce(body_site_code_col, F.lit(""))) != F.lit("")) &
            (F.trim(F.coalesce(body_site_system_col, F.lit(""))) != F.lit("")),
            generate_code_id_native(body_site_code_col, body_site_system_col)
        ).otherwise(None).alias("body_site_id"),
        # Keep code details for unique extraction
        body_site_code_col.alias("body_site_code"),
        body_site_system_col.alias("body_site_system"),
        body_site_display_col.alias("body_site_display"),
        F.col("body_site_text")
    ).filter(
        F.col("body_site_id").isNotNull() & 
        F.col("patient_id").isNotNull() &
        F.col("observation_id").isNotNull()
    )
    
    # Extract unique body sites for body_sites lookup table
    # Group by body_site_id and take first values for code, system, display, text
    unique_body_sites_df = body_sites_with_id.select(
        F.col("body_site_id"),
        F.col("body_site_code"),
        F.col("body_site_system"),
        F.col("body_site_display"),
        F.col("body_site_text")
    ).groupBy("body_site_id").agg(
        F.first("body_site_code").alias("body_site_code"),
        F.first("body_site_system").alias("body_site_system"),
        F.first("body_site_display").alias("body_site_display"),
        F.first("body_site_text").alias("body_site_text")
    ).filter(
        F.col("body_site_id").isNotNull() &
        F.col("body_site_code").isNotNull() &
        (F.trim(F.coalesce(F.col("body_site_code"), F.lit(""))) != F.lit("")) &
        F.col("body_site_system").isNotNull() &
        (F.trim(F.coalesce(F.col("body_site_system"), F.lit(""))) != F.lit(""))
    )
    
    # Truncate body_site_text to 500 chars (as per DDL)
    unique_body_sites_df = unique_body_sites_df.withColumn(
        "body_site_text",
        F.when(
            F.col("body_site_text").isNotNull(),
            F.substring(F.col("body_site_text"), 1, 500)
        ).otherwise(None)
    )
    
    # Create junction table DataFrame (observation_body_sites)
    observation_body_sites_df = body_sites_with_id.select(
        F.col("observation_id"),
        F.col("patient_id"),
        F.col("body_site_id"),
        F.col("body_site_rank")
    )
    
    logger.info("✅ Observation body sites transformation completed")
    
    return unique_body_sites_df, observation_body_sites_df


def transform_observation_body_sites(df: DataFrame) -> DataFrame:
    """
    Transform observation body sites into normalized structure (junction table only)
    Returns DataFrame with observation_id, patient_id, body_site_id, body_site_rank
    Uses shared body_sites table (same as conditions)
    
    Note: For full transformation including lookup table, use transform_observation_body_sites_to_body_sites_tables()
    """
    logger.info("Transforming observation body sites...")
    
    if "bodySite" not in df.columns:
        logger.warning("bodySite column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("patient_id"),
            F.lit(0).cast("bigint").alias("body_site_id"),
            F.lit(0).cast("int").alias("body_site_rank")
        ).filter(F.lit(False))
    
    # Handle bodySite - it can be either a single struct (CodeableConcept) or an array
    # Check if bodySite is an array or struct type
    body_site_field = df.schema["bodySite"]
    is_array_type = isinstance(body_site_field.dataType, ArrayType)
    
    if is_array_type:
        # bodySite is already an array - use posexplode directly
        logger.info("bodySite is an array type - using posexplode")
        body_sites_df = df.select(
            F.col("id").alias("observation_id"),
            extract_patient_id(df).alias("patient_id"),
            F.posexplode(F.col("bodySite")).alias("body_site_rank", "body_site_item")
        ).filter(
            F.col("body_site_item").isNotNull()
        )
    else:
        # bodySite is a single struct - wrap it in an array first, then explode
        logger.info("bodySite is a struct type - wrapping in array before exploding")
        body_sites_df = df.select(
            F.col("id").alias("observation_id"),
            extract_patient_id(df).alias("patient_id"),
            F.posexplode(F.array(F.col("bodySite"))).alias("body_site_rank", "body_site_item")
        ).filter(
            F.col("body_site_item").isNotNull()
        )
    
    # Extract body site details and explode the coding array
    body_sites_with_coding = body_sites_df.select(
        F.col("observation_id"),
        F.col("patient_id"),
        F.col("body_site_rank"),
        F.explode_outer(F.col("body_site_item.coding")).alias("coding_item"),
        F.col("body_site_item.text").alias("body_site_text")
    ).filter(
        F.col("coding_item").isNotNull()
    )
    
    # Generate body_site_id using native Spark (same hash function as codes)
    body_sites_final = body_sites_with_coding.select(
        F.col("observation_id"),
        F.col("patient_id"),
        F.col("body_site_rank").cast("int").alias("body_site_rank"),
        # Generate body_site_id using same hash function as codes (native Spark)
        F.when(
            F.col("coding_item.code").isNotNull() & F.col("coding_item.system").isNotNull() &
            (F.trim(F.coalesce(F.col("coding_item.code"), F.lit(""))) != F.lit("")) &
            (F.trim(F.coalesce(F.col("coding_item.system"), F.lit(""))) != F.lit("")),
            generate_code_id_native(
                F.col("coding_item.code"),
                F.col("coding_item.system")
            )
        ).otherwise(None).alias("body_site_id")
    ).filter(
        F.col("body_site_id").isNotNull() & 
        F.col("patient_id").isNotNull() &
        F.col("observation_id").isNotNull()
    )
    
    return body_sites_final
