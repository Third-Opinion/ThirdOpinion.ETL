"""
Main observation data transformation
Updated for normalized schema - code fields use code_id references
"""
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, BooleanType, DecimalType
import logging

from shared.utils.timestamp_utils import create_timestamp_parser
from shared.config import ProcessingConfig
from transformations.codes_transformation import generate_code_id_native

logger = logging.getLogger(__name__)


def transform_main_observation_data(
    df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None
) -> DataFrame:
    """
    Transform the main observation data
    
    Args:
        df: Source DataFrame with raw observation data
        processing_config: Optional processing configuration for debug logging
        
    Returns:
        Transformed DataFrame with flattened observation data
    """
    logger.info("Transforming main observation data...")
    
    # Log available columns for debugging
    available_columns = df.columns
    logger.info(f"Available columns: {len(available_columns)} columns")
    
    # Get normalized observation text from code enrichment
    normalized_text_col = F.lit(None).alias("normalized_observation_text")
    if processing_config and processing_config.enable_code_enrichment:
        try:
            from utils.code_enrichment import enrich_observation_code, get_normalized_text_for_code
            
            def get_normalized_text_udf_wrapper(code_text, coding_array):
                """Get normalized text for observation"""
                if not code_text:
                    return None
                
                # Try to get normalized text from existing codes first
                if coding_array:
                    try:
                        coding_list = list(coding_array) if coding_array else []
                        for coding_item in coding_list:
                            if coding_item and isinstance(coding_item, dict):
                                code_code = coding_item.get("code")
                                code_system = coding_item.get("system", "")
                                if code_code:
                                    normalized_text = get_normalized_text_for_code(
                                        code_code,
                                        code_system,
                                        code_text
                                    )
                                    if normalized_text:
                                        return normalized_text
                    except Exception:
                        pass
                
                # If no existing codes or no match, enrich to get normalized text
                enrichment_mode = processing_config.code_enrichment_mode if processing_config else "hybrid"
                _, normalized_text = enrich_observation_code(
                    code_text,
                    list(coding_array) if coding_array else [],
                    enable_enrichment=True,
                    enrichment_mode=enrichment_mode
                )
                return normalized_text
            
            get_normalized_udf = F.udf(get_normalized_text_udf_wrapper, StringType())
            normalized_text_col = get_normalized_udf(
                F.col("code").getField("text"),
                F.col("code.coding")
            ).alias("normalized_observation_text")
            
        except Exception as e:
            logger.warning(f"Could not get normalized observation text: {str(e)}")
            normalized_text_col = F.lit(None).alias("normalized_observation_text")
    
    # Build the select statement dynamically based on available columns
    select_columns = [
        F.col("id").alias("observation_id"),
        F.when(F.col("subject").isNotNull(), 
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        F.when(F.col("encounter").isNotNull(),
               F.regexp_extract(F.col("encounter").getField("reference"), r"Encounter/(.+)", 1)
              ).otherwise(None).alias("encounter_id"),
        F.when(F.col("specimen").isNotNull(),
               F.regexp_extract(F.col("specimen").getField("reference"), r"Specimen/(.+)", 1)
              ).otherwise(None).alias("specimen_id"),
        F.col("status"),
        F.col("code").getField("text").alias("observation_text"),
        normalized_text_col
    ]
    
    # Add value fields - handle different value types
    # Extract value codeable concept code/system for normalization
    value_code_code = F.col("valueCodeableConcept").getField("coding")[0].getField("code")
    value_code_system = F.col("valueCodeableConcept").getField("coding")[0].getField("system")
    
    select_columns.extend([
        F.col("valueString").alias("value_string"),
        F.col("valueQuantity").getField("value").alias("value_quantity_value"),
        F.col("valueQuantity").getField("unit").alias("value_quantity_unit"),
        F.col("valueQuantity").getField("system").alias("value_quantity_system"),
        F.col("valueCodeableConcept").getField("text").alias("value_codeable_concept_text"),  # Keep denormalized
        # Normalized: value_code_id (references codes.code_id)
        F.when(
            value_code_code.isNotNull() & value_code_system.isNotNull() &
            (F.trim(F.coalesce(value_code_code, F.lit(""))) != F.lit("")) &
            (F.trim(F.coalesce(value_code_system, F.lit(""))) != F.lit("")),
            generate_code_id_native(value_code_code, value_code_system)
        ).otherwise(None).alias("value_code_id"),
        # Handle valueDateTime with multiple possible formats
        create_timestamp_parser(F.col("valueDateTime")).alias("value_datetime"),
        F.lit(None).alias("value_boolean"),  # valueboolean field not in schema
    ])
    
    # Add data absent reason - normalized to code_id
    data_absent_code = F.col("dataAbsentReason").getField("coding")[0].getField("code")
    data_absent_system = F.col("dataAbsentReason").getField("coding")[0].getField("system")
    
    select_columns.extend([
        # Normalized: data_absent_reason_code_id (references codes.code_id)
        F.when(
            data_absent_code.isNotNull() & data_absent_system.isNotNull() &
            (F.trim(F.coalesce(data_absent_code, F.lit(""))) != F.lit("")) &
            (F.trim(F.coalesce(data_absent_system, F.lit(""))) != F.lit("")),
            generate_code_id_native(data_absent_code, data_absent_system)
        ).otherwise(None).alias("data_absent_reason_code_id"),
    ])
    
    # Add temporal information - handle multiple datetime formats
    select_columns.extend([
        # Handle effectiveDateTime with multiple possible formats
        create_timestamp_parser(F.col("effectiveDateTime")).alias("effective_datetime"),
        # Handle effectivePeriod.start with multiple formats
        create_timestamp_parser(F.col("effectivePeriod").getField("start")).alias("effective_period_start"),
        F.lit(None).alias("effective_period_end"),  # end field not in schema
        # Handle issued with multiple formats
        create_timestamp_parser(F.col("issued")).alias("issued"),
    ])
    
    # Body sites removed - now in observation_body_sites junction table
    # Add method - normalized to code_id
    method_code_col = F.when(F.col("method").isNotNull(), 
                            F.col("method").getField("coding")[0].getField("code")).otherwise(None)
    method_system_col = F.when(F.col("method").isNotNull(), 
                              F.col("method").getField("coding")[0].getField("system")).otherwise(None)
    
    select_columns.extend([
        # Normalized: method_code_id (references codes.code_id)
        F.when(
            method_code_col.isNotNull() & method_system_col.isNotNull() &
            (F.trim(F.coalesce(method_code_col, F.lit(""))) != F.lit("")) &
            (F.trim(F.coalesce(method_system_col, F.lit(""))) != F.lit("")),
            generate_code_id_native(method_code_col, method_system_col)
        ).otherwise(None).alias("method_code_id"),
        F.when(F.col("method").isNotNull(), 
               F.col("method").getField("text")).otherwise(None).alias("method_text"),  # Keep denormalized
    ])
    
    # Add metadata - using native Spark functions (no UDFs)
    select_columns.extend([
        # Handle meta.lastUpdated with multiple possible formats
        create_timestamp_parser(F.col("meta").getField("lastUpdated")).alias("meta_last_updated"),
        F.lit(None).alias("meta_source"),  # source field not in schema
        F.lit(None).alias("meta_profile"), # profile field not in schema
        F.to_json(F.col("meta").getField("security")).alias("meta_security"),  # Native Spark - no UDF
        F.lit(None).alias("meta_tag"),     # tag field not in schema
        F.to_json(F.col("extension")).alias("extensions"),  # Native Spark - no UDF
        # Store entire derivedFrom array as JSON - use F.to_json() for proper array serialization
        F.to_json(F.col("derivedFrom")).alias("derived_from"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ])
    
    # Transform main observation data
    main_df = df.select(*select_columns).filter(
        F.col("observation_id").isNotNull() &
        F.col("patient_id").isNotNull()
    )
    
    logger.info("✅ Main observation transformation completed")
    
    return main_df

