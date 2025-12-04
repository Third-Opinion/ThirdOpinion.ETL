"""
Extensions transformation for conditions
Single Responsibility: Transforms condition extensions only
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, BooleanType, DecimalType, IntegerType, TimestampType
import logging

from shared.utils.timestamp_utils import create_timestamp_parser

logger = logging.getLogger(__name__)


def transform_condition_extensions(df: DataFrame) -> DataFrame:
    """
    Transform condition extensions from FHIR Condition.extension array
    
    Returns DataFrame with columns:
    - condition_id: VARCHAR(255)
    - extension_url: VARCHAR(MAX)
    - extension_type: VARCHAR(20) - 'simple' or 'nested'
    - value_type: VARCHAR(20) - type of value (string, dateTime, reference, code, boolean, decimal, integer)
    - value_string: VARCHAR(MAX)
    - value_datetime: TIMESTAMP
    - value_reference: VARCHAR(255)
    - value_code: VARCHAR(50)
    - value_boolean: BOOLEAN
    - value_decimal: DECIMAL(18,6)
    - value_integer: INTEGER
    - parent_extension_url: VARCHAR(MAX) - for nested extensions
    - extension_order: INTEGER
    - created_at: TIMESTAMP
    - updated_at: TIMESTAMP
    
    Uses Spark native functions only (no UDFs).
    Handles both simple and nested extensions.
    Avoids shuffles by filtering early and using column-based operations.
    """
    logger.info("Transforming condition extensions...")
    
    if "extension" not in df.columns:
        logger.warning("extension column not found in data, returning empty DataFrame")
        return _create_empty_extensions_df(df)
    
    # Check the data type of the extension column
    ext_dtype = str(df.schema["extension"].dataType)
    logger.info(f"Extension column data type: {ext_dtype}")
    
    # For StringType extension data, return empty for now
    # String representations would require parsing, which is not yet implemented
    if ext_dtype.startswith("StringType") or "StringType" in ext_dtype:
        logger.warning("Extension column is StringType - returning empty DataFrame")
        logger.warning("Extension parsing from string representation is not yet implemented")
        return _create_empty_extensions_df(df)
    
    try:
        # Explode the extension array - single pass transformation
        # Filter nulls early to avoid unnecessary processing
        extensions_df = df.select(
            F.col("id").alias("condition_id"),
            F.posexplode(F.col("extension")).alias("extension_order", "ext")
        ).filter(
            F.col("ext").isNotNull()
        )
        
        # Extract extension details using native Spark column operations
        # Use timestamp parser utility for consistent timestamp handling
        value_datetime_expr = create_timestamp_parser(F.col("ext.valueDateTime"))
        
        extensions_final = extensions_df.select(
            F.col("condition_id"),
            F.col("ext.url").alias("extension_url"),
            # Determine if extension is nested or simple
            F.when(
                F.col("ext.extension").isNotNull() &
                (F.size(F.col("ext.extension")) > 0),
                F.lit("nested")
            ).otherwise(F.lit("simple")).alias("extension_type"),
            # Determine value type based on which value field is present
            # Order matters: check most specific types first
            F.when(F.col("ext.valueString").isNotNull(), F.lit("string"))
             .when(F.col("ext.valueDateTime").isNotNull(), F.lit("dateTime"))
             .when(F.col("ext.valueReference").isNotNull(), F.lit("reference"))
             .when(F.col("ext.valueCode").isNotNull(), F.lit("code"))
             .when(F.col("ext.valueBoolean").isNotNull(), F.lit("boolean"))
             .when(F.col("ext.valueDecimal").isNotNull(), F.lit("decimal"))
             .when(F.col("ext.valueInteger").isNotNull(), F.lit("integer"))
             .otherwise(F.lit("unknown")).alias("value_type"),
            # Extract value fields (only one will be populated based on value_type)
            F.col("ext.valueString").alias("value_string"),
            value_datetime_expr.alias("value_datetime"),
            # Handle valueReference which might be a struct with a reference field
            F.when(
                F.col("ext.valueReference").isNotNull(),
                F.when(
                    F.col("ext.valueReference.reference").isNotNull(),
                    F.col("ext.valueReference.reference")
                ).otherwise(F.col("ext.valueReference"))
            ).otherwise(None).alias("value_reference"),
            F.col("ext.valueCode").alias("value_code"),
            F.col("ext.valueBoolean").cast(BooleanType()).alias("value_boolean"),
            F.col("ext.valueDecimal").cast(DecimalType(18, 6)).alias("value_decimal"),
            F.col("ext.valueInteger").cast(IntegerType()).alias("value_integer"),
            # Parent extension URL for nested extensions (not yet implemented)
            F.lit(None).cast(StringType()).alias("parent_extension_url"),
            F.col("extension_order").cast(IntegerType()).alias("extension_order"),
            F.current_timestamp().cast(TimestampType()).alias("created_at"),
            F.current_timestamp().cast(TimestampType()).alias("updated_at")
        ).filter(
            # Filter early: keep only rows with extension URL
            # This reduces data volume for subsequent operations
            F.col("extension_url").isNotNull()
        )
        
        return extensions_final
    
    except Exception as e:
        logger.warning(f"Failed to transform extensions: {str(e)}")
        logger.warning("Returning empty extensions DataFrame")
        return _create_empty_extensions_df(df)


def _create_empty_extensions_df(df: DataFrame) -> DataFrame:
    """
    Helper function to create empty extensions DataFrame with correct schema
    
    This avoids code duplication and ensures consistent schema.
    """
    return df.select(
        F.col("id").alias("condition_id"),
        F.lit(None).cast(StringType()).alias("extension_url"),
        F.lit("simple").cast(StringType()).alias("extension_type"),
        F.lit("unknown").cast(StringType()).alias("value_type"),
        F.lit(None).cast(StringType()).alias("value_string"),
        F.lit(None).cast(TimestampType()).alias("value_datetime"),
        F.lit(None).cast(StringType()).alias("value_reference"),
        F.lit(None).cast(StringType()).alias("value_code"),
        F.lit(None).cast(BooleanType()).alias("value_boolean"),
        F.lit(None).cast(DecimalType(18, 6)).alias("value_decimal"),
        F.lit(None).cast(IntegerType()).alias("value_integer"),
        F.lit(None).cast(StringType()).alias("parent_extension_url"),
        F.lit(0).cast(IntegerType()).alias("extension_order"),
        F.current_timestamp().cast(TimestampType()).alias("created_at"),
        F.current_timestamp().cast(TimestampType()).alias("updated_at")
    ).filter(F.lit(False))

