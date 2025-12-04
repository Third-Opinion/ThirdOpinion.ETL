"""
Reference ranges transformation for observations
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StructType, StructField, StringType, FloatType
import logging

from utils.transformation_helpers import extract_patient_id
from transformations.codes_transformation import generate_code_id_native

logger = logging.getLogger(__name__)


def transform_observation_reference_ranges(df: DataFrame) -> DataFrame:
    """Transform observation reference ranges"""
    logger.info("Transforming observation reference ranges...")
    
    if "referenceRange" not in df.columns:
        logger.warning("referenceRange column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("patient_id"),
            F.lit(0.0).alias("range_low_value"),
            F.lit("").alias("range_low_unit"),
            F.lit(0.0).alias("range_high_value"),
            F.lit("").alias("range_high_unit"),
            F.lit(None).cast("bigint").alias("range_type_code_id"),  # Normalized
            F.lit("").alias("range_text")
        ).filter(F.lit(False))
    
    # Explode the referenceRange array
    ranges_df = df.select(
        F.col("id").alias("observation_id"),
        extract_patient_id(df).alias("patient_id"),
        F.explode(F.col("referenceRange")).alias("range_item")
    ).filter(F.col("range_item").isNotNull())
    
    # Try multiple approaches to extract data based on possible structures
    try:
        # Handle case where value might be a struct (with .double/.int) or already a decimal
        # Use UDF to safely extract value regardless of structure
        def extract_range_value(low_or_high_item):
            """Extract numeric value from range item, handling both struct and direct decimal"""
            if not low_or_high_item:
                return None
            try:
                # If it's a dict/struct, try to get value
                if isinstance(low_or_high_item, dict):
                    # Check if it has a value field
                    if "value" in low_or_high_item:
                        value_obj = low_or_high_item["value"]
                        # If value is a dict, try double or int
                        if isinstance(value_obj, dict):
                            return value_obj.get("double") or value_obj.get("int") or value_obj
                        # If value is already a number, return it
                        return value_obj
                    # If no value field, the item itself might be the value
                    return low_or_high_item.get("double") or low_or_high_item.get("int") or low_or_high_item
                # If it's already a number, return it
                return float(low_or_high_item) if low_or_high_item is not None else None
            except (TypeError, ValueError, AttributeError):
                return None
        
        extract_value_udf = F.udf(extract_range_value, DecimalType(15, 4))
        
        # Extract type info using UDF (type is a CodeableConcept with: id, extension, coding, text, _text)
        # Cannot access type.code directly - must go through coding array
        def extract_type_info(type_item):
            """Extract code, system, display from CodeableConcept type field"""
            if not type_item:
                return (None, None, None)
            try:
                # If it's a dict/struct
                if isinstance(type_item, dict):
                    # Try to get coding array
                    coding_array = type_item.get("coding")
                    if coding_array and isinstance(coding_array, list) and len(coding_array) > 0:
                        # Get first coding item
                        first_coding = coding_array[0]
                        if isinstance(first_coding, dict):
                            code = first_coding.get("code")
                            system = first_coding.get("system")
                            display = first_coding.get("display")
                            return (code, system, display)
                    # Fallback to text if no coding
                    text = type_item.get("text") or type_item.get("_text")
                    if text:
                        return (None, None, text)
                return (None, None, None)
            except (TypeError, ValueError, AttributeError):
                return (None, None, None)
        
        extract_type_schema = StructType([
            StructField("code", StringType(), True),
            StructField("system", StringType(), True),
            StructField("display", StringType(), True)
        ])
        
        extract_type_udf = F.udf(extract_type_info, extract_type_schema)
        
        ranges_with_type = ranges_df.withColumn(
            "type_info",
            extract_type_udf(F.col("range_item.type"))
        )
        
        # Extract type code and system for normalization
        range_type_code_col = F.col("type_info.code")
        range_type_system_col = F.col("type_info.system")
        
        ranges_final = ranges_with_type.select(
            F.col("observation_id"),
            F.col("patient_id"),
            # Extract low value using UDF (complex nested structure)
            extract_value_udf(F.col("range_item.low")).alias("range_low_value"),
            F.coalesce(F.col("range_item.low.unit"), F.lit(None)).alias("range_low_unit"),
            # Extract high value using UDF (complex nested structure)
            extract_value_udf(F.col("range_item.high")).alias("range_high_value"),
            F.coalesce(F.col("range_item.high.unit"), F.lit(None)).alias("range_high_unit"),
            # Normalized: range_type_code_id (references codes.code_id)
            F.when(
                range_type_code_col.isNotNull() & range_type_system_col.isNotNull() &
                (F.trim(F.coalesce(range_type_code_col, F.lit(""))) != F.lit("")) &
                (F.trim(F.coalesce(range_type_system_col, F.lit(""))) != F.lit("")),
                generate_code_id_native(range_type_code_col, range_type_system_col)
            ).otherwise(None).alias("range_type_code_id"),
            F.col("range_item.text").alias("range_text")
        )
    except Exception as e:
        logger.warning(f"Could not extract reference ranges using nested structure: {str(e)}")
        # Fallback: Just extract text field if available
        ranges_final = ranges_df.select(
            F.col("observation_id"),
            F.col("patient_id"),
            F.lit(None).cast("decimal(15,4)").alias("range_low_value"),
            F.lit(None).alias("range_low_unit"),
            F.lit(None).cast("decimal(15,4)").alias("range_high_value"),
            F.lit(None).alias("range_high_unit"),
            F.lit(None).cast("bigint").alias("range_type_code_id"),  # Normalized
            F.col("range_item.text").alias("range_text")
        )
    
    # Parse text ranges to extract numeric LLN/ULN values
    # Import parser function
    try:
        from utils.reference_range_parser import parse_reference_range_text
        
        # Create UDF for parsing reference range text
        def parse_range_udf_wrapper(text):
            """Wrapper for UDF that handles None values"""
            if text is None:
                return (None, None, None)
            low, high, unit = parse_reference_range_text(text)
            return (float(low) if low is not None else None, 
                    float(high) if high is not None else None, 
                    unit)
        
        parse_range_schema = StructType([
            StructField("low", FloatType(), True),
            StructField("high", FloatType(), True),
            StructField("unit", StringType(), True)
        ])
        
        parse_range_udf = F.udf(parse_range_udf_wrapper, parse_range_schema)
        
        # Parse text ranges and merge with existing extracted values
        ranges_with_parsed = ranges_final.withColumn(
            "parsed_range",
            parse_range_udf(F.col("range_text"))
        ).select(
            F.col("observation_id"),
            F.col("patient_id"),
            # Use parsed values if available, otherwise use original extracted values
            F.coalesce(
                F.col("parsed_range.low").cast("decimal(15,4)"),
                F.col("range_low_value")
            ).alias("range_low_value"),
            F.coalesce(
                F.col("parsed_range.unit"),
                F.col("range_low_unit")
            ).alias("range_low_unit"),
            F.coalesce(
                F.col("parsed_range.high").cast("decimal(15,4)"),
                F.col("range_high_value")
            ).alias("range_high_value"),
            F.coalesce(
                F.col("parsed_range.unit"),
                F.col("range_high_unit")
            ).alias("range_high_unit"),
            F.col("range_type_code_id"),  # Normalized
            F.col("range_text")
        )
        
        ranges_final = ranges_with_parsed
        
        logger.info("✅ Parsed reference range text to extract numeric LLN/ULN values")
        
    except ImportError:
        logger.warning("Could not import reference_range_parser - skipping text parsing")
        # Continue with original ranges_final
    except Exception as e:
        logger.warning(f"Error parsing reference ranges: {str(e)} - continuing without parsing")
        # Continue with original ranges_final
    
    # Filter to keep only records with some data
    ranges_final = ranges_final.filter(
        (F.col("range_text").isNotNull() |
         F.col("range_low_value").isNotNull() |
         F.col("range_high_value").isNotNull()) &
        F.col("patient_id").isNotNull()
    )
    
    return ranges_final

