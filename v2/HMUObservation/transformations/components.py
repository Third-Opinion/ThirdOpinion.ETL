"""
Components transformation for observations
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

from utils.transformation_helpers import extract_patient_id
from transformations.codes_transformation import generate_code_id_native

logger = logging.getLogger(__name__)


def transform_observation_components(df: DataFrame) -> DataFrame:
    """Transform observation components"""
    logger.info("Transforming observation components...")
    
    if "component" not in df.columns:
        logger.warning("component column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("patient_id"),
            F.lit(None).cast("bigint").alias("component_code_id"),  # Normalized
            F.lit("").alias("component_text"),
            F.lit("").alias("component_value_string"),
            F.lit(0.0).alias("component_value_quantity_value"),
            F.lit("").alias("component_value_quantity_unit"),
            F.lit(None).cast("bigint").alias("component_value_code_id"),  # Normalized
            F.lit(None).cast("bigint").alias("component_data_absent_reason_code_id")  # Normalized
        ).filter(F.lit(False))
    
    # Explode the component array
    components_df = df.select(
        F.col("id").alias("observation_id"),
        extract_patient_id(df).alias("patient_id"),
        F.explode(F.col("component")).alias("component_item")
    ).filter(F.col("component_item").isNotNull())
    
    # Extract component details - handle nested structures properly
    components_with_code = components_df.select(
        F.col("observation_id"),
        F.col("patient_id"),
        F.col("component_item"),  # Keep the full component item for special case handling
        F.explode(F.col("component_item.code").getField("coding")).alias("code_coding_item")
    )
    
    # Extract code fields for normalization
    component_code_col = F.regexp_replace(F.col("code_coding_item.code"), '^"|"$', '')
    component_system_col = F.col("code_coding_item.system")
    
    # Extract value codeable concept code/system for normalization
    component_value_code_col = F.when(
        (F.col("component_item.valueCodeableConcept.coding").isNotNull()) &
        (F.size(F.col("component_item.valueCodeableConcept.coding")) > 0),
        F.col("component_item.valueCodeableConcept.coding")[0].getField("code")
    ).otherwise(None)
    component_value_system_col = F.when(
        (F.col("component_item.valueCodeableConcept.coding").isNotNull()) &
        (F.size(F.col("component_item.valueCodeableConcept.coding")) > 0),
        F.col("component_item.valueCodeableConcept.coding")[0].getField("system")
    ).otherwise(None)
    
    # Extract data absent reason code/system for normalization
    component_data_absent_code_col = F.when(
        (F.col("component_item.dataAbsentReason.coding").isNotNull()) &
        (F.size(F.col("component_item.dataAbsentReason.coding")) > 0),
        F.col("component_item.dataAbsentReason.coding")[0].getField("code")
    ).otherwise(None)
    component_data_absent_system_col = F.when(
        (F.col("component_item.dataAbsentReason.coding").isNotNull()) &
        (F.size(F.col("component_item.dataAbsentReason.coding")) > 0),
        F.col("component_item.dataAbsentReason.coding")[0].getField("system")
    ).otherwise(None)
    
    # Now extract the code details and handle special case for ThirdOpinion.io
    components_final = components_with_code.select(
        F.col("observation_id"),
        F.col("patient_id"),
        # Normalized: component_code_id (references codes.code_id)
        F.when(
            component_code_col.isNotNull() & component_system_col.isNotNull() &
            (F.trim(F.coalesce(component_code_col, F.lit(""))) != F.lit("")) &
            (F.trim(F.coalesce(component_system_col, F.lit(""))) != F.lit("")),
            generate_code_id_native(component_code_col, component_system_col)
        ).otherwise(None).alias("component_code_id"),
        # Special case: When system is ThirdOpinion.io, use code.text instead of regular text
        F.when(
            F.col("code_coding_item.system") == "https://thirdopinion.io/result-code",
            F.col("component_item.code.text")
        ).otherwise(
            F.col("component_item.code.text")  # Standard case - also uses code.text
        ).alias("component_text"),
        # Special cases for value mapping
        F.when(
            F.col("code_coding_item.system") == "https://thirdopinion.io/result-code",
            F.col("component_item.valueDateTime")
        ).when(
            F.col("code_coding_item.system").startswith("http://thirdopinion.ai/fhir/CodeSystem"),
            F.col("component_item.valueString")
        ).otherwise(
            F.col("component_item.valueString")
        ).alias("component_value_string"),
        # Extract standard value fields
        F.col("component_item.valueQuantity.value").alias("component_value_quantity_value"),
        F.col("component_item.valueQuantity.unit").alias("component_value_quantity_unit"),
        # Normalized: component_value_code_id (references codes.code_id)
        F.when(
            component_value_code_col.isNotNull() & component_value_system_col.isNotNull() &
            (F.trim(F.coalesce(component_value_code_col, F.lit(""))) != F.lit("")) &
            (F.trim(F.coalesce(component_value_system_col, F.lit(""))) != F.lit("")),
            generate_code_id_native(component_value_code_col, component_value_system_col)
        ).otherwise(None).alias("component_value_code_id"),
        # Normalized: component_data_absent_reason_code_id (references codes.code_id)
        F.when(
            component_data_absent_code_col.isNotNull() & component_data_absent_system_col.isNotNull() &
            (F.trim(F.coalesce(component_data_absent_code_col, F.lit(""))) != F.lit("")) &
            (F.trim(F.coalesce(component_data_absent_system_col, F.lit(""))) != F.lit("")),
            generate_code_id_native(component_data_absent_code_col, component_data_absent_system_col)
        ).otherwise(None).alias("component_data_absent_reason_code_id")
    ).filter(
        F.col("component_code_id").isNotNull() & F.col("patient_id").isNotNull()
    )
    
    return components_final

