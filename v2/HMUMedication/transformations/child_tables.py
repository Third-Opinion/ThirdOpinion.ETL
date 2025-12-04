"""
Transform child tables for medication
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)


def transform_medication_identifiers(df: DataFrame) -> DataFrame:
    """Transform medication identifiers (multiple identifiers per medication)"""
    logger.info("Transforming medication identifiers...")
    
    # Check if identifier column exists
    if "identifier" not in df.columns:
        logger.warning("identifier column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("medication_id"),
            F.lit("").alias("identifier_system"),
            F.lit("").alias("identifier_value")
        ).filter(F.lit(False))
    
    # First explode the identifier array
    identifiers_df = df.select(
        F.col("id").alias("medication_id"),
        F.explode(F.col("identifier")).alias("identifier_item")
    ).filter(
        F.col("identifier_item").isNotNull()
    )
    
    # Extract identifier details
    identifiers_final = identifiers_df.select(
        F.col("medication_id"),
        F.col("identifier_item.system").alias("identifier_system"),
        F.col("identifier_item.value").alias("identifier_value")
    ).filter(
        F.col("identifier_value").isNotNull()
    )
    
    return identifiers_final

