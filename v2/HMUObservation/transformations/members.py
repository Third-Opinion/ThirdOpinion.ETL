"""
Members transformation for observations
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

from utils.transformation_helpers import extract_patient_id

logger = logging.getLogger(__name__)


def transform_observation_members(df: DataFrame) -> DataFrame:
    """Transform observation members"""
    logger.info("Transforming observation members...")
    
    if "hasmember" not in df.columns:
        logger.warning("hasmember column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("patient_id"),
            F.lit("").alias("member_observation_id")
        ).filter(F.lit(False))
    
    # Explode the hasmember array
    members_df = df.select(
        F.col("id").alias("observation_id"),
        extract_patient_id(df).alias("patient_id"),
        F.explode(F.col("hasmember")).alias("member_item")
    ).filter(F.col("member_item").isNotNull())
    
    # Extract member details
    members_final = members_df.select(
        F.col("observation_id"),
        F.col("patient_id"),
        F.regexp_extract(F.col("member_item").getField("reference"), r"Observation/(.+)", 1).alias("member_observation_id")
    ).filter(
        F.col("member_observation_id").isNotNull() & F.col("patient_id").isNotNull()
    )
    
    return members_final

