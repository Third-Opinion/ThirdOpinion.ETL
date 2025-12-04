"""
Performers transformation for observations
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

from utils.transformation_helpers import extract_patient_id

logger = logging.getLogger(__name__)


def transform_observation_performers(df: DataFrame) -> DataFrame:
    """Transform observation performers"""
    logger.info("Transforming observation performers...")
    
    if "performer" not in df.columns:
        logger.warning("performer column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("patient_id"),
            F.lit("").alias("performer_type"),
            F.lit("").alias("performer_id")
        ).filter(F.lit(False))
    
    # Explode the performer array
    performers_df = df.select(
        F.col("id").alias("observation_id"),
        extract_patient_id(df).alias("patient_id"),
        F.explode(F.col("performer")).alias("performer_item")
    ).filter(F.col("performer_item").isNotNull())
    
    # Extract performer details
    performers_final = performers_df.select(
        F.col("observation_id"),
        F.col("patient_id"),
        F.regexp_extract(F.col("performer_item").getField("reference"), r"([^/]+)/", 1).alias("performer_type"),
        F.regexp_extract(F.col("performer_item").getField("reference"), r"/(.+)", 1).alias("performer_id")
    ).filter(
        F.col("performer_id").isNotNull() & F.col("patient_id").isNotNull()
    )
    
    return performers_final

