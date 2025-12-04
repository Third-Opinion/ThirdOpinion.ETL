"""
Derived from transformation for observations
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

from utils.transformation_helpers import extract_patient_id

logger = logging.getLogger(__name__)


def transform_observation_derived_from(df: DataFrame) -> DataFrame:
    """
    Transform observation derived from references into normalized structure
    
    Extracts derivedFrom array elements into individual rows with parsed reference details.
    
    Returns DataFrame with columns:
    - observation_id: Source observation ID
    - patient_id: Patient ID from subject reference
    - reference: Full FHIR reference string
    - reference_type: Parsed resource type (e.g., "DocumentReference", "Observation")
    - reference_id: Parsed resource ID
    - display: Display text
    """
    logger.info("Transforming observation derivedFrom references...")
    
    if "derivedFrom" not in df.columns:
        logger.warning("derivedFrom column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("patient_id"),
            F.lit("").alias("reference"),
            F.lit("").alias("reference_type"),
            F.lit("").alias("reference_id"),
            F.lit("").alias("display")
        ).filter(F.lit(False))
    
    # Explode the derivedFrom array
    derived_df = df.select(
        F.col("id").alias("observation_id"),
        extract_patient_id(df).alias("patient_id"),
        F.explode(F.col("derivedFrom")).alias("derived_item")
    ).filter(F.col("derived_item").isNotNull())
    
    # Extract derived from details with parsed reference components
    derived_final = derived_df.select(
        F.col("observation_id"),
        F.col("patient_id"),
        # Full reference string
        F.col("derived_item").getField("reference").alias("reference"),
        # Extract reference type (part before /)
        F.when(
            F.col("derived_item").getField("reference").isNotNull(),
            F.regexp_extract(F.col("derived_item").getField("reference"), r"^([^/]+)/", 1)
        ).otherwise(
            F.col("derived_item").getField("type")
        ).alias("reference_type"),
        # Extract reference ID (part after /)
        F.when(
            F.col("derived_item").getField("reference").isNotNull(),
            F.regexp_extract(F.col("derived_item").getField("reference"), r"/(.+)$", 1)
        ).otherwise(None).alias("reference_id"),
        # Display text
        F.col("derived_item").getField("display").alias("display")
    ).filter(
        F.col("reference").isNotNull() & F.col("patient_id").isNotNull()
    )
    
    return derived_final

