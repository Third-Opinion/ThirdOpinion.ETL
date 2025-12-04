"""
Evidence transformation for conditions
Single Responsibility: Transforms condition evidence only
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

from utils.transformation_helpers import extract_patient_id

logger = logging.getLogger(__name__)


def transform_condition_evidence(df: DataFrame) -> DataFrame:
    """
    Transform condition evidence from FHIR Condition.evidence array
    
    Returns DataFrame with columns:
    - condition_id: VARCHAR(255)
    - patient_id: VARCHAR(255)
    - meta_last_updated: TIMESTAMP
    - evidence_code: VARCHAR(50)
    - evidence_system: VARCHAR(255)
    - evidence_display: VARCHAR(255)
    - evidence_detail_reference: VARCHAR(255)
    
    Uses Spark native functions only (no UDFs).
    Avoids shuffles by filtering early and using column-based operations.
    """
    logger.info("Transforming condition evidence...")
    
    if "evidence" not in df.columns:
        logger.warning("evidence column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("condition_id"),
            F.lit("").alias("patient_id"),
            F.col("meta").getField("lastUpdated").cast("timestamp").alias("meta_last_updated"),
            F.lit("").alias("evidence_code"),
            F.lit("").alias("evidence_system"),
            F.lit("").alias("evidence_display"),
            F.lit("").alias("evidence_detail_reference")
        ).filter(F.lit(False))
    
    # Explode the evidence array - single pass transformation
    # Filter nulls early to avoid unnecessary processing
    evidence_df = df.select(
        F.col("id").alias("condition_id"),
        extract_patient_id(df).alias("patient_id"),
        F.col("meta").getField("lastUpdated").cast("timestamp").alias("meta_last_updated"),
        F.explode(F.col("evidence")).alias("evidence_item")
    ).filter(
        F.col("evidence_item").isNotNull()
    )
    
    # Extract evidence details - explode coding array in separate step for clarity
    # This separates concerns: first extract evidence items, then extract codes
    evidence_with_coding = evidence_df.select(
        F.col("condition_id"),
        F.col("patient_id"),
        F.col("meta_last_updated"),
        F.col("evidence_item.detail").alias("evidence_detail"),
        F.explode(F.col("evidence_item.code.coding")).alias("coding_item")
    ).filter(
        F.col("coding_item").isNotNull()
    )
    
    # Extract code details using native Spark column operations
    evidence_final = evidence_with_coding.select(
        F.col("condition_id"),
        F.col("patient_id"),
        F.col("meta_last_updated"),
        F.col("coding_item.code").alias("evidence_code"),
        F.col("coding_item.system").alias("evidence_system"),
        F.lit(None).cast("string").alias("evidence_display"),  # Not typically populated
        # Extract reference from detail array if present
        F.when(
            F.col("evidence_detail").isNotNull() &
            (F.size(F.col("evidence_detail")) > 0),
            F.col("evidence_detail")[0].getField("reference")
        ).otherwise(None).alias("evidence_detail_reference")
    ).filter(
        # Filter early: keep only rows with evidence code
        # This reduces data volume for subsequent operations
        F.col("evidence_code").isNotNull() &
        F.col("patient_id").isNotNull()
    )
    
    return evidence_final

