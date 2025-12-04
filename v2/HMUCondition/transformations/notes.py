"""
Notes transformation for conditions
Single Responsibility: Transforms condition notes only
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

from utils.transformation_helpers import extract_patient_id
from shared.utils.timestamp_utils import create_timestamp_parser

logger = logging.getLogger(__name__)


def transform_condition_notes(df: DataFrame) -> DataFrame:
    """
    Transform condition notes from FHIR Condition.note array
    
    Returns DataFrame with columns:
    - condition_id: VARCHAR(255)
    - patient_id: VARCHAR(255)
    - meta_last_updated: TIMESTAMP
    - note_text: VARCHAR(MAX)
    - note_author_reference: VARCHAR(255)
    - note_time: TIMESTAMP
    
    Uses Spark native functions only (no UDFs).
    Avoids shuffles by filtering early and using column-based operations.
    """
    logger.info("Transforming condition notes...")
    
    if "note" not in df.columns:
        logger.warning("note column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("condition_id"),
            F.lit("").alias("patient_id"),
            F.col("meta").getField("lastUpdated").cast("timestamp").alias("meta_last_updated"),
            F.lit("").alias("note_text"),
            F.lit(None).cast("string").alias("note_author_reference"),
            F.lit(None).cast("timestamp").alias("note_time")
        ).filter(F.lit(False))
    
    # Explode the note array - single pass transformation
    # Filter nulls early to avoid unnecessary processing
    notes_df = df.select(
        F.col("id").alias("condition_id"),
        extract_patient_id(df).alias("patient_id"),
        F.col("meta").getField("lastUpdated").cast("timestamp").alias("meta_last_updated"),
        F.explode(F.col("note")).alias("note_item")
    ).filter(
        F.col("note_item").isNotNull()
    )
    
    # Extract note details using native Spark functions
    # Use timestamp parser utility for consistent timestamp handling
    note_time_expr = create_timestamp_parser(F.col("note_item.time"))
    
    notes_final = notes_df.select(
        F.col("condition_id"),
        F.col("patient_id"),
        F.col("meta_last_updated"),
        F.col("note_item.text").alias("note_text"),
        F.col("note_item.authorReference").alias("note_author_reference"),
        note_time_expr.alias("note_time")
    ).filter(
        # Filter early: keep only rows with note text and patient_id
        # This reduces data volume for subsequent operations
        F.col("note_text").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return notes_final

