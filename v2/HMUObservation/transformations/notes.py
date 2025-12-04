"""
Notes transformation for observations
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

from utils.transformation_helpers import extract_patient_id
from shared.utils.timestamp_utils import create_timestamp_parser

logger = logging.getLogger(__name__)


def transform_observation_notes(df: DataFrame) -> DataFrame:
    """Transform observation notes"""
    logger.info("Transforming observation notes...")
    
    if "note" not in df.columns:
        logger.warning("note column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("patient_id"),
            F.lit("").alias("note_text"),
            F.lit(None).alias("note_author_reference"),
            F.lit(None).cast("timestamp").alias("note_time")
        ).filter(F.lit(False))
    
    # Explode the note array
    notes_df = df.select(
        F.col("id").alias("observation_id"),
        extract_patient_id(df).alias("patient_id"),
        F.explode(F.col("note")).alias("note_item")
    ).filter(F.col("note_item").isNotNull())
    
    # Extract note details - including authorReference and time from FHIR note structure
    note_time_expr = create_timestamp_parser(F.col("note_item.time"))
    notes_final = notes_df.select(
        F.col("observation_id"),
        F.col("patient_id"),
        F.col("note_item.text").alias("note_text"),
        F.col("note_item.authorReference").alias("note_author_reference"),  # Extract from FHIR structure
        note_time_expr.alias("note_time")  # Extract from FHIR structure with timestamp parsing
    ).filter(
        F.col("note_text").isNotNull() & F.col("patient_id").isNotNull()
    )
    
    return notes_final

