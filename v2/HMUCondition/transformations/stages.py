"""
Stages transformation for conditions
Single Responsibility: Transforms condition stages (assessment only)
Note: stage.summary and stage.type are normalized to codes table via stage_tables.py
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

from utils.transformation_helpers import extract_patient_id

logger = logging.getLogger(__name__)


def transform_condition_stages(df: DataFrame) -> DataFrame:
    """
    Transform condition stages from FHIR Condition.stage array
    
    IMPORTANT: This function extracts only stage.assessment.
    stage.summary and stage.type are normalized to codes table via:
    - condition_stage_summaries (references codes.code_id)
    - condition_stage_types (references codes.code_id)
    See: transformations/stage_tables.py for summary/type extraction.
    
    Returns DataFrame with columns:
    - condition_id: VARCHAR(255)
    - patient_id: VARCHAR(255)
    - meta_last_updated: TIMESTAMP
    - stage_assessment_code: VARCHAR(255) - Can be Reference string or code
    - stage_assessment_system: VARCHAR(255) - Only if CodeableConcept
    - stage_assessment_display: VARCHAR(255) - Only if CodeableConcept
    - stage_rank: INTEGER - Order in stage array
    
    Uses Spark native functions only (no UDFs).
    Handles both Reference and CodeableConcept types for assessment.
    Avoids shuffles by filtering early and using column-based operations.
    """
    logger.info("Transforming condition stages (assessment only - summary/type normalized)...")
    
    if "stage" not in df.columns:
        logger.warning("stage column not found in data, returning empty DataFrame")
        return _create_empty_stages_df(df)
    
    # Explode the stage array with position to preserve order
    # Filter nulls early to avoid unnecessary processing
    stages_df = df.select(
        F.col("id").alias("condition_id"),
        extract_patient_id(df).alias("patient_id"),
        F.col("meta").getField("lastUpdated").cast("timestamp").alias("meta_last_updated"),
        F.posexplode(F.col("stage")).alias("stage_rank", "stage_item")
    ).filter(
        F.col("stage_item").isNotNull()
    )
    
    # Extract only stage_assessment (summary and type handled separately)
    # Handle both Reference and CodeableConcept types
    stages_final = stages_df.select(
        F.col("condition_id"),
        F.col("patient_id"),
        F.col("meta_last_updated"),
        
        # stage_assessment: Can be Reference OR CodeableConcept
        # Reference takes precedence if both are present
        F.when(
            # If it's a Reference (has reference field)
            F.col("stage_item.assessment.reference").isNotNull(),
            F.col("stage_item.assessment.reference")
        ).when(
            # If it's a CodeableConcept (has coding array)
            F.col("stage_item.assessment.coding").isNotNull() &
            (F.size(F.col("stage_item.assessment.coding")) > 0),
            F.col("stage_item.assessment.coding")[0].getField("code")
        ).otherwise(None).alias("stage_assessment_code"),
        
        # Only populate system/display if assessment is CodeableConcept (not Reference)
        F.when(
            F.col("stage_item.assessment.reference").isNull() &
            F.col("stage_item.assessment.coding").isNotNull() &
            (F.size(F.col("stage_item.assessment.coding")) > 0),
            F.col("stage_item.assessment.coding")[0].getField("system")
        ).otherwise(None).alias("stage_assessment_system"),
        
        F.when(
            F.col("stage_item.assessment.reference").isNull() &
            F.col("stage_item.assessment.coding").isNotNull() &
            (F.size(F.col("stage_item.assessment.coding")) > 0),
            F.col("stage_item.assessment.coding")[0].getField("display")
        ).otherwise(None).alias("stage_assessment_display"),
        
        F.col("stage_rank").cast("int").alias("stage_rank")
    ).filter(
        # Filter early: keep only rows with assessment populated
        # This reduces data volume for subsequent operations
        F.col("stage_assessment_code").isNotNull() &
        F.col("patient_id").isNotNull()
    )
    
    return stages_final


def _create_empty_stages_df(df: DataFrame) -> DataFrame:
    """
    Helper function to create empty stages DataFrame with correct schema
    
    This avoids code duplication and ensures consistent schema.
    """
    return df.select(
        F.col("id").alias("condition_id"),
        extract_patient_id(df).alias("patient_id"),
        F.col("meta").getField("lastUpdated").cast("timestamp").alias("meta_last_updated"),
        F.lit(None).cast("string").alias("stage_assessment_code"),
        F.lit(None).cast("string").alias("stage_assessment_system"),
        F.lit(None).cast("string").alias("stage_assessment_display"),
        F.lit(None).cast("int").alias("stage_rank")
    ).filter(F.lit(False))

