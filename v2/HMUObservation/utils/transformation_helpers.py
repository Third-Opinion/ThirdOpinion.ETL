"""
Transformation helper functions for observations
Shared utilities used by transformation modules
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def extract_patient_id(df: DataFrame) -> F.Column:
    """
    Helper to extract patient ID from subject reference
    
    Args:
        df: DataFrame with 'subject' column containing FHIR reference
        
    Returns:
        Spark Column expression for patient_id
    """
    return F.when(
        F.col("subject").isNotNull(),
        F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
    ).otherwise(None)
