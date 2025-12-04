"""
Transform main medication request data
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)


def transform_main_medication_request_data(df: DataFrame) -> DataFrame:
    """Transform the main medication request data"""
    logger.info("Transforming main medication request data...")
    
    # Build the select statement dynamically based on available columns
    select_columns = [
        F.col("id").alias("medication_request_id"),
        F.when(F.col("subject").isNotNull(), 
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        F.when(F.col("encounter").isNotNull(),
               F.regexp_extract(F.col("encounter").getField("reference"), r"Encounter/(.+)", 1)
              ).otherwise(None).alias("encounter_id"),
        F.when(F.col("medicationReference").isNotNull(),
               F.regexp_extract(F.col("medicationReference").getField("reference"), r"Medication/(.+)", 1)
              ).otherwise(None).alias("medication_id"),
        F.when(F.col("medicationReference").isNotNull(),
               F.col("medicationReference").getField("display")
              ).otherwise(None).alias("medication_display"),
        F.col("status").alias("status"),
        F.col("intent").alias("intent"),
        F.col("reportedBoolean").alias("reported_boolean"),
        # Handle authoredOn datetime with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("authoredOn"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("authoredOn"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("authoredOn"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("authoredOn"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("authoredOn"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("authored_on"),
        # Handle meta.lastUpdated datetime with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("meta_last_updated"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ]
    
    # Transform main medication request data using only available columns
    main_df = df.select(*select_columns).filter(
        F.col("medication_request_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return main_df

