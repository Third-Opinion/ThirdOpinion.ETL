"""
Transform main medication dispense data
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)


def transform_main_medication_dispense_data(df: DataFrame) -> DataFrame:
    """Transform the main medication dispense data"""
    logger.info("Transforming main medication dispense data...")
    
    # Build the select statement dynamically based on available columns
    select_columns = [
        F.col("id").alias("medication_dispense_id"),
        F.col("resourceType").alias("resource_type"),
        F.col("status").alias("status"),
        # Extract patient ID from subject.reference
        F.when(F.col("subject").isNotNull(), 
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        # Extract medication ID and display from medicationReference
        F.when(F.col("medicationReference").isNotNull(),
               F.regexp_extract(F.col("medicationReference").getField("reference"), r"Medication/(.+)", 1)
              ).otherwise(None).alias("medication_id"),
        F.when(F.col("medicationReference").isNotNull(),
               F.col("medicationReference").getField("display")
              ).otherwise(None).alias("medication_display"),
        # Extract from the 'type' field
        F.when(F.col("type.coding").isNotNull() & (F.size(F.col("type.coding")) > 0),
               F.col("type.coding")[0].getField("system")
              ).otherwise(None).alias("type_system"),
        F.when(F.col("type.coding").isNotNull() & (F.size(F.col("type.coding")) > 0),
               F.col("type.coding")[0].getField("code")
              ).otherwise(None).alias("type_code"),
        F.when(F.col("type.coding").isNotNull() & (F.size(F.col("type.coding")) > 0),
               F.col("type.coding")[0].getField("display")
              ).otherwise(None).alias("type_display"),
        # Extract quantity value, handling nested struct
        F.when(F.col("quantity").isNotNull(),
               F.col("quantity").getField("value")
              ).otherwise(None).alias("quantity_value"),
        # Convert whenHandedOver to timestamp
        # Handle whenHandedOver with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("whenHandedOver"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("whenHandedOver"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("whenHandedOver"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("whenHandedOver"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("whenHandedOver"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("when_handed_over"),
        # Meta fields
        # Handle meta.lastUpdated with multiple possible formats
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
    
    # Transform main medication dispense data using only available columns
    main_df = df.select(*select_columns).filter(
        F.col("medication_dispense_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return main_df

