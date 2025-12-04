"""
Transform child tables for medication dispense
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import logging

logger = logging.getLogger(__name__)


def transform_medication_dispense_identifiers(df: DataFrame) -> DataFrame:
    """Transform medication dispense identifiers (multiple identifiers per dispense)"""
    logger.info("Transforming medication dispense identifiers...")
    
    # Check if identifier column exists
    if "identifier" not in df.columns:
        logger.warning("identifier column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("medication_dispense_id"),
            F.lit(None).cast("timestamp").alias("meta_last_updated"),
            F.lit(None).cast(StringType()).alias("identifier_system"),
            F.lit("").alias("identifier_value")
        ).filter(F.lit(False))
    
    # First explode the identifier array
    identifiers_df = df.select(
        F.col("id").alias("medication_dispense_id"),
        F.coalesce(
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("meta_last_updated"),
        F.explode(F.col("identifier")).alias("identifier_item")
    ).filter(
        F.col("identifier_item").isNotNull()
    )
    
    # Extract identifier details
    identifiers_final = identifiers_df.select(
        F.col("medication_dispense_id"),
        F.col("meta_last_updated"),
        F.lit(None).cast(StringType()).alias("identifier_system"),
        F.col("identifier_item.value").alias("identifier_value")
    ).filter(
        F.col("identifier_value").isNotNull()
    )
    
    return identifiers_final


def transform_medication_dispense_performers(df: DataFrame) -> DataFrame:
    """Transform medication dispense performers (who dispensed the medication)"""
    logger.info("Transforming medication dispense performers...")
    
    # Check if performer column exists
    if "performer" not in df.columns:
        logger.warning("performer column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("medication_dispense_id"),
            F.lit(None).cast("timestamp").alias("meta_last_updated"),
            F.lit("").alias("performer_actor_reference")
        ).filter(F.lit(False))
    
    # First explode the performer array
    performers_df = df.select(
        F.col("id").alias("medication_dispense_id"),
        F.coalesce(
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("meta_last_updated"),
        F.explode(F.col("performer")).alias("performer_item")
    ).filter(
        F.col("performer_item").isNotNull()
    )
    
    # Extract performer details
    performers_final = performers_df.select(
        F.col("medication_dispense_id"),
        F.col("meta_last_updated"),
        F.when(F.col("performer_item.actor").isNotNull(),
               F.col("performer_item.actor.reference")
              ).otherwise(None).alias("performer_actor_reference")
    ).filter(
        F.col("performer_actor_reference").isNotNull()
    )
    
    return performers_final


def transform_medication_dispense_auth_prescriptions(df: DataFrame) -> DataFrame:
    """Transform medication dispense authorizing prescriptions"""
    logger.info("Transforming medication dispense authorizing prescriptions...")
    
    # Check if authorizingPrescription column exists
    if "authorizingPrescription" not in df.columns:
        logger.warning("authorizingPrescription column not found, returning empty DataFrame")
        return df.select(
            F.col("id").alias("medication_dispense_id"),
            F.lit(None).cast("timestamp").alias("meta_last_updated"),
            F.lit(None).cast(StringType()).alias("authorizing_prescription_id")
        ).filter(F.lit(False))
        
    # Explode the array and extract the reference ID
    auth_prescriptions_df = df.select(
        F.col("id").alias("medication_dispense_id"),
        F.coalesce(
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("meta_last_updated"),
        F.explode_outer(F.col("authorizingPrescription")).alias("prescription_item")
    ).select(
        F.col("medication_dispense_id"),
        F.col("meta_last_updated"),
        F.when(F.col("prescription_item").isNotNull(),
               F.regexp_extract(F.col("prescription_item.reference"), r"MedicationRequest/(.+)", 1)
              ).otherwise(None).alias("authorizing_prescription_id")
    ).filter(
        F.col("authorizing_prescription_id").isNotNull()
    )

    return auth_prescriptions_df


def transform_medication_dispense_dosage_instructions(df: DataFrame) -> DataFrame:
    """Transform medication dispense dosage instructions"""
    logger.info("Transforming medication dispense dosage instructions...")
    
    # Check if dosageInstruction column exists
    if "dosageInstruction" not in df.columns:
        logger.warning("dosageInstruction column not found, returning empty DataFrame")
        return df.select(
            F.col("id").alias("medication_dispense_id"),
            F.lit(None).cast("timestamp").alias("meta_last_updated"),
            F.lit("").alias("dosage_text"),
            F.lit(None).alias("dosage_timing_frequency"),
            F.lit(None).alias("dosage_timing_period"),
            F.lit(None).alias("dosage_timing_period_unit"),
            F.lit(None).alias("dosage_route_code"),
            F.lit(None).alias("dosage_route_system"),
            F.lit(None).alias("dosage_route_display"),
            F.lit(None).alias("dosage_dose_value"),
            F.lit(None).alias("dosage_dose_unit"),
            F.lit(None).alias("dosage_dose_system"),
            F.lit(None).alias("dosage_dose_code")
        ).filter(F.lit(False))
    
    # First explode the dosageInstruction array
    dosage_df = df.select(
        F.col("id").alias("medication_dispense_id"),
        F.coalesce(
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("meta.lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("meta_last_updated"),
        F.explode(F.col("dosageInstruction")).alias("dosage_item")
    ).filter(
        F.col("dosage_item").isNotNull()
    )
    
    # Extract dosage instruction details
    dosage_final = dosage_df.select(
        F.col("medication_dispense_id"),
        F.col("meta_last_updated"),
        F.col("dosage_item.text").alias("dosage_text"),
        F.col("dosage_item.timing.repeat.frequency").alias("dosage_timing_frequency"),
        F.col("dosage_item.timing.repeat.period").alias("dosage_timing_period"),
        F.col("dosage_item.timing.repeat.periodUnit").alias("dosage_timing_period_unit"),
        F.when(F.col("dosage_item.route").isNotNull() & (F.size(F.col("dosage_item.route.coding")) > 0),
               F.col("dosage_item.route.coding")[0].getField("code")
              ).otherwise(None).alias("dosage_route_code"),
        F.when(F.col("dosage_item.route").isNotNull() & (F.size(F.col("dosage_item.route.coding")) > 0),
               F.col("dosage_item.route.coding")[0].getField("system")
              ).otherwise(None).alias("dosage_route_system"),
        F.when(F.col("dosage_item.route").isNotNull() & (F.size(F.col("dosage_item.route.coding")) > 0),
               F.col("dosage_item.route.coding")[0].getField("display")
              ).otherwise(None).alias("dosage_route_display"),
        F.when(F.col("dosage_item.doseAndRate").isNotNull() & (F.size(F.col("dosage_item.doseAndRate")) > 0),
               F.col("dosage_item.doseAndRate")[0].getField("doseQuantity").getField("value")
              ).otherwise(None).alias("dosage_dose_value"),
        F.when(F.col("dosage_item.doseAndRate").isNotNull() & (F.size(F.col("dosage_item.doseAndRate")) > 0),
               F.col("dosage_item.doseAndRate")[0].getField("doseQuantity").getField("unit")
              ).otherwise(None).alias("dosage_dose_unit"),
        F.when(F.col("dosage_item.doseAndRate").isNotNull() & (F.size(F.col("dosage_item.doseAndRate")) > 0),
               F.col("dosage_item.doseAndRate")[0].getField("doseQuantity").getField("system")
              ).otherwise(None).alias("dosage_dose_system"),
        F.when(F.col("dosage_item.doseAndRate").isNotNull() & (F.size(F.col("dosage_item.doseAndRate")) > 0),
               F.col("dosage_item.doseAndRate")[0].getField("doseQuantity").getField("code")
              ).otherwise(None).alias("dosage_dose_code")
    ).filter(
        F.col("dosage_text").isNotNull() | F.col("dosage_dose_value").isNotNull()
    )
    
    return dosage_final

