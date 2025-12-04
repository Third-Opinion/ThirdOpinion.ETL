"""
Transform child tables for medication request
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)


def transform_medication_request_identifiers(df: DataFrame) -> DataFrame:
    """Transform medication request identifiers (multiple identifiers per request)"""
    logger.info("Transforming medication request identifiers...")
    
    # Check if identifier column exists
    if "identifier" not in df.columns:
        logger.warning("identifier column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("medication_request_id"),
            F.lit("").alias("identifier_system"),
            F.lit("").alias("identifier_value")
        ).filter(F.lit(False))
    
    # First explode the identifier array
    identifiers_df = df.select(
        F.col("id").alias("medication_request_id"),
        F.explode(F.col("identifier")).alias("identifier_item")
    ).filter(
        F.col("identifier_item").isNotNull()
    )
    
    # Extract identifier details
    identifiers_final = identifiers_df.select(
        F.col("medication_request_id"),
        F.col("identifier_item.system").alias("identifier_system"),
        F.col("identifier_item.value").alias("identifier_value")
    ).filter(
        F.col("identifier_value").isNotNull()
    )
    
    return identifiers_final


def transform_medication_request_notes(df: DataFrame) -> DataFrame:
    """Transform medication request notes"""
    logger.info("Transforming medication request notes...")
    
    # Check if note column exists
    if "note" not in df.columns:
        logger.warning("note column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("medication_request_id"),
            F.lit("").alias("note_text")
        ).filter(F.lit(False))
    
    # First explode the note array
    notes_df = df.select(
        F.col("id").alias("medication_request_id"),
        F.explode(F.col("note")).alias("note_item")
    ).filter(
        F.col("note_item").isNotNull()
    )
    
    # Extract note details - Simple struct with just text field
    notes_final = notes_df.select(
        F.col("medication_request_id"),
        F.col("note_item.text").alias("note_text")
    ).filter(
        F.col("note_text").isNotNull()
    )
    
    return notes_final


def transform_medication_request_dosage_instructions(df: DataFrame) -> DataFrame:
    """Transform medication request dosage instructions"""
    logger.info("Transforming medication request dosage instructions...")
    
    # Check if dosageInstruction column exists
    if "dosageInstruction" not in df.columns:
        logger.warning("dosageInstruction column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("medication_request_id"),
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
            F.lit(None).alias("dosage_dose_code"),
            F.lit(None).alias("dosage_as_needed_boolean")
        ).filter(F.lit(False))
    
    # First explode the dosageInstruction array
    dosage_df = df.select(
        F.col("id").alias("medication_request_id"),
        F.explode(F.col("dosageInstruction")).alias("dosage_item")
    ).filter(
        F.col("dosage_item").isNotNull()
    )
    
    # Extract dosage instruction details
    dosage_final = dosage_df.select(
        F.col("medication_request_id"),
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
              ).otherwise(None).alias("dosage_dose_code"),
        F.col("dosage_item.asNeededBoolean").alias("dosage_as_needed_boolean")
    ).filter(
        F.col("dosage_text").isNotNull()
    )
    
    return dosage_final


def transform_medication_request_categories(df: DataFrame) -> DataFrame:
    """Transform medication request categories"""
    logger.info("Transforming medication request categories...")
    
    # Check if category column exists
    if "category" not in df.columns:
        logger.warning("category column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("medication_request_id"),
            F.lit("").alias("category_code"),
            F.lit("").alias("category_system"),
            F.lit("").alias("category_display"),
            F.lit("").alias("category_text")
        ).filter(F.lit(False))
    
    # First explode the category array
    categories_df = df.select(
        F.col("id").alias("medication_request_id"),
        F.explode(F.col("category")).alias("category_item")
    ).filter(
        F.col("category_item").isNotNull()
    )
    
    # Extract category details and explode the coding array
    categories_final = categories_df.select(
        F.col("medication_request_id"),
        F.explode(F.col("category_item.coding")).alias("coding_item"),
        F.col("category_item.text").alias("category_text")
    ).select(
        F.col("medication_request_id"),
        F.col("coding_item.code").alias("category_code"),
        F.col("coding_item.system").alias("category_system"),
        F.col("coding_item.display").alias("category_display"),
        F.col("category_text")
    ).filter(
        F.col("category_code").isNotNull()
    )
    
    return categories_final

