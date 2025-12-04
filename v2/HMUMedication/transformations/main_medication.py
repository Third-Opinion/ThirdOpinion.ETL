"""
Transform main medication data
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import json
import logging

logger = logging.getLogger(__name__)


def convert_to_json_string(field):
    """Convert complex data to JSON strings to avoid nested structures"""
    if field is None:
        return None
    try:
        if isinstance(field, str):
            return field
        else:
            return json.dumps(field, ensure_ascii=False, default=str)
    except (TypeError, ValueError) as e:
        logger.warning(f"JSON serialization failed for field: {str(e)}")
        return str(field)


# Define UDF globally so it can be used in all transformation functions
convert_to_json_udf = F.udf(convert_to_json_string, StringType())


def transform_main_medication_data(df: DataFrame) -> DataFrame:
    """Transform the main medication data"""
    logger.info("Transforming main medication data...")
    
    # Log available columns for debugging
    available_columns = df.columns
    logger.info(f"Available columns: {available_columns}")
    
    # Build the select statement for main medication data
    select_columns = [
        F.col("id").alias("medication_id"),
        F.col("resourceType").alias("resource_type"),

        # Extract code as JSON string (SUPER column) - handle case where coding is null
        F.when(F.col("code").isNotNull() &
               F.col("code").getField("coding").isNotNull() &
               (F.size(F.col("code").getField("coding")) > 0),
               F.to_json(F.col("code").getField("coding"))
              ).otherwise(F.lit("null")).alias("code"),

        # Extract primary code fields from first element in coding array - handle null coding
        F.when(F.col("code").isNotNull() &
               F.col("code").getField("coding").isNotNull() &
               (F.size(F.col("code").getField("coding")) > 0),
               F.col("code").getField("coding")[0].getField("code")
              ).otherwise(None).alias("primary_code"),

        F.when(F.col("code").isNotNull() &
               F.col("code").getField("coding").isNotNull() &
               (F.size(F.col("code").getField("coding")) > 0),
               F.col("code").getField("coding")[0].getField("system")
              ).otherwise(None).alias("primary_system"),

        F.when(F.col("code").isNotNull(),
               F.col("code").getField("text")
              ).otherwise(None).alias("primary_text"),

        F.col("status").alias("status"),
        F.when(F.col("meta").isNotNull(),
               # Handle meta.lastUpdated with multiple possible formats
               F.coalesce(
                   F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
                   F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'"),
                   F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                   F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
                   F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                   F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss")
               )
              ).otherwise(None).alias("meta_last_updated"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ]
    
    # Transform main medication data
    main_df = df.select(*select_columns).filter(
        F.col("medication_id").isNotNull()
    )
    
    return main_df


def transform_and_enrich_medication_data(
    df: DataFrame,
    enable_enrichment: bool = False,
    enrichment_mode: str = "hybrid",
    use_lookup_table: bool = True,
    glue_context=None,
    database_config=None
) -> DataFrame:
    """
    Transform medication data and optionally enrich missing codes
    
    Args:
        df: Raw medication DataFrame from Iceberg
        enable_enrichment: Whether to enrich missing codes
        enrichment_mode: Enrichment mode ("rxnav_only", "comprehend_only", "hybrid")
        use_lookup_table: Whether to use lookup table
        glue_context: GlueContext for lookup table access
        database_config: DatabaseConfig for connections
    
    Returns:
        Transformed (and optionally enriched) medication DataFrame
    """
    # First, do the standard transformation
    main_df = transform_main_medication_data(df)
    
    # Then, optionally enrich missing codes
    if enable_enrichment:
        logger.info("Enriching medications with missing codes...")
        try:
            # Use absolute import (relative imports may fail in Glue)
            from transformations.enrich_medication import enrich_medications_with_codes
            main_df = enrich_medications_with_codes(
                main_df,
                enable_enrichment=enable_enrichment,
                enrichment_mode=enrichment_mode,
                use_lookup_table=use_lookup_table,
                glue_context=glue_context,
                database_config=database_config
            )
        except ImportError as e:
            logger.warning(f"Could not import enrichment module: {e} - skipping enrichment")
        except Exception as e:
            import traceback
            logger.warning(f"Enrichment failed: {type(e).__name__}: {e}")
            logger.debug(f"Enrichment error traceback: {traceback.format_exc()}")
            logger.warning("Continuing without enrichment")
    
    return main_df
