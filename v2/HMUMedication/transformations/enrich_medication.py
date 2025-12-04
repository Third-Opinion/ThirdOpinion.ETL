"""
Medication code enrichment transformation

Enriches medications with missing RxNorm codes using API lookups
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField
import logging
from typing import Optional

try:
    from shared.utils.redshift_connection_utils import get_redshift_connection_options
except ImportError:
    get_redshift_connection_options = None

logger = logging.getLogger(__name__)

# Import enrichment utilities
try:
    from utils.enrichment import enrich_medication_code, normalize_medication_name
except ImportError:
    logger.warning("Could not import enrichment utilities - enrichment will be disabled")
    enrich_medication_code = None
    normalize_medication_name = None


def enrich_medications_with_codes(
    df: DataFrame,
    enable_enrichment: bool = False,
    enrichment_mode: str = "hybrid",
    use_lookup_table: bool = True,
    glue_context=None,
    database_config=None
) -> DataFrame:
    """
    Enrich medications DataFrame with missing RxNorm codes
    
    Args:
        df: DataFrame with medication data (must have primary_code, primary_system, primary_text)
        enable_enrichment: Whether to enable enrichment
        enrichment_mode: "rxnav_only", "comprehend_only", or "hybrid"
        use_lookup_table: Whether to check Redshift lookup table
        glue_context: GlueContext for reading lookup table (optional)
        database_config: DatabaseConfig for connection (optional)
    
    Returns:
        DataFrame with enriched codes in primary_code, primary_system columns
    """
    if not enable_enrichment:
        logger.info("Medication code enrichment is disabled")
        return df
    
    if enrich_medication_code is None:
        logger.warning("Enrichment utilities not available - skipping enrichment")
        return df
    
    logger.info("Starting medication code enrichment...")
    
    # Ensure lookup table exists if we're using it
    lookup_cache = None
    if use_lookup_table and glue_context and database_config:
        try:
            # Import lookup table utilities
            from utils.lookup_table import ensure_lookup_table_exists
            from awsglue.context import GlueContext as GC
            
            # Ensure table exists
            spark_session = glue_context.spark_session if hasattr(glue_context, 'spark_session') else None
            if spark_session:
                ensure_lookup_table_exists(glue_context, database_config, spark_session)
            
            # Read lookup table
            logger.info("Reading medication code lookup table from Redshift...")
            if get_redshift_connection_options:
                connection_options = get_redshift_connection_options(
                    connection_name=database_config.redshift_connection,
                    database=database_config.redshift_database,
                    s3_temp_dir=database_config.s3_temp_dir,
                    dbtable="public.medication_code_lookup"
                )
            else:
                connection_options = {
                    "redshiftTmpDir": database_config.s3_temp_dir,
                    "useConnectionProperties": "true",
                    "dbtable": "public.medication_code_lookup",
                    "connectionName": database_config.redshift_connection,
                }
            
            lookup_df = glue_context.create_dynamic_frame.from_options(
                connection_type="redshift",
                connection_options=connection_options,
                transformation_ctx="read_lookup_table"
            ).toDF()
            
            # Convert lookup table to dictionary cache
            lookup_cache = {}
            lookup_rows = lookup_df.collect()
            for row in lookup_rows:
                normalized = row.get('normalized_name')
                if normalized:
                    lookup_cache[normalized] = {
                        'rxnorm_code': row.get('rxnorm_code'),
                        'rxnorm_system': row.get('rxnorm_system'),
                        'display_name': row.get('medication_name'),
                        'confidence': float(row.get('confidence_score', 1.0)) if row.get('confidence_score') else 1.0
                    }
            logger.info(f"Loaded {len(lookup_cache)} entries from lookup table")
        except Exception as e:
            logger.warning(f"Could not read lookup table: {str(e)} - continuing without lookup cache")
    
    # Identify medications that need enrichment (missing codes)
    needs_enrichment_filter = (
        (F.col("primary_code").isNull()) |
        (F.col("primary_system").isNull()) |
        (F.col("primary_system") != "http://www.nlm.nih.gov/research/umls/rxnorm")
    )
    
    enrichment_count = df.filter(needs_enrichment_filter).count()
    logger.info(f"Found {enrichment_count} medications that may need code enrichment")
    
    if enrichment_count == 0:
        logger.info("No medications need enrichment")
        return df
    
    logger.info(f"Enrichment mode: {enrichment_mode}")
    logger.info(f"Use lookup table: {use_lookup_table}")
    
    # Get Spark session for creating DataFrames
    spark_session = None
    if glue_context and hasattr(glue_context, 'spark_session'):
        spark_session = glue_context.spark_session
    elif hasattr(df, 'sparkSession'):
        spark_session = df.sparkSession
    
    # Step 1: Enrich from lookup cache (fast, can be done in Spark)
    enriched_df = df
    if lookup_cache and len(lookup_cache) > 0:
        logger.info(f"Enriching medications from lookup cache ({len(lookup_cache)} entries)...")
        
        if spark_session:
            try:
                # Create a DataFrame from lookup cache for broadcasting
                lookup_data = []
                for normalized_name, cache_data in lookup_cache.items():
                    if cache_data.get('rxnorm_code'):
                        lookup_data.append((
                            normalized_name,
                            cache_data.get('rxnorm_code'),
                            cache_data.get('rxnorm_system') or "http://www.nlm.nih.gov/research/umls/rxnorm",
                            cache_data.get('display_name'),
                            cache_data.get('confidence', 1.0)
                        ))
                
                if lookup_data:
                    lookup_schema = StructType([
                        StructField("lookup_normalized_name", StringType(), True),
                        StructField("lookup_rxnorm_code", StringType(), True),
                        StructField("lookup_rxnorm_system", StringType(), True),
                        StructField("lookup_display_name", StringType(), True),
                        StructField("lookup_confidence", StringType(), True)
                    ])
                    
                    lookup_df = spark_session.createDataFrame(lookup_data, schema=lookup_schema)
                    
                    # Normalize medication names for join (simple upper case and trim for now)
                    # For exact matching, we'll use upper case and trim
                    # More complex normalization would require UDF which can be slower
                    enriched_df = df.withColumn(
                        "_normalized_name",
                        F.upper(F.trim(F.coalesce(F.col("primary_text"), F.lit(""))))
                    )
                    
                    # Also normalize lookup table keys (they should already be normalized, but ensure consistency)
                    lookup_df = lookup_df.withColumn(
                        "_lookup_normalized",
                        F.upper(F.trim(F.coalesce(F.col("lookup_normalized_name"), F.lit(""))))
                    )
                    
                    # Join with lookup table to get enriched codes (broadcast for efficiency)
                    enriched_df = enriched_df.join(
                        F.broadcast(lookup_df),
                        enriched_df["_normalized_name"] == lookup_df["_lookup_normalized"],
                        "left"
                    )
                    
                    # Update primary_code, primary_system, primary_text where enrichment found
                    # Only update if medication needs enrichment AND we found a match
                    enriched_df = enriched_df.withColumn(
                        "primary_code",
                        F.when(
                            (F.col("lookup_rxnorm_code").isNotNull()) &
                            needs_enrichment_filter,
                            F.col("lookup_rxnorm_code")
                        ).otherwise(F.col("primary_code"))
                    ).withColumn(
                        "primary_system",
                        F.when(
                            (F.col("lookup_rxnorm_code").isNotNull()) &
                            needs_enrichment_filter,
                            F.col("lookup_rxnorm_system")
                        ).otherwise(F.col("primary_system"))
                    ).withColumn(
                        "primary_text",
                        F.when(
                            (F.col("lookup_display_name").isNotNull()) &
                            (F.col("lookup_rxnorm_code").isNotNull()) &
                            needs_enrichment_filter,
                            F.col("lookup_display_name")
                        ).otherwise(F.col("primary_text"))
                    )
                    
                    # Drop temporary columns
                    enriched_df = enriched_df.drop(
                        "_normalized_name",
                        "_lookup_normalized",
                        "lookup_normalized_name",
                        "lookup_rxnorm_code",
                        "lookup_rxnorm_system",
                        "lookup_display_name",
                        "lookup_confidence"
                    )
                    
                    # Count how many were enriched from cache
                    before_count = df.filter(
                        ~((F.col("primary_code").isNull()) |
                          (F.col("primary_system").isNull()) |
                          (F.col("primary_system") != "http://www.nlm.nih.gov/research/umls/rxnorm"))
                    ).count()
                    
                    after_count = enriched_df.filter(
                        ~((F.col("primary_code").isNull()) |
                          (F.col("primary_system").isNull()) |
                          (F.col("primary_system") != "http://www.nlm.nih.gov/research/umls/rxnorm"))
                    ).count()
                    
                    cache_enriched_count = after_count - before_count
                    logger.info(f"✅ Enriched {cache_enriched_count} medications from lookup cache")
                    
            except Exception as e:
                logger.warning(f"Error enriching from lookup cache: {str(e)} - continuing without cache enrichment")
                enriched_df = df
    
    # Step 2: Log medications that still need enrichment (API enrichment disabled - use local script instead)
    still_needs_enrichment = enriched_df.filter(
        (F.col("primary_code").isNull()) |
        (F.col("primary_system").isNull()) |
        (F.col("primary_system") != "http://www.nlm.nih.gov/research/umls/rxnorm")
    )
    
    still_needs_count = still_needs_enrichment.count()
    if still_needs_count > 0:
        logger.info(f"ℹ️  {still_needs_count} medications still need enrichment")
        logger.info(f"   Note: API enrichment is disabled. Use local script (enrich_medications_locally.py)")
        logger.info(f"   to populate the lookup table, then re-run this ETL job")
    
    return enriched_df


# NOTE: API enrichment removed - use enrich_medications_locally.py for API enrichment
# Only lookup table enrichment is performed in the Glue job

