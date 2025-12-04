"""
Code transformation functions for normalized codes table
Redshift-compatible implementation using hash-based code_id
OPTIMIZED: Uses native Spark functions instead of UDFs, reuses existing IDs from Redshift
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
import logging
import hashlib
from typing import Optional, Tuple
from awsglue.context import GlueContext

from shared.config import ProcessingConfig, DatabaseConfig
from shared.utils.redshift_connection_utils import get_redshift_connection_options

logger = logging.getLogger(__name__)


def generate_code_id_native(code_code_col, code_system_col):
    """
    Generate deterministic code_id using native Spark functions (no UDF)
    
    Uses MD5 hash of "system|code" for determinism, then converts to BIGINT using hash().
    This ensures we NEVER return NULL since hash() always returns a non-NULL integer.
    
    Args:
        code_code_col: Spark column for code_code
        code_system_col: Spark column for code_system
        
    Returns:
        Spark column expression for code_id (BIGINT) - NEVER returns NULL
    """
    # Clean inputs - ensure no NULL values
    code_code_clean = F.trim(F.coalesce(code_code_col, F.lit("")))
    code_system_clean = F.trim(F.coalesce(code_system_col, F.lit("")))
    
    # Create deterministic hash key: system|code (consistent ordering, pipe separator)
    # Always create a non-empty key to ensure MD5 works
    hash_key = F.when(
        (code_code_clean != F.lit("")) & (code_system_clean != F.lit("")),
        F.concat(code_system_clean, F.lit("|"), code_code_clean)
    ).otherwise(
        # Fallback for invalid codes - ensures we always have a key
        F.concat(F.lit("INVALID|"), F.coalesce(code_code_clean, F.lit("NULL")), F.lit("|"), F.coalesce(code_system_clean, F.lit("NULL")))
    )
    
    # Generate MD5 hash for determinism (returns hex string, always non-NULL)
    md5_hash = F.md5(hash_key)
    
    # Use hash() directly on the MD5 hex string - hash() ALWAYS returns non-NULL integer
    # This is more reliable than conv() which can return NULL
    # hash() returns INT, convert to BIGINT and ensure positive with abs() and modulo
    max_bigint_value = 9223372036854775807  # 2^63 - 1
    
    # Primary method: hash the MD5 hex string (deterministic and always non-NULL)
    code_id = (F.abs(F.hash(md5_hash)).cast("bigint") % F.lit(max_bigint_value))
    
    # Ensure result is always positive and non-NULL
    # hash() never returns NULL, so this should always work
    code_id = F.coalesce(code_id, F.lit(1))
    
    return code_id


def load_existing_code_ids(
    glue_context: GlueContext,
    config: DatabaseConfig,
    processing_config: ProcessingConfig
) -> Optional[DataFrame]:
    """
    Load existing code_ids from Redshift codes table for reuse
    
    Args:
        glue_context: AWS Glue context
        config: Database configuration
        processing_config: Processing configuration
        
    Returns:
        DataFrame with columns: code_id, code_code, code_system, or None if table doesn't exist or is empty
    """
    if processing_config.test_mode:
        logger.info("⚠️  TEST MODE: Skipping load of existing code_ids")
        return None
    
    try:
        logger.info("Loading existing code_ids from Redshift codes table...")
        
        # Read existing code_ids from Redshift using Glue connection
        existing_codes_dynamic_frame = glue_context.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options=get_redshift_connection_options(
                connection_name=config.redshift_connection,
                database=config.redshift_database,
                s3_temp_dir=config.s3_temp_dir,
                dbtable="public.codes",
                query="SELECT code_id, code_code, code_system FROM public.codes"
            ),
            transformation_ctx="read_existing_code_ids"
        )
        
        existing_codes_df = existing_codes_dynamic_frame.toDF()
        
        # Check if DataFrame is empty
        if existing_codes_df.rdd.isEmpty():
            logger.info("No existing code_ids found in Redshift (table is empty or doesn't exist)")
            return None
        
        logger.info(f"✅ Loaded existing code_ids from Redshift (count skipped to avoid action)")
        return existing_codes_df.select(
            F.col("code_id").cast("bigint").alias("code_id"),
            F.trim(F.col("code_code")).alias("code_code"),
            F.trim(F.col("code_system")).alias("code_system")
        )
        
    except Exception as e:
        logger.warning(f"Could not load existing code_ids from Redshift: {str(e)}")
        logger.warning("Will generate all code_ids (this is fine for initial load)")
        return None


def generate_code_id(code_code: str, code_system: str) -> int:
    """
    Generate deterministic code_id from code_code + code_system
    Uses MD5 hash converted to 64-bit integer (first 8 bytes)
    
    This function is deterministic: same code_code + code_system always produces same code_id
    
    Args:
        code_code: The code value (e.g., "12345-6")
        code_system: The code system (e.g., "http://loinc.org")
        
    Returns:
        Deterministic BIGINT code_id (always positive)
    """
    if not code_code or not code_system:
        return None
    
    # Normalize inputs: strip whitespace, convert to string
    code_code = str(code_code).strip() if code_code else ""
    code_system = str(code_system).strip() if code_system else ""
    
    if not code_code or not code_system:
        return None
    
    # Create hash key: system|code (consistent ordering, pipe separator avoids collisions)
    hash_key = f"{code_system}|{code_code}"
    
    # Generate MD5 hash and take first 8 bytes
    hash_bytes = hashlib.md5(hash_key.encode('utf-8')).digest()[:8]
    
    # Convert to unsigned 64-bit integer, then ensure it fits in signed BIGINT range
    # Redshift BIGINT is signed (-2^63 to 2^63-1), but we want positive IDs
    code_id = int.from_bytes(hash_bytes, byteorder='big', signed=False)
    
    # Ensure it's within signed BIGINT positive range (0 to 2^63-1)
    # Use modulo to wrap if needed (very unlikely with MD5)
    max_bigint = 0x7FFFFFFFFFFFFFFF  # 2^63 - 1
    code_id = code_id % (max_bigint + 1)
    
    return code_id


def transform_unique_codes(
    codes_df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None,
    glue_context: Optional[GlueContext] = None,
    config: Optional[DatabaseConfig] = None,
    is_initial_load: bool = False
) -> DataFrame:
    """
    Extract unique codes from observation codes DataFrame
    Creates codes table data with deterministic code_id
    
    Args:
        codes_df: DataFrame with code_code, code_system, code_display, code_text, normalized_code_text
        processing_config: Processing configuration
        
    Returns:
        DataFrame with unique codes: code_id, code_code, code_system, code_display, 
        code_text (truncated to 1000), normalized_code_text (truncated to 500)
    """
    logger.info("Extracting unique codes for codes table...")
    
    # Lightweight check: use limit(1) instead of rdd.isEmpty() to avoid full scan
    if codes_df is None:
        logger.warning("No codes data provided, returning empty DataFrame")
        return codes_df.sparkSession.createDataFrame([], StructType([
            StructField("code_id", LongType(), False),
            StructField("code_code", StringType(), False),
            StructField("code_system", StringType(), False),
            StructField("code_display", StringType(), True),
            StructField("code_text", StringType(), True),
            StructField("normalized_code_text", StringType(), True)
        ]))
    
    # OPTIMIZED: Use hybrid approach - load existing IDs and reuse, generate new ones with native Spark
    if "code_id" in codes_df.columns:
        logger.info("code_id already exists in codes_df - reusing (optimization)")
        codes_with_id = codes_df.filter(F.col("code_id").isNotNull())
    else:
        # Step 1: Load existing code_ids from Redshift (if not initial load)
        existing_codes_df = None
        if not is_initial_load and glue_context and config and processing_config:
            existing_codes_df = load_existing_code_ids(glue_context, config, processing_config)
        
        if existing_codes_df is not None and not existing_codes_df.rdd.isEmpty():
            # Step 2: Broadcast existing code_ids for efficient join
            logger.info("Broadcasting existing code_ids for reuse...")
            existing_codes_broadcast = F.broadcast(existing_codes_df)
            
            # Step 3: Join to reuse existing code_ids
            # Alias DataFrames to avoid ambiguous column references
            codes_df_alias = codes_df.alias("codes")
            existing_codes_broadcast_alias = existing_codes_broadcast.alias("existing")
            
            codes_with_existing_ids = codes_df_alias.join(
                existing_codes_broadcast_alias,
                (F.trim(F.coalesce(F.col("codes.code_code"), F.lit(""))) == F.trim(F.coalesce(F.col("existing.code_code"), F.lit("")))) &
                (F.trim(F.coalesce(F.col("codes.code_system"), F.lit(""))) == F.trim(F.coalesce(F.col("existing.code_system"), F.lit("")))),
                "left"
            )
            
            # Step 4: Generate new code_ids only for codes that don't exist (using native Spark)
            logger.info("Generating new code_ids for codes not in Redshift (using native Spark MD5)...")
            codes_with_id = codes_with_existing_ids.select(
                F.col("codes.*"),  # All columns from codes_df
                F.col("existing.code_id").alias("existing_code_id")  # Existing code_id from Redshift
            ).filter(
                # Only process records with valid code_code and code_system
                F.col("code_code").isNotNull() & 
                (F.trim(F.coalesce(F.col("code_code"), F.lit(""))) != F.lit("")) &
                F.col("code_system").isNotNull() &
                (F.trim(F.coalesce(F.col("code_system"), F.lit(""))) != F.lit(""))
            ).withColumn(
                "code_id",
                F.coalesce(
                    F.col("existing_code_id"),  # Reuse existing
                    generate_code_id_native(  # Generate new (native Spark)
                        F.col("code_code"),
                        F.col("code_system")
                    )
                )
            ).filter(F.col("code_id").isNotNull())
            
            logger.info("✅ Reused existing code_ids and generated new ones (native Spark, no UDF)")
        else:
            # No existing codes or initial load - generate all code_ids using native Spark
            logger.info("Generating all code_ids using native Spark MD5 (no UDF)...")
            codes_with_id = codes_df.filter(
                # Only process records with valid code_code and code_system
                F.col("code_code").isNotNull() & 
                (F.trim(F.coalesce(F.col("code_code"), F.lit(""))) != F.lit("")) &
                F.col("code_system").isNotNull() &
                (F.trim(F.coalesce(F.col("code_system"), F.lit(""))) != F.lit(""))
            ).withColumn(
                "code_id",
                generate_code_id_native(
                    F.col("code_code"),
                    F.col("code_system")
                )
            ).filter(F.col("code_id").isNotNull())
            
            logger.info("✅ Generated all code_ids using native Spark (no UDF)")
    
    # Truncate code_text to 1000 chars (was 65535)
    # Clean code_text: remove tabs/newlines, take first part
    codes_cleaned = codes_with_id.withColumn(
        "code_text_cleaned",
        F.when(
            F.col("code_text").isNotNull(),
            # Remove tabs and newlines, take first 1000 chars
            F.substring(
                F.regexp_replace(
                    F.regexp_replace(F.col("code_text"), "\t", " "),
                    "\n", " "
                ),
                1, 1000
            )
        ).otherwise(None)
    ).withColumn(
        "normalized_code_text_truncated",
        F.when(
            F.col("normalized_code_text").isNotNull(),
            F.substring(F.col("normalized_code_text"), 1, 500)
        ).otherwise(None)
    ).withColumn(
        "code_display_truncated",
        F.when(
            F.col("code_display").isNotNull(),
            F.substring(F.col("code_display"), 1, 2000)
        ).otherwise(None)
    )
    
    # Final validation: Ensure no NULL code_ids before grouping
    codes_cleaned = codes_cleaned.filter(
        F.col("code_id").isNotNull() &
        F.col("code_code").isNotNull() &
        (F.trim(F.coalesce(F.col("code_code"), F.lit(""))) != F.lit("")) &
        F.col("code_system").isNotNull() &
        (F.trim(F.coalesce(F.col("code_system"), F.lit(""))) != F.lit(""))
    )
    
    # Select unique codes (same code_code + code_system = same code_id)
    # For duplicates, take the first non-null values for display/text fields
    unique_codes = codes_cleaned.select(
        F.col("code_id"),
        F.col("code_code"),
        F.col("code_system"),
        F.col("code_display_truncated").alias("code_display"),
        F.col("code_text_cleaned").alias("code_text"),
        F.col("normalized_code_text_truncated").alias("normalized_code_text")
    ).groupBy("code_id", "code_code", "code_system").agg(
        F.first(F.col("code_display"), ignorenulls=True).alias("code_display"),
        F.first(F.col("code_text"), ignorenulls=True).alias("code_text"),
        F.first(F.col("normalized_code_text"), ignorenulls=True).alias("normalized_code_text")
    ).select(
        F.col("code_id"),
        F.col("code_code"),
        F.col("code_system"),
        F.col("code_display"),
        F.col("code_text"),
        F.col("normalized_code_text")
    ).filter(
        # Final safety check - ensure code_id is still not NULL after grouping
        F.col("code_id").isNotNull()
    )
    
    logger.info("✅ Extracted unique codes (count skipped to avoid shuffle failures)")
    
    return unique_codes


def transform_observation_codes_with_code_id(
    codes_df: DataFrame,
    processing_config: Optional[ProcessingConfig] = None,
    glue_context: Optional[GlueContext] = None,
    config: Optional[DatabaseConfig] = None,
    is_initial_load: bool = False
) -> DataFrame:
    """
    Transform observation codes to use code_id references
    
    Args:
        codes_df: DataFrame with observation_id, patient_id, code_code, code_system, 
                  code_display, code_text, normalized_code_text
        processing_config: Processing configuration
        
    Returns:
        DataFrame with observation_id, patient_id, code_id, code_rank
    """
    logger.info("Transforming observation codes to use code_id references...")
    
    # Lightweight check: use limit(1) instead of rdd.isEmpty() to avoid full scan
    if codes_df is None:
        logger.warning("No codes data provided, returning empty DataFrame")
        return codes_df.sparkSession.createDataFrame([], StructType([
            StructField("observation_id", StringType(), False),
            StructField("patient_id", StringType(), False),
            StructField("code_id", LongType(), False),
            StructField("code_rank", IntegerType(), True)
        ]))
    
    # Lightweight check: only process 1 row to see if DataFrame is empty
    sample_check = codes_df.limit(1).collect()
    if len(sample_check) == 0:
        logger.warning("No codes data provided, returning empty DataFrame")
    
    # OPTIMIZED: Use hybrid approach - load existing IDs and reuse, generate new ones with native Spark
    if "code_id" in codes_df.columns:
        logger.info("code_id already exists in codes_df - reusing (optimization)")
        observation_codes_normalized = codes_df.filter(
            F.col("code_id").isNotNull() & 
            F.col("observation_id").isNotNull() & 
            F.col("patient_id").isNotNull()
        )
    else:
        # Step 1: Load existing code_ids from Redshift (if not initial load)
        existing_codes_df = None
        if not is_initial_load and glue_context and config and processing_config:
            existing_codes_df = load_existing_code_ids(glue_context, config, processing_config)
        
        if existing_codes_df is not None and not existing_codes_df.rdd.isEmpty():
            # Step 2: Broadcast existing code_ids for efficient join
            logger.info("Broadcasting existing code_ids for reuse in observation_codes...")
            existing_codes_broadcast = F.broadcast(existing_codes_df)
            
            # Step 3: Join to reuse existing code_ids
            # Alias DataFrames to avoid ambiguous column references
            codes_df_alias = codes_df.alias("codes")
            existing_codes_broadcast_alias = existing_codes_broadcast.alias("existing")
            
            codes_with_existing_ids = codes_df_alias.join(
                existing_codes_broadcast_alias,
                (F.trim(F.coalesce(F.col("codes.code_code"), F.lit(""))) == F.trim(F.coalesce(F.col("existing.code_code"), F.lit("")))) &
                (F.trim(F.coalesce(F.col("codes.code_system"), F.lit(""))) == F.trim(F.coalesce(F.col("existing.code_system"), F.lit("")))),
                "left"
            )
            
            # Step 4: Generate new code_ids only for codes that don't exist (using native Spark)
            observation_codes_normalized = codes_with_existing_ids.select(
                F.col("codes.*"),  # All columns from codes_df
                F.col("existing.code_id").alias("existing_code_id")  # Existing code_id from Redshift
            ).withColumn(
                "code_id",
                F.coalesce(
                    F.col("existing_code_id"),  # Reuse existing
                    generate_code_id_native(  # Generate new (native Spark)
                        F.col("code_code"),
                        F.col("code_system")
                    )
                )
            ).filter(
                F.col("code_id").isNotNull() & 
                F.col("observation_id").isNotNull() & 
                F.col("patient_id").isNotNull()
            )
            
            logger.info("✅ Reused existing code_ids and generated new ones (native Spark, no UDF)")
        else:
            # No existing codes or initial load - generate all code_ids using native Spark
            logger.info("Generating all code_ids using native Spark MD5 (no UDF)...")
            observation_codes_normalized = codes_df.withColumn(
                "code_id",
                generate_code_id_native(
                    F.col("code_code"),
                    F.col("code_system")
                )
            ).filter(
                F.col("code_id").isNotNull() & 
                F.col("observation_id").isNotNull() & 
                F.col("patient_id").isNotNull()
            )
            
            logger.info("✅ Generated all code_ids using native Spark (no UDF)")
    
    # Use array position from posexplode (avoids expensive window function)
    # array_position is 0-based, convert to 1-based rank
    if "array_position" in observation_codes_normalized.columns:
        observation_codes_final = observation_codes_normalized.select(
            F.col("observation_id"),
            F.col("patient_id"),
            F.col("code_id"),
            (F.col("array_position") + 1).cast("int").alias("code_rank")  # Convert 0-based to 1-based
        )
    else:
        # Fallback: No array position available - set rank to NULL (optional field)
        # This shouldn't happen if posexplode is used, but handle gracefully
        logger.warning("array_position not found in codes_df - setting code_rank to NULL")
        observation_codes_final = observation_codes_normalized.select(
            F.col("observation_id"),
            F.col("patient_id"),
            F.col("code_id"),
            F.lit(None).cast("int").alias("code_rank")
        )
    
    # CRITICAL: Deduplicate codes by (observation_id, code_id) to prevent duplicate codes
    # This fixes the issue where enrichment adds codes that already exist from HealthLake
    logger.info("Deduplicating codes by (observation_id, code_id) to prevent duplicates...")
    observation_codes_final = observation_codes_final.dropDuplicates(["observation_id", "code_id"])
    logger.info("✅ Deduplicated codes (removed duplicate code_id per observation)")
    
    logger.info("✅ Transformed observation-code relationships (count skipped to avoid shuffle failures)")
    
    return observation_codes_final

