"""
Native Spark-based code enrichment utility for observations

Replaces UDF-based enrichment with native Spark functions for better performance
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from typing import Optional
import logging

from .code_enrichment_mappings import LAB_TEST_LOINC_MAPPING

logger = logging.getLogger(__name__)


def normalize_lookup_key(text: str) -> str:
    """
    Normalize a lookup key by stripping all separators (spaces, underscores, dashes, slashes, commas)
    and numeric suffixes, then converting to lowercase.
    
    This matches the normalization logic used for observation text.
    
    Examples:
        "a/g ratio" -> "agratio"
        "a_g_ratio" -> "agratio"
        "a-g ratio" -> "agratio"
        "calculated free testosterone_1" -> "calculatedfreetestosterone"
        "calculated_free_testosterone_1" -> "calculatedfreetestosterone"
    
    Args:
        text: The text to normalize
    
    Returns:
        Fully normalized key with all separators and numeric suffixes removed
    """
    if not text:
        return ""
    import re
    # Convert to lowercase, strip
    normalized = text.lower().strip()
    # Remove parentheses with numbers: (_3), (3), etc.
    normalized = re.sub(r'\(_\d+\)', '', normalized)
    normalized = re.sub(r'\((\d+)\)', '', normalized)
    # Remove numeric suffixes: _1, #1, etc. (at end, optionally with space before)
    normalized = re.sub(r'\s*[_\#]\d+$', '', normalized)
    # Remove all separators: spaces, underscores, dashes, slashes, commas, hashes
    for separator in [' ', '_', '-', '/', ',', '#']:
        normalized = normalized.replace(separator, '')
    return normalized


def create_enrichment_lookup_dataframe(spark: SparkSession) -> DataFrame:
    """
    Create a broadcast DataFrame from the LAB_TEST_LOINC_MAPPING dictionary
    
    This DataFrame will be used for efficient lookups using native Spark joins
    instead of UDFs.
    
    All mapping keys are normalized (stripping all separators) for robust matching.
    
    Returns:
        DataFrame with columns: lookup_key (normalized), code, system, display, normalized_text
    """
    # Convert dictionary to list of rows with normalized keys
    enrichment_data = []
    for text_key, mapping in LAB_TEST_LOINC_MAPPING.items():
        # Normalize the key by stripping all separators
        normalized_key = normalize_lookup_key(text_key)
        enrichment_data.append({
            "lookup_key": normalized_key,
            "code": mapping.get("code"),
            "system": mapping.get("system", "http://loinc.org"),
            "display": mapping.get("display", ""),
            "normalized_text": mapping.get("normalized_text", text_key)
        })
    
    # Create DataFrame
    schema = StructType([
        StructField("lookup_key", StringType(), False),
        StructField("code", StringType(), True),
        StructField("system", StringType(), True),
        StructField("display", StringType(), True),
        StructField("normalized_text", StringType(), True)
    ])
    
    enrichment_df = spark.createDataFrame(enrichment_data, schema)
    
    # Broadcast for efficient joins
    enrichment_df = F.broadcast(enrichment_df)
    
    logger.info(f"Created enrichment lookup DataFrame with {len(enrichment_data)} entries")
    
    return enrichment_df


def enrich_codes_native(
    codes_df: DataFrame,
    spark: SparkSession,
    enrichment_mode: str = "hybrid"
) -> DataFrame:
    """
    Enrich codes using native Spark functions (no UDFs)
    
    This replaces the UDF-based enrichment with broadcast joins and native Spark operations.
    
    Args:
        codes_df: DataFrame with columns: observation_id, patient_id, code_text, coding_array
        spark: SparkSession
        enrichment_mode: "loinc_only", "synthetic_only", or "hybrid"
    
    Returns:
        DataFrame with enriched coding_array and normalized_code_text
    """
    logger.info("Enriching codes using native Spark functions (no UDFs)...")
    
    # Create enrichment lookup DataFrame
    enrichment_lookup = create_enrichment_lookup_dataframe(spark)
    
    # Prepare codes_df for enrichment
    # Normalize code_text for lookup (lowercase, strip)
    # FIX: Remove space before comma to handle "PSA ,total" -> "PSA, total"
    # FIX: Strip numeric suffixes (_1, #1, etc.) for normalization
    # FIX: Strip parentheses with numbers (e.g., "specimen_source_(3)" -> "specimen_source")
    # Normalize code_text for lookup by:
    # 1. Lowercase and trim
    # 2. Remove parentheses with numbers: (_3), (3), etc.
    # 3. Remove numeric suffixes: _1, #1, etc. (at end of string or after space)
    # 4. Strip all separators (spaces, underscores, dashes, slashes, commas) for matching
    codes_prepared = codes_df.withColumn(
        "code_text_normalized",
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(
                        F.lower(F.trim(F.col("code_text"))),
                        r'\(_\d+\)', ''  # Remove parentheses with underscore and number: (_3), (_1), etc.
                    ),
                    r'\((\d+)\)', ''  # Remove parentheses with number: (3), (1), etc.
                ),
                r'\s*[_\#]\d+$', ''  # Remove numeric suffixes: " _1", "_1", " #1", etc. at end
            ),
            r'[_\s\-/,\#]+', ''  # Strip all separators: spaces, underscores, dashes, slashes, commas, hashes
        )
    ).withColumn(
        "lookup_key",
        F.col("code_text_normalized")  # Fully normalized key (all separators removed)
    )
    
    # Check if coding_array is empty/null
    codes_prepared = codes_prepared.withColumn(
        "has_coding",
        F.when(
            (F.col("coding_array").isNotNull()) & (F.size(F.col("coding_array")) > 0),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    
    # Join with enrichment lookup using normalized keys
    # Both sides are fully normalized (all separators stripped), so we only need one join
    codes_prepared_alias = codes_prepared.alias("codes")
    enrichment_lookup_alias = enrichment_lookup.alias("lookup")
    
    # Single join on normalized keys
    codes_with_lookup = codes_prepared_alias.join(
        enrichment_lookup_alias,
        F.col("codes.lookup_key") == F.col("lookup.lookup_key"),
        "left"
    ).select(
        F.col("codes.*"),
        F.col("lookup.code").alias("enrichment_code_final"),
        F.col("lookup.system").alias("enrichment_system_final"),
        F.col("lookup.display").alias("enrichment_display_final"),
        F.col("lookup.normalized_text").alias("enrichment_normalized_text_final")
    )
    
    # Build enriched codes array
    # If coding_array is empty and we have enrichment, create enriched codes
    # If coding_array exists, normalize it and optionally merge with enriched codes
    
    # First, normalize existing coding_array using F.transform (already native)
    normalize_schema = ArrayType(StructType([
        StructField("code", StringType(), True),
        StructField("system", StringType(), True),
        StructField("display", StringType(), True),
        StructField("text", StringType(), True)
    ]))
    
    codes_with_enriched = codes_with_lookup.withColumn(
        "normalized_existing_codes",
        F.when(
            F.col("has_coding"),
            F.transform(
                F.col("coding_array"),
                lambda x: F.struct(
                    F.coalesce(x.getField("code"), F.lit("")).alias("code"),
                    F.coalesce(x.getField("system"), F.lit("")).alias("system"),
                    F.coalesce(x.getField("display"), F.lit("")).alias("display"),
                    F.lit(None).cast("string").alias("text")
                )
            )
        ).otherwise(F.array().cast(normalize_schema))
    )
    
    # Handle synthetic codes for cases where no LOINC match is found
    # Create synthetic code key (cleaned text, max 255 chars)
    codes_with_enriched = codes_with_enriched.withColumn(
        "synthetic_code",
        F.when(
            (~F.col("has_coding")) &  # No existing codes
            (F.col("enrichment_code_final").isNull()) &  # No LOINC match
            (F.col("code_text").isNotNull()) &  # Has text
            (F.col("code_text") != ""),  # Not empty
            # Create synthetic code (similar to Python version)
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace(F.lower(F.trim(F.col("code_text"))), ' ', '_'),
                        ',', ''
                    ),
                    '-', '_'
                ),
                '/', '_'
            )
        ).otherwise(F.lit(None))
    ).withColumn(
        "synthetic_code",
        # Truncate to 255 chars (Redshift VARCHAR limit)
        F.when(
            F.length(F.col("synthetic_code")) > 255,
            F.concat(
                F.substring(F.col("synthetic_code"), 1, 250),
                F.lit("_hash")  # Simplified - full hash would require UDF
            )
        ).otherwise(F.col("synthetic_code"))
    )
    
    # Create enriched code struct (LOINC or synthetic)
    codes_with_enriched = codes_with_enriched.withColumn(
        "enriched_code_struct",
        F.when(
            (~F.col("has_coding")) &  # No existing codes
            (F.col("enrichment_code_final").isNotNull()) &  # LOINC enrichment found
            (F.col("enrichment_code_final") != ""),  # Not empty
            # Use LOINC code
            F.struct(
                F.col("enrichment_code_final").alias("code"),
                F.col("enrichment_system_final").alias("system"),
                F.substring(F.col("enrichment_display_final"), 1, 2000).alias("display"),  # Truncate to 2000 chars
                F.col("code_text").alias("text")
            )
        ).when(
            (~F.col("has_coding")) &  # No existing codes
            (F.col("enrichment_code_final").isNull()) &  # No LOINC match
            (F.col("synthetic_code").isNotNull()),  # Synthetic code created
            # Use synthetic code
            F.struct(
                F.col("synthetic_code").alias("code"),
                F.lit("http://thirdopinion.io/CodeSystem/observation-text").alias("system"),
                F.substring(F.col("code_text"), 1, 2000).alias("display"),  # Truncate to 2000 chars for code_display
                F.col("code_text").alias("text")
            )
        ).when(
            F.col("has_coding") &  # Has existing codes
            (F.col("enrichment_code_final").isNotNull()) &  # LOINC enrichment found
            (F.col("enrichment_code_final") != ""),  # Not empty
            # Merge: add enriched LOINC code to existing codes
            F.struct(
                F.col("enrichment_code_final").alias("code"),
                F.col("enrichment_system_final").alias("system"),
                F.substring(F.col("enrichment_display_final"), 1, 2000).alias("display"),  # Truncate to 2000 chars
                F.col("code_text").alias("text")
            )
        ).otherwise(F.lit(None))
    )
    
    # Build final coding_array
    codes_with_enriched = codes_with_enriched.withColumn(
        "final_coding_array",
        F.when(
            ~F.col("has_coding"),  # No existing codes
            # Use enriched code if available, otherwise empty array
            F.when(
                F.col("enriched_code_struct").isNotNull(),
                F.array(F.col("enriched_code_struct"))
            ).otherwise(F.array().cast(normalize_schema))
        ).otherwise(
            # Has existing codes - merge with enriched if available
            # NOTE: Deduplication happens later in codes_transformation.py after explode
            # This prevents most duplicates, but some may still occur if same code exists with different display
            F.when(
                F.col("enriched_code_struct").isNotNull(),
                F.concat(F.col("normalized_existing_codes"), F.array(F.col("enriched_code_struct")))
            ).otherwise(F.col("normalized_existing_codes"))
        )
    )
    
    # Set normalized_code_text
    codes_with_enriched = codes_with_enriched.withColumn(
        "normalized_code_text",
        F.coalesce(
            F.col("enrichment_normalized_text_final"),
            F.col("code_text")  # Fallback to original text
        )
    )
    
    # Select final columns
    result = codes_with_enriched.select(
        F.col("observation_id"),
        F.col("patient_id"),
        F.col("code_text"),
        F.col("final_coding_array").alias("coding_array"),
        F.col("normalized_code_text")
    )
    
    logger.info("✅ Code enrichment completed using native Spark functions")
    
    return result

