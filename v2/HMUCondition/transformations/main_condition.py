"""
Main condition data transformation
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, BooleanType, DecimalType
import json
import logging

from shared.utils.timestamp_utils import create_timestamp_parser

logger = logging.getLogger(__name__)


def convert_to_json_string(field) -> str:
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


def transform_main_condition_data(df: DataFrame) -> DataFrame:
    """
    Transform the main condition data
    
    Args:
        df: Source DataFrame with raw condition data
        
    Returns:
        Transformed DataFrame with flattened condition data
    """
    logger.info("Transforming main condition data...")
    
    # Log available columns for debugging
    available_columns = df.columns
    logger.info(f"Available columns: {len(available_columns)} columns")
    
    # Create UDF for JSON conversion
    convert_to_json_udf = F.udf(convert_to_json_string, StringType())
    
    # Build the select statement dynamically based on available columns
    select_columns = [
        F.col("id").alias("condition_id"),
        F.when(F.col("subject").isNotNull(), 
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        F.when(F.col("encounter").isNotNull(),
               F.regexp_extract(F.col("encounter").getField("reference"), r"Encounter/(.+)", 1)
              ).otherwise(None).alias("encounter_id"),
    ]
    
    # Add clinical status information
    select_columns.extend([
        F.when(F.col("clinicalStatus").isNotNull() & (F.size(F.col("clinicalStatus").getField("coding")) > 0),
               F.col("clinicalStatus").getField("coding")[0].getField("code")
              ).otherwise(None).alias("clinical_status_code"),
        F.when(F.col("clinicalStatus").isNotNull() & (F.size(F.col("clinicalStatus").getField("coding")) > 0),
               F.col("clinicalStatus").getField("coding")[0].getField("system")
              ).otherwise(None).alias("clinical_status_system"),
        F.lit(None).alias("clinical_status_display"),
    ])
    
    # Add verification status information
    select_columns.extend([
        F.when(F.col("verificationStatus").isNotNull() & (F.size(F.col("verificationStatus").getField("coding")) > 0),
               F.col("verificationStatus").getField("coding")[0].getField("code")
              ).otherwise(None).alias("verification_status_code"),
        F.when(F.col("verificationStatus").isNotNull() & (F.size(F.col("verificationStatus").getField("coding")) > 0),
               F.col("verificationStatus").getField("coding")[0].getField("system")
              ).otherwise(None).alias("verification_status_system"),
        F.lit(None).alias("verification_status_display"),
    ])
    
    # Add condition text information
    condition_text_expr = F.col("code").getField("text")
    select_columns.extend([
        condition_text_expr.alias("condition_text"),
        # Add diagnosis_name as alias of condition_text (required field)
        condition_text_expr.alias("diagnosis_name"),
    ])
    
    # Add severity information (if available)
    if "severity" in available_columns:
        select_columns.extend([
            F.when(F.col("severity").isNotNull(),
                   F.col("severity").getField("coding")[0].getField("code")
                  ).otherwise(None).alias("severity_code"),
            F.when(F.col("severity").isNotNull(),
                   F.col("severity").getField("coding")[0].getField("system")
                  ).otherwise(None).alias("severity_system"),
            F.lit(None).alias("severity_display"),
        ])
    else:
        select_columns.extend([
            F.lit(None).alias("severity_code"),
            F.lit(None).alias("severity_system"),
            F.lit(None).alias("severity_display"),
        ])
    
    # Add onset information using timestamp parser
    onset_timestamp_expr = create_timestamp_parser(F.col("onsetDateTime"))
    select_columns.extend([
        onset_timestamp_expr.alias("onset_datetime"),
        F.lit(None).alias("onset_age_value"),
        F.lit(None).alias("onset_age_unit"),
        F.lit(None).alias("onset_period_start"),
        F.lit(None).alias("onset_period_end"),
        F.lit(None).alias("onset_text"),
    ])
    
    # Add abatement information using timestamp parser
    abatement_timestamp_expr = create_timestamp_parser(F.col("abatementDateTime"))
    select_columns.extend([
        abatement_timestamp_expr.alias("abatement_datetime"),
        F.lit(None).alias("abatement_age_value"),
        F.lit(None).alias("abatement_age_unit"),
        F.lit(None).alias("abatement_period_start"),
        F.lit(None).alias("abatement_period_end"),
        F.lit(None).alias("abatement_text"),
        F.lit(None).alias("abatement_boolean"),
    ])
    
    # Add recorded date using timestamp parser
    recorded_timestamp_expr = create_timestamp_parser(F.col("recordedDate"))
    select_columns.extend([
        recorded_timestamp_expr.alias("recorded_date"),
        F.lit(None).alias("recorder_type"),
        F.lit(None).alias("recorder_id"),
        F.lit(None).alias("asserter_type"),
        F.lit(None).alias("asserter_id"),
        # Add effective_datetime (required field) - COALESCE onset_datetime and recorded_date
        F.coalesce(onset_timestamp_expr, recorded_timestamp_expr).alias("effective_datetime"),
    ])
    
    # Add metadata information using timestamp parser
    meta_timestamp_expr = create_timestamp_parser(F.col("meta").getField("lastUpdated"))
    select_columns.extend([
        meta_timestamp_expr.alias("meta_last_updated"),
        F.lit(None).alias("meta_source"),
        convert_to_json_udf(F.col("meta").getField("profile")).alias("meta_profile"),
        F.lit(None).alias("meta_security"),
        F.lit(None).alias("meta_tag"),
    ])
    
    # Extract category and code display information for status computation
    # Get first category code and text if available
    category_code_expr = F.when(
        F.col("category").isNotNull() & (F.size(F.col("category")) > 0),
        F.col("category")[0].getField("coding")[0].getField("code")
    ).otherwise(None)
    
    category_text_expr = F.when(
        F.col("category").isNotNull() & (F.size(F.col("category")) > 0),
        F.col("category")[0].getField("text")
    ).otherwise(None)
    
    # Get code display text for history detection
    code_display_expr = F.when(
        F.col("code").isNotNull() & 
        F.col("code").getField("coding").isNotNull() & 
        (F.size(F.col("code").getField("coding")) > 0),
        F.col("code").getField("coding")[0].getField("display")
    ).otherwise(None)
    
    # Compute status field (required field)
    # Status values: "current", "past", "primary", "secondary", "history_of"
    status_expr = (
        # Priority 1: Check for history_of
        F.when(
            (F.lower(category_text_expr).contains("history")) |
            (F.lower(code_display_expr).contains("history")) |
            (category_code_expr.isNotNull() & F.lower(category_code_expr).contains("history")),
            F.lit("history_of")
        )
        # Priority 2: Check verification status (entered-in-error - set to None)
        .when(
            F.when(F.col("verificationStatus").isNotNull() & (F.size(F.col("verificationStatus").getField("coding")) > 0),
                   F.col("verificationStatus").getField("coding")[0].getField("code")
                  ).otherwise(None) == "entered-in-error",
            F.lit(None)  # Exclude entered-in-error conditions from status
        )
        # Priority 3 & 4: Determine current vs past, primary vs secondary
        .when(
            (F.when(F.col("clinicalStatus").isNotNull() & (F.size(F.col("clinicalStatus").getField("coding")) > 0),
                    F.col("clinicalStatus").getField("coding")[0].getField("code")
                   ).otherwise(None) == "active") & 
            (F.when(F.col("verificationStatus").isNotNull() & (F.size(F.col("verificationStatus").getField("coding")) > 0),
                    F.col("verificationStatus").getField("coding")[0].getField("code")
                   ).otherwise(None) == "confirmed"),
            # Active and confirmed - check if primary or secondary
            F.when(
                category_code_expr == "problem-list-item",
                F.lit("primary")
            ).when(
                category_code_expr == "encounter-diagnosis",
                F.lit("secondary")
            ).otherwise(
                F.lit("current")
            )
        )
        .when(
            (F.when(F.col("clinicalStatus").isNotNull() & (F.size(F.col("clinicalStatus").getField("coding")) > 0),
                    F.col("clinicalStatus").getField("coding")[0].getField("code")
                   ).otherwise(None) == "inactive") & 
            (F.when(F.col("verificationStatus").isNotNull() & (F.size(F.col("verificationStatus").getField("coding")) > 0),
                    F.col("verificationStatus").getField("coding")[0].getField("code")
                   ).otherwise(None) == "confirmed"),
            F.lit("past")
        )
        # Default: active but unconfirmed = current, otherwise past
        .when(
            F.when(F.col("clinicalStatus").isNotNull() & (F.size(F.col("clinicalStatus").getField("coding")) > 0),
                   F.col("clinicalStatus").getField("coding")[0].getField("code")
                  ).otherwise(None) == "active",
            F.lit("current")
        )
        .otherwise(
            F.lit("past")
        )
    )
    
    # Add status to select columns
    select_columns.append(status_expr.alias("status"))
    
    # Add timestamps
    select_columns.extend([
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ])
    
    # Transform main condition data
    main_df = df.select(*select_columns).filter(
        F.col("condition_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return main_df

