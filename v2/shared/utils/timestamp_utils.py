"""
Timestamp parsing utilities for FHIR data
"""
from typing import Optional
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType


def create_timestamp_parser(column_expr) -> F.Column:
    """
    Create a timestamp parser expression that handles multiple FHIR timestamp formats
    
    Args:
        column_expr: Column expression to parse
        
    Returns:
        Column expression that parses timestamps with multiple format fallbacks
    """
    return F.coalesce(
        F.to_timestamp(column_expr, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),  # With nanoseconds
        F.to_timestamp(column_expr, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),        # With milliseconds
        F.to_timestamp(column_expr, "yyyy-MM-dd'T'HH:mm:ssXXX"),             # No milliseconds
        F.to_timestamp(column_expr, "yyyy-MM-dd'T'HH:mm:ss'Z'"),              # Z timezone
        F.to_timestamp(column_expr, "yyyy-MM-dd'T'HH:mm:ss"),                 # No timezone
        F.to_timestamp(column_expr, "yyyy-MM-dd"),                            # Date only (common for vital signs)
        F.to_timestamp(column_expr, "M/d/yy"),                                # Short date format
        F.to_timestamp(column_expr, "M/d/yyyy")                               # Short date with 4-digit year
    )


def parse_fhir_timestamp(df, column_path: str, alias: str = None) -> F.Column:
    """
    Parse a FHIR timestamp field from a DataFrame
    
    Args:
        df: DataFrame containing the timestamp field
        column_path: Path to the timestamp column (e.g., "meta.lastUpdated")
        alias: Optional alias for the parsed column
        
    Returns:
        Column expression for the parsed timestamp
    """
    # Handle nested paths like "meta.lastUpdated"
    if "." in column_path:
        parts = column_path.split(".")
        col_expr = df[parts[0]]
        for part in parts[1:]:
            col_expr = col_expr.getField(part)
    else:
        col_expr = F.col(column_path)
    
    parsed = create_timestamp_parser(col_expr)
    
    if alias:
        return parsed.alias(alias)
    return parsed

