"""
Utility functions for HMUObservation ETL

Note: Using lazy imports to avoid PySpark dependency issues at module import time.
Functions are imported when needed, not at module level.
"""

# Define exports but don't import at module level
# This avoids PySpark import issues when utils is imported before Spark is initialized
__all__ = [
    # Observation-specific utilities (kept in this package)
    "identify_entered_in_error_records",
    "delete_entered_in_error_records",
    "delete_child_records_for_observations",
    "parse_reference_range_text",
    "enrich_observation_code",
    "get_normalized_text_for_code",
    "extract_patient_id",  # Transformation utility
    # Note: Shared utilities (bookmark_utils, deduplication_utils, timestamp_utils, version_utils)
    # are now imported from shared.utils
]

# Lazy import function - imports are done when functions are actually called
# This prevents PySpark import errors when utils is imported before Spark is ready
def _lazy_import(module_name, *names):
    """Lazy import helper - not used, but kept for reference"""
    pass

# Note: The main script imports directly from submodules (e.g., utils.bookmark_utils)
# So these top-level imports aren't strictly necessary
# Keeping __all__ for documentation purposes

