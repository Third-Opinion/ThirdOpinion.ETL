"""
Configuration management for HMUObservation ETL
"""
from typing import Optional
import os

# Import shared configuration classes
from shared.config import (
    SparkConfig,
    DatabaseConfig,
    ProcessingConfig,
    ETLConfig
)


class TableNames:
    """Redshift table names"""
    OBSERVATIONS = "observations"
    OBSERVATION_CODES = "observation_codes"
    OBSERVATION_CATEGORIES = "observation_categories"
    OBSERVATION_INTERPRETATIONS = "observation_interpretations"
    OBSERVATION_REFERENCE_RANGES = "observation_reference_ranges"
    OBSERVATION_COMPONENTS = "observation_components"
    OBSERVATION_BODY_SITES = "observation_body_sites"
    OBSERVATION_NOTES = "observation_notes"
    OBSERVATION_PERFORMERS = "observation_performers"
    OBSERVATION_MEMBERS = "observation_members"
    OBSERVATION_DERIVED_FROM = "observation_derived_from"
    
    @classmethod
    def all_tables(cls) -> list[str]:
        """Return list of all table names"""
        return [
            cls.OBSERVATIONS,
            cls.OBSERVATION_CODES,
            cls.OBSERVATION_CATEGORIES,
            cls.OBSERVATION_INTERPRETATIONS,
            cls.OBSERVATION_REFERENCE_RANGES,
            cls.OBSERVATION_COMPONENTS,
            cls.OBSERVATION_BODY_SITES,
            cls.OBSERVATION_NOTES,
            cls.OBSERVATION_PERFORMERS,
            cls.OBSERVATION_MEMBERS,
            cls.OBSERVATION_DERIVED_FROM
        ]
    
    @classmethod
    def child_tables(cls) -> list[str]:
        """Return list of child table names (excluding main observations table)"""
        return [
            cls.OBSERVATION_CODES,
            cls.OBSERVATION_CATEGORIES,
            cls.OBSERVATION_COMPONENTS,
            cls.OBSERVATION_REFERENCE_RANGES,
            cls.OBSERVATION_INTERPRETATIONS,
            cls.OBSERVATION_BODY_SITES,
            cls.OBSERVATION_NOTES,
            cls.OBSERVATION_PERFORMERS,
            cls.OBSERVATION_MEMBERS,
            cls.OBSERVATION_DERIVED_FROM
        ]


# Extend ETLConfig for Observation-specific configuration
class ObservationETLConfig(ETLConfig):
    """Observation-specific ETL configuration"""
    
    @classmethod
    def from_environment(cls) -> "ObservationETLConfig":
        """Create configuration from environment variables and job arguments"""
        processing = ProcessingConfig.from_environment()
        # Override table_name for observations
        database = DatabaseConfig()
        database.table_name = "observation"
        
        return cls(
            spark=SparkConfig(),
            database=database,
            processing=processing
        )
