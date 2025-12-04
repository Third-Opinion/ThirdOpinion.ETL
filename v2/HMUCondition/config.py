"""
Configuration management for HMUCondition ETL
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


# Extend ETLConfig for Condition-specific configuration
class ConditionETLConfig(ETLConfig):
    """Condition-specific ETL configuration"""
    
    @classmethod
    def from_environment(cls) -> "ConditionETLConfig":
        """Create configuration from environment variables and job arguments"""
        processing = ProcessingConfig.from_environment()
        # Override table_name for conditions
        database = DatabaseConfig()
        database.table_name = "condition"
        
        return cls(
            spark=SparkConfig(),
            database=database,
            processing=processing
        )


# Table names constants
class TableNames:
    """Redshift table names"""
    CONDITIONS = "conditions"
    CONDITION_CODES = "condition_codes"
    CONDITION_CATEGORIES = "condition_categories"
    CONDITION_BODY_SITES = "condition_body_sites"
    CONDITION_EVIDENCE = "condition_evidence"
    CONDITION_EXTENSIONS = "condition_extensions"
    CONDITION_NOTES = "condition_notes"
    CONDITION_STAGES = "condition_stages"
    CONDITION_STAGE_SUMMARIES = "condition_stage_summaries"
    CONDITION_STAGE_TYPES = "condition_stage_types"
    
    @classmethod
    def all_tables(cls) -> list[str]:
        """Return list of all table names"""
        return [
            cls.CONDITIONS,
            cls.CONDITION_CODES,
            cls.CONDITION_CATEGORIES,
            cls.CONDITION_BODY_SITES,
            cls.CONDITION_EVIDENCE,
            cls.CONDITION_EXTENSIONS,
            cls.CONDITION_NOTES,
            cls.CONDITION_STAGES,
            cls.CONDITION_STAGE_SUMMARIES,
            cls.CONDITION_STAGE_TYPES
        ]
    
    @classmethod
    def child_tables(cls) -> list[str]:
        """Return list of child table names (excluding main conditions table)"""
        return [
            cls.CONDITION_CODES,
            cls.CONDITION_CATEGORIES,
            cls.CONDITION_BODY_SITES,
            cls.CONDITION_EVIDENCE,
            cls.CONDITION_EXTENSIONS,
            cls.CONDITION_NOTES,
            cls.CONDITION_STAGES,
            cls.CONDITION_STAGE_SUMMARIES,
            cls.CONDITION_STAGE_TYPES
        ]

