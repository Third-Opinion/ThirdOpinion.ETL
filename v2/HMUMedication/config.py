"""
Configuration management for HMUMedication ETL
"""
from dataclasses import dataclass
import os
from shared.config import (
    SparkConfig,
    DatabaseConfig,
    ProcessingConfig,
    ETLConfig
)


class TableNames:
    """Redshift table names for this job"""
    MEDICATIONS = "medications"
    MEDICATION_IDENTIFIERS = "medication_identifiers"
    
    @classmethod
    def all_tables(cls) -> list[str]:
        """Return list of all table names"""
        return [
            cls.MEDICATIONS,
            cls.MEDICATION_IDENTIFIERS,
        ]
    
    @classmethod
    def child_tables(cls) -> list[str]:
        """Return list of child table names (excluding main table)"""
        return [
            cls.MEDICATION_IDENTIFIERS,
        ]


@dataclass
class MedicationEnrichmentConfig:
    """Medication code enrichment configuration"""
    enable_enrichment: bool = True  # Enabled by default
    enrichment_mode: str = "hybrid"  # "rxnav_only", "comprehend_only", or "hybrid"
    use_lookup_table: bool = True  # Check Redshift lookup table first
    
    @classmethod
    def from_environment(cls) -> "MedicationEnrichmentConfig":
        """Create enrichment configuration from environment variables"""
        return cls(
            enable_enrichment=os.getenv("ENABLE_MEDICATION_ENRICHMENT", "true").lower() in ["true", "1", "yes"],
            enrichment_mode=os.getenv("MEDICATION_ENRICHMENT_MODE", "hybrid"),
            use_lookup_table=os.getenv("USE_MEDICATION_LOOKUP_TABLE", "true").lower() in ["true", "1", "yes"]
        )


# Extend ETLConfig for job-specific configuration
class MedicationETLConfig(ETLConfig):
    """Job-specific ETL configuration"""
    enrichment: MedicationEnrichmentConfig = None
    
    @classmethod
    def from_environment(cls) -> "MedicationETLConfig":
        """Create configuration from environment variables and job arguments"""
        processing = ProcessingConfig.from_environment()
        database = DatabaseConfig()
        database.table_name = "medication"
        enrichment = MedicationEnrichmentConfig.from_environment()
        
        config = cls(
            spark=SparkConfig(),
            database=database,
            processing=processing
        )
        config.enrichment = enrichment
        return config
