"""
Configuration management for HMUMedicationRequest ETL
"""
from shared.config import (
    SparkConfig,
    DatabaseConfig,
    ProcessingConfig,
    ETLConfig
)


class TableNames:
    """Redshift table names for this job"""
    MEDICATION_REQUESTS = "medication_requests"
    MEDICATION_REQUEST_IDENTIFIERS = "medication_request_identifiers"
    MEDICATION_REQUEST_NOTES = "medication_request_notes"
    MEDICATION_REQUEST_DOSAGE_INSTRUCTIONS = "medication_request_dosage_instructions"
    MEDICATION_REQUEST_CATEGORIES = "medication_request_categories"
    
    @classmethod
    def all_tables(cls) -> list[str]:
        """Return list of all table names"""
        return [
            cls.MEDICATION_REQUESTS,
            cls.MEDICATION_REQUEST_IDENTIFIERS,
            cls.MEDICATION_REQUEST_NOTES,
            cls.MEDICATION_REQUEST_DOSAGE_INSTRUCTIONS,
            cls.MEDICATION_REQUEST_CATEGORIES,
        ]
    
    @classmethod
    def child_tables(cls) -> list[str]:
        """Return list of child table names (excluding main table)"""
        return [
            cls.MEDICATION_REQUEST_IDENTIFIERS,
            cls.MEDICATION_REQUEST_NOTES,
            cls.MEDICATION_REQUEST_DOSAGE_INSTRUCTIONS,
            cls.MEDICATION_REQUEST_CATEGORIES,
        ]


# Extend ETLConfig for job-specific configuration
class MedicationRequestETLConfig(ETLConfig):
    """Job-specific ETL configuration"""
    
    @classmethod
    def from_environment(cls) -> "MedicationRequestETLConfig":
        """Create configuration from environment variables and job arguments"""
        processing = ProcessingConfig.from_environment()
        database = DatabaseConfig()
        database.table_name = "medicationrequest"
        
        return cls(
            spark=SparkConfig(),
            database=database,
            processing=processing
        )

