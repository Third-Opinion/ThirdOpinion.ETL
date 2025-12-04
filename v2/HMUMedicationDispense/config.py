"""
Configuration management for HMUMedicationDispense ETL
"""
from shared.config import (
    SparkConfig,
    DatabaseConfig,
    ProcessingConfig,
    ETLConfig
)


class TableNames:
    """Redshift table names for this job"""
    MEDICATION_DISPENSES = "medication_dispenses"
    MEDICATION_DISPENSE_IDENTIFIERS = "medication_dispense_identifiers"
    MEDICATION_DISPENSE_PERFORMERS = "medication_dispense_performers"
    MEDICATION_DISPENSE_AUTH_PRESCRIPTIONS = "medication_dispense_auth_prescriptions"
    MEDICATION_DISPENSE_DOSAGE_INSTRUCTIONS = "medication_dispense_dosage_instructions"
    
    @classmethod
    def all_tables(cls) -> list[str]:
        """Return list of all table names"""
        return [
            cls.MEDICATION_DISPENSES,
            cls.MEDICATION_DISPENSE_IDENTIFIERS,
            cls.MEDICATION_DISPENSE_PERFORMERS,
            cls.MEDICATION_DISPENSE_AUTH_PRESCRIPTIONS,
            cls.MEDICATION_DISPENSE_DOSAGE_INSTRUCTIONS,
        ]
    
    @classmethod
    def child_tables(cls) -> list[str]:
        """Return list of child table names (excluding main table)"""
        return [
            cls.MEDICATION_DISPENSE_IDENTIFIERS,
            cls.MEDICATION_DISPENSE_PERFORMERS,
            cls.MEDICATION_DISPENSE_AUTH_PRESCRIPTIONS,
            cls.MEDICATION_DISPENSE_DOSAGE_INSTRUCTIONS,
        ]


# Extend ETLConfig for job-specific configuration
class MedicationDispenseETLConfig(ETLConfig):
    """Job-specific ETL configuration"""
    
    @classmethod
    def from_environment(cls) -> "MedicationDispenseETLConfig":
        """Create configuration from environment variables and job arguments"""
        processing = ProcessingConfig.from_environment()
        database = DatabaseConfig()
        database.table_name = "medicationdispense"
        
        return cls(
            spark=SparkConfig(),
            database=database,
            processing=processing
        )

