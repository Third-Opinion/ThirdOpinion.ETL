"""
Shared configuration management for v2 ETL jobs
"""
from typing import Optional
from dataclasses import dataclass
import os


@dataclass
class SparkConfig:
    """Spark configuration settings"""
    shuffle_partitions: int = 50  # Reduced to minimize shuffle overhead and executor memory pressure
    adaptive_enabled: bool = True
    adaptive_coalesce_enabled: bool = True
    adaptive_skew_join_enabled: bool = True
    auto_broadcast_threshold: int = -1  # Disable to prevent hanging
    broadcast_timeout: int = 300
    network_timeout: str = "600s"
    executor_heartbeat_interval: str = "30s"
    max_partition_bytes: str = "134217728"  # 128MB
    advisory_partition_size: str = "134217728"  # 128MB
    # Shuffle storage settings to reduce executor memory pressure and prevent shuffle data loss
    # Cloud shuffle storage prevents data loss when executors fail
    # Using S3 for shuffle storage is more resilient than executor memory, especially for long-running jobs
    shuffle_storage_path: Optional[str] = None  # Set to S3 path for cloud shuffle storage (e.g., "s3://bucket/shuffle")
    write_shuffle_files_to_s3: bool = True  # Enable cloud shuffle storage - prevents MetadataFetchFailedException errors
    
    def get_shuffle_storage_path(self, database_config) -> Optional[str]:
        """Get shuffle storage path, using database config if not explicitly set"""
        if self.shuffle_storage_path:
            return self.shuffle_storage_path
        if hasattr(database_config, 'shuffle_storage_bucket') and database_config.shuffle_storage_bucket:
            return database_config.shuffle_storage_bucket
        return None


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    catalog_name: str = "glue_catalog"
    database_name: str = "hmu_fhir_data_store_836e877666cebf177ce6370ec1478a92_healthlake_view"
    table_name: str = ""  # Set by each job
    redshift_connection: str = "Redshift connection"
    redshift_database: str = "test"  # Redshift database name (v2 uses "test" instead of "dev")
    s3_temp_dir: str = "s3://aws-glue-assets-442042533707-us-east-2/temporary/"
    table_catalog_id: str = "457560472834"
    s3_bucket: str = "s3://7df690fd40c734f8937daf02f39b2ec3-457560472834-group/datalake/hmu_fhir_data_store_836e877666cebf177ce6370ec1478a92_healthlake_view/"
    s3_output_bucket: str = "s3://healthlake-glue-output-2025"
    shuffle_storage_bucket: str = "s3://healthlake-glue-output-2025/shuffle"  # S3 path for cloud shuffle storage


@dataclass
class ProcessingConfig:
    """Processing configuration"""
    test_mode: bool = False
    backdate_days: int = 0
    use_sample: bool = False
    sample_size: int = 100000
    batch_size: int = 100  # For deletion operations
    enable_debug_logging: bool = False
    enable_code_enrichment: bool = True  # Feature flag for code enrichment (Observation-specific)
    code_enrichment_mode: str = "hybrid"  # "loinc_only", "synthetic_only", "hybrid" (Observation-specific)
    
    @classmethod
    def from_environment(cls) -> "ProcessingConfig":
        """Create processing configuration from environment variables"""
        return cls(
            test_mode=os.getenv("TEST_MODE", "false").lower() in ["true", "1", "yes"],
            backdate_days=int(os.getenv("BACKDATE_DAYS", "0")),
            use_sample=os.getenv("USE_SAMPLE", "false").lower() in ["true", "1", "yes"],
            sample_size=int(os.getenv("SAMPLE_SIZE", "100000")),
            enable_debug_logging=os.getenv("ENABLE_DEBUG_LOGGING", "false").lower() in ["true", "1", "yes"],
            enable_code_enrichment=os.getenv("ENABLE_CODE_ENRICHMENT", "true").lower() in ["true", "1", "yes"],
            code_enrichment_mode=os.getenv("CODE_ENRICHMENT_MODE", "hybrid")
        )


@dataclass
class ETLConfig:
    """Main ETL configuration"""
    spark: SparkConfig
    database: DatabaseConfig
    processing: ProcessingConfig
    
    @classmethod
    def from_environment(cls) -> "ETLConfig":
        """Create configuration from environment variables and job arguments"""
        return cls(
            spark=SparkConfig(),
            database=DatabaseConfig(),
            processing=ProcessingConfig.from_environment()
        )

