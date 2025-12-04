"""
Utility functions for creating Redshift connection options
"""
import boto3
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


def get_redshift_connection_options(
    connection_name: str,
    database: str,
    s3_temp_dir: str,
    region: str = "us-east-2",
    **additional_options
) -> Dict[str, Any]:
    """
    Get Redshift connection options using the Glue connection.
    
    The Glue connection "Redshift connection" is now configured to use "test" database
    as the default, so we can use useConnectionProperties to leverage the connection's
    configuration.
    
    Args:
        connection_name: Name of the Glue connection
        database: Target database name (e.g., "test", "dev") - should match connection default
        s3_temp_dir: S3 temporary directory for Redshift operations
        region: AWS region (default: us-east-2)
        **additional_options: Additional connection options to include (e.g., dbtable, preactions, query)
        
    Returns:
        Dictionary of connection options ready for use in Glue DynamicFrame operations
    """
    # Use connection properties since the connection is configured with the correct database
    connection_options = {
        "redshiftTmpDir": s3_temp_dir,
        "useConnectionProperties": "true",
        "connectionName": connection_name,
        **additional_options
    }
    
    logger.debug(f"Using Glue connection '{connection_name}' with database: {database} (from connection config)")
    return connection_options

