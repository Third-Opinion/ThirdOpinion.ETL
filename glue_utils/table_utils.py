"""
Utility functions for Redshift table management in Glue jobs.
This module provides functions to check table existence and print table schemas.
"""

import logging

logger = logging.getLogger(__name__)

def check_and_log_table_schema(glueContext, table_name, redshift_connection, s3_temp_dir):
    """
    Check if a Redshift table exists and log its column information.

    Args:
        glueContext: AWS Glue context
        table_name: Name of the table to check (without schema prefix)
        redshift_connection: Name of the Redshift connection
        s3_temp_dir: S3 temp directory for Redshift operations

    Returns:
        bool: True if table exists, False otherwise
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"🔍 Checking table: public.{table_name}")
    logger.info(f"{'='*60}")

    try:
        # Try to read the table metadata (just schema, no data)
        existing_table = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "redshiftTmpDir": s3_temp_dir,
                "useConnectionProperties": "true",
                "dbtable": f"public.{table_name}",
                "connectionName": redshift_connection
            },
            transformation_ctx=f"check_table_{table_name}"
        )

        # Convert to DataFrame to access schema
        df = existing_table.toDF()

        # Log table information
        logger.info(f"✅ Table 'public.{table_name}' EXISTS")
        logger.info(f"\n📋 Table Schema:")
        logger.info(f"{'   Column Name':<40} {'Data Type':<20}")
        logger.info(f"   {'-'*40} {'-'*20}")

        for field in df.schema.fields:
            logger.info(f"   {field.name:<40} {str(field.dataType):<20}")

        row_count = df.count()
        logger.info(f"\n📊 Table Statistics:")
        logger.info(f"   Total columns: {len(df.schema.fields)}")
        logger.info(f"   Total rows: {row_count:,}")

        return True

    except Exception as e:
        logger.warning(f"⚠️  Table 'public.{table_name}' DOES NOT EXIST or cannot be accessed")
        logger.debug(f"   Error details: {str(e)}")
        logger.info(f"   Table will be created on first write operation")
        return False

def check_all_tables(glueContext, table_names, redshift_connection, s3_temp_dir):
    """
    Check existence and schema for multiple tables.

    Args:
        glueContext: AWS Glue context
        table_names: List of table names to check
        redshift_connection: Name of the Redshift connection
        s3_temp_dir: S3 temp directory for Redshift operations

    Returns:
        dict: Dictionary mapping table names to their existence status (bool)
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"🔍 CHECKING REDSHIFT TABLES")
    logger.info(f"{'='*80}")
    logger.info(f"Tables to check: {', '.join(table_names)}")

    table_status = {}

    for table_name in table_names:
        exists = check_and_log_table_schema(glueContext, table_name, redshift_connection, s3_temp_dir)
        table_status[table_name] = exists

    # Summary
    logger.info(f"\n{'='*80}")
    logger.info(f"📊 TABLE CHECK SUMMARY")
    logger.info(f"{'='*80}")

    existing_count = sum(1 for exists in table_status.values() if exists)
    missing_count = len(table_names) - existing_count

    logger.info(f"Total tables checked: {len(table_names)}")
    logger.info(f"✅ Existing tables: {existing_count}")
    logger.info(f"⚠️  Missing tables: {missing_count}")

    if missing_count > 0:
        missing_tables = [name for name, exists in table_status.items() if not exists]
        logger.info(f"\nMissing tables (will be created):")
        for table in missing_tables:
            logger.info(f"  - {table}")

    logger.info(f"{'='*80}\n")

    return table_status
