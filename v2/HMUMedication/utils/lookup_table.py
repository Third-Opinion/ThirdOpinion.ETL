"""
Lookup table operations for medication code enrichment

Handles creation and management of the medication_code_lookup table in Redshift
"""
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
import logging

try:
    from shared.utils.redshift_connection_utils import get_redshift_connection_options
except ImportError:
    get_redshift_connection_options = None

logger = logging.getLogger(__name__)


def ensure_lookup_table_exists(
    glue_context: GlueContext,
    database_config,
    spark_session: SparkSession
) -> bool:
    """
    Ensure the medication_code_lookup table exists in Redshift
    
    Creates the table if it doesn't exist using CREATE TABLE IF NOT EXISTS via preactions.
    Uses a dummy write operation to execute the CREATE TABLE statement.
    
    Args:
        glue_context: GlueContext for database operations
        database_config: DatabaseConfig with connection info
        spark_session: SparkSession for creating empty DataFrame
    
    Returns:
        True if table exists or was created, False otherwise
    """
    logger.info("Checking if medication_code_lookup table exists...")
    
    try:
        # Try to read from the table to check if it exists
        if get_redshift_connection_options:
            connection_options = get_redshift_connection_options(
                connection_name=database_config.redshift_connection,
                database=database_config.redshift_database,
                s3_temp_dir=database_config.s3_temp_dir,
                dbtable="public.medication_code_lookup"
            )
        else:
            connection_options = {
                "redshiftTmpDir": database_config.s3_temp_dir,
                "useConnectionProperties": "true",
                "dbtable": "public.medication_code_lookup",
                "connectionName": database_config.redshift_connection,
            }
        
        lookup_df = glue_context.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options=connection_options,
            transformation_ctx="check_lookup_table_exists"
        ).toDF()
        
        # Table exists - get row count
        row_count = lookup_df.count()
        logger.info(f"✅ medication_code_lookup table exists with {row_count:,} entries")
        return True
        
    except Exception as e:
        # Table doesn't exist - create it
        logger.info(f"medication_code_lookup table does not exist - creating it...")
        
        try:
            # Import lookup table schema
            from utils.enrichment import get_lookup_table_schema
            
            # Get CREATE TABLE SQL
            create_table_sql = get_lookup_table_schema()
            
            # Create a DataFrame with explicit schema to avoid type inference issues
            from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
            
            # Define explicit schema matching the Redshift table
            schema = StructType([
                StructField("normalized_name", StringType(), False),
                StructField("rxnorm_code", StringType(), True),
                StructField("rxnorm_system", StringType(), True),
                StructField("medication_name", StringType(), True),
                StructField("confidence_score", DecimalType(3, 2), True),
                StructField("enrichment_source", StringType(), True),
                StructField("created_at", TimestampType(), True),
                StructField("updated_at", TimestampType(), True)
            ])
            
            # Execute CREATE TABLE directly using a query-based approach
            # Since we can't write an empty DataFrame easily, we'll log instructions for manual creation
            logger.info("Attempting to create medication_code_lookup table...")
            logger.info("If automatic creation fails, please create the table manually using:")
            logger.info("=" * 60)
            logger.info(create_table_sql.strip())
            logger.info("=" * 60)
            
            # Try to create the table using a minimal write operation with preactions
            # Use a single-row DataFrame with explicit values
            from pyspark.sql import Row
            dummy_row = Row(
                normalized_name="__TEMP_CREATE__",
                rxnorm_code=None,
                rxnorm_system=None,
                medication_name=None,
                confidence_score=None,
                enrichment_source=None,
                created_at=None,
                updated_at=None
            )
            
            # Create DataFrame with explicit schema
            dummy_df = spark_session.createDataFrame([dummy_row], schema)
            dummy_dynamic_frame = DynamicFrame.fromDF(dummy_df, glue_context, "dummy_lookup_frame")
            
            # Write with CREATE TABLE in preactions and DELETE in postactions
            if get_redshift_connection_options:
                connection_options = get_redshift_connection_options(
                    connection_name=database_config.redshift_connection,
                    database=database_config.redshift_database,
                    s3_temp_dir=database_config.s3_temp_dir,
                    dbtable="public.medication_code_lookup",
                    preactions=create_table_sql,
                    postactions="DELETE FROM public.medication_code_lookup WHERE normalized_name = '__TEMP_CREATE__';"
                )
            else:
                connection_options = {
                    "redshiftTmpDir": database_config.s3_temp_dir,
                    "useConnectionProperties": "true",
                    "dbtable": "public.medication_code_lookup",
                    "connectionName": database_config.redshift_connection,
                    "preactions": create_table_sql,
                    "postactions": "DELETE FROM public.medication_code_lookup WHERE normalized_name = '__TEMP_CREATE__';"
                }
            
            glue_context.write_dynamic_frame.from_options(
                frame=dummy_dynamic_frame,
                connection_type="redshift",
                connection_options=connection_options,
                transformation_ctx="create_lookup_table"
            )
            
            logger.info("✅ Successfully created medication_code_lookup table")
            return True
        except Exception as create_error:
            logger.warning(f"Could not create lookup table: {str(create_error)}")
            logger.warning("Table may already exist or there may be a permission issue")
            logger.warning("Enrichment will continue - lookup table caching may not work until table is created manually")
            return False


def check_lookup_table_exists(
    glue_context: GlueContext,
    database_config
) -> bool:
    """
    Check if lookup table exists without creating it
    
    Returns:
        True if table exists, False otherwise
    """
    try:
        if get_redshift_connection_options:
            connection_options = get_redshift_connection_options(
                connection_name=database_config.redshift_connection,
                database=database_config.redshift_database,
                s3_temp_dir=database_config.s3_temp_dir,
                dbtable="public.medication_code_lookup"
            )
        else:
            connection_options = {
                "redshiftTmpDir": database_config.s3_temp_dir,
                "useConnectionProperties": "true",
                "dbtable": "public.medication_code_lookup",
                "connectionName": database_config.redshift_connection,
            }
        
        lookup_df = glue_context.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options=connection_options,
            transformation_ctx="check_lookup_table"
        ).toDF()
        return True
    except Exception:
        return False

