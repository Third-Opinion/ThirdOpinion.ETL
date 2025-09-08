from datetime import datetime
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, BooleanType, DecimalType, IntegerType, DoubleType

import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
DATABASE_NAME = "hmu-healthlake-database"
TABLE_NAME = "observation"
REDSHIFT_CONNECTION = "Redshift connection"
S3_TEMP_DIR = "s3://aws-glue-assets-442042533707-us-east-2/temporary/"

def transform_main_observation_data(df):
    """Transform the main observation data"""
    logger.info("Transforming main observation data...")
    
    # Drop the 'search' field
    if 'search' in df.columns:
        df = df.drop('search')

    select_columns = [
        F.col("id").alias("observation_id"),
        F.col("status").alias("status"),
        F.when(F.col("subject").isNotNull(), 
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        F.when(F.col("encounter").isNotNull(),
               F.regexp_extract(F.col("encounter").getField("reference"), r"Encounter/(.+)", 1)
              ).otherwise(None).alias("encounter_id"),
        F.to_timestamp(F.col("effectiveDateTime")).alias("effective_date_time"),
        F.to_timestamp(F.col("issued")).alias("issued"),
        
        # Code
        F.col("code.text").alias("code_text"),
        F.when(F.col("code.coding").isNotNull(), F.col("code.coding")[0].getField("system")).alias("code_system"),
        F.when(F.col("code.coding").isNotNull(), F.col("code.coding")[0].getField("code")).alias("code_code"),
        F.when(F.col("code.coding").isNotNull(), F.col("code.coding")[0].getField("display")).alias("code_display"),
        
        # Value fields
        F.col("valueString").alias("value_string"),
        F.col("valueQuantity.value").alias("value_quantity_value"),
        F.col("valueQuantity.unit").alias("value_quantity_unit"),
        F.col("valueQuantity.system").alias("value_quantity_system"),
        F.col("valueQuantity.code").alias("value_quantity_code"),
        F.col("valueCodeableConcept.text").alias("value_codeable_concept_text"),
        
        F.col("dataAbsentReason.text").alias("data_absent_reason_text"),

        F.col("meta.versionId").alias("meta_version_id"),
        F.to_timestamp(F.col("meta.lastUpdated")).alias("meta_last_updated"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ]
    
    main_df = df.select(*[c for c in select_columns if c is not None]).filter(
        F.col("observation_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return main_df

def create_observations_table_sql():
    """Generate SQL for creating observations table in Redshift"""
    return """
    DROP TABLE IF EXISTS public.observations CASCADE;
    CREATE TABLE public.observations (
        observation_id VARCHAR(255) PRIMARY KEY,
        patient_id VARCHAR(255) NOT NULL,
        encounter_id VARCHAR(255),
        status VARCHAR(50),
        effective_date_time TIMESTAMP,
        issued TIMESTAMP,
        code_text VARCHAR(1000),
        code_system VARCHAR(255),
        code_code VARCHAR(50),
        code_display VARCHAR(500),
        value_string VARCHAR(MAX),
        value_quantity_value DECIMAL(18, 5),
        value_quantity_unit VARCHAR(50),
        value_quantity_system VARCHAR(255),
        value_quantity_code VARCHAR(50),
        value_codeable_concept_text VARCHAR(1000),
        data_absent_reason_text VARCHAR(1000),
        meta_version_id VARCHAR(50),
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY(patient_id) SORTKEY(patient_id, effective_date_time);
    """

def write_to_redshift(dynamic_frame, table_name, preactions=""):
    """Write DynamicFrame to Redshift"""
    logger.info(f"Writing {table_name} to Redshift...")
    try:
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="redshift",
            connection_options={
                "redshiftTmpDir": S3_TEMP_DIR,
                "useConnectionProperties": "true",
                "dbtable": f"public.{table_name}",
                "connectionName": REDSHIFT_CONNECTION,
                "preactions": preactions
            },
            transformation_ctx=f"write_{table_name}_to_redshift"
        )
        logger.info(f"Successfully wrote {table_name} to Redshift")
    except Exception as e:
        logger.error(f"Failed to write {table_name} to Redshift: {str(e)}")
        raise e

def main():
    """Main ETL process for Observation"""
    start_time = datetime.now()
    try:
        logger.info("Starting Observation ETL process")
        
        # Read data
        observation_dyf = glueContext.create_dynamic_frame.from_catalog(
            database=DATABASE_NAME,
            table_name=TABLE_NAME,
            transformation_ctx="read_observation"
        )
        observation_df = observation_dyf.toDF()

        # Drop search field
        if 'entry' in observation_df.columns:
             observation_df = observation_df.select(F.explode("entry").alias("entry"))
             observation_df = observation_df.select("entry.resource.*")


        if 'search' in observation_df.columns:
            observation_df = observation_df.drop('search')
        
        # Transform data
        main_observation_df = transform_main_observation_data(observation_df)
        
        # Convert to DynamicFrame
        main_observation_dyf = DynamicFrame.fromDF(main_observation_df, glueContext, "main_observation_dyf")
        
        # Write to Redshift
        write_to_redshift(main_observation_dyf, "observations", create_observations_table_sql())
        
        logger.info(f"Observation ETL process finished successfully. Total time: {datetime.now() - start_time}")

    except Exception as e:
        logger.error(f"Observation ETL process failed: {e}")
        raise
    
if __name__ == "__main__":
    main()
    job.commit()
