from datetime import datetime
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, BooleanType
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
TABLE_NAME = "diagnosticreport"
REDSHIFT_CONNECTION = "Redshift connection"
S3_TEMP_DIR = "s3://aws-glue-assets-442042533707-us-east-2/temporary/"

def transform_main_diagnostic_report_data(df):
    """Transform the main diagnostic report data"""
    logger.info("Transforming main diagnostic report data...")
    
    select_columns = [
        F.col("id").alias("diagnostic_report_id"),
        F.col("status").alias("status"),
        F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1).alias("patient_id"),
        F.regexp_extract(F.col("encounter").getField("reference"), r"Encounter/(.+)", 1).alias("encounter_id"),
        F.to_timestamp(F.col("effectiveDateTime")).alias("effective_date_time"),
        F.to_timestamp(F.col("issued")).alias("issued"),
        F.col("code").getField("text").alias("code_text"),
        F.col("meta").getField("versionId").alias("meta_version_id"),
        F.to_timestamp(F.col("meta").getField("lastUpdated")).alias("meta_last_updated"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ]
    
    main_df = df.select(*select_columns).filter(
        F.col("diagnostic_report_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return main_df

def transform_diagnostic_report_categories(df):
    """Transform diagnostic report categories"""
    logger.info("Transforming diagnostic report categories...")
    
    if "category" not in df.columns:
        return spark.createDataFrame([], schema="diagnostic_report_id string, category_code string, category_system string, category_display string")
    
    return df.select(
        F.col("id").alias("diagnostic_report_id"),
        F.explode(F.col("category")).alias("category_item")
    ).select(
        "diagnostic_report_id",
        F.explode(F.col("category_item").getField("coding")).alias("coding_item")
    ).select(
        "diagnostic_report_id",
        F.col("coding_item").getField("code").alias("category_code"),
        F.col("coding_item").getField("system").alias("category_system"),
        F.col("coding_item").getField("display").alias("category_display")
    )

def transform_diagnostic_report_codes(df):
    """Transform diagnostic report codes"""
    logger.info("Transforming diagnostic report codes...")
    
    if "code.coding" not in df.columns:
        return spark.createDataFrame([], schema="diagnostic_report_id string, code_code string, code_system string, code_display string")

    return df.select(
        F.col("id").alias("diagnostic_report_id"),
        F.explode(F.col("code").getField("coding")).alias("coding_item")
    ).select(
        "diagnostic_report_id",
        F.col("coding_item").getField("code").alias("code_code"),
        F.col("coding_item").getField("system").alias("code_system"),
        F.col("coding_item").getField("display").alias("code_display")
    )

def transform_diagnostic_report_performers(df):
    """Transform diagnostic report performers"""
    logger.info("Transforming diagnostic report performers...")
    
    if "performer" not in df.columns:
        return spark.createDataFrame([], schema="diagnostic_report_id string, performer_id string")
    
    return df.select(
        F.col("id").alias("diagnostic_report_id"),
        F.explode(F.col("performer")).alias("performer_item")
    ).select(
        "diagnostic_report_id",
        F.regexp_extract(F.col("performer_item").getField("reference"), r"Organization/(.+)", 1).alias("performer_id")
    )

def transform_diagnostic_report_results(df):
    """Transform diagnostic report results"""
    logger.info("Transforming diagnostic report results...")
    
    if "result" not in df.columns:
        return spark.createDataFrame([], schema="diagnostic_report_id string, result_id string")
        
    return df.select(
        F.col("id").alias("diagnostic_report_id"),
        F.explode(F.col("result")).alias("result_item")
    ).select(
        "diagnostic_report_id",
        F.regexp_extract(F.col("result_item").getField("reference"), r"Observation/(.+)", 1).alias("result_id")
    )

def transform_diagnostic_report_media(df):
    """Transform diagnostic report media"""
    logger.info("Transforming diagnostic report media...")

    if "media" not in df.columns:
        return spark.createDataFrame([], schema="diagnostic_report_id string, media_link_id string")

    return df.select(
        F.col("id").alias("diagnostic_report_id"),
        F.explode(F.col("media")).alias("media_item")
    ).select(
        "diagnostic_report_id",
        F.regexp_extract(F.col("media_item").getField("link").getField("reference"), r"Media/(.+)", 1).alias("media_link_id")
    )

def create_redshift_tables_sql():
    return """
    DROP TABLE IF EXISTS public.diagnostic_reports CASCADE;
    CREATE TABLE public.diagnostic_reports (
        diagnostic_report_id VARCHAR(255) PRIMARY KEY,
        patient_id VARCHAR(255) NOT NULL,
        encounter_id VARCHAR(255),
        status VARCHAR(50),
        effective_date_time TIMESTAMP,
        issued TIMESTAMP,
        code_text VARCHAR(MAX),
        meta_version_id VARCHAR(50),
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, effective_date_time);
    """

def create_diagnostic_report_categories_table_sql():
    return """
    DROP TABLE IF EXISTS public.diagnostic_report_categories CASCADE;
    CREATE TABLE public.diagnostic_report_categories (
        diagnostic_report_id VARCHAR(255),
        category_code VARCHAR(50),
        category_system VARCHAR(255),
        category_display VARCHAR(255)
    ) SORTKEY (diagnostic_report_id);
    """

def create_diagnostic_report_codes_table_sql():
    return """
    DROP TABLE IF EXISTS public.diagnostic_report_codes CASCADE;
    CREATE TABLE public.diagnostic_report_codes (
        diagnostic_report_id VARCHAR(255),
        code_code VARCHAR(50),
        code_system VARCHAR(255),
        code_display VARCHAR(255)
    ) SORTKEY (diagnostic_report_id);
    """

def create_diagnostic_report_performers_table_sql():
    return """
    DROP TABLE IF EXISTS public.diagnostic_report_performers CASCADE;
    CREATE TABLE public.diagnostic_report_performers (
        diagnostic_report_id VARCHAR(255),
        performer_id VARCHAR(255)
    ) SORTKEY (diagnostic_report_id);
    """

def create_diagnostic_report_results_table_sql():
    return """
    DROP TABLE IF EXISTS public.diagnostic_report_results CASCADE;
    CREATE TABLE public.diagnostic_report_results (
        diagnostic_report_id VARCHAR(255),
        result_id VARCHAR(255)
    ) SORTKEY (diagnostic_report_id);
    """

def create_diagnostic_report_media_table_sql():
    return """
    DROP TABLE IF EXISTS public.diagnostic_report_media CASCADE;
    CREATE TABLE public.diagnostic_report_media (
        diagnostic_report_id VARCHAR(255),
        media_link_id VARCHAR(255)
    ) SORTKEY (diagnostic_report_id);
    """

def write_to_redshift(dynamic_frame, table_name, preactions=""):
    """Write DynamicFrame to Redshift using JDBC connection"""
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
        logger.info(f"✅ Successfully wrote {table_name} to Redshift")
    except Exception as e:
        logger.error(f"❌ Failed to write {table_name} to Redshift: {str(e)}")
        raise e

def main():
    """Main ETL process"""
    start_time = datetime.now()
    try:
        logger.info("🚀 STARTING FHIR DIAGNOSTICREPORT ETL PROCESS")
        
        dyf = glueContext.create_dynamic_frame.from_catalog(
            database=DATABASE_NAME, 
            table_name=TABLE_NAME, 
            transformation_ctx="AWSGlueDataCatalog"
        )
        
        df = dyf.toDF()
        
        if df.count() == 0:
            logger.warning("No data found in source. Exiting job.")
            return

        main_df = transform_main_diagnostic_report_data(df)
        categories_df = transform_diagnostic_report_categories(df)
        codes_df = transform_diagnostic_report_codes(df)
        performers_df = transform_diagnostic_report_performers(df)
        results_df = transform_diagnostic_report_results(df)
        media_df = transform_diagnostic_report_media(df)

        main_dyf = DynamicFrame.fromDF(main_df, glueContext, "main_dyf")
        categories_dyf = DynamicFrame.fromDF(categories_df, glueContext, "categories_dyf")
        codes_dyf = DynamicFrame.fromDF(codes_df, glueContext, "codes_dyf")
        performers_dyf = DynamicFrame.fromDF(performers_df, glueContext, "performers_dyf")
        results_dyf = DynamicFrame.fromDF(results_df, glueContext, "results_dyf")
        media_dyf = DynamicFrame.fromDF(media_df, glueContext, "media_dyf")

        write_to_redshift(main_dyf, "diagnostic_reports", create_redshift_tables_sql())
        write_to_redshift(categories_dyf, "diagnostic_report_categories", create_diagnostic_report_categories_table_sql())
        write_to_redshift(codes_dyf, "diagnostic_report_codes", create_diagnostic_report_codes_table_sql())
        write_to_redshift(performers_dyf, "diagnostic_report_performers", create_diagnostic_report_performers_table_sql())
        write_to_redshift(results_dyf, "diagnostic_report_results", create_diagnostic_report_results_table_sql())
        write_to_redshift(media_dyf, "diagnostic_report_media", create_diagnostic_report_media_table_sql())

        end_time = datetime.now()
        logger.info(f"🎉 ETL PROCESS COMPLETED SUCCESSFULLY! Total processing time: {end_time - start_time}")

    except Exception as e:
        logger.error(f"❌ ETL PROCESS FAILED: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
    job.commit()
