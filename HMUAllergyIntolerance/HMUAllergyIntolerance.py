from datetime import datetime
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, BooleanType, DecimalType, IntegerType
import json
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
TABLE_NAME = "allergyintolerance"
REDSHIFT_CONNECTION = "Redshift connection"
S3_TEMP_DIR = "s3://aws-glue-assets-442042533707-us-east-2/temporary/"

def transform_main_allergy_intolerance_data(df):
    """Transform the main allergy intolerance data"""
    logger.info("Transforming main allergy intolerance data...")

    available_columns = df.columns
    logger.info(f"Available columns: {available_columns}")

    select_columns = [
        F.col("id").alias("allergy_intolerance_id"),
        F.col("resourceType").alias("resource_type"),

        # Clinical Status
        F.when(F.col("clinicalStatus").isNotNull(),
               F.when(F.col("clinicalStatus").getField("text").isNotNull(),
                      F.col("clinicalStatus").getField("text"))
               .otherwise(
                   F.when(F.size(F.col("clinicalStatus").getField("coding")) > 0,
                          F.col("clinicalStatus").getField("coding").getItem(0).getField("display"))
                   .otherwise(None)
               )
              ).otherwise(None).alias("clinical_status"),

        F.when(F.col("clinicalStatus").isNotNull() &
               (F.size(F.col("clinicalStatus").getField("coding")) > 0),
               F.col("clinicalStatus").getField("coding").getItem(0).getField("code")
              ).otherwise(None).alias("clinical_status_code"),

        # Verification Status
        F.when(F.col("verificationStatus").isNotNull(),
               F.when(F.col("verificationStatus").getField("text").isNotNull(),
                      F.col("verificationStatus").getField("text"))
               .otherwise(
                   F.when(F.size(F.col("verificationStatus").getField("coding")) > 0,
                          F.col("verificationStatus").getField("coding").getItem(0).getField("display"))
                   .otherwise(None)
               )
              ).otherwise(None).alias("verification_status"),

        F.when(F.col("verificationStatus").isNotNull() &
               (F.size(F.col("verificationStatus").getField("coding")) > 0),
               F.col("verificationStatus").getField("coding").getItem(0).getField("code")
              ).otherwise(None).alias("verification_status_code"),

        # Type, Category, Criticality
        F.col("type").alias("allergy_type"),
        F.when(F.size(F.col("category")) > 0,
               F.col("category").getItem(0)
              ).otherwise(None).alias("category"),
        F.col("criticality").alias("criticality"),

        # Code information
        F.when(F.col("code").isNotNull(),
               F.col("code").getField("text")
              ).otherwise(None).alias("code_text"),

        # Patient reference
        F.when(F.col("patient").isNotNull(),
               F.regexp_extract(F.col("patient").getField("reference"), "Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),

        # Dates
        F.to_date(F.col("recordedDate"), "yyyy-MM-dd").alias("recorded_date"),

        # Meta information
        F.when(F.col("meta").isNotNull(),
               F.col("meta").getField("versionId")
              ).otherwise(None).alias("meta_version_id"),

        F.when(F.col("meta").isNotNull(),
               # Handle meta.lastUpdated with multiple possible formats
               F.coalesce(
                   F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
                   F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'"),
                   F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                   F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
                   F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                   F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss")
               )
              ).otherwise(None).alias("meta_last_updated"),

        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ]

    main_df = df.select(*select_columns).filter(
        F.col("allergy_intolerance_id").isNotNull()
    )

    return main_df

def transform_allergy_intolerance_codings(df):
    """Transform allergy intolerance codings"""
    logger.info("Transforming allergy intolerance codings...")

    if "code" not in df.columns:
        logger.warning("code column not found")
        return spark.createDataFrame([], df.select(F.col("id").alias("allergy_intolerance_id")).schema
                                     .add("system", StringType())
                                     .add("code", StringType())
                                     .add("display", StringType()))

    codings_df = df.select(
        F.col("id").alias("allergy_intolerance_id"),
        F.col("code.coding").alias("coding_array")
    ).filter(
        F.col("coding_array").isNotNull() & (F.size(F.col("coding_array")) > 0)
    )

    codings_exploded = codings_df.select(
        F.col("allergy_intolerance_id"),
        F.explode(F.col("coding_array")).alias("coding_item")
    )

    codings_final = codings_exploded.select(
        F.col("allergy_intolerance_id"),
        F.col("coding_item.system").alias("system"),
        F.col("coding_item.code").alias("code"),
        F.col("coding_item.display").alias("display")
    ).filter(
        F.col("system").isNotNull() | F.col("code").isNotNull() | F.col("display").isNotNull()
    )

    return codings_final

def transform_allergy_intolerance_extensions(df):
    """Transform allergy intolerance extensions"""
    logger.info("Transforming allergy intolerance extensions...")

    if "extension" not in df.columns:
        logger.warning("extension column not found")
        return spark.createDataFrame([], df.select(F.col("id").alias("allergy_intolerance_id")).schema
                                     .add("extension_url", StringType())
                                     .add("value_string", StringType())
                                     .add("value_reference", StringType()))

    extensions_df = df.select(
        F.col("id").alias("allergy_intolerance_id"),
        F.col("extension").alias("extension_array")
    ).filter(
        F.col("extension_array").isNotNull() & (F.size(F.col("extension_array")) > 0)
    )

    extensions_exploded = extensions_df.select(
        F.col("allergy_intolerance_id"),
        F.explode(F.col("extension_array")).alias("extension_item")
    )

    extensions_final = extensions_exploded.select(
        F.col("allergy_intolerance_id"),
        F.col("extension_item.url").alias("extension_url"),
        F.col("extension_item.valueString").alias("value_string"),
        F.when(F.col("extension_item.valueReference").isNotNull(),
               F.col("extension_item.valueReference.reference")
              ).otherwise(None).alias("value_reference")
    ).filter(
        F.col("extension_url").isNotNull()
    )

    return extensions_final

def create_redshift_tables_sql():
    return """
    DROP TABLE IF EXISTS public.allergy_intolerances CASCADE;
    CREATE TABLE public.allergy_intolerances (
        allergy_intolerance_id VARCHAR(255) PRIMARY KEY,
        resource_type VARCHAR(50),
        clinical_status VARCHAR(50),
        clinical_status_code VARCHAR(50),
        verification_status VARCHAR(50),
        verification_status_code VARCHAR(50),
        allergy_type VARCHAR(50),
        category VARCHAR(50),
        criticality VARCHAR(50),
        code_text VARCHAR(500),
        patient_id VARCHAR(255),
        recorded_date DATE,
        meta_version_id VARCHAR(50),
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) SORTKEY (allergy_intolerance_id, patient_id);
    """

def create_allergy_codings_table_sql():
    return """
    DROP TABLE IF EXISTS public.allergy_intolerance_codings CASCADE;
    CREATE TABLE public.allergy_intolerance_codings (
        allergy_intolerance_id VARCHAR(255),
        \"system\" VARCHAR(255),
        code VARCHAR(100),
        display VARCHAR(500)
    ) SORTKEY (allergy_intolerance_id, \"system\");
    """

def create_allergy_extensions_table_sql():
    return """
    DROP TABLE IF EXISTS public.allergy_intolerance_extensions CASCADE;
    CREATE TABLE public.allergy_intolerance_extensions (
        allergy_intolerance_id VARCHAR(255),
        extension_url VARCHAR(500),
        value_string TEXT,
        value_reference VARCHAR(255)
    ) SORTKEY (allergy_intolerance_id, extension_url);
    """

def write_to_redshift(dynamic_frame, table_name, preactions=""):
    logger.info(f"Writing {table_name} to Redshift...")
    logger.info(f"🔧 Preactions SQL for {table_name}:\\n{preactions}")
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
    start_time = datetime.now()
    try:
        logger.info("="*80)
        logger.info("🚀 STARTING FHIR ALLERGY INTOLERANCE ETL PROCESS")
        logger.info(f"⏰ Job started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        allergy_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=DATABASE_NAME,
            table_name=TABLE_NAME,
            transformation_ctx="AWSGlueDataCatalog_allergy_intolerance_node"
        )

        allergy_df = allergy_dynamic_frame.toDF()

        # TESTING MODE: Sample data for quick testing
        USE_SAMPLE = False  # Set to True for testing with limited data
        SAMPLE_SIZE = 1000

        if USE_SAMPLE:
            logger.info(f"⚠️  TESTING MODE: Sampling {SAMPLE_SIZE} records for quick testing")
            logger.info("⚠️  Set USE_SAMPLE = False for production runs")
            allergy_df = allergy_df.limit(SAMPLE_SIZE)
        else:
            logger.info("✅ Processing full dataset")

        total_records = allergy_df.count()
        logger.info(f"📊 Read {total_records:,} raw allergy intolerance records")

        if total_records == 0:
            logger.warning("No records found. Exiting job.")
            job.commit()
            return

        main_allergy_df = transform_main_allergy_intolerance_data(allergy_df)
        main_count = main_allergy_df.count()
        logger.info(f"✅ Transformed {main_count:,} main allergy intolerance records")

        codings_df = transform_allergy_intolerance_codings(allergy_df)
        codings_count = codings_df.count()
        logger.info(f"✅ Transformed {codings_count:,} allergy intolerance coding records")

        extensions_df = transform_allergy_intolerance_extensions(allergy_df)
        extensions_count = extensions_df.count()
        logger.info(f"✅ Transformed {extensions_count:,} allergy intolerance extension records")

        main_dynamic_frame = DynamicFrame.fromDF(main_allergy_df, glueContext, "main_allergy_dynamic_frame")
        codings_dynamic_frame = DynamicFrame.fromDF(codings_df, glueContext, "codings_dynamic_frame")
        extensions_dynamic_frame = DynamicFrame.fromDF(extensions_df, glueContext, "extensions_dynamic_frame")

        main_resolved_frame = main_dynamic_frame.resolveChoice(specs=[('allergy_intolerance_id', 'cast:string')])
        codings_resolved_frame = codings_dynamic_frame.resolveChoice(specs=[('allergy_intolerance_id', 'cast:string')])
        extensions_resolved_frame = extensions_dynamic_frame.resolveChoice(specs=[('allergy_intolerance_id', 'cast:string')])

        write_to_redshift(main_resolved_frame, "allergy_intolerances", create_redshift_tables_sql())
        write_to_redshift(codings_resolved_frame, "allergy_intolerance_codings", create_allergy_codings_table_sql())
        write_to_redshift(extensions_resolved_frame, "allergy_intolerance_extensions", create_allergy_extensions_table_sql())

        end_time = datetime.now()
        if USE_SAMPLE:
            logger.info("⚠️  WARNING: THIS WAS A TEST RUN WITH SAMPLED DATA")
            logger.info(f"⚠️  Only {SAMPLE_SIZE} records were processed")
            logger.info("⚠️  Set USE_SAMPLE = False for production runs")
        logger.info(f"🎉 ETL PROCESS COMPLETED SUCCESSFULLY in {end_time - start_time}")

    except Exception as e:
        logger.error(f"❌ ETL PROCESS FAILED: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
    job.commit()