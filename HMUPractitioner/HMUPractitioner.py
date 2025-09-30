from datetime import datetime
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, DateType, BooleanType, IntegerType, DecimalType
import json
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
catalog_nm = "glue_catalog"

s3_bucket = "s3://7df690fd40c734f8937daf02f39b2ec3-457560472834-group/datalake/hmu_fhir_data_store_836e877666cebf177ce6370ec1478a92_healthlake_view/"
ahl_database = "hmu_fhir_data_store_836e877666cebf177ce6370ec1478a92_healthlake_view"
tableCatalogId = "457560472834"  # AHL service account
s3_output_bucket = "s3://healthlake-glue-output-2025"

spark = (SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{catalog_nm}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{catalog_nm}.warehouse", s3_bucket)
    .config(f"spark.sql.catalog.{catalog_nm}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{catalog_nm}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.glue.lakeformation-enabled", "true")
    .config("spark.sql.catalog.glue_catalog.glue.id", tableCatalogId)
    .getOrCreate())
sc = spark.sparkContext
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Configuration - Updated to use S3/Iceberg instead of Glue Catalog
DATABASE_NAME = ahl_database  # Using AHL Iceberg database
TABLE_NAME = "practitioner"
REDSHIFT_CONNECTION = "Redshift connection"
S3_TEMP_DIR = "s3://aws-glue-assets-442042533707-us-east-2/temporary/"

# Note: We now read data from S3 using Iceberg catalog instead of Glue Catalog
# This provides better performance and direct access to S3 data

def convert_to_json_string(field):
    """Convert complex data to JSON strings to avoid nested structures"""
    if field is None:
        return None
    try:
        if isinstance(field, str):
            return field
        else:
            return json.dumps(field, ensure_ascii=False, default=str)
    except (TypeError, ValueError) as e:
        logger.warning(f"JSON serialization failed for field: {str(e)}")
        return str(field)

# Define UDF globally so it can be used in all transformation functions
convert_to_json_udf = F.udf(convert_to_json_string, StringType())

def transform_main_practitioner_data(df):
    """Transform the main practitioner data"""
    logger.info("Transforming main practitioner data...")
    
    available_columns = df.columns
    logger.info(f"Available columns: {available_columns}")
    
    select_columns = [
        F.col("id").alias("practitioner_id"),
        F.col("resourceType").alias("resource_type"),
        F.col("active").alias("active"),
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
        F.col("practitioner_id").isNotNull()
    )
    
    return main_df


def transform_practitioner_names(df):
    """Transform practitioner names"""
    logger.info("Transforming practitioner names...")

    if "name" not in df.columns:
        logger.warning("name column not found")
        return spark.createDataFrame([], df.select(F.col("id").alias("practitioner_id")).schema
                                     .add("text", StringType())
                                     .add("family", StringType())
                                     .add("given", StringType()))

    names_df = df.select(
        F.col("id").alias("practitioner_id"),
        F.explode(F.col("name")).alias("name_item")
    )
    
    names_final = names_df.select(
        F.col("practitioner_id"),
        F.col("name_item.text").alias("text"),
        F.col("name_item.family").alias("family"),
        F.concat_ws(" ", F.col("name_item.given")).alias("given")
    ).filter(
        F.col("text").isNotNull()
    )
    
    return names_final

def transform_practitioner_telecoms(df):
    """Transform practitioner telecoms"""
    logger.info("Transforming practitioner telecoms...")

    if "telecom" not in df.columns:
        logger.warning("telecom column not found")
        return spark.createDataFrame([], df.select(F.col("id").alias("practitioner_id")).schema
                                     .add("system", StringType())
                                     .add("value", StringType()))

    telecoms_df = df.select(
        F.col("id").alias("practitioner_id"),
        F.explode(F.col("telecom")).alias("telecom_item")
    )
    
    telecoms_final = telecoms_df.select(
        F.col("practitioner_id"),
        F.col("telecom_item.system").alias("system"),
        F.col("telecom_item.value").alias("value")
    ).filter(
        F.col("value").isNotNull()
    )
    
    return telecoms_final

def transform_practitioner_addresses(df):
    """Transform practitioner addresses"""
    logger.info("Transforming practitioner addresses...")

    if "address" not in df.columns:
        logger.warning("address column not found")
        return spark.createDataFrame([], df.select(F.col("id").alias("practitioner_id")).schema
                                     .add("line", StringType())
                                     .add("city", StringType())
                                     .add("state", StringType())
                                     .add("postal_code", StringType()))

    addresses_df = df.select(
        F.col("id").alias("practitioner_id"),
        F.explode(F.col("address")).alias("address_item")
    )
    
    addresses_final = addresses_df.select(
        F.col("practitioner_id"),
        F.concat_ws(", ", F.col("address_item.line")).alias("line"),
        F.col("address_item.city").alias("city"),
        F.col("address_item.state").alias("state"),
        F.col("address_item.postalCode").alias("postal_code")
    )
    
    return addresses_final

def create_redshift_tables_sql():
    return """
    DROP TABLE IF EXISTS public.practitioners CASCADE;
    CREATE TABLE public.practitioners (
        practitioner_id VARCHAR(255) PRIMARY KEY,
        resource_type VARCHAR(50),
        active BOOLEAN,
        meta_version_id VARCHAR(50),
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) SORTKEY (practitioner_id);
    """


def create_practitioner_names_table_sql():
    return """
    DROP TABLE IF EXISTS public.practitioner_names CASCADE;
    CREATE TABLE public.practitioner_names (
        practitioner_id VARCHAR(255),
        text VARCHAR(500),
        family VARCHAR(255),
        given VARCHAR(255)
    ) SORTKEY (practitioner_id, family);
    """

def create_practitioner_telecoms_table_sql():
    return """
    DROP TABLE IF EXISTS public.practitioner_telecoms CASCADE;
    CREATE TABLE public.practitioner_telecoms (
        practitioner_id VARCHAR(255),
        "system" VARCHAR(50),
        value VARCHAR(255)
    ) SORTKEY (practitioner_id, "system");
    """

def create_practitioner_addresses_table_sql():
    return """
    DROP TABLE IF EXISTS public.practitioner_addresses CASCADE;
    CREATE TABLE public.practitioner_addresses (
        practitioner_id VARCHAR(255),
        line VARCHAR(500),
        city VARCHAR(100),
        state VARCHAR(50),
        postal_code VARCHAR(20)
    ) SORTKEY (practitioner_id, state);
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
        logger.info("🚀 STARTING FHIR PRACTITIONER ETL PROCESS")
        logger.info(f"⏰ Job started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Use Iceberg to read data from S3
        table_name_full = f"{catalog_nm}.{DATABASE_NAME}.{TABLE_NAME}"
        logger.info(f"Reading from table: {table_name_full}")
        df_raw = spark.table(table_name_full)
        
        practitioner_df = df_raw

        
        # TESTING MODE: Sample data for quick testing

        
        # Set to True to process only a sample of records

        
        USE_SAMPLE = False  # Set to True for testing with limited data

        
        SAMPLE_SIZE = 1000

        
        

        
        if USE_SAMPLE:

        
            logger.info(f"⚠️  TESTING MODE: Sampling {SAMPLE_SIZE} records for quick testing")

        
            logger.info("⚠️  Set USE_SAMPLE = False for production runs")

        
            practitioner_df = practitioner_df.limit(SAMPLE_SIZE)

        
        else:

        
            logger.info("✅ Processing full dataset")

        
            practitioner_df = practitioner_df

        total_records = practitioner_df.count()
        logger.info(f"📊 Read {total_records:,} raw practitioner records")

        if total_records == 0:
            logger.warning("No records found. Exiting job.")
            job.commit()
            return
        
        main_practitioner_df = transform_main_practitioner_data(practitioner_df)
        main_count = main_practitioner_df.count()
        logger.info(f"✅ Transformed {main_count:,} main practitioner records")
        
        practitioner_names_df = transform_practitioner_names(practitioner_df)
        names_count = practitioner_names_df.count()
        logger.info(f"✅ Transformed {names_count:,} practitioner name records")
        
        practitioner_telecoms_df = transform_practitioner_telecoms(practitioner_df)
        telecoms_count = practitioner_telecoms_df.count()
        logger.info(f"✅ Transformed {telecoms_count:,} practitioner telecom records")
        
        practitioner_addresses_df = transform_practitioner_addresses(practitioner_df)
        addresses_count = practitioner_addresses_df.count()
        logger.info(f"✅ Transformed {addresses_count:,} practitioner address records")

        main_dynamic_frame = DynamicFrame.fromDF(main_practitioner_df, glueContext, "main_practitioner_dynamic_frame")
        names_dynamic_frame = DynamicFrame.fromDF(practitioner_names_df, glueContext, "names_dynamic_frame")
        telecoms_dynamic_frame = DynamicFrame.fromDF(practitioner_telecoms_df, glueContext, "telecoms_dynamic_frame")
        addresses_dynamic_frame = DynamicFrame.fromDF(practitioner_addresses_df, glueContext, "addresses_dynamic_frame")
        
        main_resolved_frame = main_dynamic_frame.resolveChoice(specs=[('practitioner_id', 'cast:string')])
        names_resolved_frame = names_dynamic_frame.resolveChoice(specs=[('practitioner_id', 'cast:string')])
        telecoms_resolved_frame = telecoms_dynamic_frame.resolveChoice(specs=[('practitioner_id', 'cast:string')])
        addresses_resolved_frame = addresses_dynamic_frame.resolveChoice(specs=[('practitioner_id', 'cast:string')])

        write_to_redshift(main_resolved_frame, "practitioners", create_redshift_tables_sql())
        write_to_redshift(names_resolved_frame, "practitioner_names", create_practitioner_names_table_sql())
        write_to_redshift(telecoms_resolved_frame, "practitioner_telecoms", create_practitioner_telecoms_table_sql())
        write_to_redshift(addresses_resolved_frame, "practitioner_addresses", create_practitioner_addresses_table_sql())
        
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
