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
TABLE_NAME = "procedure"
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

def transform_main_procedure_data(df):
    """Transform the main procedure data"""
    logger.info("Transforming main procedure data...")
    
    available_columns = df.columns
    logger.info(f"Available columns: {available_columns}")
    
    select_columns = [
        F.col("id").alias("procedure_id"),
        F.col("resourceType").alias("resource_type"),
        F.col("status").alias("status"),
        F.when(F.col("subject").isNotNull(), 
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        F.when(F.col("code").isNotNull(),
               F.col("code").getField("text")
              ).otherwise(None).alias("code_text"),
        # Handle performedDateTime with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("performedDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("performedDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("performedDateTime"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("performedDateTime"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("performedDateTime"), "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp(F.col("performedDateTime"), "yyyy-MM-dd")
        ).alias("performed_date_time"),
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
        F.col("procedure_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return main_df

def transform_procedure_identifiers(df):
    """Transform procedure identifiers"""
    logger.info("Transforming procedure identifiers...")
    
    if "identifier" not in df.columns:
        logger.warning("identifier column not found")
        return spark.createDataFrame([], df.select(F.col("id").alias("procedure_id")).schema.add("identifier_system", StringType()).add("identifier_value", StringType()))

    identifiers_df = df.select(
        F.col("id").alias("procedure_id"),
        F.explode(F.col("identifier")).alias("identifier_item")
    ).filter(
        F.col("identifier_item").isNotNull()
    )
    
    identifiers_final = identifiers_df.select(
        F.col("procedure_id"),
        F.col("identifier_item.system").alias("identifier_system"),
        F.col("identifier_item.value").alias("identifier_value")
    ).filter(
        F.col("identifier_value").isNotNull()
    )
    
    return identifiers_final

def transform_procedure_code_codings(df):
    """Transform procedure code codings"""
    logger.info("Transforming procedure code codings...")

    if "code" not in df.columns or "coding" not in df.select("code.*").columns:
        logger.warning("code.coding column not found")
        return spark.createDataFrame([], df.select(F.col("id").alias("procedure_id")).schema.add("code_system", StringType()).add("code_code", StringType()).add("code_display", StringType()))

    codings_df = df.select(
        F.col("id").alias("procedure_id"),
        F.explode(F.col("code.coding")).alias("coding_item")
    )
    
    codings_final = codings_df.select(
        F.col("procedure_id"),
        F.col("coding_item.system").alias("code_system"),
        F.col("coding_item.code").alias("code_code"),
        F.col("coding_item.display").alias("code_display")
    ).filter(
        F.col("code_code").isNotNull()
    )
    
    return codings_final

def create_redshift_tables_sql():
    return """
    DROP TABLE IF EXISTS public.procedures CASCADE;
    CREATE TABLE public.procedures (
        procedure_id VARCHAR(255) PRIMARY KEY,
        resource_type VARCHAR(50),
        status VARCHAR(50),
        patient_id VARCHAR(255) NOT NULL,
        code_text VARCHAR(500),
        performed_date_time TIMESTAMP,
        meta_version_id VARCHAR(50),
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, performed_date_time);
    """

def create_procedure_identifiers_table_sql():
    return """
    DROP TABLE IF EXISTS public.procedure_identifiers CASCADE;
    CREATE TABLE public.procedure_identifiers (
        procedure_id VARCHAR(255),
        identifier_system VARCHAR(255),
        identifier_value VARCHAR(255)
    ) SORTKEY (procedure_id, identifier_system);
    """

def create_procedure_code_codings_table_sql():
    return """
    DROP TABLE IF EXISTS public.procedure_code_codings CASCADE;
    CREATE TABLE public.procedure_code_codings (
        procedure_id VARCHAR(255),
        code_system VARCHAR(255),
        code_code VARCHAR(100),
        code_display VARCHAR(500)
    ) SORTKEY (procedure_id, code_system);
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
        logger.info("🚀 STARTING FHIR PROCEDURE ETL PROCESS")
        logger.info(f"⏰ Job started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Use Iceberg to read data from S3
        table_name_full = f"{catalog_nm}.{DATABASE_NAME}.{TABLE_NAME}"
        logger.info(f"Reading from table: {table_name_full}")
        df_raw = spark.table(table_name_full)
        
        procedure_df = df_raw

        
        # TESTING MODE: Sample data for quick testing

        
        # Set to True to process only a sample of records

        
        USE_SAMPLE = False  # Set to True for testing with limited data

        
        SAMPLE_SIZE = 1000

        
        

        
        if USE_SAMPLE:

        
            logger.info(f"⚠️  TESTING MODE: Sampling {SAMPLE_SIZE} records for quick testing")

        
            logger.info("⚠️  Set USE_SAMPLE = False for production runs")

        
            procedure_df = procedure_df.limit(SAMPLE_SIZE)

        
        else:

        
            logger.info("✅ Processing full dataset")

        
            procedure_df = procedure_df

        total_records = procedure_df.count()
        logger.info(f"📊 Read {total_records:,} raw procedure records")

        if total_records == 0:
            logger.warning("No records found. Exiting job.")
            job.commit()
            return
        
        main_procedure_df = transform_main_procedure_data(procedure_df)
        main_count = main_procedure_df.count()
        logger.info(f"✅ Transformed {main_count:,} main procedure records")
        
        procedure_identifiers_df = transform_procedure_identifiers(procedure_df)
        identifiers_count = procedure_identifiers_df.count()
        logger.info(f"✅ Transformed {identifiers_count:,} procedure identifier records")
        
        procedure_code_codings_df = transform_procedure_code_codings(procedure_df)
        codings_count = procedure_code_codings_df.count()
        logger.info(f"✅ Transformed {codings_count:,} procedure code coding records")

        main_dynamic_frame = DynamicFrame.fromDF(main_procedure_df, glueContext, "main_procedure_dynamic_frame")
        identifiers_dynamic_frame = DynamicFrame.fromDF(procedure_identifiers_df, glueContext, "identifiers_dynamic_frame")
        codings_dynamic_frame = DynamicFrame.fromDF(procedure_code_codings_df, glueContext, "codings_dynamic_frame")
        
        main_resolved_frame = main_dynamic_frame.resolveChoice(specs=[('procedure_id', 'cast:string')])
        identifiers_resolved_frame = identifiers_dynamic_frame.resolveChoice(specs=[('procedure_id', 'cast:string')])
        codings_resolved_frame = codings_dynamic_frame.resolveChoice(specs=[('procedure_id', 'cast:string')])

        write_to_redshift(main_resolved_frame, "procedures", create_redshift_tables_sql())
        write_to_redshift(identifiers_resolved_frame, "procedure_identifiers", create_procedure_identifiers_table_sql())
        write_to_redshift(codings_resolved_frame, "procedure_code_codings", create_procedure_code_codings_table_sql())
        
        end_time = datetime.now()
        logger.info(f"🎉 ETL PROCESS COMPLETED SUCCESSFULLY in {end_time - start_time}")
        
    except Exception as e:
        logger.error(f"❌ ETL PROCESS FAILED: {str(e)}")
        raise e

if __name__ == "__main__":
if __name__ == "__main__":
    main()
    job.commit()
