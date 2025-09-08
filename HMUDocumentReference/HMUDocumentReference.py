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
TABLE_NAME = "documentreference"
REDSHIFT_CONNECTION = "Redshift connection"
S3_TEMP_DIR = "s3://aws-glue-assets-442042533707-us-east-2/temporary/"

def extract_patient_id_from_reference(reference_field):
    """Extract patient ID from FHIR reference format"""
    if reference_field:
        if hasattr(reference_field, 'reference'):
            reference = reference_field.reference
            if reference and "/" in reference:
                return reference.split("/")[-1]
        elif isinstance(reference_field, dict):
            reference = reference_field.get('reference')
            if reference and "/" in reference:
                return reference.split("/")[-1]
        elif isinstance(reference_field, str):
            if "/" in reference_field:
                return reference_field.split("/")[-1]
    return None

def transform_main_document_reference_data(df):
    """Transform the main document reference data"""
    logger.info("Transforming main document reference data...")
    
    available_columns = df.columns
    logger.info(f"Available columns: {available_columns}")
    
    select_columns = [
        F.col("id").alias("document_reference_id"),
        F.when(F.col("subject").isNotNull(), 
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        F.col("status").alias("status"),
        F.when(F.col("type").isNotNull() & (F.size(F.col("type.coding")) > 0),
               F.col("type.coding")[0].getField("code")
              ).otherwise(None).alias("type_code"),
        F.when(F.col("type").isNotNull() & (F.size(F.col("type.coding")) > 0),
               F.col("type.coding")[0].getField("system")
              ).otherwise(None).alias("type_system"),
        F.when(F.col("type").isNotNull() & (F.size(F.col("type.coding")) > 0),
               F.col("type.coding")[0].getField("display")
              ).otherwise(None).alias("type_display"),
        F.to_timestamp(F.col("date")).alias("date"),
        F.when(F.col("custodian").isNotNull(),
               F.regexp_extract(F.col("custodian").getField("reference"), r"Organization/(.+)", 1)
              ).otherwise(None).alias("custodian_id"),
        F.col("description").alias("description"),
        F.to_timestamp(F.col("context.period.start")).alias("context_period_start"),
        F.to_timestamp(F.col("context.period.end")).alias("context_period_end"),
        F.col("meta.versionId").alias("meta_version_id"),
        F.to_timestamp(F.col("meta.lastUpdated")).alias("meta_last_updated"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ]
    
    main_df = df.select(*select_columns).filter(
        F.col("document_reference_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return main_df

def transform_document_reference_identifiers(df):
    """Transform document reference identifiers"""
    logger.info("Transforming document reference identifiers...")
    
    if "identifier" not in df.columns:
        logger.warning("identifier column not found, returning empty DataFrame")
        return spark.createDataFrame([], schema="document_reference_id string, identifier_system string, identifier_value string")

    identifiers_df = df.select(
        F.col("id").alias("document_reference_id"),
        F.explode(F.col("identifier")).alias("identifier_item")
    ).filter(
        F.col("identifier_item").isNotNull()
    )
    
    identifiers_final = identifiers_df.select(
        F.col("document_reference_id"),
        F.col("identifier_item.system").alias("identifier_system"),
        F.col("identifier_item.value").alias("identifier_value")
    ).filter(
        F.col("identifier_value").isNotNull()
    )
    
    return identifiers_final

def transform_document_reference_categories(df):
    """Transform document reference categories"""
    logger.info("Transforming document reference categories...")
    
    if "category" not in df.columns:
        logger.warning("category column not found, returning empty DataFrame")
        return spark.createDataFrame([], schema="document_reference_id string, category_code string, category_system string, category_display string")

    categories_df = df.select(
        F.col("id").alias("document_reference_id"),
        F.explode(F.col("category")).alias("category_item")
    ).filter(
        F.col("category_item").isNotNull()
    )
    
    categories_final = categories_df.select(
        F.col("document_reference_id"),
        F.explode(F.col("category_item.coding")).alias("coding_item")
    ).select(
        F.col("document_reference_id"),
        F.col("coding_item.code").alias("category_code"),
        F.col("coding_item.system").alias("category_system"),
        F.col("coding_item.display").alias("category_display")
    ).filter(
        F.col("category_code").isNotNull()
    )
    
    return categories_final

def transform_document_reference_authors(df):
    """Transform document reference authors"""
    logger.info("Transforming document reference authors...")
    
    if "author" not in df.columns:
        logger.warning("author column not found, returning empty DataFrame")
        return spark.createDataFrame([], schema="document_reference_id string, author_id string")
    
    authors_df = df.select(
        F.col("id").alias("document_reference_id"),
        F.explode(F.col("author")).alias("author_item")
    ).filter(
        F.col("author_item").isNotNull()
    )
    
    authors_final = authors_df.select(
        F.col("document_reference_id"),
        F.regexp_extract(F.col("author_item.reference"), r"Practitioner/(.+)", 1).alias("author_id")
    ).filter(
        F.col("author_id") != ""
    )
    
    return authors_final

def transform_document_reference_content(df):
    """Transform document reference content"""
    logger.info("Transforming document reference content...")
    
    if "content" not in df.columns:
        logger.warning("content column not found, returning empty DataFrame")
        return spark.createDataFrame([], schema="document_reference_id string, attachment_content_type string, attachment_url string")

    content_df = df.select(
        F.col("id").alias("document_reference_id"),
        F.explode(F.col("content")).alias("content_item")
    ).filter(
        F.col("content_item").isNotNull()
    )
    
    content_final = content_df.select(
        F.col("document_reference_id"),
        F.col("content_item.attachment.contentType").alias("attachment_content_type"),
        F.col("content_item.attachment.url").alias("attachment_url")
    ).filter(
        F.col("attachment_url").isNotNull()
    )
    
    return content_final

def create_redshift_tables_sql():
    """Generate SQL for creating main document references table in Redshift"""
    return """
    DROP TABLE IF EXISTS public.document_references CASCADE;
    
    CREATE TABLE public.document_references (
        document_reference_id VARCHAR(255) PRIMARY KEY,
        patient_id VARCHAR(255) NOT NULL,
        status VARCHAR(50),
        type_code VARCHAR(50),
        type_system VARCHAR(255),
        type_display VARCHAR(500),
        date TIMESTAMP,
        custodian_id VARCHAR(255),
        description VARCHAR(MAX),
        context_period_start TIMESTAMP,
        context_period_end TIMESTAMP,
        meta_version_id VARCHAR(50),
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, date);
    """

def create_document_reference_identifiers_table_sql():
    """Generate SQL for creating document_reference_identifiers table"""
    return """
    DROP TABLE IF EXISTS public.document_reference_identifiers CASCADE;
    
    CREATE TABLE public.document_reference_identifiers (
        document_reference_id VARCHAR(255),
        identifier_system VARCHAR(255),
        identifier_value VARCHAR(255)
    ) SORTKEY (document_reference_id, identifier_system);
    """

def create_document_reference_categories_table_sql():
    """Generate SQL for creating document_reference_categories table"""
    return """
    DROP TABLE IF EXISTS public.document_reference_categories CASCADE;
    
    CREATE TABLE public.document_reference_categories (
        document_reference_id VARCHAR(255),
        category_code VARCHAR(50),
        category_system VARCHAR(255),
        category_display VARCHAR(255)
    ) SORTKEY (document_reference_id, category_code);
    """

def create_document_reference_authors_table_sql():
    """Generate SQL for creating document_reference_authors table"""
    return """
    DROP TABLE IF EXISTS public.document_reference_authors CASCADE;
    
    CREATE TABLE public.document_reference_authors (
        document_reference_id VARCHAR(255),
        author_id VARCHAR(255)
    ) SORTKEY (document_reference_id);
    """

def create_document_reference_content_table_sql():
    """Generate SQL for creating document_reference_content table"""
    return """
    DROP TABLE IF EXISTS public.document_reference_content CASCADE;
    
    CREATE TABLE public.document_reference_content (
        document_reference_id VARCHAR(255),
        attachment_content_type VARCHAR(100),
        attachment_url VARCHAR(MAX)
    ) SORTKEY (document_reference_id);
    """

def write_to_redshift(dynamic_frame, table_name, preactions=""):
    """Write DynamicFrame to Redshift using JDBC connection"""
    logger.info(f"Writing {table_name} to Redshift...")
    
    logger.info(f"🔧 Preactions SQL for {table_name}:")
    logger.info(preactions)
    
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
        logger.error(f"🔧 Preactions that were executed: {preactions}")
        raise e

def main():
    """Main ETL process"""
    start_time = datetime.now()
    try:
        logger.info("=" * 80)
        logger.info("🚀 STARTING FHIR DOCUMENTREFERENCE ETL PROCESS")
        logger.info("=" * 80)
        
        # Step 1: Read data from HealthLake using AWS Glue Data Catalog
        document_reference_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
                database=DATABASE_NAME, 
            table_name=TABLE_NAME, 
            transformation_ctx="AWSGlueDataCatalog_document_reference_node"
        )
        
        document_reference_df_raw = document_reference_dynamic_frame.toDF()
        total_records = document_reference_df_raw.count()
        logger.info(f"📊 Read {total_records:,} raw document reference records")

        if total_records == 0:
            logger.error("❌ No raw data found! Check the data source.")
            return

        # Step 2: Transform main document reference data
        main_document_reference_df = transform_main_document_reference_data(document_reference_df_raw)
        main_count = main_document_reference_df.count()
        logger.info(f"✅ Transformed {main_count:,} main document reference records")

        if main_count == 0:
            logger.error("❌ No main document reference records after transformation! Check filtering criteria.")
            return
            
        # Step 3: Transform multi-valued data
        doc_ref_identifiers_df = transform_document_reference_identifiers(document_reference_df_raw)
        identifiers_count = doc_ref_identifiers_df.count()
        logger.info(f"✅ Transformed {identifiers_count:,} document reference identifier records")
        
        doc_ref_categories_df = transform_document_reference_categories(document_reference_df_raw)
        categories_count = doc_ref_categories_df.count()
        logger.info(f"✅ Transformed {categories_count:,} document reference category records")
        
        doc_ref_authors_df = transform_document_reference_authors(document_reference_df_raw)
        authors_count = doc_ref_authors_df.count()
        logger.info(f"✅ Transformed {authors_count:,} document reference author records")
        
        doc_ref_content_df = transform_document_reference_content(document_reference_df_raw)
        content_count = doc_ref_content_df.count()
        logger.info(f"✅ Transformed {content_count:,} document reference content records")

        # Step 4: Convert to DynamicFrames
        main_dynamic_frame = DynamicFrame.fromDF(main_document_reference_df, glueContext, "main_doc_ref_dynamic_frame")
        identifiers_dynamic_frame = DynamicFrame.fromDF(doc_ref_identifiers_df, glueContext, "identifiers_dynamic_frame")
        categories_dynamic_frame = DynamicFrame.fromDF(doc_ref_categories_df, glueContext, "categories_dynamic_frame")
        authors_dynamic_frame = DynamicFrame.fromDF(doc_ref_authors_df, glueContext, "authors_dynamic_frame")
        content_dynamic_frame = DynamicFrame.fromDF(doc_ref_content_df, glueContext, "content_dynamic_frame")

        # Step 5: Write to Redshift
        logger.info("\n" + "=" * 50)
        logger.info("💾 STEP 5: WRITING DATA TO REDSHIFT")
        logger.info("=" * 50)
        
        document_references_table_sql = create_redshift_tables_sql()
        write_to_redshift(main_dynamic_frame, "document_references", document_references_table_sql)
        
        identifiers_table_sql = create_document_reference_identifiers_table_sql()
        write_to_redshift(identifiers_dynamic_frame, "document_reference_identifiers", identifiers_table_sql)
        
        categories_table_sql = create_document_reference_categories_table_sql()
        write_to_redshift(categories_dynamic_frame, "document_reference_categories", categories_table_sql)
        
        authors_table_sql = create_document_reference_authors_table_sql()
        write_to_redshift(authors_dynamic_frame, "document_reference_authors", authors_table_sql)
        
        content_table_sql = create_document_reference_content_table_sql()
        write_to_redshift(content_dynamic_frame, "document_reference_content", content_table_sql)

        end_time = datetime.now()
        logger.info("\n" + "=" * 80)
        logger.info("🎉 ETL PROCESS COMPLETED SUCCESSFULLY!")
        logger.info(f"⏱️  Total processing time: {end_time - start_time}")
        logger.info("=" * 80)

    except Exception as e:
        logger.error("\n" + "=" * 80)
        logger.error("❌ ETL PROCESS FAILED!")
        logger.error(f"🚨 Error: {str(e)}")
        logger.error("=" * 80)
        raise e

if __name__ == "__main__":
    main()
    job.commit()
