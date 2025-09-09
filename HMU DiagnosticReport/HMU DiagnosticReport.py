from datetime import datetime
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, IntegerType
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
TABLE_NAME = "diagnosticreport"
REDSHIFT_CONNECTION = "Redshift connection"
S3_TEMP_DIR = "s3://aws-glue-assets-442042533707-us-east-2/temporary/"

def extract_reference_id(reference_field):
    """Extract ID from FHIR reference format"""
    if reference_field:
        # Handle Row/struct format: Row(reference="Patient/123", display="Name")
        if hasattr(reference_field, 'reference'):
            reference = reference_field.reference
            if reference and "/" in reference:
                return reference.split("/")[-1]
        # Handle dict format: {"reference": "Patient/123", "display": "Name"}
        elif isinstance(reference_field, dict):
            reference = reference_field.get('reference')
            if reference and "/" in reference:
                return reference.split("/")[-1]
        # Handle string format: "Patient/123"
        elif isinstance(reference_field, str):
            if "/" in reference_field:
                return reference_field.split("/")[-1]
    return None

def safe_get_field(df, column_name, field_name=None):
    """Safely get a field from a column, handling cases where column might not exist"""
    try:
        if field_name:
            return F.col(column_name).getField(field_name)
        else:
            return F.col(column_name)
    except:
        return F.lit(None)

def transform_main_diagnostic_report_data(df):
    """Transform the main diagnostic report data"""
    logger.info("Transforming main diagnostic report data...")
    
    # Log available columns for debugging
    available_columns = df.columns
    logger.info(f"Available columns: {available_columns}")
    
    # Convert complex data to JSON strings to avoid nested structures
    def convert_to_json_string(field):
        if field is None:
            return None
        try:
            if isinstance(field, str):
                return field
            else:
                return json.dumps(field)
        except:
            return str(field)
    
    convert_to_json_udf = F.udf(convert_to_json_string, StringType())
    
    # Build the select statement for main diagnostic report data
    select_columns = [
        F.col("id").alias("diagnostic_report_id"),
        F.col("resourcetype").alias("resource_type"),
        F.col("status"),
        F.when(F.col("effectivedatetime").isNotNull(),
               F.when(F.col("effectivedatetime").rlike("T"),
                      F.to_timestamp(F.col("effectivedatetime"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
                     ).otherwise(
                      F.to_timestamp(F.col("effectivedatetime"), "yyyy-MM-dd")
                     )
              ).otherwise(None).alias("effective_datetime"),
        F.when(F.col("issued").isNotNull(),
               F.when(F.col("issued").rlike("\\."),
                      F.to_timestamp(F.col("issued"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                     ).otherwise(
                      F.to_timestamp(F.col("issued"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
                     )
              ).otherwise(None).alias("issued_datetime"),
        F.col("code.text").alias("code_text"),
        F.when(F.col("code.coding").isNotNull() & (F.size(F.col("code.coding")) > 0),
               F.col("code.coding")[0].getField("code")
              ).otherwise(None).alias("code_primary_code"),
        F.when(F.col("code.coding").isNotNull() & (F.size(F.col("code.coding")) > 0),
               F.col("code.coding")[0].getField("system")
              ).otherwise(None).alias("code_primary_system"),
        F.when(F.col("code.coding").isNotNull() & (F.size(F.col("code.coding")) > 0),
               F.col("code.coding")[0].getField("display")
              ).otherwise(None).alias("code_primary_display"),
        F.when(F.col("subject").isNotNull(),
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        F.when(F.col("encounter").isNotNull(),
               F.regexp_extract(F.col("encounter").getField("reference"), r"Encounter/(.+)", 1)
              ).otherwise(None).alias("encounter_id"),
        convert_to_json_udf(F.col("meta")).alias("meta_data"),
        convert_to_json_udf(F.col("extension")).alias("extensions"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ]
    
    # Transform main diagnostic report data
    main_df = df.select(*select_columns).filter(
        F.col("diagnostic_report_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return main_df

def transform_diagnostic_report_categories(df):
    """Transform diagnostic report categories (multiple categories per report)"""
    logger.info("Transforming diagnostic report categories...")
    
    # Use Spark's native column operations to handle the nested structure
    # category: array -> element: struct -> coding: array -> element: struct
    
    # First explode the category array
    categories_df = df.select(
        F.col("id").alias("diagnostic_report_id"),
        F.explode(F.col("category")).alias("category_item")
    ).filter(
        F.col("category_item").isNotNull()
    )
    
    # Extract category details and explode the coding array
    categories_final = categories_df.select(
        F.col("diagnostic_report_id"),
        F.explode(F.col("category_item.coding")).alias("coding_item")
    ).select(
        F.col("diagnostic_report_id"),
        F.col("coding_item.code").alias("category_code"),
        F.col("coding_item.system").alias("category_system"),
        F.col("coding_item.display").alias("category_display")
    ).filter(
        F.col("category_code").isNotNull()
    )
    
    return categories_final

def transform_diagnostic_report_performers(df):
    """Transform diagnostic report performers"""
    logger.info("Transforming diagnostic report performers...")
    
    # Check if performer column exists and has data
    if "performer" not in df.columns:
        logger.warning("performer column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("diagnostic_report_id"),
            F.lit("").alias("performer_type"),
            F.lit("").alias("performer_id")
        ).filter(F.lit(False))
    
    # Check if performer column has any non-null data
    performer_count = df.filter(F.col("performer").isNotNull() & (F.size(F.col("performer")) > 0)).count()
    if performer_count == 0:
        logger.warning("performer column exists but contains no data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("diagnostic_report_id"),
            F.lit("").alias("performer_type"),
            F.lit("").alias("performer_id")
        ).filter(F.lit(False))
    
    # Use Spark's native column operations to handle the nested structure
    # performer: array -> element: struct -> reference
    
    # First explode the performer array
    performers_df = df.select(
        F.col("id").alias("diagnostic_report_id"),
        F.explode(F.col("performer")).alias("performer_item")
    ).filter(
        F.col("performer_item").isNotNull()
    )
    
    # Extract performer details
    performers_final = performers_df.select(
        F.col("diagnostic_report_id"),
        F.when(F.col("performer_item.reference").isNotNull(),
               F.regexp_extract(F.col("performer_item.reference"), r"([^/]+)/", 1)
              ).otherwise(None).alias("performer_type"),
        F.when(F.col("performer_item.reference").isNotNull(),
               F.regexp_extract(F.col("performer_item.reference"), r"[^/]+/(.+)", 1)
              ).otherwise(None).alias("performer_id")
    ).filter(
        F.col("performer_type").isNotNull() & F.col("performer_id").isNotNull()
    )
    
    return performers_final

def transform_diagnostic_report_based_on(df):
    """Transform diagnostic report based on (service requests)"""
    logger.info("Transforming diagnostic report based on...")
    
    # Check if basedOn column exists and has data (note: camelCase in actual data)
    if "basedOn" not in df.columns:
        logger.warning("basedOn column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("diagnostic_report_id"),
            F.lit("").alias("service_request_id")
        ).filter(F.lit(False))
    
    # Check if basedOn column has any non-null data
    based_on_count = df.filter(F.col("basedOn").isNotNull() & (F.size(F.col("basedOn")) > 0)).count()
    if based_on_count == 0:
        logger.warning("basedOn column exists but contains no data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("diagnostic_report_id"),
            F.lit("").alias("service_request_id")
        ).filter(F.lit(False))
    
    # Use Spark's native column operations to handle the nested structure
    # basedOn: array -> element: struct -> reference
    
    # First explode the basedOn array
    based_on_df = df.select(
        F.col("id").alias("diagnostic_report_id"),
        F.explode(F.col("basedOn")).alias("based_on_item")
    ).filter(
        F.col("based_on_item").isNotNull()
    )
    
    # Extract service request ID
    based_on_final = based_on_df.select(
        F.col("diagnostic_report_id"),
        F.when(F.col("based_on_item.reference").isNotNull(),
               F.regexp_extract(F.col("based_on_item.reference"), r"ServiceRequest/(.+)", 1)
              ).otherwise(None).alias("service_request_id")
    ).filter(
        F.col("service_request_id").isNotNull()
    )
    
    return based_on_final

def transform_diagnostic_report_results(df):
    """Transform diagnostic report results (observations)"""
    logger.info("Transforming diagnostic report results...")
    
    # Check if result column exists and has data
    if "result" not in df.columns:
        logger.warning("result column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("diagnostic_report_id"),
            F.lit("").alias("observation_id")
        ).filter(F.lit(False))
    
    # Check if result column has any non-null data
    result_count = df.filter(F.col("result").isNotNull() & (F.size(F.col("result")) > 0)).count()
    if result_count == 0:
        logger.warning("result column exists but contains no data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("diagnostic_report_id"),
            F.lit("").alias("observation_id")
        ).filter(F.lit(False))
    
    # Use Spark's native column operations to handle the nested structure
    # result: array -> element: struct -> reference
    
    # First explode the result array
    results_df = df.select(
        F.col("id").alias("diagnostic_report_id"),
        F.explode(F.col("result")).alias("result_item")
    ).filter(
        F.col("result_item").isNotNull()
    )
    
    # Extract observation ID
    results_final = results_df.select(
        F.col("diagnostic_report_id"),
        F.when(F.col("result_item.reference").isNotNull(),
               F.regexp_extract(F.col("result_item.reference"), r"Observation/(.+)", 1)
              ).otherwise(None).alias("observation_id")
    ).filter(
        F.col("observation_id").isNotNull()
    )
    
    return results_final

def transform_diagnostic_report_media(df):
    """Transform diagnostic report media references"""
    logger.info("Transforming diagnostic report media...")
    
    # Check if media column exists and has data
    if "media" not in df.columns:
        logger.warning("media column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("diagnostic_report_id"),
            F.lit("").alias("media_id")
        ).filter(F.lit(False))
    
    # Check if media column has any non-null data
    media_count = df.filter(F.col("media").isNotNull() & (F.size(F.col("media")) > 0)).count()
    if media_count == 0:
        logger.warning("media column exists but contains no data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("diagnostic_report_id"),
            F.lit("").alias("media_id")
        ).filter(F.lit(False))
    
    # Use Spark's native column operations to handle the nested structure
    # media: array -> element: struct -> link: struct -> reference
    
    # First explode the media array
    media_df = df.select(
        F.col("id").alias("diagnostic_report_id"),
        F.explode(F.col("media")).alias("media_item")
    ).filter(
        F.col("media_item").isNotNull()
    )
    
    # Extract media ID
    media_final = media_df.select(
        F.col("diagnostic_report_id"),
        F.when(F.col("media_item.link").isNotNull() & F.col("media_item.link.reference").isNotNull(),
               F.regexp_extract(F.col("media_item.link.reference"), r"Media/(.+)", 1)
              ).otherwise(None).alias("media_id")
    ).filter(
        F.col("media_id").isNotNull()
    )
    
    return media_final

def transform_diagnostic_report_presented_forms(df):
    """Transform diagnostic report presented forms"""
    logger.info("Transforming diagnostic report presented forms...")
    
    # Check if presentedForm column exists and has data (note: camelCase in actual data)
    if "presentedForm" not in df.columns:
        logger.warning("presentedForm column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("diagnostic_report_id"),
            F.lit("").alias("content_type"),
            F.lit("").alias("data"),
            F.lit("").alias("title")
        ).filter(F.lit(False))
    
    # Check if presentedForm column has any non-null data
    presented_form_count = df.filter(F.col("presentedForm").isNotNull() & (F.size(F.col("presentedForm")) > 0)).count()
    if presented_form_count == 0:
        logger.warning("presentedForm column exists but contains no data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("diagnostic_report_id"),
            F.lit("").alias("content_type"),
            F.lit("").alias("data"),
            F.lit("").alias("title")
        ).filter(F.lit(False))
    
    # Use Spark's native column operations to handle the nested structure
    # presentedForm: array -> element: struct -> {contentType, data, title}
    
    # First explode the presentedForm array
    presented_forms_df = df.select(
        F.col("id").alias("diagnostic_report_id"),
        F.explode(F.col("presentedForm")).alias("presented_form_item")
    ).filter(
        F.col("presented_form_item").isNotNull()
    )
    
    # Extract presented form details
    presented_forms_final = presented_forms_df.select(
        F.col("diagnostic_report_id"),
        F.col("presented_form_item.contentType").alias("content_type"),
        F.col("presented_form_item.data").alias("data"),  # Changed from url to data
        F.col("presented_form_item.title").alias("title")
    ).filter(
        F.col("content_type").isNotNull() | F.col("data").isNotNull()
    )
    
    return presented_forms_final

def create_redshift_tables_sql():
    """Generate SQL for creating main diagnostic_reports table in Redshift"""
    return """
    -- Main diagnostic reports table
    CREATE TABLE IF NOT EXISTS public.diagnostic_reports (
        diagnostic_report_id VARCHAR(255) NOT NULL,
        resource_type VARCHAR(50),
        status VARCHAR(50),
        effective_datetime TIMESTAMP,
        issued_datetime TIMESTAMP,
        code_text VARCHAR(500),
        code_primary_code VARCHAR(50),
        code_primary_system VARCHAR(255),
        code_primary_display VARCHAR(255),
        patient_id VARCHAR(255),
        encounter_id VARCHAR(255),
        meta_data TEXT,
        extensions TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (diagnostic_report_id)
    ) DISTKEY (patient_id) SORTKEY (patient_id, effective_datetime)
    """

def create_diagnostic_report_categories_table_sql():
    """Generate SQL for creating diagnostic_report_categories table"""
    return """
    CREATE TABLE IF NOT EXISTS public.diagnostic_report_categories (
        diagnostic_report_id VARCHAR(255),
        category_code VARCHAR(50),
        category_system VARCHAR(255),
        category_display VARCHAR(255)
    ) SORTKEY (diagnostic_report_id, category_code)
    """

def create_diagnostic_report_performers_table_sql():
    """Generate SQL for creating diagnostic_report_performers table"""
    return """
    CREATE TABLE IF NOT EXISTS public.diagnostic_report_performers (
        diagnostic_report_id VARCHAR(255),
        performer_type VARCHAR(50),
        performer_id VARCHAR(255)
    ) SORTKEY (diagnostic_report_id, performer_type)
    """

def create_diagnostic_report_based_on_table_sql():
    """Generate SQL for creating diagnostic_report_based_on table"""
    return """
    CREATE TABLE IF NOT EXISTS public.diagnostic_report_based_on (
        diagnostic_report_id VARCHAR(255),
        service_request_id VARCHAR(255)
    ) SORTKEY (diagnostic_report_id)
    """

def create_diagnostic_report_results_table_sql():
    """Generate SQL for creating diagnostic_report_results table"""
    return """
    CREATE TABLE IF NOT EXISTS public.diagnostic_report_results (
        diagnostic_report_id VARCHAR(255),
        observation_id VARCHAR(255)
    ) SORTKEY (diagnostic_report_id)
    """

def create_diagnostic_report_media_table_sql():
    """Generate SQL for creating diagnostic_report_media table"""
    return """
    CREATE TABLE IF NOT EXISTS public.diagnostic_report_media (
        diagnostic_report_id VARCHAR(255),
        media_id VARCHAR(255)
    ) SORTKEY (diagnostic_report_id)
    """

def create_diagnostic_report_presented_forms_table_sql():
    """Generate SQL for creating diagnostic_report_presented_forms table"""
    return """
    CREATE TABLE IF NOT EXISTS public.diagnostic_report_presented_forms (
        diagnostic_report_id VARCHAR(255),
        content_type VARCHAR(100),
        data TEXT,
        title VARCHAR(255)
    ) SORTKEY (diagnostic_report_id)
    """

def write_to_redshift(dynamic_frame, table_name, preactions=""):
    """Write DynamicFrame to Redshift using JDBC connection"""
    logger.info(f"Writing {table_name} to Redshift...")
    
    # Use DELETE to clear data while preserving table structure and relationships
    # DELETE is the most reliable option for healthcare data with foreign key constraints
    # It handles referential integrity properly and can be rolled back if needed
    if preactions:
        preactions = f"DELETE FROM public.{table_name}; " + preactions
    else:
        preactions = f"DELETE FROM public.{table_name};"
    
    try:
        logger.info(f"Executing preactions for {table_name}: {preactions}")
        logger.info(f"Writing to table: public.{table_name}")
        logger.info(f"Using S3 temp directory: {S3_TEMP_DIR}")
        logger.info(f"Using connection: {REDSHIFT_CONNECTION}")
        
        # Log sample data being written for debugging
        sample_df = dynamic_frame.toDF()
        record_count = sample_df.count()
        logger.info(f"Writing {record_count} records to {table_name}")
        
        if record_count > 0:
            logger.info(f"Sample data for {table_name}:")
            sample_df.show(3, truncate=False)
            logger.info(f"Schema for {table_name}:")
            sample_df.printSchema()
        
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
        logger.error(f"Preactions that were executed: {preactions}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error details: {str(e)}")
        
        # Try to get more details about the data being written
        try:
            sample_df = dynamic_frame.toDF()
            logger.error(f"DataFrame schema that failed:")
            sample_df.printSchema()
            logger.error(f"Sample of failed data:")
            sample_df.show(5, truncate=False)
        except Exception as debug_e:
            logger.error(f"Could not get debug info: {debug_e}")
        
        raise e

def main():
    """Main ETL process"""
    start_time = datetime.now()
    try:
        logger.info("=" * 80)
        logger.info("🚀 STARTING ENHANCED FHIR DIAGNOSTIC REPORT ETL PROCESS")
        logger.info("=" * 80)
        logger.info(f"⏰ Job started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📊 Source: {DATABASE_NAME}.{TABLE_NAME}")
        logger.info(f"🎯 Target: Redshift (7 tables)")
        logger.info("📋 Reading all available columns from Glue Catalog")
        logger.info("🔄 Process: 7 steps (Read → Transform → Convert → Resolve → Validate → Write)")
        
        # Step 1: Read data from HealthLake using AWS Glue Data Catalog
        logger.info("\n" + "=" * 50)
        logger.info("📥 STEP 1: READING DATA FROM GLUE CATALOG")
        logger.info("=" * 50)
        logger.info(f"Database: {DATABASE_NAME}")
        logger.info(f"Table: {TABLE_NAME}")
        logger.info("Reading all available columns from Glue Catalog")
        
        # Use the AWS Glue Data Catalog to read diagnostic report data (all columns)
        diagnostic_report_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=DATABASE_NAME, 
            table_name=TABLE_NAME, 
            transformation_ctx="AWSGlueDataCatalog_diagnostic_report_node"
        )
        
        # Convert to DataFrame first to check available columns
        diagnostic_report_df_raw = diagnostic_report_dynamic_frame.toDF()
        available_columns = diagnostic_report_df_raw.columns
        logger.info(f"📋 Available columns in source: {available_columns}")
        
        # Use all available columns
        logger.info(f"✅ Using all {len(available_columns)} available columns")
        diagnostic_report_df = diagnostic_report_df_raw
        
        logger.info("✅ Successfully read data using AWS Glue Data Catalog")
        
        total_records = diagnostic_report_df.count()
        logger.info(f"📊 Read {total_records:,} raw diagnostic report records")
        
        # Debug: Show sample of raw data and schema
        if total_records > 0:
            logger.info("\n🔍 DATA QUALITY CHECKS:")
            logger.info("Sample of raw diagnostic report data:")
            diagnostic_report_df.show(3, truncate=False)
            logger.info("Raw data schema:")
            diagnostic_report_df.printSchema()
            
            # Check for NULL values in key fields
            null_checks = {
                "id": diagnostic_report_df.filter(F.col("id").isNull()).count(),
                "subject.reference": diagnostic_report_df.filter(F.col("subject").isNull() | F.col("subject.reference").isNull()).count(),
                "status": diagnostic_report_df.filter(F.col("status").isNull()).count(),
                "code": diagnostic_report_df.filter(F.col("code").isNull()).count()
            }
            
            logger.info("NULL value analysis in key fields:")
            for field, null_count in null_checks.items():
                percentage = (null_count / total_records) * 100 if total_records > 0 else 0
                logger.info(f"  {field}: {null_count:,} NULLs ({percentage:.1f}%)")
        else:
            logger.error("❌ No raw data found! Check the data source.")
            return
        
        # Step 2: Transform main diagnostic report data
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 2: TRANSFORMING MAIN DIAGNOSTIC REPORT DATA")
        logger.info("=" * 50)
        
        main_diagnostic_report_df = transform_main_diagnostic_report_data(diagnostic_report_df)
        main_count = main_diagnostic_report_df.count()
        logger.info(f"✅ Transformed {main_count:,} main diagnostic report records")
        
        if main_count == 0:
            logger.error("❌ No main diagnostic report records after transformation! Check filtering criteria.")
            return
        
        # Step 3: Transform multi-valued data (all supporting tables)
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 3: TRANSFORMING MULTI-VALUED DATA")
        logger.info("=" * 50)
        
        categories_df = transform_diagnostic_report_categories(diagnostic_report_df)
        categories_count = categories_df.count()
        logger.info(f"✅ Transformed {categories_count:,} category records")
        
        performers_df = transform_diagnostic_report_performers(diagnostic_report_df)
        performers_count = performers_df.count()
        logger.info(f"✅ Transformed {performers_count:,} performer records")
        
        based_on_df = transform_diagnostic_report_based_on(diagnostic_report_df)
        based_on_count = based_on_df.count()
        logger.info(f"✅ Transformed {based_on_count:,} based on records")
        
        results_df = transform_diagnostic_report_results(diagnostic_report_df)
        results_count = results_df.count()
        logger.info(f"✅ Transformed {results_count:,} result records")
        
        media_df = transform_diagnostic_report_media(diagnostic_report_df)
        media_count = media_df.count()
        logger.info(f"✅ Transformed {media_count:,} media records")
        
        presented_forms_df = transform_diagnostic_report_presented_forms(diagnostic_report_df)
        presented_forms_count = presented_forms_df.count()
        logger.info(f"✅ Transformed {presented_forms_count:,} presented form records")
        
        # Debug: Show samples of multi-valued data if available
        if categories_count > 0:
            logger.info("Sample of diagnostic report categories data:")
            categories_df.show(3, truncate=False)
        
        if performers_count > 0:
            logger.info("Sample of diagnostic report performers data:")
            performers_df.show(3, truncate=False)
        
        if based_on_count > 0:
            logger.info("Sample of diagnostic report based on data:")
            based_on_df.show(3, truncate=False)
        
        if results_count > 0:
            logger.info("Sample of diagnostic report results data:")
            results_df.show(3, truncate=False)
        
        if media_count > 0:
            logger.info("Sample of diagnostic report media data:")
            media_df.show(3, truncate=False)
        
        if presented_forms_count > 0:
            logger.info("Sample of diagnostic report presented forms data:")
            presented_forms_df.show(3, truncate=False)
        
        # Step 4: Convert to DynamicFrames and ensure data is flat for Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 4: CONVERTING TO DYNAMICFRAMES")
        logger.info("=" * 50)
        logger.info("Converting to DynamicFrames and ensuring Redshift compatibility...")
        
        # Convert main diagnostic reports DataFrame and ensure flat structure
        main_flat_df = main_diagnostic_report_df.select(
            F.col("diagnostic_report_id").cast(StringType()).alias("diagnostic_report_id"),
            F.col("resource_type").cast(StringType()).alias("resource_type"),
            F.col("status").cast(StringType()).alias("status"),
            F.col("effective_datetime").cast(TimestampType()).alias("effective_datetime"),
            F.col("issued_datetime").cast(TimestampType()).alias("issued_datetime"),
            F.col("code_text").cast(StringType()).alias("code_text"),
            F.col("code_primary_code").cast(StringType()).alias("code_primary_code"),
            F.col("code_primary_system").cast(StringType()).alias("code_primary_system"),
            F.col("code_primary_display").cast(StringType()).alias("code_primary_display"),
            F.col("patient_id").cast(StringType()).alias("patient_id"),
            F.col("encounter_id").cast(StringType()).alias("encounter_id"),
            F.col("meta_data").cast(StringType()).alias("meta_data"),
            F.col("extensions").cast(StringType()).alias("extensions"),
            F.col("created_at").cast(TimestampType()).alias("created_at"),
            F.col("updated_at").cast(TimestampType()).alias("updated_at")
        )
        
        main_dynamic_frame = DynamicFrame.fromDF(main_flat_df, glueContext, "main_diagnostic_report_dynamic_frame")
        
        # Convert other DataFrames with type casting
        categories_flat_df = categories_df.select(
            F.col("diagnostic_report_id").cast(StringType()).alias("diagnostic_report_id"),
            F.col("category_code").cast(StringType()).alias("category_code"),
            F.col("category_system").cast(StringType()).alias("category_system"),
            F.col("category_display").cast(StringType()).alias("category_display")
        )
        categories_dynamic_frame = DynamicFrame.fromDF(categories_flat_df, glueContext, "categories_dynamic_frame")
        
        performers_flat_df = performers_df.select(
            F.col("diagnostic_report_id").cast(StringType()).alias("diagnostic_report_id"),
            F.col("performer_type").cast(StringType()).alias("performer_type"),
            F.col("performer_id").cast(StringType()).alias("performer_id")
        )
        performers_dynamic_frame = DynamicFrame.fromDF(performers_flat_df, glueContext, "performers_dynamic_frame")
        
        based_on_flat_df = based_on_df.select(
            F.col("diagnostic_report_id").cast(StringType()).alias("diagnostic_report_id"),
            F.col("service_request_id").cast(StringType()).alias("service_request_id")
        )
        based_on_dynamic_frame = DynamicFrame.fromDF(based_on_flat_df, glueContext, "based_on_dynamic_frame")
        
        results_flat_df = results_df.select(
            F.col("diagnostic_report_id").cast(StringType()).alias("diagnostic_report_id"),
            F.col("observation_id").cast(StringType()).alias("observation_id")
        )
        results_dynamic_frame = DynamicFrame.fromDF(results_flat_df, glueContext, "results_dynamic_frame")
        
        media_flat_df = media_df.select(
            F.col("diagnostic_report_id").cast(StringType()).alias("diagnostic_report_id"),
            F.col("media_id").cast(StringType()).alias("media_id")
        )
        media_dynamic_frame = DynamicFrame.fromDF(media_flat_df, glueContext, "media_dynamic_frame")
        
        presented_forms_flat_df = presented_forms_df.select(
            F.col("diagnostic_report_id").cast(StringType()).alias("diagnostic_report_id"),
            F.col("content_type").cast(StringType()).alias("content_type"),
            F.col("data").cast(StringType()).alias("data"),
            F.col("title").cast(StringType()).alias("title")
        )
        presented_forms_dynamic_frame = DynamicFrame.fromDF(presented_forms_flat_df, glueContext, "presented_forms_dynamic_frame")
        
        # Step 5: Resolve any remaining choice types to ensure Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 5: RESOLVING CHOICE TYPES")
        logger.info("=" * 50)
        logger.info("Resolving choice types for Redshift compatibility...")
        
        main_resolved_frame = main_dynamic_frame.resolveChoice(
            specs=[
                ("diagnostic_report_id", "cast:string"),
                ("resource_type", "cast:string"),
                ("status", "cast:string"),
                ("effective_datetime", "cast:timestamp"),
                ("issued_datetime", "cast:timestamp"),
                ("code_text", "cast:string"),
                ("code_primary_code", "cast:string"),
                ("code_primary_system", "cast:string"),
                ("code_primary_display", "cast:string"),
                ("patient_id", "cast:string"),
                ("encounter_id", "cast:string"),
                ("meta_data", "cast:string"),
                ("extensions", "cast:string"),
                ("created_at", "cast:timestamp"),
                ("updated_at", "cast:timestamp")
            ]
        )
        
        categories_resolved_frame = categories_dynamic_frame.resolveChoice(
            specs=[
                ("diagnostic_report_id", "cast:string"),
                ("category_code", "cast:string"),
                ("category_system", "cast:string"),
                ("category_display", "cast:string")
            ]
        )
        
        performers_resolved_frame = performers_dynamic_frame.resolveChoice(
            specs=[
                ("diagnostic_report_id", "cast:string"),
                ("performer_type", "cast:string"),
                ("performer_id", "cast:string")
            ]
        )
        
        based_on_resolved_frame = based_on_dynamic_frame.resolveChoice(
            specs=[
                ("diagnostic_report_id", "cast:string"),
                ("service_request_id", "cast:string")
            ]
        )
        
        results_resolved_frame = results_dynamic_frame.resolveChoice(
            specs=[
                ("diagnostic_report_id", "cast:string"),
                ("observation_id", "cast:string")
            ]
        )
        
        media_resolved_frame = media_dynamic_frame.resolveChoice(
            specs=[
                ("diagnostic_report_id", "cast:string"),
                ("media_id", "cast:string")
            ]
        )
        
        presented_forms_resolved_frame = presented_forms_dynamic_frame.resolveChoice(
            specs=[
                ("diagnostic_report_id", "cast:string"),
                ("content_type", "cast:string"),
                ("data", "cast:string"),
                ("title", "cast:string")
            ]
        )
        
        # Step 6: Final validation before writing
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 6: FINAL VALIDATION")
        logger.info("=" * 50)
        logger.info("Performing final validation before writing to Redshift...")
        
        # Validate main diagnostic reports data
        main_final_df = main_resolved_frame.toDF()
        main_final_count = main_final_df.count()
        logger.info(f"Final main diagnostic reports count: {main_final_count}")
        
        if main_final_count == 0:
            logger.error("No main diagnostic report records to write to Redshift! Stopping the process.")
            return
        
        # Additional data validation for Redshift compatibility
        logger.info("Performing data validation for Redshift compatibility...")
        
        # Check for any problematic data that might cause Redshift write failures
        problematic_records = main_final_df.filter(
            F.col("diagnostic_report_id").isNull() | 
            (F.col("diagnostic_report_id") == "") |
            F.col("patient_id").isNull() |
            (F.col("patient_id") == "")
        ).count()
        
        if problematic_records > 0:
            logger.warning(f"Found {problematic_records} records with missing required fields")
            logger.warning("These records will be filtered out before writing to Redshift")
            # Filter out problematic records
            main_final_df = main_final_df.filter(
                F.col("diagnostic_report_id").isNotNull() & 
                (F.col("diagnostic_report_id") != "") &
                F.col("patient_id").isNotNull() &
                (F.col("patient_id") != "")
            )
            main_final_count = main_final_df.count()
            logger.info(f"After filtering: {main_final_count} valid records")
        
        # Check for extremely long strings that might exceed Redshift limits
        long_string_issues = main_final_df.filter(
            (F.length(F.col("code_text")) > 500) |
            (F.length(F.col("code_primary_display")) > 255) |
            (F.length(F.col("code_primary_system")) > 255)
        ).count()
        
        if long_string_issues > 0:
            logger.warning(f"Found {long_string_issues} records with strings that might exceed Redshift column limits")
            logger.warning("These will be truncated during write")
        
        # Recreate the main DynamicFrame with validated data
        if problematic_records > 0:
            logger.info("Recreating main DynamicFrame with validated data...")
            main_resolved_frame = DynamicFrame.fromDF(main_final_df, glueContext, "main_diagnostic_report_validated")
        
        # Validate other tables
        categories_final_count = categories_resolved_frame.toDF().count()
        performers_final_count = performers_resolved_frame.toDF().count()
        based_on_final_count = based_on_resolved_frame.toDF().count()
        results_final_count = results_resolved_frame.toDF().count()
        media_final_count = media_resolved_frame.toDF().count()
        presented_forms_final_count = presented_forms_resolved_frame.toDF().count()
        
        logger.info(f"Final counts - Categories: {categories_final_count}, Performers: {performers_final_count}, Based On: {based_on_final_count}, Results: {results_final_count}, Media: {media_final_count}, Presented Forms: {presented_forms_final_count}")
        
        # Debug: Show final sample data being written
        logger.info("Final sample data being written to Redshift (main diagnostic reports):")
        main_final_df.show(3, truncate=False)
        
        # Show sample data for other tables as well
        logger.info("Final sample data for diagnostic report categories:")
        categories_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for diagnostic report performers:")
        performers_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for diagnostic report based on:")
        based_on_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for diagnostic report results:")
        results_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for diagnostic report media:")
        media_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for diagnostic report presented forms:")
        presented_forms_resolved_frame.toDF().show(3, truncate=False)
        
        # Step 7: Create tables and write to Redshift
        logger.info("\n" + "=" * 50)
        logger.info("💾 STEP 7: WRITING DATA TO REDSHIFT")
        logger.info("=" * 50)
        logger.info(f"🔗 Using connection: {REDSHIFT_CONNECTION}")
        logger.info(f"📁 S3 temp directory: {S3_TEMP_DIR}")
        
        # Create all tables individually
        # Note: Each write_to_redshift call now includes DELETE to prevent duplicates
        logger.info("📝 Creating main diagnostic reports table...")
        diagnostic_reports_table_sql = create_redshift_tables_sql()
        write_to_redshift(main_resolved_frame, "diagnostic_reports", diagnostic_reports_table_sql)
        logger.info("✅ Main diagnostic reports table created and written successfully")
        
        logger.info("📝 Creating diagnostic report categories table...")
        categories_table_sql = create_diagnostic_report_categories_table_sql()
        write_to_redshift(categories_resolved_frame, "diagnostic_report_categories", categories_table_sql)
        logger.info("✅ Diagnostic report categories table created and written successfully")
        
        logger.info("📝 Creating diagnostic report performers table...")
        performers_table_sql = create_diagnostic_report_performers_table_sql()
        write_to_redshift(performers_resolved_frame, "diagnostic_report_performers", performers_table_sql)
        logger.info("✅ Diagnostic report performers table created and written successfully")
        
        logger.info("📝 Creating diagnostic report based on table...")
        based_on_table_sql = create_diagnostic_report_based_on_table_sql()
        write_to_redshift(based_on_resolved_frame, "diagnostic_report_based_on", based_on_table_sql)
        logger.info("✅ Diagnostic report based on table created and written successfully")
        
        logger.info("📝 Creating diagnostic report results table...")
        results_table_sql = create_diagnostic_report_results_table_sql()
        write_to_redshift(results_resolved_frame, "diagnostic_report_results", results_table_sql)
        logger.info("✅ Diagnostic report results table created and written successfully")
        
        logger.info("📝 Creating diagnostic report media table...")
        media_table_sql = create_diagnostic_report_media_table_sql()
        write_to_redshift(media_resolved_frame, "diagnostic_report_media", media_table_sql)
        logger.info("✅ Diagnostic report media table created and written successfully")
        
        logger.info("📝 Creating diagnostic report presented forms table...")
        presented_forms_table_sql = create_diagnostic_report_presented_forms_table_sql()
        write_to_redshift(presented_forms_resolved_frame, "diagnostic_report_presented_forms", presented_forms_table_sql)
        logger.info("✅ Diagnostic report presented forms table created and written successfully")
        
        # Calculate processing time
        end_time = datetime.now()
        processing_time = end_time - start_time
        
        logger.info("\n" + "=" * 80)
        logger.info("🎉 ETL PROCESS COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        logger.info(f"⏰ Job completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"⏱️  Total processing time: {processing_time}")
        
        logger.info("\n📋 TABLES WRITTEN TO REDSHIFT:")
        logger.info("  ✅ public.diagnostic_reports (main diagnostic report data)")
        logger.info("  ✅ public.diagnostic_report_categories (LAB, RAD, PATH categories)")
        logger.info("  ✅ public.diagnostic_report_performers (organizations, practitioners)")
        logger.info("  ✅ public.diagnostic_report_based_on (service request references)")
        logger.info("  ✅ public.diagnostic_report_results (observation references)")
        logger.info("  ✅ public.diagnostic_report_media (scanned documents, images)")
        logger.info("  ✅ public.diagnostic_report_presented_forms (presented forms)")
        
        logger.info("\n📊 FINAL ETL STATISTICS:")
        logger.info(f"  📥 Total raw records processed: {total_records:,}")
        logger.info(f"  🏥 Main diagnostic report records: {main_count:,}")
        logger.info(f"  🏷️  Category records: {categories_count:,}")
        logger.info(f"  👥 Performer records: {performers_count:,}")
        logger.info(f"  📋 Based on records: {based_on_count:,}")
        logger.info(f"  🔬 Result records: {results_count:,}")
        logger.info(f"  📎 Media records: {media_count:,}")
        logger.info(f"  📄 Presented form records: {presented_forms_count:,}")
        
        # Calculate data expansion ratio
        total_output_records = main_count + categories_count + performers_count + based_on_count + results_count + media_count + presented_forms_count
        expansion_ratio = total_output_records / total_records if total_records > 0 else 0
        logger.info(f"  📈 Data expansion ratio: {expansion_ratio:.2f}x (output records / input records)")
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ ETL JOB COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        
    except Exception as e:
        end_time = datetime.now()
        processing_time = end_time - start_time
        logger.error("\n" + "=" * 80)
        logger.error("❌ ETL PROCESS FAILED!")
        logger.error("=" * 80)
        logger.error(f"⏰ Job failed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.error(f"⏱️  Processing time before failure: {processing_time}")
        logger.error(f"🚨 Error: {str(e)}")
        logger.error("=" * 80)
        raise e

if __name__ == "__main__":
    main()
    job.commit()
