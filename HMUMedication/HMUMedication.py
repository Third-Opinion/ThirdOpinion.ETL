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
TABLE_NAME = "medication"
REDSHIFT_CONNECTION = "Redshift connection"
S3_TEMP_DIR = "s3://aws-glue-assets-442042533707-us-east-2/temporary/"

def transform_main_medication_data(df):
    """Transform the main medication data"""
    logger.info("Transforming main medication data...")
    
    # Log available columns for debugging
    available_columns = df.columns
    logger.info(f"Available columns: {available_columns}")
    
    # Build the select statement dynamically based on available columns
    select_columns = [
        F.col("id").alias("medication_id"),
        F.col("code").getField("text").alias("code_text"),
        F.col("status").alias("status"),
        F.col("meta").getField("versionId").alias("meta_version_id"),
        F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("meta_last_updated"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ]
    
    # Transform main medication data using only available columns
    main_df = df.select(*select_columns).filter(
        F.col("medication_id").isNotNull()
    )
    
    return main_df

def transform_medication_identifiers(df):
    """Transform medication identifiers (multiple identifiers per medication)"""
    logger.info("Transforming medication identifiers...")
    
    # Check if identifier column exists
    if "identifier" not in df.columns:
        logger.warning("identifier column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("medication_id"),
            F.lit("").alias("identifier_system"),
            F.lit("").alias("identifier_value")
        ).filter(F.lit(False))
    
    # First explode the identifier array
    identifiers_df = df.select(
        F.col("id").alias("medication_id"),
        F.explode(F.col("identifier")).alias("identifier_item")
    ).filter(
        F.col("identifier_item").isNotNull()
    )
    
    # Extract identifier details
    identifiers_final = identifiers_df.select(
        F.col("medication_id"),
        F.col("identifier_item.system").alias("identifier_system"),
        F.col("identifier_item.value").alias("identifier_value")
    ).filter(
        F.col("identifier_value").isNotNull()
    )
    
    return identifiers_final

def transform_medication_code_codings(df):
    """Transform medication code codings"""
    logger.info("Transforming medication code codings...")
    
    # Check if code.coding exists
    if "code" not in df.columns:
        logger.warning("code column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("medication_id"),
            F.lit("").alias("code_system"),
            F.lit("").alias("code_value"),
            F.lit("").alias("code_display")
        ).filter(F.lit(False))
    
    # Filter to only records that have coding arrays
    coding_df = df.filter(
        F.col("code.coding").isNotNull() & 
        (F.size(F.col("code.coding")) > 0)
    )
    
    if coding_df.count() == 0:
        logger.info("No code.coding data found, returning empty DataFrame")
        return df.select(
            F.col("id").alias("medication_id"),
            F.lit("").alias("code_system"),
            F.lit("").alias("code_value"),
            F.lit("").alias("code_display")
        ).filter(F.lit(False))
    
    # Explode the coding array
    codings_df = coding_df.select(
        F.col("id").alias("medication_id"),
        F.explode(F.col("code.coding")).alias("coding_item")
    )
    
    # Extract coding details
    codings_final = codings_df.select(
        F.col("medication_id"),
        F.col("coding_item.system").alias("code_system"),
        F.col("coding_item.code").alias("code_value"),
        F.col("coding_item.display").alias("code_display")
    ).filter(
        F.col("code_value").isNotNull()
    )
    
    return codings_final

def create_redshift_tables_sql():
    """Generate SQL for creating main medications table in Redshift"""
    return """
    -- Drop existing table if it exists
    DROP TABLE IF EXISTS public.medications CASCADE;
    
    -- Main medications table
    CREATE TABLE public.medications (
        medication_id VARCHAR(255) PRIMARY KEY,
        code_text VARCHAR(500),
        status VARCHAR(50),
        meta_version_id VARCHAR(50),
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) SORTKEY (medication_id);
    """

def create_medication_identifiers_table_sql():
    """Generate SQL for creating medication_identifiers table"""
    return """
    -- Drop existing table if it exists
    DROP TABLE IF EXISTS public.medication_identifiers CASCADE;
    
    CREATE TABLE public.medication_identifiers (
        medication_id VARCHAR(255),
        identifier_system VARCHAR(255),
        identifier_value VARCHAR(255)
    ) SORTKEY (medication_id, identifier_system)
    """

def create_medication_code_codings_table_sql():
    """Generate SQL for creating medication_code_codings table"""
    return """
    -- Drop existing table if it exists
    DROP TABLE IF EXISTS public.medication_code_codings CASCADE;
    
    CREATE TABLE public.medication_code_codings (
        medication_id VARCHAR(255),
        code_system VARCHAR(255),
        code_value VARCHAR(100),
        code_display VARCHAR(500)
    ) SORTKEY (medication_id, code_system)
    """

def write_to_redshift(dynamic_frame, table_name, preactions=""):
    """Write DynamicFrame to Redshift using JDBC connection"""
    logger.info(f"Writing {table_name} to Redshift...")
    
    # Log the preactions SQL for debugging
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
        logger.info("🚀 STARTING FHIR MEDICATION ETL PROCESS")
        logger.info("=" * 80)
        logger.info(f"⏰ Job started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📊 Source: {DATABASE_NAME}.{TABLE_NAME}")
        logger.info(f"🎯 Target: Redshift (3 tables)")
        logger.info("📋 Reading all available columns from Glue Catalog")
        logger.info("🔄 Process: 7 steps (Read → Transform → Convert → Resolve → Validate → Write)")
        
        # Step 1: Read data from HealthLake using AWS Glue Data Catalog
        logger.info("\n" + "=" * 50)
        logger.info("📥 STEP 1: READING DATA FROM GLUE CATALOG")
        logger.info("=" * 50)
        logger.info(f"Database: {DATABASE_NAME}")
        logger.info(f"Table: {TABLE_NAME}")
        logger.info("Reading all available columns from Glue Catalog")
        
        # Use the AWS Glue Data Catalog to read medication data (all columns)
        medication_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=DATABASE_NAME, 
            table_name=TABLE_NAME, 
            transformation_ctx="AWSGlueDataCatalog_medication_node"
        )
        
        # Convert to DataFrame first to check available columns
        medication_df_raw = medication_dynamic_frame.toDF()
        available_columns = medication_df_raw.columns
        logger.info(f"📋 Available columns in source: {available_columns}")
        
        # Use all available columns
        logger.info(f"✅ Using all {len(available_columns)} available columns")
        medication_df = medication_df_raw
        
        logger.info("✅ Successfully read data using AWS Glue Data Catalog")
        
        total_records = medication_df.count()
        logger.info(f"📊 Read {total_records:,} raw medication records")
        
        # Debug: Show sample of raw data and schema
        if total_records > 0:
            logger.info("\n🔍 DATA QUALITY CHECKS:")
            logger.info("Sample of raw medication data:")
            medication_df.show(3, truncate=False)
            logger.info("Raw data schema:")
            medication_df.printSchema()
            
            # Check for NULL values in key fields
            null_checks = {
                "id": medication_df.filter(F.col("id").isNull()).count(),
                "code": medication_df.filter(F.col("code").isNull()).count(),
                "status": medication_df.filter(F.col("status").isNull()).count()
            }
            
            logger.info("NULL value analysis in key fields:")
            for field, null_count in null_checks.items():
                percentage = (null_count / total_records) * 100 if total_records > 0 else 0
                logger.info(f"  {field}: {null_count:,} NULLs ({percentage:.1f}%)")
        else:
            logger.error("❌ No raw data found! Check the data source.")
            return
        
        # Step 2: Transform main medication data
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 2: TRANSFORMING MAIN MEDICATION DATA")
        logger.info("=" * 50)
        
        main_medication_df = transform_main_medication_data(medication_df)
        main_count = main_medication_df.count()
        logger.info(f"✅ Transformed {main_count:,} main medication records")
        
        if main_count == 0:
            logger.error("❌ No main medication records after transformation! Check filtering criteria.")
            return
        
        # Debug: Show actual DataFrame schema
        logger.info("🔍 Main medication DataFrame schema:")
        logger.info(f"Columns: {main_medication_df.columns}")
        main_medication_df.printSchema()
        
        # Debug: Show sample of transformed main data
        logger.info("Sample of transformed main medication data:")
        main_medication_df.show(3, truncate=False)
        
        # Step 3: Transform multi-valued data (all supporting tables)
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 3: TRANSFORMING MULTI-VALUED DATA")
        logger.info("=" * 50)
        
        medication_identifiers_df = transform_medication_identifiers(medication_df)
        identifiers_count = medication_identifiers_df.count()
        logger.info(f"✅ Transformed {identifiers_count:,} medication identifier records")
        
        medication_code_codings_df = transform_medication_code_codings(medication_df)
        codings_count = medication_code_codings_df.count()
        logger.info(f"✅ Transformed {codings_count:,} medication code coding records")
        
        # Debug: Show samples of multi-valued data if available
        if identifiers_count > 0:
            logger.info("Sample of medication identifiers data:")
            medication_identifiers_df.show(3, truncate=False)
        
        if codings_count > 0:
            logger.info("Sample of medication code codings data:")
            medication_code_codings_df.show(3, truncate=False)
        
        # Step 4: Convert to DynamicFrames and ensure data is flat for Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 4: CONVERTING TO DYNAMICFRAMES")
        logger.info("=" * 50)
        logger.info("Converting to DynamicFrames and ensuring Redshift compatibility...")
        
        # Convert main medications DataFrame and ensure flat structure
        main_flat_df = main_medication_df.select(
            F.col("medication_id").cast(StringType()).alias("medication_id"),
            F.col("code_text").cast(StringType()).alias("code_text"),
            F.col("status").cast(StringType()).alias("status"),
            F.col("meta_version_id").cast(StringType()).alias("meta_version_id"),
            F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
            F.col("created_at").cast(TimestampType()).alias("created_at"),
            F.col("updated_at").cast(TimestampType()).alias("updated_at")
        )
        
        main_dynamic_frame = DynamicFrame.fromDF(main_flat_df, glueContext, "main_medication_dynamic_frame")
        
        # Convert other DataFrames with type casting
        identifiers_flat_df = medication_identifiers_df.select(
            F.col("medication_id").cast(StringType()).alias("medication_id"),
            F.col("identifier_system").cast(StringType()).alias("identifier_system"),
            F.col("identifier_value").cast(StringType()).alias("identifier_value")
        )
        identifiers_dynamic_frame = DynamicFrame.fromDF(identifiers_flat_df, glueContext, "identifiers_dynamic_frame")
        
        codings_flat_df = medication_code_codings_df.select(
            F.col("medication_id").cast(StringType()).alias("medication_id"),
            F.col("code_system").cast(StringType()).alias("code_system"),
            F.col("code_value").cast(StringType()).alias("code_value"),
            F.col("code_display").cast(StringType()).alias("code_display")
        )
        codings_dynamic_frame = DynamicFrame.fromDF(codings_flat_df, glueContext, "codings_dynamic_frame")
        
        # Step 5: Resolve any remaining choice types to ensure Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 5: RESOLVING CHOICE TYPES")
        logger.info("=" * 50)
        logger.info("Resolving choice types for Redshift compatibility...")
        
        main_resolved_frame = main_dynamic_frame.resolveChoice(
            specs=[
                ("medication_id", "cast:string"),
                ("code_text", "cast:string"),
                ("status", "cast:string"),
                ("meta_version_id", "cast:string"),
                ("meta_last_updated", "cast:timestamp"),
                ("created_at", "cast:timestamp"),
                ("updated_at", "cast:timestamp")
            ]
        )
        
        identifiers_resolved_frame = identifiers_dynamic_frame.resolveChoice(
            specs=[
                ("medication_id", "cast:string"),
                ("identifier_system", "cast:string"),
                ("identifier_value", "cast:string")
            ]
        )
        
        codings_resolved_frame = codings_dynamic_frame.resolveChoice(
            specs=[
                ("medication_id", "cast:string"),
                ("code_system", "cast:string"),
                ("code_value", "cast:string"),
                ("code_display", "cast:string")
            ]
        )
        
        # Step 6: Final validation before writing
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 6: FINAL VALIDATION")
        logger.info("=" * 50)
        logger.info("Performing final validation before writing to Redshift...")
        
        # Validate main medications data
        main_final_df = main_resolved_frame.toDF()
        main_final_count = main_final_df.count()
        logger.info(f"Final main medications count: {main_final_count}")
        
        if main_final_count == 0:
            logger.error("No main medication records to write to Redshift! Stopping the process.")
            return
        
        # Validate other tables
        identifiers_final_count = identifiers_resolved_frame.toDF().count()
        codings_final_count = codings_resolved_frame.toDF().count()
        
        logger.info(f"Final counts - Identifiers: {identifiers_final_count}, Code Codings: {codings_final_count}")
        
        # Debug: Show final sample data being written
        logger.info("Final sample data being written to Redshift (main medications):")
        main_final_df.show(3, truncate=False)
        
        # Additional validation: Check for any remaining issues
        logger.info("🔍 Final validation before Redshift write:")
        logger.info(f"Final DataFrame columns: {main_final_df.columns}")
        logger.info(f"Final DataFrame schema:")
        main_final_df.printSchema()
        
        # Check for any null values in critical fields
        critical_field_checks = {
            "medication_id": main_final_df.filter(F.col("medication_id").isNull()).count()
        }
        
        logger.info("Critical field null checks:")
        for field, null_count in critical_field_checks.items():
            logger.info(f"  {field}: {null_count} NULLs")
        
        if critical_field_checks["medication_id"] > 0:
            logger.error("❌ Found NULL medication_id values - this will cause Redshift write to fail!")
        
        # Step 7: Create tables and write to Redshift
        logger.info("\n" + "=" * 50)
        logger.info("💾 STEP 7: WRITING DATA TO REDSHIFT")
        logger.info("=" * 50)
        logger.info(f"🔗 Using connection: {REDSHIFT_CONNECTION}")
        logger.info(f"📁 S3 temp directory: {S3_TEMP_DIR}")
        
        # Create all tables individually
        logger.info("📝 Dropping and recreating main medications table...")
        medications_table_sql = create_redshift_tables_sql()
        write_to_redshift(main_resolved_frame, "medications", medications_table_sql)
        logger.info("✅ Main medications table dropped, recreated and written successfully")
        
        logger.info("📝 Dropping and recreating medication identifiers table...")
        identifiers_table_sql = create_medication_identifiers_table_sql()
        write_to_redshift(identifiers_resolved_frame, "medication_identifiers", identifiers_table_sql)
        logger.info("✅ Medication identifiers table dropped, recreated and written successfully")
        
        logger.info("📝 Dropping and recreating medication code codings table...")
        codings_table_sql = create_medication_code_codings_table_sql()
        write_to_redshift(codings_resolved_frame, "medication_code_codings", codings_table_sql)
        logger.info("✅ Medication code codings table dropped, recreated and written successfully")
        
        # Calculate processing time
        end_time = datetime.now()
        processing_time = end_time - start_time
        
        logger.info("\n" + "=" * 80)
        logger.info("🎉 ETL PROCESS COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        logger.info(f"⏰ Job completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"⏱️  Total processing time: {processing_time}")
        logger.info(f"📊 Processing rate: {total_records / processing_time.total_seconds():.2f} records/second")
        
        logger.info("\n📋 TABLES WRITTEN TO REDSHIFT:")
        logger.info("  ✅ public.medications (main medication data)")
        logger.info("  ✅ public.medication_identifiers (identifier system/value pairs)")
        logger.info("  ✅ public.medication_code_codings (medication code codings)")
        
        logger.info("\n📊 FINAL ETL STATISTICS:")
        logger.info(f"  📥 Total raw records processed: {total_records:,}")
        logger.info(f"  💊 Main medication records: {main_count:,}")
        logger.info(f"  🏷️  Identifier records: {identifiers_count:,}")
        logger.info(f"  🔢 Code coding records: {codings_count:,}")
        
        # Calculate data expansion ratio
        total_output_records = main_count + identifiers_count + codings_count
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
