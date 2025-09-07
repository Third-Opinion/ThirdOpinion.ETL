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
TABLE_NAME = "medicationdispense"
REDSHIFT_CONNECTION = "Redshift connection"
S3_TEMP_DIR = "s3://aws-glue-assets-442042533707-us-east-2/temporary/"

def transform_main_medication_dispense_data(df):
    """Transform the main medication dispense data"""
    logger.info("Transforming main medication dispense data...")
    
    # Log available columns for debugging
    available_columns = df.columns
    logger.info(f"Available columns: {available_columns}")
    
    # Build the select statement dynamically based on available columns
    select_columns = [
        F.col("id").alias("medication_dispense_id"),
        F.col("resourceType").alias("resource_type"),
        F.col("status").alias("status"),
        
        # Extract patient ID from subject.reference
        F.when(F.col("subject").isNotNull(), 
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        
        # Extract medication ID and display from medicationReference
        F.when(F.col("medicationReference").isNotNull(),
               F.regexp_extract(F.col("medicationReference").getField("reference"), r"Medication/(.+)", 1)
              ).otherwise(None).alias("medication_id"),
        F.when(F.col("medicationReference").isNotNull(),
               F.col("medicationReference").getField("display")
              ).otherwise(None).alias("medication_display"),
        
        # Extract quantity value
        F.when(F.col("quantity").isNotNull(),
               F.col("quantity").getField("value")
              ).otherwise(None).alias("quantity_value"),
        F.when(F.col("quantity").isNotNull(),
               F.col("quantity").getField("unit")
              ).otherwise(None).alias("quantity_unit"),
        
        # Convert whenHandedOver to timestamp
        F.to_timestamp(F.col("whenHandedOver"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("when_handed_over"),
        
        # Meta fields
        F.when(F.col("meta").isNotNull(),
               F.col("meta").getField("versionId")
              ).otherwise(None).alias("meta_version_id"),
        F.when(F.col("meta").isNotNull(),
               F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'")
              ).otherwise(None).alias("meta_last_updated"),
        
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ]
    
    # Transform main medication dispense data using only available columns
    main_df = df.select(*select_columns).filter(
        F.col("medication_dispense_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return main_df

def transform_medication_dispense_identifiers(df):
    """Transform medication dispense identifiers (multiple identifiers per dispense)"""
    logger.info("Transforming medication dispense identifiers...")
    
    # Check if identifier column exists
    if "identifier" not in df.columns:
        logger.warning("identifier column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("medication_dispense_id"),
            F.lit("").alias("identifier_system"),
            F.lit("").alias("identifier_value")
        ).filter(F.lit(False))
    
    # First explode the identifier array
    identifiers_df = df.select(
        F.col("id").alias("medication_dispense_id"),
        F.explode(F.col("identifier")).alias("identifier_item")
    ).filter(
        F.col("identifier_item").isNotNull()
    )
    
    # Extract identifier details
    identifiers_final = identifiers_df.select(
        F.col("medication_dispense_id"),
        F.col("identifier_item.system").alias("identifier_system"),
        F.col("identifier_item.value").alias("identifier_value")
    ).filter(
        F.col("identifier_value").isNotNull()
    )
    
    return identifiers_final

def transform_medication_dispense_performers(df):
    """Transform medication dispense performers (who dispensed the medication)"""
    logger.info("Transforming medication dispense performers...")
    
    # Check if performer column exists
    if "performer" not in df.columns:
        logger.warning("performer column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("medication_dispense_id"),
            F.lit("").alias("performer_actor_reference"),
            F.lit("").alias("performer_function_code"),
            F.lit("").alias("performer_function_display")
        ).filter(F.lit(False))
    
    # First explode the performer array
    performers_df = df.select(
        F.col("id").alias("medication_dispense_id"),
        F.explode(F.col("performer")).alias("performer_item")
    ).filter(
        F.col("performer_item").isNotNull()
    )
    
    # Extract performer details
    performers_final = performers_df.select(
        F.col("medication_dispense_id"),
        F.when(F.col("performer_item.actor").isNotNull(),
               F.col("performer_item.actor.reference")
              ).otherwise(None).alias("performer_actor_reference"),
        F.when(F.col("performer_item.function.coding").isNotNull() & 
               (F.size(F.col("performer_item.function.coding")) > 0),
               F.col("performer_item.function.coding")[0].getField("code")
              ).otherwise(None).alias("performer_function_code"),
        F.when(F.col("performer_item.function.coding").isNotNull() & 
               (F.size(F.col("performer_item.function.coding")) > 0),
               F.col("performer_item.function.coding")[0].getField("display")
              ).otherwise(None).alias("performer_function_display")
    ).filter(
        F.col("performer_actor_reference").isNotNull()
    )
    
    return performers_final

def create_redshift_tables_sql():
    """Generate SQL for creating main medication_dispenses table in Redshift"""
    return """
    -- Drop existing table if it exists
    DROP TABLE IF EXISTS public.medication_dispenses CASCADE;
    
    -- Main medication dispenses table
    CREATE TABLE public.medication_dispenses (
        medication_dispense_id VARCHAR(255) PRIMARY KEY,
        resource_type VARCHAR(50),
        status VARCHAR(50),
        patient_id VARCHAR(255) NOT NULL,
        medication_id VARCHAR(255),
        medication_display VARCHAR(500),
        quantity_value DECIMAL(10,2),
        quantity_unit VARCHAR(50),
        when_handed_over TIMESTAMP,
        meta_version_id VARCHAR(50),
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, when_handed_over);
    """

def create_medication_dispense_identifiers_table_sql():
    """Generate SQL for creating medication_dispense_identifiers table"""
    return """
    -- Drop existing table if it exists
    DROP TABLE IF EXISTS public.medication_dispense_identifiers CASCADE;
    
    CREATE TABLE public.medication_dispense_identifiers (
        medication_dispense_id VARCHAR(255),
        identifier_system VARCHAR(255),
        identifier_value VARCHAR(255)
    ) SORTKEY (medication_dispense_id, identifier_system)
    """

def create_medication_dispense_performers_table_sql():
    """Generate SQL for creating medication_dispense_performers table"""
    return """
    -- Drop existing table if it exists
    DROP TABLE IF EXISTS public.medication_dispense_performers CASCADE;
    
    CREATE TABLE public.medication_dispense_performers (
        medication_dispense_id VARCHAR(255),
        performer_actor_reference VARCHAR(255),
        performer_function_code VARCHAR(50),
        performer_function_display VARCHAR(255)
    ) SORTKEY (medication_dispense_id, performer_actor_reference)
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
        logger.info("🚀 STARTING FHIR MEDICATION DISPENSE ETL PROCESS")
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
        
        # Use the AWS Glue Data Catalog to read medication dispense data (all columns)
        medication_dispense_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=DATABASE_NAME, 
            table_name=TABLE_NAME, 
            transformation_ctx="AWSGlueDataCatalog_medication_dispense_node"
        )
        
        # Convert to DataFrame first to check available columns
        medication_dispense_df_raw = medication_dispense_dynamic_frame.toDF()
        available_columns = medication_dispense_df_raw.columns
        logger.info(f"📋 Available columns in source: {available_columns}")
        
        # Use all available columns
        logger.info(f"✅ Using all {len(available_columns)} available columns")
        medication_dispense_df = medication_dispense_df_raw
        
        logger.info("✅ Successfully read data using AWS Glue Data Catalog")
        
        total_records = medication_dispense_df.count()
        logger.info(f"📊 Read {total_records:,} raw medication dispense records")
        
        # Debug: Show sample of raw data and schema
        if total_records > 0:
            logger.info("\n🔍 DATA QUALITY CHECKS:")
            logger.info("Sample of raw medication dispense data:")
            medication_dispense_df.show(3, truncate=False)
            logger.info("Raw data schema:")
            medication_dispense_df.printSchema()
            
            # Check for NULL values in key fields
            null_checks = {
                "id": medication_dispense_df.filter(F.col("id").isNull()).count(),
                "subject.reference": medication_dispense_df.filter(F.col("subject").isNull() | F.col("subject.reference").isNull()).count(),
                "medicationReference": medication_dispense_df.filter(F.col("medicationReference").isNull()).count(),
                "status": medication_dispense_df.filter(F.col("status").isNull()).count()
            }
            
            logger.info("NULL value analysis in key fields:")
            for field, null_count in null_checks.items():
                percentage = (null_count / total_records) * 100 if total_records > 0 else 0
                logger.info(f"  {field}: {null_count:,} NULLs ({percentage:.1f}%)")
        else:
            logger.error("❌ No raw data found! Check the data source.")
            return
        
        # Step 2: Transform main medication dispense data
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 2: TRANSFORMING MAIN MEDICATION DISPENSE DATA")
        logger.info("=" * 50)
        
        main_medication_dispense_df = transform_main_medication_dispense_data(medication_dispense_df)
        main_count = main_medication_dispense_df.count()
        logger.info(f"✅ Transformed {main_count:,} main medication dispense records")
        
        if main_count == 0:
            logger.error("❌ No main medication dispense records after transformation! Check filtering criteria.")
            return
        
        # Debug: Show actual DataFrame schema
        logger.info("🔍 Main medication dispense DataFrame schema:")
        logger.info(f"Columns: {main_medication_dispense_df.columns}")
        main_medication_dispense_df.printSchema()
        
        # Debug: Show sample of transformed main data
        logger.info("Sample of transformed main medication dispense data:")
        main_medication_dispense_df.show(3, truncate=False)
        
        # Step 3: Transform multi-valued data (all supporting tables)
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 3: TRANSFORMING MULTI-VALUED DATA")
        logger.info("=" * 50)
        
        medication_dispense_identifiers_df = transform_medication_dispense_identifiers(medication_dispense_df)
        identifiers_count = medication_dispense_identifiers_df.count()
        logger.info(f"✅ Transformed {identifiers_count:,} medication dispense identifier records")
        
        medication_dispense_performers_df = transform_medication_dispense_performers(medication_dispense_df)
        performers_count = medication_dispense_performers_df.count()
        logger.info(f"✅ Transformed {performers_count:,} medication dispense performer records")
        
        # Debug: Show samples of multi-valued data if available
        if identifiers_count > 0:
            logger.info("Sample of medication dispense identifiers data:")
            medication_dispense_identifiers_df.show(3, truncate=False)
        
        if performers_count > 0:
            logger.info("Sample of medication dispense performers data:")
            medication_dispense_performers_df.show(3, truncate=False)
        
        # Step 4: Convert to DynamicFrames and ensure data is flat for Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 4: CONVERTING TO DYNAMICFRAMES")
        logger.info("=" * 50)
        logger.info("Converting to DynamicFrames and ensuring Redshift compatibility...")
        
        # Convert main medication dispenses DataFrame and ensure flat structure
        main_flat_df = main_medication_dispense_df.select(
            F.col("medication_dispense_id").cast(StringType()).alias("medication_dispense_id"),
            F.col("resource_type").cast(StringType()).alias("resource_type"),
            F.col("status").cast(StringType()).alias("status"),
            F.col("patient_id").cast(StringType()).alias("patient_id"),
            F.col("medication_id").cast(StringType()).alias("medication_id"),
            F.col("medication_display").cast(StringType()).alias("medication_display"),
            F.col("quantity_value").cast(DecimalType(10,2)).alias("quantity_value"),
            F.col("quantity_unit").cast(StringType()).alias("quantity_unit"),
            F.col("when_handed_over").cast(TimestampType()).alias("when_handed_over"),
            F.col("meta_version_id").cast(StringType()).alias("meta_version_id"),
            F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
            F.col("created_at").cast(TimestampType()).alias("created_at"),
            F.col("updated_at").cast(TimestampType()).alias("updated_at")
        )
        
        main_dynamic_frame = DynamicFrame.fromDF(main_flat_df, glueContext, "main_medication_dispense_dynamic_frame")
        
        # Convert other DataFrames with type casting
        identifiers_flat_df = medication_dispense_identifiers_df.select(
            F.col("medication_dispense_id").cast(StringType()).alias("medication_dispense_id"),
            F.col("identifier_system").cast(StringType()).alias("identifier_system"),
            F.col("identifier_value").cast(StringType()).alias("identifier_value")
        )
        identifiers_dynamic_frame = DynamicFrame.fromDF(identifiers_flat_df, glueContext, "identifiers_dynamic_frame")
        
        performers_flat_df = medication_dispense_performers_df.select(
            F.col("medication_dispense_id").cast(StringType()).alias("medication_dispense_id"),
            F.col("performer_actor_reference").cast(StringType()).alias("performer_actor_reference"),
            F.col("performer_function_code").cast(StringType()).alias("performer_function_code"),
            F.col("performer_function_display").cast(StringType()).alias("performer_function_display")
        )
        performers_dynamic_frame = DynamicFrame.fromDF(performers_flat_df, glueContext, "performers_dynamic_frame")
        
        # Step 5: Resolve any remaining choice types to ensure Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 5: RESOLVING CHOICE TYPES")
        logger.info("=" * 50)
        logger.info("Resolving choice types for Redshift compatibility...")
        
        main_resolved_frame = main_dynamic_frame.resolveChoice(
            specs=[
                ("medication_dispense_id", "cast:string"),
                ("resource_type", "cast:string"),
                ("status", "cast:string"),
                ("patient_id", "cast:string"),
                ("medication_id", "cast:string"),
                ("medication_display", "cast:string"),
                ("quantity_value", "cast:decimal"),
                ("quantity_unit", "cast:string"),
                ("when_handed_over", "cast:timestamp"),
                ("meta_version_id", "cast:string"),
                ("meta_last_updated", "cast:timestamp"),
                ("created_at", "cast:timestamp"),
                ("updated_at", "cast:timestamp")
            ]
        )
        
        identifiers_resolved_frame = identifiers_dynamic_frame.resolveChoice(
            specs=[
                ("medication_dispense_id", "cast:string"),
                ("identifier_system", "cast:string"),
                ("identifier_value", "cast:string")
            ]
        )
        
        performers_resolved_frame = performers_dynamic_frame.resolveChoice(
            specs=[
                ("medication_dispense_id", "cast:string"),
                ("performer_actor_reference", "cast:string"),
                ("performer_function_code", "cast:string"),
                ("performer_function_display", "cast:string")
            ]
        )
        
        # Step 6: Final validation before writing
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 6: FINAL VALIDATION")
        logger.info("=" * 50)
        logger.info("Performing final validation before writing to Redshift...")
        
        # Validate main medication dispenses data
        main_final_df = main_resolved_frame.toDF()
        main_final_count = main_final_df.count()
        logger.info(f"Final main medication dispenses count: {main_final_count}")
        
        if main_final_count == 0:
            logger.error("No main medication dispense records to write to Redshift! Stopping the process.")
            return
        
        # Validate other tables
        identifiers_final_count = identifiers_resolved_frame.toDF().count()
        performers_final_count = performers_resolved_frame.toDF().count()
        
        logger.info(f"Final counts - Identifiers: {identifiers_final_count}, Performers: {performers_final_count}")
        
        # Debug: Show final sample data being written
        logger.info("Final sample data being written to Redshift (main medication dispenses):")
        main_final_df.show(3, truncate=False)
        
        # Additional validation: Check for any remaining issues
        logger.info("🔍 Final validation before Redshift write:")
        logger.info(f"Final DataFrame columns: {main_final_df.columns}")
        logger.info(f"Final DataFrame schema:")
        main_final_df.printSchema()
        
        # Check for any null values in critical fields
        critical_field_checks = {
            "medication_dispense_id": main_final_df.filter(F.col("medication_dispense_id").isNull()).count(),
            "patient_id": main_final_df.filter(F.col("patient_id").isNull()).count(),
        }
        
        logger.info("Critical field null checks:")
        for field, null_count in critical_field_checks.items():
            logger.info(f"  {field}: {null_count} NULLs")
        
        if critical_field_checks["medication_dispense_id"] > 0:
            logger.error("❌ Found NULL medication_dispense_id values - this will cause Redshift write to fail!")
        
        if critical_field_checks["patient_id"] > 0:
            logger.error("❌ Found NULL patient_id values - this will cause Redshift write to fail!")
        
        # Step 7: Create tables and write to Redshift
        logger.info("\n" + "=" * 50)
        logger.info("💾 STEP 7: WRITING DATA TO REDSHIFT")
        logger.info("=" * 50)
        logger.info(f"🔗 Using connection: {REDSHIFT_CONNECTION}")
        logger.info(f"📁 S3 temp directory: {S3_TEMP_DIR}")
        
        # Create all tables individually
        logger.info("📝 Dropping and recreating main medication dispenses table...")
        medication_dispenses_table_sql = create_redshift_tables_sql()
        write_to_redshift(main_resolved_frame, "medication_dispenses", medication_dispenses_table_sql)
        logger.info("✅ Main medication dispenses table dropped, recreated and written successfully")
        
        logger.info("📝 Dropping and recreating medication dispense identifiers table...")
        identifiers_table_sql = create_medication_dispense_identifiers_table_sql()
        write_to_redshift(identifiers_resolved_frame, "medication_dispense_identifiers", identifiers_table_sql)
        logger.info("✅ Medication dispense identifiers table dropped, recreated and written successfully")
        
        logger.info("📝 Dropping and recreating medication dispense performers table...")
        performers_table_sql = create_medication_dispense_performers_table_sql()
        write_to_redshift(performers_resolved_frame, "medication_dispense_performers", performers_table_sql)
        logger.info("✅ Medication dispense performers table dropped, recreated and written successfully")
        
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
        logger.info("  ✅ public.medication_dispenses (main medication dispense data)")
        logger.info("  ✅ public.medication_dispense_identifiers (identifier system/value pairs)")
        logger.info("  ✅ public.medication_dispense_performers (performer/dispenser information)")
        
        logger.info("\n📊 FINAL ETL STATISTICS:")
        logger.info(f"  📥 Total raw records processed: {total_records:,}")
        logger.info(f"  💊 Main medication dispense records: {main_count:,}")
        logger.info(f"  🏷️  Identifier records: {identifiers_count:,}")
        logger.info(f"  👤 Performer records: {performers_count:,}")
        
        # Calculate data expansion ratio
        total_output_records = main_count + identifiers_count + performers_count
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
