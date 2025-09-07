from datetime import datetime
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, BooleanType, DecimalType
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
TABLE_NAME = "medicationrequest"
REDSHIFT_CONNECTION = "Redshift connection"
S3_TEMP_DIR = "s3://aws-glue-assets-442042533707-us-east-2/temporary/"

def extract_patient_id_from_reference(reference_field):
    """Extract patient ID from FHIR reference format"""
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

def extract_medication_id_from_reference(reference_field):
    """Extract medication ID from FHIR reference format"""
    if reference_field:
        # Handle Row/struct format: Row(reference="Medication/123", display="Name")
        if hasattr(reference_field, 'reference'):
            reference = reference_field.reference
            if reference and "/" in reference:
                return reference.split("/")[-1]
        # Handle dict format: {"reference": "Medication/123", "display": "Name"}
        elif isinstance(reference_field, dict):
            reference = reference_field.get('reference')
            if reference and "/" in reference:
                return reference.split("/")[-1]
        # Handle string format: "Medication/123"
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

def convert_to_json_string(field):
    """Convert complex data to JSON strings to avoid nested structures"""
    if field is None:
        return None
    try:
        if isinstance(field, str):
            return field
        else:
            return json.dumps(field)
    except:
        return str(field)

# Define UDFs globally so they can be used in all transformation functions
convert_to_json_udf = F.udf(convert_to_json_string, StringType())

def transform_main_medication_request_data(df):
    """Transform the main medication request data"""
    logger.info("Transforming main medication request data...")
    
    # Log available columns for debugging
    available_columns = df.columns
    logger.info(f"Available columns: {available_columns}")
    
    # Build the select statement dynamically based on available columns
    select_columns = [
        F.col("id").alias("medication_request_id"),
        F.when(F.col("subject").isNotNull(), 
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        F.when(F.col("medicationReference").isNotNull(),
               F.regexp_extract(F.col("medicationReference").getField("reference"), r"Medication/(.+)", 1)
              ).otherwise(None).alias("medication_id"),
        F.col("medicationReference").getField("display").alias("medication_display"),
        F.col("status").alias("status"),
        F.col("intent").alias("intent"),
        F.col("reportedBoolean").alias("reported_boolean"),
        F.to_timestamp(F.col("authoredOn"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("authored_on"),
        F.col("meta").getField("versionId").alias("meta_version_id"),
        F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("meta_last_updated"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ]
    
    # Transform main medication request data using only available columns
    main_df = df.select(*select_columns).filter(
        F.col("medication_request_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return main_df

def transform_medication_request_identifiers(df):
    """Transform medication request identifiers (multiple identifiers per request)"""
    logger.info("Transforming medication request identifiers...")
    
    # Check if identifier column exists
    if "identifier" not in df.columns:
        logger.warning("identifier column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("medication_request_id"),
            F.lit("").alias("identifier_system"),
            F.lit("").alias("identifier_value")
        ).filter(F.lit(False))
    
    # First explode the identifier array
    identifiers_df = df.select(
        F.col("id").alias("medication_request_id"),
        F.explode(F.col("identifier")).alias("identifier_item")
    ).filter(
        F.col("identifier_item").isNotNull()
    )
    
    # Extract identifier details
    identifiers_final = identifiers_df.select(
        F.col("medication_request_id"),
        F.col("identifier_item.system").alias("identifier_system"),
        F.col("identifier_item.value").alias("identifier_value")
    ).filter(
        F.col("identifier_value").isNotNull()
    )
    
    return identifiers_final

def transform_medication_request_notes(df):
    """Transform medication request notes"""
    logger.info("Transforming medication request notes...")
    
    # Check if note column exists
    if "note" not in df.columns:
        logger.warning("note column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("medication_request_id"),
            F.lit("").alias("note_text"),
            F.lit(None).alias("note_author_reference"),
            F.lit(None).alias("note_time")
        ).filter(F.lit(False))
    
    # First explode the note array
    notes_df = df.select(
        F.col("id").alias("medication_request_id"),
        F.explode(F.col("note")).alias("note_item")
    ).filter(
        F.col("note_item").isNotNull()
    )
    
    # Extract note details
    notes_final = notes_df.select(
        F.col("medication_request_id"),
        F.col("note_item.text").alias("note_text"),
        F.col("note_item.authorReference").alias("note_author_reference"),
        F.to_timestamp(F.col("note_item.time"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("note_time")
    ).filter(
        F.col("note_text").isNotNull()
    )
    
    return notes_final

def transform_medication_request_dosage_instructions(df):
    """Transform medication request dosage instructions"""
    logger.info("Transforming medication request dosage instructions...")
    
    # Check if dosageInstruction column exists
    if "dosageInstruction" not in df.columns:
        logger.warning("dosageInstruction column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("medication_request_id"),
            F.lit("").alias("dosage_text"),
            F.lit(None).alias("dosage_timing_frequency"),
            F.lit(None).alias("dosage_timing_period"),
            F.lit(None).alias("dosage_timing_period_unit"),
            F.lit(None).alias("dosage_route_code"),
            F.lit(None).alias("dosage_route_system"),
            F.lit(None).alias("dosage_route_display"),
            F.lit(None).alias("dosage_dose_value"),
            F.lit(None).alias("dosage_dose_unit"),
            F.lit(None).alias("dosage_dose_system"),
            F.lit(None).alias("dosage_dose_code"),
            F.lit(None).alias("dosage_as_needed_boolean")
        ).filter(F.lit(False))
    
    # First explode the dosageInstruction array
    dosage_df = df.select(
        F.col("id").alias("medication_request_id"),
        F.explode(F.col("dosageInstruction")).alias("dosage_item")
    ).filter(
        F.col("dosage_item").isNotNull()
    )
    
    # Extract dosage instruction details
    dosage_final = dosage_df.select(
        F.col("medication_request_id"),
        F.col("dosage_item.text").alias("dosage_text"),
        F.col("dosage_item.timing.repeat.frequency").alias("dosage_timing_frequency"),
        F.col("dosage_item.timing.repeat.period").alias("dosage_timing_period"),
        F.col("dosage_item.timing.repeat.periodUnit").alias("dosage_timing_period_unit"),
        F.when(F.col("dosage_item.route").isNotNull() & (F.size(F.col("dosage_item.route.coding")) > 0),
               F.col("dosage_item.route.coding")[0].getField("code")
              ).otherwise(None).alias("dosage_route_code"),
        F.when(F.col("dosage_item.route").isNotNull() & (F.size(F.col("dosage_item.route.coding")) > 0),
               F.col("dosage_item.route.coding")[0].getField("system")
              ).otherwise(None).alias("dosage_route_system"),
        F.when(F.col("dosage_item.route").isNotNull() & (F.size(F.col("dosage_item.route.coding")) > 0),
               F.col("dosage_item.route.coding")[0].getField("display")
              ).otherwise(None).alias("dosage_route_display"),
        F.when(F.col("dosage_item.doseAndRate").isNotNull() & (F.size(F.col("dosage_item.doseAndRate")) > 0),
               F.col("dosage_item.doseAndRate")[0].getField("doseQuantity").getField("value")
              ).otherwise(None).alias("dosage_dose_value"),
        F.when(F.col("dosage_item.doseAndRate").isNotNull() & (F.size(F.col("dosage_item.doseAndRate")) > 0),
               F.col("dosage_item.doseAndRate")[0].getField("doseQuantity").getField("unit")
              ).otherwise(None).alias("dosage_dose_unit"),
        F.when(F.col("dosage_item.doseAndRate").isNotNull() & (F.size(F.col("dosage_item.doseAndRate")) > 0),
               F.col("dosage_item.doseAndRate")[0].getField("doseQuantity").getField("system")
              ).otherwise(None).alias("dosage_dose_system"),
        F.when(F.col("dosage_item.doseAndRate").isNotNull() & (F.size(F.col("dosage_item.doseAndRate")) > 0),
               F.col("dosage_item.doseAndRate")[0].getField("doseQuantity").getField("code")
              ).otherwise(None).alias("dosage_dose_code"),
        F.col("dosage_item.asNeededBoolean").alias("dosage_as_needed_boolean")
    ).filter(
        F.col("dosage_text").isNotNull()
    )
    
    return dosage_final

def create_redshift_tables_sql():
    """Generate SQL for creating main medication requests table in Redshift"""
    return """
    -- Drop existing table if it exists
    DROP TABLE IF EXISTS public.medication_requests CASCADE;
    
    -- Main medication requests table
    CREATE TABLE public.medication_requests (
        medication_request_id VARCHAR(255) PRIMARY KEY,
        patient_id VARCHAR(255) NOT NULL,
        medication_id VARCHAR(255),
        medication_display VARCHAR(500),
        status VARCHAR(50),
        intent VARCHAR(50),
        reported_boolean BOOLEAN,
        authored_on TIMESTAMP,
        meta_version_id VARCHAR(50),
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, authored_on);
    """

def create_medication_request_identifiers_table_sql():
    """Generate SQL for creating medication_request_identifiers table"""
    return """
    -- Drop existing table if it exists
    DROP TABLE IF EXISTS public.medication_request_identifiers CASCADE;
    
    CREATE TABLE public.medication_request_identifiers (
        medication_request_id VARCHAR(255),
        identifier_system VARCHAR(255),
        identifier_value VARCHAR(255)
    ) SORTKEY (medication_request_id, identifier_system)
    """

def create_medication_request_notes_table_sql():
    """Generate SQL for creating medication_request_notes table"""
    return """
    -- Drop existing table if it exists
    DROP TABLE IF EXISTS public.medication_request_notes CASCADE;
    
    CREATE TABLE public.medication_request_notes (
        medication_request_id VARCHAR(255),
        note_text VARCHAR(MAX),
        note_author_reference VARCHAR(255),
        note_time TIMESTAMP
    ) SORTKEY (medication_request_id, note_time)
    """

def create_medication_request_dosage_instructions_table_sql():
    """Generate SQL for creating medication_request_dosage_instructions table"""
    return """
    -- Drop existing table if it exists
    DROP TABLE IF EXISTS public.medication_request_dosage_instructions CASCADE;
    
    CREATE TABLE public.medication_request_dosage_instructions (
        medication_request_id VARCHAR(255),
        dosage_text VARCHAR(MAX),
        dosage_timing_frequency INTEGER,
        dosage_timing_period INTEGER,
        dosage_timing_period_unit VARCHAR(20),
        dosage_route_code VARCHAR(50),
        dosage_route_system VARCHAR(255),
        dosage_route_display VARCHAR(255),
        dosage_dose_value DECIMAL(10,2),
        dosage_dose_unit VARCHAR(100),
        dosage_dose_system VARCHAR(255),
        dosage_dose_code VARCHAR(50),
        dosage_as_needed_boolean BOOLEAN
    ) SORTKEY (medication_request_id, dosage_timing_frequency)
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
        logger.info("🚀 STARTING FHIR MEDICATION REQUEST ETL PROCESS")
        logger.info("=" * 80)
        logger.info(f"⏰ Job started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📊 Source: {DATABASE_NAME}.{TABLE_NAME}")
        logger.info(f"🎯 Target: Redshift (4 tables)")
        logger.info("📋 Reading all available columns from Glue Catalog")
        logger.info("🔄 Process: 7 steps (Read → Transform → Convert → Resolve → Validate → Write)")
        
        # Step 1: Read data from HealthLake using AWS Glue Data Catalog
        logger.info("\n" + "=" * 50)
        logger.info("📥 STEP 1: READING DATA FROM GLUE CATALOG")
        logger.info("=" * 50)
        logger.info(f"Database: {DATABASE_NAME}")
        logger.info(f"Table: {TABLE_NAME}")
        logger.info("Reading all available columns from Glue Catalog")
        
        # Use the AWS Glue Data Catalog to read medication request data (all columns)
        medication_request_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
                database=DATABASE_NAME, 
            table_name=TABLE_NAME, 
            transformation_ctx="AWSGlueDataCatalog_medication_request_node"
        )
        
        # Convert to DataFrame first to check available columns
        medication_request_df_raw = medication_request_dynamic_frame.toDF()
        available_columns = medication_request_df_raw.columns
        logger.info(f"📋 Available columns in source: {available_columns}")
        
        # Use all available columns
        logger.info(f"✅ Using all {len(available_columns)} available columns")
        medication_request_df = medication_request_df_raw
        
        logger.info("✅ Successfully read data using AWS Glue Data Catalog")
        
        total_records = medication_request_df.count()
        logger.info(f"📊 Read {total_records:,} raw medication request records")
        
        # Debug: Show sample of raw data and schema
        if total_records > 0:
            logger.info("\n🔍 DATA QUALITY CHECKS:")
            logger.info("Sample of raw medication request data:")
            medication_request_df.show(3, truncate=False)
            logger.info("Raw data schema:")
            medication_request_df.printSchema()
            
            # Check for NULL values in key fields
            null_checks = {
                "id": medication_request_df.filter(F.col("id").isNull()).count(),
                "subject.reference": medication_request_df.filter(F.col("subject").isNull() | F.col("subject.reference").isNull()).count(),
                "medicationReference": medication_request_df.filter(F.col("medicationReference").isNull()).count(),
                "status": medication_request_df.filter(F.col("status").isNull()).count()
            }
            
            logger.info("NULL value analysis in key fields:")
            for field, null_count in null_checks.items():
                percentage = (null_count / total_records) * 100 if total_records > 0 else 0
                logger.info(f"  {field}: {null_count:,} NULLs ({percentage:.1f}%)")
        else:
            logger.error("❌ No raw data found! Check the data source.")
            return
        
        # Step 2: Transform main medication request data
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 2: TRANSFORMING MAIN MEDICATION REQUEST DATA")
        logger.info("=" * 50)
        
        main_medication_request_df = transform_main_medication_request_data(medication_request_df)
        main_count = main_medication_request_df.count()
        logger.info(f"✅ Transformed {main_count:,} main medication request records")
        
        if main_count == 0:
            logger.error("❌ No main medication request records after transformation! Check filtering criteria.")
            return
        
        # Debug: Show actual DataFrame schema
        logger.info("🔍 Main medication request DataFrame schema:")
        logger.info(f"Columns: {main_medication_request_df.columns}")
        main_medication_request_df.printSchema()
        
        # Debug: Show sample of transformed main data
        logger.info("Sample of transformed main medication request data:")
        main_medication_request_df.show(3, truncate=False)
        
        # Step 3: Transform multi-valued data (all supporting tables)
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 3: TRANSFORMING MULTI-VALUED DATA")
        logger.info("=" * 50)
        
        medication_request_identifiers_df = transform_medication_request_identifiers(medication_request_df)
        identifiers_count = medication_request_identifiers_df.count()
        logger.info(f"✅ Transformed {identifiers_count:,} medication request identifier records")
        
        medication_request_notes_df = transform_medication_request_notes(medication_request_df)
        notes_count = medication_request_notes_df.count()
        logger.info(f"✅ Transformed {notes_count:,} medication request note records")
        
        medication_request_dosage_df = transform_medication_request_dosage_instructions(medication_request_df)
        dosage_count = medication_request_dosage_df.count()
        logger.info(f"✅ Transformed {dosage_count:,} medication request dosage instruction records")
        
        # Debug: Show samples of multi-valued data if available
        if identifiers_count > 0:
            logger.info("Sample of medication request identifiers data:")
            medication_request_identifiers_df.show(3, truncate=False)
        
        if notes_count > 0:
            logger.info("Sample of medication request notes data:")
            medication_request_notes_df.show(3, truncate=False)
        
        if dosage_count > 0:
            logger.info("Sample of medication request dosage instructions data:")
            medication_request_dosage_df.show(3, truncate=False)
        
        # Step 4: Convert to DynamicFrames and ensure data is flat for Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 4: CONVERTING TO DYNAMICFRAMES")
        logger.info("=" * 50)
        logger.info("Converting to DynamicFrames and ensuring Redshift compatibility...")
        
        # Convert main medication requests DataFrame and ensure flat structure
        main_flat_df = main_medication_request_df.select(
            F.col("medication_request_id").cast(StringType()).alias("medication_request_id"),
            F.col("patient_id").cast(StringType()).alias("patient_id"),
            F.col("medication_id").cast(StringType()).alias("medication_id"),
            F.col("medication_display").cast(StringType()).alias("medication_display"),
            F.col("status").cast(StringType()).alias("status"),
            F.col("intent").cast(StringType()).alias("intent"),
            F.col("reported_boolean").cast(BooleanType()).alias("reported_boolean"),
            F.col("authored_on").cast(TimestampType()).alias("authored_on"),
            F.col("meta_version_id").cast(StringType()).alias("meta_version_id"),
            F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
            F.col("created_at").cast(TimestampType()).alias("created_at"),
            F.col("updated_at").cast(TimestampType()).alias("updated_at")
        )
        
        main_dynamic_frame = DynamicFrame.fromDF(main_flat_df, glueContext, "main_medication_request_dynamic_frame")
        
        # Convert other DataFrames with type casting
        identifiers_flat_df = medication_request_identifiers_df.select(
            F.col("medication_request_id").cast(StringType()).alias("medication_request_id"),
            F.col("identifier_system").cast(StringType()).alias("identifier_system"),
            F.col("identifier_value").cast(StringType()).alias("identifier_value")
        )
        identifiers_dynamic_frame = DynamicFrame.fromDF(identifiers_flat_df, glueContext, "identifiers_dynamic_frame")
        
        notes_flat_df = medication_request_notes_df.select(
            F.col("medication_request_id").cast(StringType()).alias("medication_request_id"),
            F.col("note_text").cast(StringType()).alias("note_text"),
            F.col("note_author_reference").cast(StringType()).alias("note_author_reference"),
            F.col("note_time").cast(TimestampType()).alias("note_time")
        )
        notes_dynamic_frame = DynamicFrame.fromDF(notes_flat_df, glueContext, "notes_dynamic_frame")
        
        dosage_flat_df = medication_request_dosage_df.select(
            F.col("medication_request_id").cast(StringType()).alias("medication_request_id"),
            F.col("dosage_text").cast(StringType()).alias("dosage_text"),
            F.col("dosage_timing_frequency").cast("int").alias("dosage_timing_frequency"),
            F.col("dosage_timing_period").cast("int").alias("dosage_timing_period"),
            F.col("dosage_timing_period_unit").cast(StringType()).alias("dosage_timing_period_unit"),
            F.col("dosage_route_code").cast(StringType()).alias("dosage_route_code"),
            F.col("dosage_route_system").cast(StringType()).alias("dosage_route_system"),
            F.col("dosage_route_display").cast(StringType()).alias("dosage_route_display"),
            F.col("dosage_dose_value").cast(DecimalType(10,2)).alias("dosage_dose_value"),
            F.col("dosage_dose_unit").cast(StringType()).alias("dosage_dose_unit"),
            F.col("dosage_dose_system").cast(StringType()).alias("dosage_dose_system"),
            F.col("dosage_dose_code").cast(StringType()).alias("dosage_dose_code"),
            F.col("dosage_as_needed_boolean").cast(BooleanType()).alias("dosage_as_needed_boolean")
        )
        dosage_dynamic_frame = DynamicFrame.fromDF(dosage_flat_df, glueContext, "dosage_dynamic_frame")
        
        # Step 5: Resolve any remaining choice types to ensure Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 5: RESOLVING CHOICE TYPES")
        logger.info("=" * 50)
        logger.info("Resolving choice types for Redshift compatibility...")
        
        main_resolved_frame = main_dynamic_frame.resolveChoice(
            specs=[
                ("medication_request_id", "cast:string"),
                ("patient_id", "cast:string"),
                ("medication_id", "cast:string"),
                ("medication_display", "cast:string"),
                ("status", "cast:string"),
                ("intent", "cast:string"),
                ("reported_boolean", "cast:boolean"),
                ("authored_on", "cast:timestamp"),
                ("meta_version_id", "cast:string"),
                ("meta_last_updated", "cast:timestamp"),
                ("created_at", "cast:timestamp"),
                ("updated_at", "cast:timestamp")
            ]
        )
        
        identifiers_resolved_frame = identifiers_dynamic_frame.resolveChoice(
            specs=[
                ("medication_request_id", "cast:string"),
                ("identifier_system", "cast:string"),
                ("identifier_value", "cast:string")
            ]
        )
        
        notes_resolved_frame = notes_dynamic_frame.resolveChoice(
            specs=[
                ("medication_request_id", "cast:string"),
                ("note_text", "cast:string"),
                ("note_author_reference", "cast:string"),
                ("note_time", "cast:timestamp")
            ]
        )
        
        dosage_resolved_frame = dosage_dynamic_frame.resolveChoice(
            specs=[
                ("medication_request_id", "cast:string"),
                ("dosage_text", "cast:string"),
                ("dosage_timing_frequency", "cast:int"),
                ("dosage_timing_period", "cast:int"),
                ("dosage_timing_period_unit", "cast:string"),
                ("dosage_route_code", "cast:string"),
                ("dosage_route_system", "cast:string"),
                ("dosage_route_display", "cast:string"),
                ("dosage_dose_value", "cast:decimal"),
                ("dosage_dose_unit", "cast:string"),
                ("dosage_dose_system", "cast:string"),
                ("dosage_dose_code", "cast:string"),
                ("dosage_as_needed_boolean", "cast:boolean")
            ]
        )
        
        # Step 6: Final validation before writing
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 6: FINAL VALIDATION")
        logger.info("=" * 50)
        logger.info("Performing final validation before writing to Redshift...")
        
        # Validate main medication requests data
        main_final_df = main_resolved_frame.toDF()
        main_final_count = main_final_df.count()
        logger.info(f"Final main medication requests count: {main_final_count}")
        
        if main_final_count == 0:
            logger.error("No main medication request records to write to Redshift! Stopping the process.")
            return
        
        # Validate other tables
        identifiers_final_count = identifiers_resolved_frame.toDF().count()
        notes_final_count = notes_resolved_frame.toDF().count()
        dosage_final_count = dosage_resolved_frame.toDF().count()
        
        logger.info(f"Final counts - Identifiers: {identifiers_final_count}, Notes: {notes_final_count}, Dosage Instructions: {dosage_final_count}")
        
        # Debug: Show final sample data being written
        logger.info("Final sample data being written to Redshift (main medication requests):")
        main_final_df.show(3, truncate=False)
        
        # Additional validation: Check for any remaining issues
        logger.info("🔍 Final validation before Redshift write:")
        logger.info(f"Final DataFrame columns: {main_final_df.columns}")
        logger.info(f"Final DataFrame schema:")
        main_final_df.printSchema()
        
        # Check for any null values in critical fields
        critical_field_checks = {
            "medication_request_id": main_final_df.filter(F.col("medication_request_id").isNull()).count(),
            "patient_id": main_final_df.filter(F.col("patient_id").isNull()).count(),
        }
        
        logger.info("Critical field null checks:")
        for field, null_count in critical_field_checks.items():
            logger.info(f"  {field}: {null_count} NULLs")
        
        if critical_field_checks["medication_request_id"] > 0:
            logger.error("❌ Found NULL medication_request_id values - this will cause Redshift write to fail!")
        
        if critical_field_checks["patient_id"] > 0:
            logger.error("❌ Found NULL patient_id values - this will cause Redshift write to fail!")
        
        # Step 7: Create tables and write to Redshift
        logger.info("\n" + "=" * 50)
        logger.info("💾 STEP 7: WRITING DATA TO REDSHIFT")
        logger.info("=" * 50)
        logger.info(f"🔗 Using connection: {REDSHIFT_CONNECTION}")
        logger.info(f"📁 S3 temp directory: {S3_TEMP_DIR}")
        
        # Create all tables individually
        logger.info("📝 Dropping and recreating main medication requests table...")
        medication_requests_table_sql = create_redshift_tables_sql()
        write_to_redshift(main_resolved_frame, "medication_requests", medication_requests_table_sql)
        logger.info("✅ Main medication requests table dropped, recreated and written successfully")
        
        logger.info("📝 Dropping and recreating medication request identifiers table...")
        identifiers_table_sql = create_medication_request_identifiers_table_sql()
        write_to_redshift(identifiers_resolved_frame, "medication_request_identifiers", identifiers_table_sql)
        logger.info("✅ Medication request identifiers table dropped, recreated and written successfully")
        
        logger.info("📝 Dropping and recreating medication request notes table...")
        notes_table_sql = create_medication_request_notes_table_sql()
        write_to_redshift(notes_resolved_frame, "medication_request_notes", notes_table_sql)
        logger.info("✅ Medication request notes table dropped, recreated and written successfully")
        
        logger.info("📝 Dropping and recreating medication request dosage instructions table...")
        dosage_table_sql = create_medication_request_dosage_instructions_table_sql()
        write_to_redshift(dosage_resolved_frame, "medication_request_dosage_instructions", dosage_table_sql)
        logger.info("✅ Medication request dosage instructions table dropped, recreated and written successfully")
        
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
        logger.info("  ✅ public.medication_requests (main medication request data)")
        logger.info("  ✅ public.medication_request_identifiers (identifier system/value pairs)")
        logger.info("  ✅ public.medication_request_notes (notes and annotations)")
        logger.info("  ✅ public.medication_request_dosage_instructions (dosage instructions with timing and route)")
        
        logger.info("\n📊 FINAL ETL STATISTICS:")
        logger.info(f"  📥 Total raw records processed: {total_records:,}")
        logger.info(f"  💊 Main medication request records: {main_count:,}")
        logger.info(f"  🏷️  Identifier records: {identifiers_count:,}")
        logger.info(f"  📝 Note records: {notes_count:,}")
        logger.info(f"  💉 Dosage instruction records: {dosage_count:,}")
        
        # Calculate data expansion ratio
        total_output_records = main_count + identifiers_count + notes_count + dosage_count
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
