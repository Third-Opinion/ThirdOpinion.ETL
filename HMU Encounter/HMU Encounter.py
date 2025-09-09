from datetime import datetime
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType
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
TABLE_NAME = "encounter"
REDSHIFT_CONNECTION = "Redshift connection"
S3_TEMP_DIR = "s3://aws-glue-assets-442042533707-us-east-2/temporary/"

# Note: We now read all available columns from the Glue Catalog
# instead of filtering to specific columns

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

def extract_organization_id_from_reference(reference_field):
    """Extract organization ID from FHIR reference format"""
    if reference_field:
        # Handle Row/struct format: Row(reference="Organization/123", display="Name")
        if hasattr(reference_field, 'reference'):
            reference = reference_field.reference
            if reference and "/" in reference:
                return reference.split("/")[-1]
        # Handle dict format: {"reference": "Organization/123", "display": "Name"}
        elif isinstance(reference_field, dict):
            reference = reference_field.get('reference')
            if reference and "/" in reference:
                return reference.split("/")[-1]
        # Handle string format: "Organization/123"
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

def transform_main_encounter_data(df):
    """Transform the main encounter data"""
    logger.info("Transforming main encounter data...")
    
    # Log available columns for debugging
    available_columns = df.columns
    logger.info(f"Available columns: {available_columns}")
    
    # Using native Spark column operations for better performance
    
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
    
    # Build the select statement dynamically based on available columns
    select_columns = [
        F.col("id").alias("encounter_id"),
        F.when(F.col("subject").isNotNull(), 
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        F.col("status"),
        F.lit("Encounter").alias("resourcetype"),  # Always "Encounter" for encounter records
        F.col("class").getField("code").alias("class_code"),
        F.col("class").getField("system").alias("class_display"),
        F.to_timestamp(F.col("period").getField("start"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("start_time"),
        F.to_timestamp(F.col("period").getField("end"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("end_time"),
    ]
    
    # Add service_provider_id handling - extract from serviceprovider field
    select_columns.append(
        F.when(F.col("serviceprovider").isNotNull(),
               F.regexp_extract(F.col("serviceprovider").getField("reference"), r"Organization/(.+)", 1)
              ).otherwise(None).alias("service_provider_id")
    )
    
    # Add remaining columns
    select_columns.extend([
        F.lit(None).alias("appointment_id"),  # Not available in schema
        F.lit(None).alias("parent_encounter_id"),
        convert_to_json_udf(F.col("meta")).alias("meta_data"),  # Convert to JSON string
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ])
    
    # Transform main encounter data using only available columns and flatten complex structures
    main_df = df.select(*select_columns).filter(
        F.col("encounter_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return main_df

def transform_encounter_types(df):
    """Transform encounter types (multiple CPT codes per encounter)"""
    logger.info("Transforming encounter types...")
    
    # Use Spark's native column operations to handle the nested structure
    # type: array -> element: struct -> coding: array -> element: struct
    
    # First explode the type array
    types_df = df.select(
        F.col("id").alias("encounter_id"),
        F.explode(F.col("type")).alias("type_item")
    ).filter(
        F.col("type_item").isNotNull()
    )
    
    # Extract type details and explode the coding array
    encounter_types_final = types_df.select(
        F.col("encounter_id"),
        F.explode(F.col("type_item.coding")).alias("coding_item"),
        F.lit("").alias("type_text")  # Use empty string as default since text field may not exist
    ).select(
        F.col("encounter_id"),
        F.col("coding_item.code").alias("type_code"),
        F.col("coding_item.system").alias("type_system"),
        F.col("coding_item.display").alias("type_display"),
        F.col("type_text")
    ).filter(
        F.col("type_code").isNotNull()
    )
    
    return encounter_types_final

def transform_encounter_participants(df):
    """Transform encounter participants"""
    logger.info("Transforming encounter participants...")
    
    # Use Spark's native column operations to handle the nested structure
    # participant: array -> element: struct -> type: array -> element: struct -> coding: array -> element: struct
    
    # First explode the participant array
    participants_df = df.select(
        F.col("id").alias("encounter_id"),
        F.explode(F.col("participant")).alias("participant_item")
    ).filter(
        F.col("participant_item").isNotNull()
    )
    
    # Extract participant details and explode the type array
    participants_with_types = participants_df.select(
        F.col("encounter_id"),
        F.explode(F.col("participant_item.type")).alias("type_item"),
        F.col("participant_item.period").alias("period_data"),
        F.col("participant_item.individual").alias("individual_data")
    ).filter(
        F.col("type_item").isNotNull() & F.col("individual_data").isNotNull()
    )
    
    # Extract type information and explode the coding array
    participants_final = participants_with_types.select(
        F.col("encounter_id"),
        F.explode(F.col("type_item.coding")).alias("coding_item"),
        F.col("period_data"),
        F.col("individual_data")
    ).select(
        F.col("encounter_id"),
        F.col("coding_item.code").alias("participant_type"),
        F.when(F.col("individual_data").isNotNull(),
               F.regexp_extract(F.col("individual_data").getField("reference"), r"Practitioner/(.+)", 1)
              ).otherwise(None).alias("participant_id"),
        F.lit(None).alias("participant_display"),  # display field not available in individual_data structure
        F.to_timestamp(F.col("period_data.start"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("period_start"),
        F.to_timestamp(F.col("period_data.end"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("period_end")
    ).filter(
        F.col("participant_type").isNotNull() & F.col("participant_id").isNotNull()
    )
    
    return participants_final

def transform_encounter_reasons(df):
    """Transform encounter reason codes"""
    logger.info("Transforming encounter reasons...")
    
    # Check if reasonCode column exists (note: camelCase in schema)
    if "reasonCode" not in df.columns:
        logger.warning("reasonCode column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("encounter_id"),
            F.lit("").alias("reason_code"),
            F.lit("").alias("reason_system"),
            F.lit("").alias("reason_display"),
            F.lit("").alias("reason_text")
        ).filter(F.lit(False))
    
    # Use Spark's native column operations to handle the nested structure
    # reasoncode: array -> element: struct -> {text: string, coding: array or null}
    
    # First explode the reasoncode array
    reasons_df = df.select(
        F.col("id").alias("encounter_id"),
        F.explode(F.col("reasonCode")).alias("reason_item")
    ).filter(
        F.col("reason_item").isNotNull()
    )
    
    # Extract reason details - handle both text and coding fields
    # The coding field can be null, so we need to handle that case
    reasons_with_coding = reasons_df.select(
        F.col("encounter_id"),
        F.col("reason_item.text").alias("reason_text"),
        F.col("reason_item.coding").alias("coding_array")
    ).filter(
        F.col("reason_text").isNotNull()  # Only process records with text
    )
    
    # Handle cases where coding exists vs where it's null
    reasons_final = reasons_with_coding.select(
        F.col("encounter_id"),
        F.col("reason_text"),
        # Extract coding details if coding array exists and is not null
        F.when(F.col("coding_array").isNotNull() & (F.size(F.col("coding_array")) > 0),
               F.col("coding_array")[0].getField("code")
              ).otherwise(None).alias("reason_code"),
        F.when(F.col("coding_array").isNotNull() & (F.size(F.col("coding_array")) > 0),
               F.col("coding_array")[0].getField("system")
              ).otherwise(None).alias("reason_system"),
        F.when(F.col("coding_array").isNotNull() & (F.size(F.col("coding_array")) > 0),
               F.col("coding_array")[0].getField("display")
              ).otherwise(None).alias("reason_display")
    )
    
    return reasons_final


def transform_encounter_locations(df):
    """Transform encounter locations"""
    logger.info("Transforming encounter locations...")
    
    # Use Spark's native column operations to handle the nested structure
    # location: array -> element: struct -> location.reference
    
    # First explode the location array
    locations_df = df.select(
        F.col("id").alias("encounter_id"),
        F.explode(F.col("location")).alias("location_item")
    ).filter(
        F.col("location_item").isNotNull()
    )
    
    # Extract location details
    locations_final = locations_df.select(
        F.col("encounter_id"),
        F.when(F.col("location_item").isNotNull() & F.col("location_item.location").isNotNull(),
               F.regexp_extract(F.col("location_item.location").getField("reference"), r"Location/(.+)", 1)
              ).otherwise(None).alias("location_id")
    ).filter(
        F.col("location_id").isNotNull()
    )
    
    return locations_final

def transform_encounter_hospitalization(df):
    """Transform encounter hospitalization/discharge information"""
    logger.info("Transforming encounter hospitalization...")
    
    # Check if hospitalization column exists and has data
    if "hospitalization" not in df.columns:
        logger.warning("hospitalization column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("encounter_id"),
            F.lit("").alias("discharge_disposition_text"),
            F.lit("").alias("discharge_code"),
            F.lit("").alias("discharge_system")
        ).filter(F.lit(False))
    
    # Extract hospitalization details
    hospitalization_final = df.select(
        F.col("id").alias("encounter_id"),
        F.col("hospitalization").getField("dischargedisposition").getField("text").alias("discharge_disposition_text"),
        F.col("hospitalization").getField("dischargedisposition").getField("coding").alias("discharge_coding")
    ).filter(
        F.col("hospitalization").isNotNull()
    )
    
    # Explode discharge coding if it exists
    hospitalization_with_coding = hospitalization_final.select(
        F.col("encounter_id"),
        F.col("discharge_disposition_text"),
        F.explode(F.col("discharge_coding")).alias("coding_item")
    ).select(
        F.col("encounter_id"),
        F.col("discharge_disposition_text"),
        F.col("coding_item.code").alias("discharge_code"),
        F.col("coding_item.system").alias("discharge_system")
    ).filter(
        F.col("discharge_code").isNotNull()
    )
    
    return hospitalization_with_coding

def create_redshift_tables_sql():
    """Generate SQL for creating all tables in Redshift with proper syntax"""
    return """
    -- Main encounters table
    CREATE TABLE IF NOT EXISTS public.encounters (
        encounter_id VARCHAR(255) PRIMARY KEY,
        patient_id VARCHAR(255),
        status VARCHAR(50),
        resourcetype VARCHAR(50),
        class_code VARCHAR(10),
        class_display VARCHAR(255),
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        service_provider_id VARCHAR(255),
        appointment_id VARCHAR(255),
        parent_encounter_id VARCHAR(255),
        meta_data TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, start_time)
    """

def create_encounter_types_table_sql():
    """Generate SQL for creating encounter_types table"""
    return """
    CREATE TABLE IF NOT EXISTS public.encounter_types (
        encounter_id VARCHAR(255),
        type_code VARCHAR(50),
        type_system VARCHAR(255),
        type_display VARCHAR(255),
        type_text VARCHAR(500)
    ) SORTKEY (encounter_id, type_code)
    """

def create_encounter_participants_table_sql():
    """Generate SQL for creating encounter_participants table"""
    return """
    CREATE TABLE IF NOT EXISTS public.encounter_participants (
        encounter_id VARCHAR(255),
        participant_type VARCHAR(50),
        participant_id VARCHAR(255),
        participant_display VARCHAR(255),
        period_start TIMESTAMP,
        period_end TIMESTAMP
    ) SORTKEY (encounter_id, participant_type)
    """

def create_encounter_reasons_table_sql():
    """Generate SQL for creating encounter_reasons table"""
    return """
    CREATE TABLE IF NOT EXISTS public.encounter_reasons (
        encounter_id VARCHAR(255),
        reason_code VARCHAR(50),
        reason_system VARCHAR(255),
        reason_display VARCHAR(255),
        reason_text VARCHAR(500)
    ) SORTKEY (encounter_id, reason_code)
    """

def create_encounter_locations_table_sql():
    """Generate SQL for creating encounter_locations table"""
    return """
    CREATE TABLE IF NOT EXISTS public.encounter_locations (
        encounter_id VARCHAR(255),
        location_id VARCHAR(255)
    ) SORTKEY (encounter_id)
    """

def create_encounter_hospitalization_table_sql():
    """Generate SQL for creating encounter_hospitalization table"""
    return """
    CREATE TABLE IF NOT EXISTS public.encounter_hospitalization (
        encounter_id VARCHAR(255),
        discharge_disposition_text VARCHAR(500),
        discharge_code VARCHAR(50),
        discharge_system VARCHAR(500)
    ) SORTKEY (encounter_id)
    """

# Note: Redshift doesn't support traditional indexes
# Performance optimization in Redshift is achieved through:
# 1. SORTKEY - for sorting data within each slice
# 2. DISTKEY - for distributing data across slices
# 3. Column compression - for reducing storage and improving query performance
# 4. Query optimization - using appropriate WHERE clauses and JOIN patterns

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
        raise e


def main():
    """Main ETL process"""
    start_time = datetime.now()
    try:
        logger.info("=" * 80)
        logger.info("🚀 STARTING ENHANCED FHIR ENCOUNTER ETL PROCESS")
        logger.info("=" * 80)
        logger.info(f"⏰ Job started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📊 Source: {DATABASE_NAME}.{TABLE_NAME}")
        logger.info(f"🎯 Target: Redshift (6 tables)")
        logger.info("📋 Reading all available columns from Glue Catalog")
        logger.info("🔄 Process: 7 steps (Read → Transform → Convert → Resolve → Validate → Write)")
        
        # Step 1: Read data from HealthLake using AWS Glue Data Catalog
        logger.info("\n" + "=" * 50)
        logger.info("📥 STEP 1: READING DATA FROM GLUE CATALOG")
        logger.info("=" * 50)
        logger.info(f"Database: {DATABASE_NAME}")
        logger.info(f"Table: {TABLE_NAME}")
        logger.info("Reading all available columns from Glue Catalog")
        # Use the AWS Glue Data Catalog to read encounter data (all columns)
        encounter_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
                database=DATABASE_NAME, 
            table_name=TABLE_NAME, 
            transformation_ctx="AWSGlueDataCatalog_node1756919830525"
        )
        
        # Convert to DataFrame first to check available columns
        encounter_df_raw = encounter_dynamic_frame.toDF()
        available_columns = encounter_df_raw.columns
        logger.info(f"📋 Available columns in source: {available_columns}")
        
        # Use all available columns (don't filter based on COLUMNS_TO_READ)
        logger.info(f"✅ Using all {len(available_columns)} available columns")
        encounter_df = encounter_df_raw
        
        logger.info("✅ Successfully read data using AWS Glue Data Catalog")
        
        total_records = encounter_df.count()
        logger.info(f"📊 Read {total_records:,} raw encounter records")
        
        # Debug: Show sample of raw data and schema
        if total_records > 0:
            logger.info("\n🔍 DATA QUALITY CHECKS:")
            logger.info("Sample of raw encounter data:")
            encounter_df.show(3, truncate=False)
            logger.info("Raw data schema:")
            encounter_df.printSchema()
            
            # Check for NULL values in key fields
            null_checks = {
                "id": encounter_df.filter(F.col("id").isNull()).count(),
                "subject.reference": encounter_df.filter(F.col("subject").isNull() | F.col("subject.reference").isNull()).count(),
                "status": encounter_df.filter(F.col("status").isNull()).count(),
                "class": encounter_df.filter(F.col("class").isNull()).count()
            }
            
            logger.info("NULL value analysis in key fields:")
            for field, null_count in null_checks.items():
                percentage = (null_count / total_records) * 100 if total_records > 0 else 0
                logger.info(f"  {field}: {null_count:,} NULLs ({percentage:.1f}%)")
        else:
            logger.error("❌ No raw data found! Check the data source.")
            return
        
        # Step 2: Transform main encounter data
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 2: TRANSFORMING MAIN ENCOUNTER DATA")
        logger.info("=" * 50)
        
        main_encounter_df = transform_main_encounter_data(encounter_df)
        main_count = main_encounter_df.count()
        logger.info(f"✅ Transformed {main_count:,} main encounter records")
        
        if main_count == 0:
            logger.error("❌ No main encounter records after transformation! Check filtering criteria.")
            return
        
        # Debug: Show sample of transformed main data (commented out for performance)
        # logger.info("Sample of transformed main encounter data:")
        # main_encounter_df.show(3, truncate=False)
        
        # Step 3: Transform multi-valued data (all supporting tables)
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 3: TRANSFORMING MULTI-VALUED DATA")
        logger.info("=" * 50)
        
        encounter_types_df = transform_encounter_types(encounter_df)
        types_count = encounter_types_df.count()
        logger.info(f"✅ Transformed {types_count:,} encounter type records")
        
        encounter_participants_df = transform_encounter_participants(encounter_df)
        participants_count = encounter_participants_df.count()
        logger.info(f"✅ Transformed {participants_count:,} participant records")
        
        encounter_reasons_df = transform_encounter_reasons(encounter_df)
        reasons_count = encounter_reasons_df.count()
        logger.info(f"✅ Transformed {reasons_count:,} reason records")
        
        
        encounter_locations_df = transform_encounter_locations(encounter_df)
        locations_count = encounter_locations_df.count()
        logger.info(f"✅ Transformed {locations_count:,} location records")
        
        encounter_hospitalization_df = transform_encounter_hospitalization(encounter_df)
        hospitalization_count = encounter_hospitalization_df.count()
        logger.info(f"✅ Transformed {hospitalization_count:,} hospitalization records")
        
        # Debug: Show samples of multi-valued data if available
        if types_count > 0:
            logger.info("Sample of encounter types data:")
            encounter_types_df.show(3, truncate=False)
        
        if participants_count > 0:
            logger.info("Sample of encounter participants data:")
            encounter_participants_df.show(3, truncate=False)
        
        if reasons_count > 0:
            logger.info("Sample of encounter reasons data:")
            encounter_reasons_df.show(3, truncate=False)
        
        
        if locations_count > 0:
            logger.info("Sample of encounter locations data:")
            encounter_locations_df.show(3, truncate=False)
        
        if hospitalization_count > 0:
            logger.info("Sample of encounter hospitalization data:")
            encounter_hospitalization_df.show(3, truncate=False)
        
        # Step 4: Convert to DynamicFrames and ensure data is flat for Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 4: CONVERTING TO DYNAMICFRAMES")
        logger.info("=" * 50)
        logger.info("Converting to DynamicFrames and ensuring Redshift compatibility...")
        
        # Convert main encounters DataFrame and ensure flat structure
        main_flat_df = main_encounter_df.select(
            F.col("encounter_id").cast(StringType()).alias("encounter_id"),
            F.col("patient_id").cast(StringType()).alias("patient_id"),
            F.col("status").cast(StringType()).alias("status"),
            F.col("resourcetype").cast(StringType()).alias("resourcetype"),
            F.col("class_code").cast(StringType()).alias("class_code"),
            F.col("class_display").cast(StringType()).alias("class_display"),
            F.col("start_time").cast(TimestampType()).alias("start_time"),
            F.col("end_time").cast(TimestampType()).alias("end_time"),
            F.col("service_provider_id").cast(StringType()).alias("service_provider_id"),
            F.col("appointment_id").cast(StringType()).alias("appointment_id"),
            F.col("parent_encounter_id").cast(StringType()).alias("parent_encounter_id"),
            F.col("meta_data").cast(StringType()).alias("meta_data"),
            F.col("created_at").cast(TimestampType()).alias("created_at"),
            F.col("updated_at").cast(TimestampType()).alias("updated_at")
        )
        
        main_dynamic_frame = DynamicFrame.fromDF(main_flat_df, glueContext, "main_encounter_dynamic_frame")
        
        # Convert other DataFrames with type casting
        types_flat_df = encounter_types_df.select(
            F.col("encounter_id").cast(StringType()).alias("encounter_id"),
            F.col("type_code").cast(StringType()).alias("type_code"),
            F.col("type_system").cast(StringType()).alias("type_system"),
            F.col("type_display").cast(StringType()).alias("type_display"),
            F.col("type_text").cast(StringType()).alias("type_text")
        )
        types_dynamic_frame = DynamicFrame.fromDF(types_flat_df, glueContext, "types_dynamic_frame")
        
        participants_flat_df = encounter_participants_df.select(
            F.col("encounter_id").cast(StringType()).alias("encounter_id"),
            F.col("participant_type").cast(StringType()).alias("participant_type"),
            F.col("participant_id").cast(StringType()).alias("participant_id"),
            F.col("participant_display").cast(StringType()).alias("participant_display"),
            F.col("period_start").cast(TimestampType()).alias("period_start"),
            F.col("period_end").cast(TimestampType()).alias("period_end")
        )
        participants_dynamic_frame = DynamicFrame.fromDF(participants_flat_df, glueContext, "participants_dynamic_frame")
        
        reasons_flat_df = encounter_reasons_df.select(
            F.col("encounter_id").cast(StringType()).alias("encounter_id"),
            F.col("reason_code").cast(StringType()).alias("reason_code"),
            F.col("reason_system").cast(StringType()).alias("reason_system"),
            F.col("reason_display").cast(StringType()).alias("reason_display"),
            F.col("reason_text").cast(StringType()).alias("reason_text")
        )
        reasons_dynamic_frame = DynamicFrame.fromDF(reasons_flat_df, glueContext, "reasons_dynamic_frame")
        
        
        locations_flat_df = encounter_locations_df.select(
            F.col("encounter_id").cast(StringType()).alias("encounter_id"),
            F.col("location_id").cast(StringType()).alias("location_id")
        )
        locations_dynamic_frame = DynamicFrame.fromDF(locations_flat_df, glueContext, "locations_dynamic_frame")
        
        hospitalization_flat_df = encounter_hospitalization_df.select(
            F.col("encounter_id").cast(StringType()).alias("encounter_id"),
            F.col("discharge_disposition_text").cast(StringType()).alias("discharge_disposition_text"),
            F.col("discharge_code").cast(StringType()).alias("discharge_code"),
            F.col("discharge_system").cast(StringType()).alias("discharge_system")
        )
        hospitalization_dynamic_frame = DynamicFrame.fromDF(hospitalization_flat_df, glueContext, "hospitalization_dynamic_frame")
        
        # Step 5: Resolve any remaining choice types to ensure Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 5: RESOLVING CHOICE TYPES")
        logger.info("=" * 50)
        logger.info("Resolving choice types for Redshift compatibility...")
        
        main_resolved_frame = main_dynamic_frame.resolveChoice(
            specs=[
                ("encounter_id", "cast:string"),
                ("patient_id", "cast:string"),
                ("status", "cast:string"),
                ("resourcetype", "cast:string"),
                ("class_code", "cast:string"),
                ("class_display", "cast:string"),
                ("start_time", "cast:timestamp"),
                ("end_time", "cast:timestamp"),
                ("service_provider_id", "cast:string"),
                ("appointment_id", "cast:string"),
                ("parent_encounter_id", "cast:string"),
                ("meta_data", "cast:string"),
                ("created_at", "cast:timestamp"),
                ("updated_at", "cast:timestamp")
            ]
        )
        
        types_resolved_frame = types_dynamic_frame.resolveChoice(
            specs=[
                ("encounter_id", "cast:string"),
                ("type_code", "cast:string"),
                ("type_system", "cast:string"),
                ("type_display", "cast:string"),
                ("type_text", "cast:string")
            ]
        )
        
        participants_resolved_frame = participants_dynamic_frame.resolveChoice(
            specs=[
                ("encounter_id", "cast:string"),
                ("participant_type", "cast:string"),
                ("participant_id", "cast:string"),
                ("participant_display", "cast:string"),
                ("period_start", "cast:timestamp"),
                ("period_end", "cast:timestamp")
            ]
        )
        
        reasons_resolved_frame = reasons_dynamic_frame.resolveChoice(
            specs=[
                ("encounter_id", "cast:string"),
                ("reason_code", "cast:string"),
                ("reason_system", "cast:string"),
                ("reason_display", "cast:string"),
                ("reason_text", "cast:string")
            ]
        )
        
        
        locations_resolved_frame = locations_dynamic_frame.resolveChoice(
            specs=[
                ("encounter_id", "cast:string"),
                ("location_id", "cast:string")
            ]
        )
        
        hospitalization_resolved_frame = hospitalization_dynamic_frame.resolveChoice(
            specs=[
                ("encounter_id", "cast:string"),
                ("discharge_disposition_text", "cast:string"),
                ("discharge_code", "cast:string"),
                ("discharge_system", "cast:string")
            ]
        )
        
        # Step 6: Final validation before writing
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 6: FINAL VALIDATION")
        logger.info("=" * 50)
        logger.info("Performing final validation before writing to Redshift...")
        
        # Validate main encounters data
        main_final_df = main_resolved_frame.toDF()
        main_final_count = main_final_df.count()
        logger.info(f"Final main encounters count: {main_final_count}")
        
        if main_final_count == 0:
            logger.error("No main encounter records to write to Redshift! Stopping the process.")
            return
        
        # Validate other tables
        types_final_count = types_resolved_frame.toDF().count()
        participants_final_count = participants_resolved_frame.toDF().count()
        reasons_final_count = reasons_resolved_frame.toDF().count()
        locations_final_count = locations_resolved_frame.toDF().count()
        hospitalization_final_count = hospitalization_resolved_frame.toDF().count()
        
        logger.info(f"Final counts - Types: {types_final_count}, Participants: {participants_final_count}, Reasons: {reasons_final_count}, Locations: {locations_final_count}, Hospitalization: {hospitalization_final_count}")
        
        # Debug: Show final sample data being written
        logger.info("Final sample data being written to Redshift (main encounters):")
        main_final_df.show(3, truncate=False)
        
        # Show sample data for other tables as well
        logger.info("Final sample data for encounter types:")
        types_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for encounter participants:")
        participants_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for encounter reasons:")
        reasons_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for encounter locations:")
        locations_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for encounter hospitalization:")
        hospitalization_resolved_frame.toDF().show(3, truncate=False)
        
        # Step 7: Create tables and write to Redshift
        logger.info("\n" + "=" * 50)
        logger.info("💾 STEP 7: WRITING DATA TO REDSHIFT")
        logger.info("=" * 50)
        logger.info(f"🔗 Using connection: {REDSHIFT_CONNECTION}")
        logger.info(f"📁 S3 temp directory: {S3_TEMP_DIR}")
        
        # Create all tables individually
        # Note: Each write_to_redshift call now includes TRUNCATE to prevent duplicates
        logger.info("📝 Creating main encounters table...")
        encounters_table_sql = create_redshift_tables_sql()
        write_to_redshift(main_resolved_frame, "encounters", encounters_table_sql)
        logger.info("✅ Main encounters table created and written successfully")
        
        logger.info("📝 Creating encounter types table...")
        types_table_sql = create_encounter_types_table_sql()
        write_to_redshift(types_resolved_frame, "encounter_types", types_table_sql)
        logger.info("✅ Encounter types table created and written successfully")
        
        logger.info("📝 Creating encounter participants table...")
        participants_table_sql = create_encounter_participants_table_sql()
        write_to_redshift(participants_resolved_frame, "encounter_participants", participants_table_sql)
        logger.info("✅ Encounter participants table created and written successfully")
        
        logger.info("📝 Creating encounter reasons table...")
        reasons_table_sql = create_encounter_reasons_table_sql()
        write_to_redshift(reasons_resolved_frame, "encounter_reasons", reasons_table_sql)
        logger.info("✅ Encounter reasons table created and written successfully")
        
        logger.info("📝 Creating encounter locations table...")
        locations_table_sql = create_encounter_locations_table_sql()
        write_to_redshift(locations_resolved_frame, "encounter_locations", locations_table_sql)
        logger.info("✅ Encounter locations table created and written successfully")
        
        logger.info("📝 Creating encounter hospitalization table...")
        hospitalization_table_sql = create_encounter_hospitalization_table_sql()
        write_to_redshift(hospitalization_resolved_frame, "encounter_hospitalization", hospitalization_table_sql)
        logger.info("✅ Encounter hospitalization table created and written successfully")
        
        # Calculate processing time
        end_time = datetime.now()
        processing_time = end_time - start_time
        
        logger.info("\n" + "=" * 80)
        logger.info("🎉 ETL PROCESS COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        logger.info(f"⏰ Job completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"⏱️  Total processing time: {processing_time}")
        # logger.info(f"📊 Processing rate: {total_records / processing_time.total_seconds():.2f} records/second")
        
        logger.info("\n📋 TABLES WRITTEN TO REDSHIFT:")
        logger.info("  ✅ public.encounters (main encounter data)")
        logger.info("  ✅ public.encounter_types (CPT codes and procedure types)")
        logger.info("  ✅ public.encounter_participants (doctors, nurses, etc.)")
        logger.info("  ✅ public.encounter_reasons (diagnosis codes and reasons)")
        logger.info("  ✅ public.encounter_locations (encounter locations)")
        logger.info("  ✅ public.encounter_hospitalization (discharge information)")
        
        logger.info("\n📊 FINAL ETL STATISTICS:")
        logger.info(f"  📥 Total raw records processed: {total_records:,}")
        logger.info(f"  🏥 Main encounter records: {main_count:,}")
        logger.info(f"  🏷️  Encounter type records: {types_count:,}")
        logger.info(f"  👥 Participant records: {participants_count:,}")
        logger.info(f"  🩺 Reason records: {reasons_count:,}")
        logger.info(f"  📍 Location records: {locations_count:,}")
        logger.info(f"  🏥 Hospitalization records: {hospitalization_count:,}")
        
        # Calculate data expansion ratio
        total_output_records = main_count + types_count + participants_count + reasons_count + locations_count + hospitalization_count
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
