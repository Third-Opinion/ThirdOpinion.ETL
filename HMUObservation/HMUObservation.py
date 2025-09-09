from datetime import datetime
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, DecimalType, BooleanType
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
TABLE_NAME = "observation"
REDSHIFT_CONNECTION = "Redshift connection"
S3_TEMP_DIR = "s3://aws-glue-assets-442042533707-us-east-2/temporary/"

def extract_id_from_reference(reference_field):
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

def transform_main_observation_data(df):
    """Transform the main observation data"""
    logger.info("Transforming main observation data...")
    
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
    
    # Build the select statement dynamically based on available columns
    select_columns = [
        F.col("id").alias("observation_id"),
        F.when(F.col("subject").isNotNull(), 
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        F.when(F.col("encounter").isNotNull(),
               F.regexp_extract(F.col("encounter").getField("reference"), r"Encounter/(.+)", 1)
              ).otherwise(None).alias("encounter_id"),
        F.when(F.col("specimen").isNotNull(),
               F.regexp_extract(F.col("specimen").getField("reference"), r"Specimen/(.+)", 1)
              ).otherwise(None).alias("specimen_id"),
        F.col("status"),
        F.col("code").getField("text").alias("observation_text"),
        F.lit(None).alias("primary_code"),    # coding is null in sample data
        F.lit(None).alias("primary_system"),  # coding is null in sample data
        F.lit(None).alias("primary_display"), # coding is null in sample data
    ]
    
    # Add value fields - handle different value types (using actual schema field names)
    select_columns.extend([
        F.col("valueString").alias("value_string"),
        F.col("valueQuantity").getField("value").getField("double").alias("value_quantity_value"),
        F.col("valueQuantity").getField("unit").alias("value_quantity_unit"),
        F.col("valueQuantity").getField("system").alias("value_quantity_system"),
        F.col("valueCodeableConcept").getField("text").alias("value_codeable_concept_text"),
        F.col("valueCodeableConcept").getField("coding")[0].getField("code").alias("value_codeable_concept_code"),
        F.col("valueCodeableConcept").getField("coding")[0].getField("system").alias("value_codeable_concept_system"),
        F.col("valueCodeableConcept").getField("coding")[0].getField("display").alias("value_codeable_concept_display"),
        F.to_timestamp(F.col("valueDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX").alias("value_datetime"),
        F.lit(None).alias("value_boolean"),  # valueboolean field not in schema
    ])
    
    # Add data absent reason (using actual schema field names)
    select_columns.extend([
        F.col("dataAbsentReason").getField("coding")[0].getField("code").alias("data_absent_reason_code"),
        F.col("dataAbsentReason").getField("coding")[0].getField("display").alias("data_absent_reason_display"),
        F.col("dataAbsentReason").getField("coding")[0].getField("system").alias("data_absent_reason_system"),
    ])
    
    # Add temporal information - using actual schema field names
    select_columns.extend([
        F.to_timestamp(F.col("effectiveDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX").alias("effective_datetime"),
        F.to_timestamp(F.col("effectivePeriod").getField("start"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX").alias("effective_period_start"),
        F.lit(None).alias("effective_period_end"),  # end field not in schema
        F.to_timestamp(F.col("issued"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX").alias("issued"),
    ])
    
    # Add body site and method (fields not in schema)
    select_columns.extend([
        F.lit(None).alias("body_site_code"),    # bodySite field not in schema
        F.lit(None).alias("body_site_system"),  # bodySite field not in schema
        F.lit(None).alias("body_site_display"), # bodySite field not in schema
        F.lit(None).alias("body_site_text"),    # bodySite field not in schema
        F.lit(None).alias("method_code"),       # method field not in schema
        F.lit(None).alias("method_system"),     # method field not in schema
        F.lit(None).alias("method_display"),    # method field not in schema
        F.lit(None).alias("method_text"),       # method field not in schema
    ])
    
    # Add metadata (using actual schema field names)
    select_columns.extend([
        F.col("meta").getField("versionId").alias("meta_version_id"),
        F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX").alias("meta_last_updated"),
        F.lit(None).alias("meta_source"),  # source field not in schema
        F.lit(None).alias("meta_profile"), # profile field not in schema
        convert_to_json_udf(F.col("meta").getField("security")).alias("meta_security"),
        F.lit(None).alias("meta_tag"),     # tag field not in schema
        convert_to_json_udf(F.col("extension")).alias("extensions"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ])
    
    # Transform main observation data using only available columns and flatten complex structures
    main_df = df.select(*select_columns).filter(
        F.col("observation_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return main_df

def transform_observation_categories(df):
    """Transform observation categories (multiple categories per observation)"""
    logger.info("Transforming observation categories...")
    
    # Check if category column exists
    if "category" not in df.columns:
        logger.warning("category column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("category_code"),
            F.lit("").alias("category_system"),
            F.lit("").alias("category_display"),
            F.lit("").alias("category_text")
        ).filter(F.lit(False))
    
    # Explode the category array
    categories_df = df.select(
        F.col("id").alias("observation_id"),
        F.explode(F.col("category")).alias("category_item")
    ).filter(
        F.col("category_item").isNotNull()
    )
    
    # Extract category details and explode the coding array
    # First get the text from category level, then explode coding
    categories_with_text = categories_df.select(
        F.col("observation_id"),
        F.col("category_item.text").alias("category_text"),
        F.col("category_item.coding").alias("coding_array")
    )
    
    # Now explode the coding array
    categories_final = categories_with_text.select(
        F.col("observation_id"),
        F.explode(F.col("coding_array")).alias("coding_item"),
        F.col("category_text")
    ).select(
        F.col("observation_id"),
        F.col("coding_item.code").alias("category_code"),
        F.col("coding_item.system").alias("category_system"),
        F.col("coding_item.display").alias("category_display"),
        F.col("category_text")
    ).filter(
        F.col("category_code").isNotNull()
    )
    
    return categories_final

def transform_observation_interpretations(df):
    """Transform observation interpretations"""
    logger.info("Transforming observation interpretations...")
    
    # Check if interpretation column exists
    if "interpretation" not in df.columns:
        logger.warning("interpretation column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("interpretation_code"),
            F.lit("").alias("interpretation_system"),
            F.lit("").alias("interpretation_display")
        ).filter(F.lit(False))
    
    # Explode the interpretation array
    interpretations_df = df.select(
        F.col("id").alias("observation_id"),
        F.explode(F.col("interpretation")).alias("interpretation_item")
    ).filter(
        F.col("interpretation_item").isNotNull()
    )
    
    # Extract interpretation details and explode the coding array
    # First get the text from interpretation level, then explode coding
    interpretations_with_text = interpretations_df.select(
        F.col("observation_id"),
        F.col("interpretation_item.text").alias("interpretation_text"),
        F.col("interpretation_item.coding").alias("coding_array")
    )
    
    # Now explode the coding array
    interpretations_final = interpretations_with_text.select(
        F.col("observation_id"),
        F.explode(F.col("coding_array")).alias("coding_item"),
        F.col("interpretation_text")
    ).select(
        F.col("observation_id"),
        F.col("coding_item.code").alias("interpretation_code"),
        F.col("coding_item.system").alias("interpretation_system"),
        F.col("coding_item.display").alias("interpretation_display"),
        F.col("interpretation_text")
    ).filter(
        F.col("interpretation_code").isNotNull()
    )
    
    return interpretations_final

def transform_observation_reference_ranges(df):
    """Transform observation reference ranges"""
    logger.info("Transforming observation reference ranges...")
    
    # Check if referencerange column exists (using actual CSV field name)
    if "referencerange" not in df.columns:
        logger.warning("referencerange column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit(0.0).alias("range_low_value"),
            F.lit("").alias("range_low_unit"),
            F.lit(0.0).alias("range_high_value"),
            F.lit("").alias("range_high_unit"),
            F.lit("").alias("range_type_code"),
            F.lit("").alias("range_type_system"),
            F.lit("").alias("range_type_display"),
            F.lit("").alias("range_text")
        ).filter(F.lit(False))
    
    # Explode the referencerange array
    ranges_df = df.select(
        F.col("id").alias("observation_id"),
        F.explode(F.col("referencerange")).alias("range_item")
    ).filter(
        F.col("range_item").isNotNull()
    )
    
    # Extract range details - based on actual schema, only text field is available
    ranges_final = ranges_df.select(
        F.col("observation_id"),
        F.lit(None).alias("range_low_value"),  # low field not in schema
        F.lit(None).alias("range_low_unit"),   # low field not in schema
        F.lit(None).alias("range_high_value"), # high field not in schema
        F.lit(None).alias("range_high_unit"),  # high field not in schema
        F.lit(None).alias("range_type_code"),  # type field not in schema
        F.lit(None).alias("range_type_system"), # type field not in schema
        F.lit(None).alias("range_type_display"), # type field not in schema
        F.col("range_item.text").alias("range_text")
    ).filter(
        F.col("range_text").isNotNull()
    )
    
    return ranges_final

def transform_observation_components(df):
    """Transform observation components"""
    logger.info("Transforming observation components...")
    
    # Check if component column exists
    if "component" not in df.columns:
        logger.warning("component column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("component_code"),
            F.lit("").alias("component_system"),
            F.lit("").alias("component_display"),
            F.lit("").alias("component_text"),
            F.lit("").alias("component_value_string"),
            F.lit(0.0).alias("component_value_quantity_value"),
            F.lit("").alias("component_value_quantity_unit"),
            F.lit("").alias("component_value_codeable_concept_code"),
            F.lit("").alias("component_value_codeable_concept_system"),
            F.lit("").alias("component_value_codeable_concept_display"),
            F.lit("").alias("component_data_absent_reason_code"),
            F.lit("").alias("component_data_absent_reason_display")
        ).filter(F.lit(False))
    
    # Explode the component array
    components_df = df.select(
        F.col("id").alias("observation_id"),
        F.explode(F.col("component")).alias("component_item")
    ).filter(
        F.col("component_item").isNotNull()
    )
    
    # Extract component details - handle nested structures properly
    # First explode the code.coding array
    components_with_code = components_df.select(
        F.col("observation_id"),
        F.explode(F.col("component_item.code").getField("coding")).alias("code_coding_item"),
        F.lit(None).alias("component_text"),  # text field not available in component.code structure
        F.lit(None).alias("component_value_string"),  # valueString not available in component structure
        F.col("component_item.valueQuantity").getField("value").alias("component_value_quantity_value"),
        F.col("component_item.valueQuantity").getField("unit").alias("component_value_quantity_unit"),
        F.col("component_item.valueCodeableConcept").alias("value_codeable_concept"),
        F.col("component_item.dataAbsentReason").alias("data_absent_reason")
    )
    
    # Now extract the code details
    components_final = components_with_code.select(
        F.col("observation_id"),
        F.col("code_coding_item.code").alias("component_code"),
        F.col("code_coding_item.system").alias("component_system"),
        F.col("code_coding_item.display").alias("component_display"),
        F.col("component_text"),
        F.col("component_value_string"),
        F.col("component_value_quantity_value"),
        F.col("component_value_quantity_unit"),
        # Handle valueCodeableConcept - safely get first coding element
        F.when((F.col("value_codeable_concept").getField("coding").isNotNull()) & 
               (F.size(F.col("value_codeable_concept").getField("coding")) > 0),
               F.col("value_codeable_concept").getField("coding")[0].getField("code")
              ).otherwise(None).alias("component_value_codeable_concept_code"),
        F.when((F.col("value_codeable_concept").getField("coding").isNotNull()) & 
               (F.size(F.col("value_codeable_concept").getField("coding")) > 0),
               F.col("value_codeable_concept").getField("coding")[0].getField("system")
              ).otherwise(None).alias("component_value_codeable_concept_system"),
        F.when((F.col("value_codeable_concept").getField("coding").isNotNull()) & 
               (F.size(F.col("value_codeable_concept").getField("coding")) > 0),
               F.col("value_codeable_concept").getField("coding")[0].getField("display")
              ).otherwise(None).alias("component_value_codeable_concept_display"),
        # Handle dataAbsentReason - safely get first coding element
        F.when((F.col("data_absent_reason").getField("coding").isNotNull()) & 
               (F.size(F.col("data_absent_reason").getField("coding")) > 0),
               F.col("data_absent_reason").getField("coding")[0].getField("code")
              ).otherwise(None).alias("component_data_absent_reason_code"),
        F.when((F.col("data_absent_reason").getField("coding").isNotNull()) & 
               (F.size(F.col("data_absent_reason").getField("coding")) > 0),
               F.col("data_absent_reason").getField("coding")[0].getField("display")
              ).otherwise(None).alias("component_data_absent_reason_display")
    ).filter(
        F.col("component_code").isNotNull()
    )
    
    return components_final

def transform_observation_notes(df):
    """Transform observation notes"""
    logger.info("Transforming observation notes...")
    
    # Check if note column exists
    if "note" not in df.columns:
        logger.warning("note column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("note_text"),
            F.lit("").alias("note_author_reference"),
            F.current_timestamp().alias("note_time")
        ).filter(F.lit(False))
    
    # Explode the note array
    notes_df = df.select(
        F.col("id").alias("observation_id"),
        F.explode(F.col("note")).alias("note_item")
    ).filter(
        F.col("note_item").isNotNull()
    )
    
    # Extract note details
    notes_final = notes_df.select(
        F.col("observation_id"),
        F.col("note_item.text").alias("note_text"),
        F.lit(None).alias("note_author_reference"),  # authorReference not available in note structure
        F.lit(None).alias("note_time")  # time not available in note structure
    ).filter(
        F.col("note_text").isNotNull()
    )
    
    return notes_final

def transform_observation_performers(df):
    """Transform observation performers"""
    logger.info("Transforming observation performers...")
    
    # Check if performer column exists
    if "performer" not in df.columns:
        logger.warning("performer column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("performer_type"),
            F.lit("").alias("performer_id")
        ).filter(F.lit(False))
    
    # Explode the performer array
    performers_df = df.select(
        F.col("id").alias("observation_id"),
        F.explode(F.col("performer")).alias("performer_item")
    ).filter(
        F.col("performer_item").isNotNull()
    )
    
    # Extract performer details
    performers_final = performers_df.select(
        F.col("observation_id"),
        F.regexp_extract(F.col("performer_item").getField("reference"), r"([^/]+)/", 1).alias("performer_type"),
        F.regexp_extract(F.col("performer_item").getField("reference"), r"/(.+)", 1).alias("performer_id")
    ).filter(
        F.col("performer_id").isNotNull()
    )
    
    return performers_final

def transform_observation_members(df):
    """Transform observation members"""
    logger.info("Transforming observation members...")
    
    # Check if hasmember column exists (using actual CSV field name)
    if "hasmember" not in df.columns:
        logger.warning("hasmember column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("member_observation_id")
        ).filter(F.lit(False))
    
    # Explode the hasmember array
    members_df = df.select(
        F.col("id").alias("observation_id"),
        F.explode(F.col("hasmember")).alias("member_item")
    ).filter(
        F.col("member_item").isNotNull()
    )
    
    # Extract member details
    members_final = members_df.select(
        F.col("observation_id"),
        F.regexp_extract(F.col("member_item").getField("reference"), r"Observation/(.+)", 1).alias("member_observation_id")
    ).filter(
        F.col("member_observation_id").isNotNull()
    )
    
    return members_final

def transform_observation_derived_from(df):
    """Transform observation derived from references"""
    logger.info("Transforming observation derived from...")
    
    # Check if derivedfrom column exists (using actual CSV field name)
    if "derivedfrom" not in df.columns:
        logger.warning("derivedfrom column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("observation_id"),
            F.lit("").alias("derived_from_reference")
        ).filter(F.lit(False))
    
    # Explode the derivedfrom array
    derived_df = df.select(
        F.col("id").alias("observation_id"),
        F.explode(F.col("derivedfrom")).alias("derived_item")
    ).filter(
        F.col("derived_item").isNotNull()
    )
    
    # Extract derived from details
    derived_final = derived_df.select(
        F.col("observation_id"),
        F.col("derived_item").getField("reference").alias("derived_from_reference")
    ).filter(
        F.col("derived_from_reference").isNotNull()
    )
    
    return derived_final

def create_redshift_tables_sql():
    """Generate SQL for creating main observations table in Redshift with proper syntax"""
    return """
    -- Main observations table
    CREATE TABLE IF NOT EXISTS public.observations (
        observation_id VARCHAR(255) PRIMARY KEY,
        patient_id VARCHAR(255) NOT NULL,
        encounter_id VARCHAR(255),
        specimen_id VARCHAR(255),
        status VARCHAR(50),
        observation_text VARCHAR(500),
        primary_code VARCHAR(50),
        primary_system VARCHAR(255),
        primary_display VARCHAR(255),
        value_string VARCHAR(500),
        value_quantity_value DECIMAL(15,4),
        value_quantity_unit VARCHAR(50),
        value_quantity_system VARCHAR(255),
        value_codeable_concept_code VARCHAR(50),
        value_codeable_concept_system VARCHAR(255),
        value_codeable_concept_display VARCHAR(255),
        value_codeable_concept_text VARCHAR(500),
        value_datetime TIMESTAMP,
        value_boolean BOOLEAN,
        data_absent_reason_code VARCHAR(50),
        data_absent_reason_display VARCHAR(255),
        data_absent_reason_system VARCHAR(255),
        effective_datetime TIMESTAMP,
        effective_period_start TIMESTAMP,
        effective_period_end TIMESTAMP,
        issued TIMESTAMP,
        body_site_code VARCHAR(50),
        body_site_system VARCHAR(255),
        body_site_display VARCHAR(255),
        body_site_text VARCHAR(500),
        method_code VARCHAR(50),
        method_system VARCHAR(255),
        method_display VARCHAR(255),
        method_text VARCHAR(500),
        meta_version_id VARCHAR(50),
        meta_last_updated TIMESTAMP,
        meta_source VARCHAR(255),
        meta_profile TEXT,
        meta_security TEXT,
        meta_tag TEXT,
        extensions TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, effective_datetime)
    """

def create_observation_categories_table_sql():
    """Generate SQL for creating observation_categories table"""
    return """
    CREATE TABLE IF NOT EXISTS public.observation_categories (
        observation_id VARCHAR(255),
        category_code VARCHAR(50),
        category_system VARCHAR(255),
        category_display VARCHAR(255),
        category_text VARCHAR(500)
    ) SORTKEY (observation_id, category_code)
    """

def create_observation_interpretations_table_sql():
    """Generate SQL for creating observation_interpretations table"""
    return """
    CREATE TABLE IF NOT EXISTS public.observation_interpretations (
        observation_id VARCHAR(255),
        interpretation_code VARCHAR(50),
        interpretation_system VARCHAR(255),
        interpretation_display VARCHAR(255)
    ) SORTKEY (observation_id, interpretation_code)
    """

def create_observation_reference_ranges_table_sql():
    """Generate SQL for creating observation_reference_ranges table"""
    return """
    CREATE TABLE IF NOT EXISTS public.observation_reference_ranges (
        observation_id VARCHAR(255),
        range_low_value DECIMAL(15,4),
        range_low_unit VARCHAR(50),
        range_high_value DECIMAL(15,4),
        range_high_unit VARCHAR(50),
        range_type_code VARCHAR(50),
        range_type_system VARCHAR(255),
        range_type_display VARCHAR(255),
        range_text VARCHAR(500)
    ) SORTKEY (observation_id, range_type_code)
    """

def create_observation_components_table_sql():
    """Generate SQL for creating observation_components table"""
    return """
    CREATE TABLE IF NOT EXISTS public.observation_components (
        observation_id VARCHAR(255),
        component_code VARCHAR(50),
        component_system VARCHAR(255),
        component_display VARCHAR(255),
        component_text VARCHAR(500),
        component_value_string VARCHAR(500),
        component_value_quantity_value DECIMAL(15,4),
        component_value_quantity_unit VARCHAR(50),
        component_value_codeable_concept_code VARCHAR(50),
        component_value_codeable_concept_system VARCHAR(255),
        component_value_codeable_concept_display VARCHAR(255),
        component_data_absent_reason_code VARCHAR(50),
        component_data_absent_reason_display VARCHAR(255)
    ) SORTKEY (observation_id, component_code)
    """

def create_observation_notes_table_sql():
    """Generate SQL for creating observation_notes table"""
    return """
    CREATE TABLE IF NOT EXISTS public.observation_notes (
        observation_id VARCHAR(255),
        note_text TEXT,
        note_author_reference VARCHAR(255),
        note_time TIMESTAMP
    ) SORTKEY (observation_id, note_time)
    """

def create_observation_performers_table_sql():
    """Generate SQL for creating observation_performers table"""
    return """
    CREATE TABLE IF NOT EXISTS public.observation_performers (
        observation_id VARCHAR(255),
        performer_type VARCHAR(50),
        performer_id VARCHAR(255)
    ) SORTKEY (observation_id, performer_type)
    """

def create_observation_members_table_sql():
    """Generate SQL for creating observation_members table"""
    return """
    CREATE TABLE IF NOT EXISTS public.observation_members (
        observation_id VARCHAR(255),
        member_observation_id VARCHAR(255)
    ) SORTKEY (observation_id, member_observation_id)
    """

def create_observation_derived_from_table_sql():
    """Generate SQL for creating observation_derived_from table"""
    return """
    CREATE TABLE IF NOT EXISTS public.observation_derived_from (
        observation_id VARCHAR(255),
        derived_from_reference VARCHAR(255)
    ) SORTKEY (observation_id, derived_from_reference)
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
        logger.info("🚀 STARTING ENHANCED FHIR OBSERVATION ETL PROCESS")
        logger.info("=" * 80)
        logger.info(f"⏰ Job started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📊 Source: {DATABASE_NAME}.{TABLE_NAME}")
        logger.info(f"🎯 Target: Redshift (9 tables)")
        logger.info("📋 Reading all available columns from Glue Catalog")
        logger.info("🔄 Process: 7 steps (Read → Transform → Convert → Resolve → Validate → Write)")
        
        # Step 1: Read data from HealthLake using AWS Glue Data Catalog
        logger.info("\n" + "=" * 50)
        logger.info("📥 STEP 1: READING DATA FROM GLUE CATALOG")
        logger.info("=" * 50)
        logger.info(f"Database: {DATABASE_NAME}")
        logger.info(f"Table: {TABLE_NAME}")
        logger.info("Reading all available columns from Glue Catalog")
        
        # Use the AWS Glue Data Catalog to read observation data (all columns)
        observation_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
                database=DATABASE_NAME, 
            table_name=TABLE_NAME, 
            transformation_ctx="AWSGlueDataCatalog_observation_node"
        )
        
        # Convert to DataFrame first to check available columns
        observation_df_raw = observation_dynamic_frame.toDF()
        available_columns = observation_df_raw.columns
        logger.info(f"📋 Available columns in source: {available_columns}")
        
        # Use all available columns (don't filter based on specific columns)
        logger.info(f"✅ Using all {len(available_columns)} available columns")
        observation_df = observation_df_raw
        
        logger.info("✅ Successfully read data using AWS Glue Data Catalog")
        
        total_records = observation_df.count()
        logger.info(f"📊 Read {total_records:,} raw observation records")
        
        # Debug: Show sample of raw data and schema
        if total_records > 0:
            logger.info("\n🔍 DATA QUALITY CHECKS:")
            logger.info("Sample of raw observation data:")
            observation_df.show(3, truncate=False)
            logger.info("Raw data schema:")
            observation_df.printSchema()
            
            # Check for NULL values in key fields
            null_checks = {
                "id": observation_df.filter(F.col("id").isNull()).count(),
                "subject.reference": observation_df.filter(F.col("subject").isNull() | F.col("subject.reference").isNull()).count(),
                "status": observation_df.filter(F.col("status").isNull()).count(),
                "code": observation_df.filter(F.col("code").isNull()).count()
            }
            
            logger.info("NULL value analysis in key fields:")
            for field, null_count in null_checks.items():
                percentage = (null_count / total_records) * 100 if total_records > 0 else 0
                logger.info(f"  {field}: {null_count:,} NULLs ({percentage:.1f}%)")
        else:
            logger.error("❌ No raw data found! Check the data source.")
            return
        
        # Step 2: Transform main observation data
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 2: TRANSFORMING MAIN OBSERVATION DATA")
        logger.info("=" * 50)
        
        main_observation_df = transform_main_observation_data(observation_df)
        main_count = main_observation_df.count()
        logger.info(f"✅ Transformed {main_count:,} main observation records")
        
        if main_count == 0:
            logger.error("❌ No main observation records after transformation! Check filtering criteria.")
            return
        
        # Step 3: Transform multi-valued data (all supporting tables)
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 3: TRANSFORMING MULTI-VALUED DATA")
        logger.info("=" * 50)
        
        observation_categories_df = transform_observation_categories(observation_df)
        categories_count = observation_categories_df.count()
        logger.info(f"✅ Transformed {categories_count:,} observation category records")
        
        observation_interpretations_df = transform_observation_interpretations(observation_df)
        interpretations_count = observation_interpretations_df.count()
        logger.info(f"✅ Transformed {interpretations_count:,} interpretation records")
        
        observation_reference_ranges_df = transform_observation_reference_ranges(observation_df)
        reference_ranges_count = observation_reference_ranges_df.count()
        logger.info(f"✅ Transformed {reference_ranges_count:,} reference range records")
        
        observation_components_df = transform_observation_components(observation_df)
        components_count = observation_components_df.count()
        logger.info(f"✅ Transformed {components_count:,} component records")
        
        observation_notes_df = transform_observation_notes(observation_df)
        notes_count = observation_notes_df.count()
        logger.info(f"✅ Transformed {notes_count:,} note records")
        
        observation_performers_df = transform_observation_performers(observation_df)
        performers_count = observation_performers_df.count()
        logger.info(f"✅ Transformed {performers_count:,} performer records")
        
        observation_members_df = transform_observation_members(observation_df)
        members_count = observation_members_df.count()
        logger.info(f"✅ Transformed {members_count:,} member records")
        
        observation_derived_from_df = transform_observation_derived_from(observation_df)
        derived_from_count = observation_derived_from_df.count()
        logger.info(f"✅ Transformed {derived_from_count:,} derived from records")
        
        # Debug: Show samples of multi-valued data if available
        if categories_count > 0:
            logger.info("Sample of observation categories data:")
            observation_categories_df.show(3, truncate=False)
        
        if interpretations_count > 0:
            logger.info("Sample of observation interpretations data:")
            observation_interpretations_df.show(3, truncate=False)
        
        if reference_ranges_count > 0:
            logger.info("Sample of observation reference ranges data:")
            observation_reference_ranges_df.show(3, truncate=False)
        
        if components_count > 0:
            logger.info("Sample of observation components data:")
            observation_components_df.show(3, truncate=False)
        
        if notes_count > 0:
            logger.info("Sample of observation notes data:")
            observation_notes_df.show(3, truncate=False)
        
        if performers_count > 0:
            logger.info("Sample of observation performers data:")
            observation_performers_df.show(3, truncate=False)
        
        if members_count > 0:
            logger.info("Sample of observation members data:")
            observation_members_df.show(3, truncate=False)
        
        if derived_from_count > 0:
            logger.info("Sample of observation derived from data:")
            observation_derived_from_df.show(3, truncate=False)
        
        # Step 4: Convert to DynamicFrames and ensure data is flat for Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 4: CONVERTING TO DYNAMICFRAMES")
        logger.info("=" * 50)
        logger.info("Converting to DynamicFrames and ensuring Redshift compatibility...")
        
        # Convert main observations DataFrame and ensure flat structure
        main_flat_df = main_observation_df.select(
            F.col("observation_id").cast(StringType()).alias("observation_id"),
            F.col("patient_id").cast(StringType()).alias("patient_id"),
            F.col("encounter_id").cast(StringType()).alias("encounter_id"),
            F.col("specimen_id").cast(StringType()).alias("specimen_id"),
            F.col("status").cast(StringType()).alias("status"),
            F.col("observation_text").cast(StringType()).alias("observation_text"),
            F.col("primary_code").cast(StringType()).alias("primary_code"),
            F.col("primary_system").cast(StringType()).alias("primary_system"),
            F.col("primary_display").cast(StringType()).alias("primary_display"),
            F.col("value_string").cast(StringType()).alias("value_string"),
            F.col("value_quantity_value").cast(DecimalType(15,4)).alias("value_quantity_value"),
            F.col("value_quantity_unit").cast(StringType()).alias("value_quantity_unit"),
            F.col("value_quantity_system").cast(StringType()).alias("value_quantity_system"),
            F.col("value_codeable_concept_code").cast(StringType()).alias("value_codeable_concept_code"),
            F.col("value_codeable_concept_system").cast(StringType()).alias("value_codeable_concept_system"),
            F.col("value_codeable_concept_display").cast(StringType()).alias("value_codeable_concept_display"),
            F.col("value_codeable_concept_text").cast(StringType()).alias("value_codeable_concept_text"),
            F.col("value_datetime").cast(TimestampType()).alias("value_datetime"),
            F.col("value_boolean").cast(BooleanType()).alias("value_boolean"),
            F.col("data_absent_reason_code").cast(StringType()).alias("data_absent_reason_code"),
            F.col("data_absent_reason_display").cast(StringType()).alias("data_absent_reason_display"),
            F.col("data_absent_reason_system").cast(StringType()).alias("data_absent_reason_system"),
            F.col("effective_datetime").cast(TimestampType()).alias("effective_datetime"),
            F.col("effective_period_start").cast(TimestampType()).alias("effective_period_start"),
            F.col("effective_period_end").cast(TimestampType()).alias("effective_period_end"),
            F.col("issued").cast(TimestampType()).alias("issued"),
            F.col("body_site_code").cast(StringType()).alias("body_site_code"),
            F.col("body_site_system").cast(StringType()).alias("body_site_system"),
            F.col("body_site_display").cast(StringType()).alias("body_site_display"),
            F.col("body_site_text").cast(StringType()).alias("body_site_text"),
            F.col("method_code").cast(StringType()).alias("method_code"),
            F.col("method_system").cast(StringType()).alias("method_system"),
            F.col("method_display").cast(StringType()).alias("method_display"),
            F.col("method_text").cast(StringType()).alias("method_text"),
            F.col("meta_version_id").cast(StringType()).alias("meta_version_id"),
            F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
            F.col("meta_source").cast(StringType()).alias("meta_source"),
            F.col("meta_profile").cast(StringType()).alias("meta_profile"),
            F.col("meta_security").cast(StringType()).alias("meta_security"),
            F.col("meta_tag").cast(StringType()).alias("meta_tag"),
            F.col("extensions").cast(StringType()).alias("extensions"),
            F.col("created_at").cast(TimestampType()).alias("created_at"),
            F.col("updated_at").cast(TimestampType()).alias("updated_at")
        )
        
        main_dynamic_frame = DynamicFrame.fromDF(main_flat_df, glueContext, "main_observation_dynamic_frame")
        
        # Convert other DataFrames with type casting
        categories_flat_df = observation_categories_df.select(
            F.col("observation_id").cast(StringType()).alias("observation_id"),
            F.col("category_code").cast(StringType()).alias("category_code"),
            F.col("category_system").cast(StringType()).alias("category_system"),
            F.col("category_display").cast(StringType()).alias("category_display"),
            F.col("category_text").cast(StringType()).alias("category_text")
        )
        categories_dynamic_frame = DynamicFrame.fromDF(categories_flat_df, glueContext, "categories_dynamic_frame")
        
        interpretations_flat_df = observation_interpretations_df.select(
            F.col("observation_id").cast(StringType()).alias("observation_id"),
            F.col("interpretation_code").cast(StringType()).alias("interpretation_code"),
            F.col("interpretation_system").cast(StringType()).alias("interpretation_system"),
            F.col("interpretation_display").cast(StringType()).alias("interpretation_display")
        )
        interpretations_dynamic_frame = DynamicFrame.fromDF(interpretations_flat_df, glueContext, "interpretations_dynamic_frame")
        
        reference_ranges_flat_df = observation_reference_ranges_df.select(
            F.col("observation_id").cast(StringType()).alias("observation_id"),
            F.col("range_low_value").cast(DecimalType(15,4)).alias("range_low_value"),
            F.col("range_low_unit").cast(StringType()).alias("range_low_unit"),
            F.col("range_high_value").cast(DecimalType(15,4)).alias("range_high_value"),
            F.col("range_high_unit").cast(StringType()).alias("range_high_unit"),
            F.col("range_type_code").cast(StringType()).alias("range_type_code"),
            F.col("range_type_system").cast(StringType()).alias("range_type_system"),
            F.col("range_type_display").cast(StringType()).alias("range_type_display"),
            F.col("range_text").cast(StringType()).alias("range_text")
        )
        reference_ranges_dynamic_frame = DynamicFrame.fromDF(reference_ranges_flat_df, glueContext, "reference_ranges_dynamic_frame")
        
        components_flat_df = observation_components_df.select(
            F.col("observation_id").cast(StringType()).alias("observation_id"),
            F.col("component_code").cast(StringType()).alias("component_code"),
            F.col("component_system").cast(StringType()).alias("component_system"),
            F.col("component_display").cast(StringType()).alias("component_display"),
            F.col("component_text").cast(StringType()).alias("component_text"),
            F.col("component_value_string").cast(StringType()).alias("component_value_string"),
            F.when(F.col("component_value_quantity_value").getField("double").isNotNull(),
                   F.col("component_value_quantity_value").getField("double")
                  ).when(F.col("component_value_quantity_value").getField("int").isNotNull(),
                         F.col("component_value_quantity_value").getField("int")
                        ).otherwise(None).cast(DecimalType(15,4)).alias("component_value_quantity_value"),
            F.col("component_value_quantity_unit").cast(StringType()).alias("component_value_quantity_unit"),
            F.col("component_value_codeable_concept_code").cast(StringType()).alias("component_value_codeable_concept_code"),
            F.col("component_value_codeable_concept_system").cast(StringType()).alias("component_value_codeable_concept_system"),
            F.col("component_value_codeable_concept_display").cast(StringType()).alias("component_value_codeable_concept_display"),
            F.col("component_data_absent_reason_code").cast(StringType()).alias("component_data_absent_reason_code"),
            F.col("component_data_absent_reason_display").cast(StringType()).alias("component_data_absent_reason_display")
        )
        components_dynamic_frame = DynamicFrame.fromDF(components_flat_df, glueContext, "components_dynamic_frame")
        
        notes_flat_df = observation_notes_df.select(
            F.col("observation_id").cast(StringType()).alias("observation_id"),
            F.col("note_text").cast(StringType()).alias("note_text"),
            F.col("note_author_reference").cast(StringType()).alias("note_author_reference"),
            F.col("note_time").cast(TimestampType()).alias("note_time")
        )
        notes_dynamic_frame = DynamicFrame.fromDF(notes_flat_df, glueContext, "notes_dynamic_frame")
        
        performers_flat_df = observation_performers_df.select(
            F.col("observation_id").cast(StringType()).alias("observation_id"),
            F.col("performer_type").cast(StringType()).alias("performer_type"),
            F.col("performer_id").cast(StringType()).alias("performer_id")
        )
        performers_dynamic_frame = DynamicFrame.fromDF(performers_flat_df, glueContext, "performers_dynamic_frame")
        
        members_flat_df = observation_members_df.select(
            F.col("observation_id").cast(StringType()).alias("observation_id"),
            F.col("member_observation_id").cast(StringType()).alias("member_observation_id")
        )
        members_dynamic_frame = DynamicFrame.fromDF(members_flat_df, glueContext, "members_dynamic_frame")
        
        derived_from_flat_df = observation_derived_from_df.select(
            F.col("observation_id").cast(StringType()).alias("observation_id"),
            F.col("derived_from_reference").cast(StringType()).alias("derived_from_reference")
        )
        derived_from_dynamic_frame = DynamicFrame.fromDF(derived_from_flat_df, glueContext, "derived_from_dynamic_frame")
        
        # Step 5: Resolve any remaining choice types to ensure Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 5: RESOLVING CHOICE TYPES")
        logger.info("=" * 50)
        logger.info("Resolving choice types for Redshift compatibility...")
        
        main_resolved_frame = main_dynamic_frame.resolveChoice(
            specs=[
                ("observation_id", "cast:string"),
                ("patient_id", "cast:string"),
                ("encounter_id", "cast:string"),
                ("specimen_id", "cast:string"),
                ("status", "cast:string"),
                ("observation_text", "cast:string"),
                ("primary_code", "cast:string"),
                ("primary_system", "cast:string"),
                ("primary_display", "cast:string"),
                ("value_string", "cast:string"),
                ("value_quantity_value", "cast:decimal"),
                ("value_quantity_unit", "cast:string"),
                ("value_quantity_system", "cast:string"),
                ("value_codeable_concept_code", "cast:string"),
                ("value_codeable_concept_system", "cast:string"),
                ("value_codeable_concept_display", "cast:string"),
                ("value_codeable_concept_text", "cast:string"),
                ("value_datetime", "cast:timestamp"),
                ("value_boolean", "cast:boolean"),
                ("data_absent_reason_code", "cast:string"),
                ("data_absent_reason_display", "cast:string"),
                ("data_absent_reason_system", "cast:string"),
                ("effective_datetime", "cast:timestamp"),
                ("effective_period_start", "cast:timestamp"),
                ("effective_period_end", "cast:timestamp"),
                ("issued", "cast:timestamp"),
                ("body_site_code", "cast:string"),
                ("body_site_system", "cast:string"),
                ("body_site_display", "cast:string"),
                ("body_site_text", "cast:string"),
                ("method_code", "cast:string"),
                ("method_system", "cast:string"),
                ("method_display", "cast:string"),
                ("method_text", "cast:string"),
                ("meta_version_id", "cast:string"),
                ("meta_last_updated", "cast:timestamp"),
                ("meta_source", "cast:string"),
                ("meta_profile", "cast:string"),
                ("meta_security", "cast:string"),
                ("meta_tag", "cast:string"),
                ("extensions", "cast:string"),
                ("created_at", "cast:timestamp"),
                ("updated_at", "cast:timestamp")
            ]
        )
        
        categories_resolved_frame = categories_dynamic_frame.resolveChoice(
            specs=[
                ("observation_id", "cast:string"),
                ("category_code", "cast:string"),
                ("category_system", "cast:string"),
                ("category_display", "cast:string"),
                ("category_text", "cast:string")
            ]
        )
        
        interpretations_resolved_frame = interpretations_dynamic_frame.resolveChoice(
            specs=[
                ("observation_id", "cast:string"),
                ("interpretation_code", "cast:string"),
                ("interpretation_system", "cast:string"),
                ("interpretation_display", "cast:string")
            ]
        )
        
        reference_ranges_resolved_frame = reference_ranges_dynamic_frame.resolveChoice(
            specs=[
                ("observation_id", "cast:string"),
                ("range_low_value", "cast:decimal"),
                ("range_low_unit", "cast:string"),
                ("range_high_value", "cast:decimal"),
                ("range_high_unit", "cast:string"),
                ("range_type_code", "cast:string"),
                ("range_type_system", "cast:string"),
                ("range_type_display", "cast:string"),
                ("range_text", "cast:string")
            ]
        )
        
        components_resolved_frame = components_dynamic_frame.resolveChoice(
            specs=[
                ("observation_id", "cast:string"),
                ("component_code", "cast:string"),
                ("component_system", "cast:string"),
                ("component_display", "cast:string"),
                ("component_text", "cast:string"),
                ("component_value_string", "cast:string"),
                ("component_value_quantity_value", "cast:decimal"),
                ("component_value_quantity_unit", "cast:string"),
                ("component_value_codeable_concept_code", "cast:string"),
                ("component_value_codeable_concept_system", "cast:string"),
                ("component_value_codeable_concept_display", "cast:string"),
                ("component_data_absent_reason_code", "cast:string"),
                ("component_data_absent_reason_display", "cast:string")
            ]
        )
        
        notes_resolved_frame = notes_dynamic_frame.resolveChoice(
            specs=[
                ("observation_id", "cast:string"),
                ("note_text", "cast:string"),
                ("note_author_reference", "cast:string"),
                ("note_time", "cast:timestamp")
            ]
        )
        
        performers_resolved_frame = performers_dynamic_frame.resolveChoice(
            specs=[
                ("observation_id", "cast:string"),
                ("performer_type", "cast:string"),
                ("performer_id", "cast:string")
            ]
        )
        
        members_resolved_frame = members_dynamic_frame.resolveChoice(
            specs=[
                ("observation_id", "cast:string"),
                ("member_observation_id", "cast:string")
            ]
        )
        
        derived_from_resolved_frame = derived_from_dynamic_frame.resolveChoice(
            specs=[
                ("observation_id", "cast:string"),
                ("derived_from_reference", "cast:string")
            ]
        )
        
        # Step 6: Final validation before writing
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 6: FINAL VALIDATION")
        logger.info("=" * 50)
        logger.info("Performing final validation before writing to Redshift...")
        
        # Validate main observations data
        main_final_df = main_resolved_frame.toDF()
        main_final_count = main_final_df.count()
        logger.info(f"Final main observations count: {main_final_count}")
        
        if main_final_count == 0:
            logger.error("No main observation records to write to Redshift! Stopping the process.")
            return
        
        # Validate other tables
        categories_final_count = categories_resolved_frame.toDF().count()
        interpretations_final_count = interpretations_resolved_frame.toDF().count()
        reference_ranges_final_count = reference_ranges_resolved_frame.toDF().count()
        components_final_count = components_resolved_frame.toDF().count()
        notes_final_count = notes_resolved_frame.toDF().count()
        performers_final_count = performers_resolved_frame.toDF().count()
        members_final_count = members_resolved_frame.toDF().count()
        derived_from_final_count = derived_from_resolved_frame.toDF().count()
        
        logger.info(f"Final counts - Categories: {categories_final_count}, Interpretations: {interpretations_final_count}, Reference Ranges: {reference_ranges_final_count}, Components: {components_final_count}, Notes: {notes_final_count}, Performers: {performers_final_count}, Members: {members_final_count}, Derived From: {derived_from_final_count}")
        
        # Debug: Show final sample data being written
        logger.info("Final sample data being written to Redshift (main observations):")
        main_final_df.show(3, truncate=False)
        
        # Show sample data for other tables as well
        logger.info("Final sample data for observation categories:")
        categories_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for observation interpretations:")
        interpretations_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for observation reference ranges:")
        reference_ranges_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for observation components:")
        components_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for observation notes:")
        notes_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for observation performers:")
        performers_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for observation members:")
        members_resolved_frame.toDF().show(3, truncate=False)
        
        logger.info("Final sample data for observation derived from:")
        derived_from_resolved_frame.toDF().show(3, truncate=False)
        
        # Step 7: Create tables and write to Redshift
        logger.info("\n" + "=" * 50)
        logger.info("💾 STEP 7: WRITING DATA TO REDSHIFT")
        logger.info("=" * 50)
        logger.info(f"🔗 Using connection: {REDSHIFT_CONNECTION}")
        logger.info(f"📁 S3 temp directory: {S3_TEMP_DIR}")
        
        # Create all tables individually
        # Note: Each write_to_redshift call now includes DELETE to prevent duplicates
        logger.info("📝 Creating main observations table...")
        observations_table_sql = create_redshift_tables_sql()
        write_to_redshift(main_resolved_frame, "observations", observations_table_sql)
        logger.info("✅ Main observations table created and written successfully")
        
        logger.info("📝 Creating observation categories table...")
        categories_table_sql = create_observation_categories_table_sql()
        write_to_redshift(categories_resolved_frame, "observation_categories", categories_table_sql)
        logger.info("✅ Observation categories table created and written successfully")
        
        logger.info("📝 Creating observation interpretations table...")
        interpretations_table_sql = create_observation_interpretations_table_sql()
        write_to_redshift(interpretations_resolved_frame, "observation_interpretations", interpretations_table_sql)
        logger.info("✅ Observation interpretations table created and written successfully")
        
        logger.info("📝 Creating observation reference ranges table...")
        reference_ranges_table_sql = create_observation_reference_ranges_table_sql()
        write_to_redshift(reference_ranges_resolved_frame, "observation_reference_ranges", reference_ranges_table_sql)
        logger.info("✅ Observation reference ranges table created and written successfully")
        
        logger.info("📝 Creating observation components table...")
        components_table_sql = create_observation_components_table_sql()
        write_to_redshift(components_resolved_frame, "observation_components", components_table_sql)
        logger.info("✅ Observation components table created and written successfully")
        
        logger.info("📝 Creating observation notes table...")
        notes_table_sql = create_observation_notes_table_sql()
        write_to_redshift(notes_resolved_frame, "observation_notes", notes_table_sql)
        logger.info("✅ Observation notes table created and written successfully")
        
        logger.info("📝 Creating observation performers table...")
        performers_table_sql = create_observation_performers_table_sql()
        write_to_redshift(performers_resolved_frame, "observation_performers", performers_table_sql)
        logger.info("✅ Observation performers table created and written successfully")
        
        logger.info("📝 Creating observation members table...")
        members_table_sql = create_observation_members_table_sql()
        write_to_redshift(members_resolved_frame, "observation_members", members_table_sql)
        logger.info("✅ Observation members table created and written successfully")
        
        logger.info("📝 Creating observation derived from table...")
        derived_from_table_sql = create_observation_derived_from_table_sql()
        write_to_redshift(derived_from_resolved_frame, "observation_derived_from", derived_from_table_sql)
        logger.info("✅ Observation derived from table created and written successfully")
        
        # Calculate processing time
        end_time = datetime.now()
        processing_time = end_time - start_time
        
        logger.info("\n" + "=" * 80)
        logger.info("🎉 ETL PROCESS COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        logger.info(f"⏰ Job completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"⏱️  Total processing time: {processing_time}")
        
        logger.info("\n📋 TABLES WRITTEN TO REDSHIFT:")
        logger.info("  ✅ public.observations (main observation data)")
        logger.info("  ✅ public.observation_categories (observation categories)")
        logger.info("  ✅ public.observation_interpretations (observation interpretations)")
        logger.info("  ✅ public.observation_reference_ranges (reference ranges)")
        logger.info("  ✅ public.observation_components (observation components)")
        logger.info("  ✅ public.observation_notes (observation notes)")
        logger.info("  ✅ public.observation_performers (observation performers)")
        logger.info("  ✅ public.observation_members (observation members)")
        logger.info("  ✅ public.observation_derived_from (derived from references)")
        
        logger.info("\n📊 FINAL ETL STATISTICS:")
        logger.info(f"  📥 Total raw records processed: {total_records:,}")
        logger.info(f"  🔬 Main observation records: {main_count:,}")
        logger.info(f"  🏷️  Category records: {categories_count:,}")
        logger.info(f"  📊 Interpretation records: {interpretations_count:,}")
        logger.info(f"  📏 Reference range records: {reference_ranges_count:,}")
        logger.info(f"  🔧 Component records: {components_count:,}")
        logger.info(f"  📝 Note records: {notes_count:,}")
        logger.info(f"  👥 Performer records: {performers_count:,}")
        logger.info(f"  🔗 Member records: {members_count:,}")
        logger.info(f"  📋 Derived from records: {derived_from_count:,}")
        
        # Calculate data expansion ratio
        total_output_records = main_count + categories_count + interpretations_count + reference_ranges_count + components_count + notes_count + performers_count + members_count + derived_from_count
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
