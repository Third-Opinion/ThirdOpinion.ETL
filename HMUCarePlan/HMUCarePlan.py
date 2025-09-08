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
TABLE_NAME = "careplan"
REDSHIFT_CONNECTION = "Redshift connection"
S3_TEMP_DIR = "s3://aws-glue-assets-442042533707-us-east-2/temporary/"

def extract_id_from_reference(reference_field, resource_type):
    """Extract ID from FHIR reference format"""
    if reference_field:
        # Handle Row/struct format: Row(reference="Patient/123", display="Name")
        if hasattr(reference_field, 'reference'):
            reference = reference_field.reference
            if reference and f"{resource_type}/" in reference:
                return reference.split("/")[-1]
        # Handle dict format: {"reference": "Patient/123", "display": "Name"}
        elif isinstance(reference_field, dict):
            reference = reference_field.get('reference')
            if reference and f"{resource_type}/" in reference:
                return reference.split("/")[-1]
        # Handle string format: "Patient/123"
        elif isinstance(reference_field, str):
            if f"{resource_type}/" in reference_field:
                return reference_field.split("/")[-1]
    return None

extract_patient_id_udf = F.udf(lambda ref: extract_id_from_reference(ref, "Patient"), StringType())
extract_encounter_id_udf = F.udf(lambda ref: extract_id_from_reference(ref, "Encounter"), StringType())
extract_care_team_id_udf = F.udf(lambda ref: extract_id_from_reference(ref, "CareTeam"), StringType())
extract_goal_id_udf = F.udf(lambda ref: extract_id_from_reference(ref, "Goal"), StringType())

def transform_main_care_plan_data(df):
    """Transform the main care plan data"""
    logger.info("Transforming main care plan data...")
    
    select_columns = [
        F.col("id").alias("care_plan_id"),
        F.when(F.col("subject").isNotNull(), 
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        F.col("status").alias("status"),
        F.col("intent").alias("intent"),
        F.col("title").alias("title"),
        F.col("meta").getField("versionId").alias("meta_version_id"),
        F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("meta_last_updated"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ]
    
    main_df = df.select(*select_columns).filter(
        F.col("care_plan_id").isNotNull() & 
        F.col("patient_id").isNotNull()
    )
    
    return main_df

def transform_care_plan_identifiers(df):
    """Transform care plan identifiers"""
    logger.info("Transforming care plan identifiers...")
    
    if "identifier" not in df.columns:
        logger.warning("identifier column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("care_plan_id"),
            F.lit("").alias("identifier_system"),
            F.lit("").alias("identifier_value")
        ).filter(F.lit(False))
    
    identifiers_df = df.select(
        F.col("id").alias("care_plan_id"),
        F.explode(F.col("identifier")).alias("identifier_item")
    ).filter(
        F.col("identifier_item").isNotNull()
    )
    
    identifiers_final = identifiers_df.select(
        F.col("care_plan_id"),
        F.lit(None).cast(StringType()).alias("identifier_system"),
        F.col("identifier_item.value").alias("identifier_value")
    ).filter(
        F.col("identifier_value").isNotNull()
    )
    
    return identifiers_final

def transform_care_plan_categories(df):
    """Transform care plan categories"""
    logger.info("Transforming care plan categories...")
    
    if "category" not in df.columns:
        logger.warning("category column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("care_plan_id"),
            F.lit("").alias("category_code"),
            F.lit("").alias("category_system"),
            F.lit("").alias("category_display"),
            F.lit("").alias("category_text")
        ).filter(F.lit(False))
    
    categories_df = df.select(
        F.col("id").alias("care_plan_id"),
        F.explode(F.col("category")).alias("category_item")
    ).filter(
        F.col("category_item").isNotNull()
    )
    
    # First check if coding array exists and is not null
    categories_with_coding = categories_df.filter(
        F.col("category_item.coding").isNotNull() &
        (F.size(F.col("category_item.coding")) > 0)
    )
    
    if categories_with_coding.count() == 0:
        logger.warning("No categories with coding found, returning empty DataFrame")
        return categories_df.select(
            F.col("care_plan_id"),
            F.lit("").alias("category_code"),
            F.lit("").alias("category_system"),
            F.lit("").alias("category_display"),
            F.lit("").alias("category_text")
        ).filter(F.lit(False))
    
    categories_exploded = categories_with_coding.select(
        F.col("care_plan_id"),
        F.explode(F.col("category_item.coding")).alias("coding_item"),
        F.lit(None).cast(StringType()).alias("category_text")  # Set to null since text field doesn't exist
    )
    
    categories_final = categories_exploded.select(
        F.col("care_plan_id"),
        F.when(F.col("coding_item.code").isNotNull(), 
               F.col("coding_item.code")).otherwise(None).alias("category_code"),
        F.when(F.col("coding_item.system").isNotNull(), 
               F.col("coding_item.system")).otherwise(None).alias("category_system"),
        F.lit(None).cast(StringType()).alias("category_display"),  # Set to null since display field doesn't exist
        F.col("category_text")
    ).filter(
        F.col("category_code").isNotNull()
    )
    
    return categories_final

def transform_care_plan_care_teams(df):
    """Transform care plan care teams"""
    logger.info("Transforming care plan care teams...")
    
    if "careTeam" not in df.columns:
        logger.warning("careTeam column not found, returning empty DataFrame")
        return df.select(
            F.col("id").alias("care_plan_id"),
            F.lit("").alias("care_team_id")
        ).filter(F.lit(False))
    
    care_teams_df = df.select(
        F.col("id").alias("care_plan_id"),
        F.explode(F.col("careTeam")).alias("care_team_item")
    ).filter(
        F.col("care_team_item").isNotNull() &
        F.col("care_team_item.reference").isNotNull()
    )
    
    care_teams_final = care_teams_df.select(
        F.col("care_plan_id"),
        F.regexp_extract(F.col("care_team_item.reference"), r"CareTeam/(.+)", 1).alias("care_team_id")
    ).filter(F.col("care_team_id") != "")
    
    return care_teams_final

def transform_care_plan_goals(df):
    """Transform care plan goals"""
    logger.info("Transforming care plan goals...")
    
    if "goal" not in df.columns:
        logger.warning("goal column not found, returning empty DataFrame")
        return df.select(
            F.col("id").alias("care_plan_id"),
            F.lit("").alias("goal_id")
        ).filter(F.lit(False))
        
    goals_df = df.select(
        F.col("id").alias("care_plan_id"),
        F.explode(F.col("goal")).alias("goal_item")
    ).filter(
        F.col("goal_item").isNotNull() &
        F.col("goal_item.reference").isNotNull()
    )
    
    goals_final = goals_df.select(
        F.col("care_plan_id"),
        F.regexp_extract(F.col("goal_item.reference"), r"Goal/(.+)", 1).alias("goal_id")
    ).filter(F.col("goal_id") != "")
    
    return goals_final

def create_care_plans_table_sql():
    """Generate SQL for creating main care_plans table in Redshift"""
    return """
    DROP TABLE IF EXISTS public.care_plans CASCADE;
    CREATE TABLE public.care_plans (
        care_plan_id VARCHAR(255) PRIMARY KEY,
        patient_id VARCHAR(255) NOT NULL,
        status VARCHAR(50),
        intent VARCHAR(50),
        title VARCHAR(500),
        meta_version_id VARCHAR(50),
        meta_last_updated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id);
    """

def create_care_plan_identifiers_table_sql():
    """Generate SQL for creating care_plan_identifiers table"""
    return """
    DROP TABLE IF EXISTS public.care_plan_identifiers CASCADE;
    CREATE TABLE public.care_plan_identifiers (
        care_plan_id VARCHAR(255),
        identifier_system VARCHAR(255),
        identifier_value VARCHAR(255)
    ) SORTKEY (care_plan_id, identifier_system);
    """

def create_care_plan_categories_table_sql():
    """Generate SQL for creating care_plan_categories table"""
    return """
    DROP TABLE IF EXISTS public.care_plan_categories CASCADE;
    CREATE TABLE public.care_plan_categories (
        care_plan_id VARCHAR(255),
        category_code VARCHAR(50),
        category_system VARCHAR(255),
        category_display VARCHAR(255),
        category_text VARCHAR(500)
    ) SORTKEY (care_plan_id, category_code);
    """

def create_care_plan_care_teams_table_sql():
    """Generate SQL for creating care_plan_care_teams table"""
    return """
    DROP TABLE IF EXISTS public.care_plan_care_teams CASCADE;
    CREATE TABLE public.care_plan_care_teams (
        care_plan_id VARCHAR(255),
        care_team_id VARCHAR(255)
    ) SORTKEY (care_plan_id);
    """

def create_care_plan_goals_table_sql():
    """Generate SQL for creating care_plan_goals table"""
    return """
    DROP TABLE IF EXISTS public.care_plan_goals CASCADE;
    CREATE TABLE public.care_plan_goals (
        care_plan_id VARCHAR(255),
        goal_id VARCHAR(255)
    ) SORTKEY (care_plan_id);
    """

def write_to_redshift(dynamic_frame, table_name, preactions=""):
    """Write DynamicFrame to Redshift using JDBC connection"""
    logger.info(f"Writing {table_name} to Redshift...")
    logger.info(f"🔧 Preactions SQL for {table_name}:\n{preactions}")
    
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
    """Main ETL process"""
    start_time = datetime.now()
    try:
        logger.info("=" * 80)
        logger.info("🚀 STARTING FHIR CARE PLAN ETL PROCESS")
        logger.info("=" * 80)
        
        # Step 1: Read data from Glue Catalog
        logger.info("📥 STEP 1: READING DATA FROM GLUE CATALOG")
        care_plan_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=DATABASE_NAME, 
            table_name=TABLE_NAME, 
            transformation_ctx="AWSGlueDataCatalog_care_plan_node"
        )
        care_plan_df = care_plan_dynamic_frame.toDF()
        total_records = care_plan_df.count()
        logger.info(f"📊 Read {total_records:,} raw care plan records")
        if total_records == 0:
            logger.error("❌ No raw data found!")
            return

        # Step 2: Transform main care plan data
        logger.info("🔄 STEP 2: TRANSFORMING MAIN CARE PLAN DATA")
        main_care_plan_df = transform_main_care_plan_data(care_plan_df)
        main_count = main_care_plan_df.count()
        logger.info(f"✅ Transformed {main_count:,} main care plan records")
        if main_count == 0:
            logger.error("❌ No main care plan records after transformation!")
            return

        # Step 3: Transform multi-valued data
        logger.info("🔄 STEP 3: TRANSFORMING MULTI-VALUED DATA")
        identifiers_df = transform_care_plan_identifiers(care_plan_df)
        identifiers_count = identifiers_df.count()
        logger.info(f"✅ Transformed {identifiers_count:,} identifier records")
        
        categories_df = transform_care_plan_categories(care_plan_df)
        categories_count = categories_df.count()
        logger.info(f"✅ Transformed {categories_count:,} category records")

        care_teams_df = transform_care_plan_care_teams(care_plan_df)
        care_teams_count = care_teams_df.count()
        logger.info(f"✅ Transformed {care_teams_count:,} care team records")

        goals_df = transform_care_plan_goals(care_plan_df)
        goals_count = goals_df.count()
        logger.info(f"✅ Transformed {goals_count:,} goal records")

        # Step 4: Convert to DynamicFrames
        logger.info("🔄 STEP 4: CONVERTING TO DYNAMICFRAMES")
        main_dynamic_frame = DynamicFrame.fromDF(main_care_plan_df, glueContext, "main_care_plan_dynamic_frame")
        identifiers_dynamic_frame = DynamicFrame.fromDF(identifiers_df, glueContext, "identifiers_dynamic_frame")
        categories_dynamic_frame = DynamicFrame.fromDF(categories_df, glueContext, "categories_dynamic_frame")
        care_teams_dynamic_frame = DynamicFrame.fromDF(care_teams_df, glueContext, "care_teams_dynamic_frame")
        goals_dynamic_frame = DynamicFrame.fromDF(goals_df, glueContext, "goals_dynamic_frame")

        # Step 5: Resolve choice types
        logger.info("🔄 STEP 5: RESOLVING CHOICE TYPES")
        main_resolved_frame = main_dynamic_frame.resolveChoice(specs=[(c, "cast:string") for c in main_care_plan_df.columns])
        
        # Step 6: Write to Redshift
        logger.info("💾 STEP 6: WRITING DATA TO REDSHIFT")
        write_to_redshift(main_resolved_frame, "care_plans", create_care_plans_table_sql())
        if identifiers_count > 0:
            write_to_redshift(identifiers_dynamic_frame, "care_plan_identifiers", create_care_plan_identifiers_table_sql())
        if categories_count > 0:
            write_to_redshift(categories_dynamic_frame, "care_plan_categories", create_care_plan_categories_table_sql())
        if care_teams_count > 0:
            write_to_redshift(care_teams_dynamic_frame, "care_plan_care_teams", create_care_plan_care_teams_table_sql())
        if goals_count > 0:
            write_to_redshift(goals_dynamic_frame, "care_plan_goals", create_care_plan_goals_table_sql())

        end_time = datetime.now()
        logger.info("=" * 80)
        logger.info("🎉 ETL PROCESS COMPLETED SUCCESSFULLY!")
        logger.info(f"⏱️  Total processing time: {end_time - start_time}")
        logger.info("=" * 80)

    except Exception as e:
        logger.error("❌ ETL PROCESS FAILED!")
        logger.error(f"🚨 Error: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
    job.commit()
