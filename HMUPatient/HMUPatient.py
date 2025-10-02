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
from pyspark.sql.types import StringType, TimestampType, DateType, BooleanType, IntegerType
import json
import logging

# Import FHIR version comparison utilities
# FHIR version comparison utilities implemented inline below

# FHIR version comparison utilities are implemented inline below

# Set up logging to write to stdout (CloudWatch)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Add handler to write logs to stdout so they appear in CloudWatch
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

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
TABLE_NAME = "patient"
REDSHIFT_CONNECTION = "Redshift connection"
S3_TEMP_DIR = "s3://aws-glue-assets-442042533707-us-east-2/temporary/"

# Note: We now read data from S3 using Iceberg catalog instead of Glue Catalog
# This provides better performance and direct access to S3 data

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

def extract_practitioner_id_from_reference(reference_field):
    """Extract practitioner ID from FHIR reference format"""
    if reference_field:
        # Handle Row/struct format: Row(reference="Practitioner/123", display="Name")
        if hasattr(reference_field, 'reference'):
            reference = reference_field.reference
            if reference and "/" in reference:
                return reference.split("/")[-1]
        # Handle dict format: {"reference": "Practitioner/123", "display": "Name"}
        elif isinstance(reference_field, dict):
            reference = reference_field.get('reference')
            if reference and "/" in reference:
                return reference.split("/")[-1]
        # Handle string format: "Practitioner/123"
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
    except Exception as e:
        logger.warning(f"Column {column_name} or field {field_name} not found: {str(e)}")
        return F.lit(None)

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

def transform_main_patient_data(df):
    """Transform the main patient data"""
    logger.info("Transforming main patient data...")
    
    # Log available columns for debugging
    available_columns = df.columns
    logger.info(f"Available columns: {available_columns}")
    
    # Build the select statement dynamically based on available columns
    select_columns = [
        F.col("id").alias("patient_id"),
        F.col("active").alias("active"),
        F.col("gender").alias("gender"),
        F.to_date(F.col("birthDate"), "yyyy-MM-dd").alias("birth_date"),
        F.when(F.col("deceasedDateTime").isNotNull(), True).otherwise(False).alias("deceased"),
        # Handle deceasedDateTime with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("deceasedDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("deceasedDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("deceasedDateTime"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("deceasedDateTime"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("deceasedDateTime"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("deceased_date"),
    F.lit("Patient").alias("resourcetype"),  # Always "Patient" for patient records
        F.lit(None).alias("photos"),  # photo field not available in schema
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ]
    
    # Add marital status handling
    select_columns.append(
        F.when(F.col("maritalStatus").isNotNull() & F.col("maritalStatus.coding").isNotNull() & (F.size(F.col("maritalStatus.coding")) > 0),
               F.col("maritalStatus.coding")[0].getField("code")
              ).otherwise(None).alias("marital_status_code")
    )
    
    select_columns.append(
        F.when(F.col("maritalStatus").isNotNull() & F.col("maritalStatus.coding").isNotNull() & (F.size(F.col("maritalStatus.coding")) > 0),
               F.col("maritalStatus.coding")[0].getField("display")
              ).otherwise(None).alias("marital_status_display")
    )
    
    select_columns.append(
        F.when(F.col("maritalStatus").isNotNull() & F.col("maritalStatus.coding").isNotNull() & (F.size(F.col("maritalStatus.coding")) > 0),
               F.col("maritalStatus.coding")[0].getField("system")
              ).otherwise(None).alias("marital_status_system")
    )
    
    # Add multiple birth handling (field not available in schema)
    select_columns.append(
        F.lit(None).alias("multiple_birth")
    )
    
    select_columns.append(
        F.lit(None).alias("birth_order")
    )
    
    # Add managing organization handling (field not available in schema)
    select_columns.append(
        F.lit(None).alias("managing_organization_id")
    )
    
    # Add meta data handling
    select_columns.extend([
        # Handle meta.lastUpdated datetime with multiple possible formats
        F.coalesce(
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss")
        ).alias("meta_last_updated"),
        F.lit(None).alias("meta_source"),  # source field not available in meta struct
        convert_to_json_udf(F.col("meta").getField("security")).alias("meta_security"),
        F.lit(None).alias("meta_tag"),  # tag field not available in meta struct
        convert_to_json_udf(F.col("extension")).alias("extensions")
    ])
    
    # Transform main patient data using only available columns and flatten complex structures
    main_df = df.select(*select_columns).filter(
        F.col("patient_id").isNotNull()
    )
    
    return main_df

def transform_patient_names(df):
    """Transform patient names (multiple names per patient)"""
    logger.info("Transforming patient names...")
    
    # Use Spark's native column operations to handle the nested structure
    # name: array -> element: struct -> {use, text, family, given, prefix, suffix, period}
    
    # First explode the name array
    names_df = df.select(
        F.col("id").alias("patient_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode(F.col("name")).alias("name_item")
    ).filter(
        F.col("name_item").isNotNull()
    )
    
    # Extract name details
    names_final = names_df.select(
        F.col("patient_id"),
        F.col("meta_last_updated"),
        F.col("name_item.use").alias("name_use"),
        F.col("name_item.text").alias("name_text"),  # text field should be available in name struct
        F.col("name_item.family").alias("family_name"),
        F.when(F.col("name_item.given").isNotNull() & (F.size(F.col("name_item.given")) > 0),
               F.array_join(F.col("name_item.given"), " ")
              ).otherwise(None).alias("given_names"),
        F.col("name_item.prefix").alias("prefix"),  # prefix field should be available in name struct
        F.col("name_item.suffix").alias("suffix"),
        F.to_date(F.col("name_item.period.start"), "yyyy-MM-dd").alias("period_start"),
        F.to_date(F.col("name_item.period.end"), "yyyy-MM-dd").alias("period_end")
    ).filter(
        F.col("family_name").isNotNull() | F.col("name_text").isNotNull()  # filter on either family name or text
    )
    
    return names_final

def transform_patient_telecoms(df):
    """Transform patient telecom information (phone, email, etc.)"""
    logger.info("Transforming patient telecoms...")
    
    # Use Spark's native column operations to handle the nested structure
    # telecom: array -> element: struct -> {system, value, use, rank, period}
    
    # First explode the telecom array
    telecoms_df = df.select(
        F.col("id").alias("patient_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode(F.col("telecom")).alias("telecom_item")
    ).filter(
        F.col("telecom_item").isNotNull()
    )
    
    # Extract telecom details
    telecoms_final = telecoms_df.select(
        F.col("patient_id"),
        F.col("meta_last_updated"),
        F.col("telecom_item.system").alias("telecom_system"),
        F.col("telecom_item.value").alias("telecom_value"),
        F.when(F.col("telecom_item.use").isNull(), "home").otherwise(F.col("telecom_item.use")).alias("telecom_use"),
        F.lit(None).alias("telecom_rank"),  # rank field not available in telecom struct
        F.lit(None).alias("period_start"),  # period field not available in telecom struct
        F.lit(None).alias("period_end")     # period field not available in telecom struct
    ).filter(
        F.col("telecom_value").isNotNull()
    )
    
    return telecoms_final

def transform_patient_addresses(df):
    """Transform patient addresses"""
    logger.info("Transforming patient addresses...")
    
    # Use Spark's native column operations to handle the nested structure
    # address: array -> element: struct -> {use, type, text, line, city, district, state, postalCode, country, period}
    
    # First explode the address array
    addresses_df = df.select(
        F.col("id").alias("patient_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode(F.col("address")).alias("address_item")
    ).filter(
        F.col("address_item").isNotNull()
    )
    
    # Extract address details
    addresses_final = addresses_df.select(
        F.col("patient_id"),
        F.col("meta_last_updated"),
        F.col("address_item.use").alias("address_use"),
        F.col("address_item.type").alias("address_type"),
        F.col("address_item.text").alias("address_text"),  # text field should be available in address struct
        convert_to_json_udf(F.col("address_item.line")).alias("address_line"),
        F.col("address_item.city").alias("city"),
        F.col("address_item.district").alias("district"),  # district field should be available in address struct
        F.col("address_item.state").alias("state"),
        F.col("address_item.postalCode").alias("postal_code"),
        F.col("address_item.country").alias("country"),
        F.to_date(F.col("address_item.period.start"), "yyyy-MM-dd").alias("period_start"),
        F.to_date(F.col("address_item.period.end"), "yyyy-MM-dd").alias("period_end")
    ).filter(
        F.col("city").isNotNull() | F.col("address_text").isNotNull()  # filter on either city or text
    )
    
    return addresses_final

def transform_patient_contacts(df):
    """Transform patient contacts (emergency contacts, next-of-kin)"""
    logger.info("Transforming patient contacts...")
    
    # Check if contact column exists
    if "contact" not in df.columns:
        logger.warning("contact column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("patient_id"),
            F.lit("").alias("contact_relationship_code"),
            F.lit("").alias("contact_relationship_system"),
            F.lit("").alias("contact_relationship_display"),
            F.lit("").alias("contact_name_text"),
            F.lit("").alias("contact_name_family"),
            F.lit("").alias("contact_name_given"),
            F.lit("").alias("contact_telecom_system"),
            F.lit("").alias("contact_telecom_value"),
            F.lit("").alias("contact_telecom_use"),
            F.lit("").alias("contact_gender"),
            F.lit("").alias("contact_organization_id"),
            F.lit(None).cast(DateType()).alias("period_start"),
            F.lit(None).cast(DateType()).alias("period_end")
        ).filter(F.lit(False))
    
    # Use Spark's native column operations to handle the nested structure
    # contact: array -> element: struct -> {relationship, name, telecom, address, gender, organization, period}
    
    # First explode the contact array
    contacts_df = df.select(
        F.col("id").alias("patient_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode(F.col("contact")).alias("contact_item")
    ).filter(
        F.col("contact_item").isNotNull()
    )
    
    # Extract contact details
    contacts_final = contacts_df.select(
        F.col("patient_id"),
        F.col("meta_last_updated"),
        F.when(F.col("contact_item.relationship").isNotNull() & F.col("contact_item.relationship.coding").isNotNull() & (F.size(F.col("contact_item.relationship.coding")) > 0),
               F.col("contact_item.relationship.coding")[0].getField("code")
              ).otherwise(None).alias("contact_relationship_code"),
        F.when(F.col("contact_item.relationship").isNotNull() & F.col("contact_item.relationship.coding").isNotNull() & (F.size(F.col("contact_item.relationship.coding")) > 0),
               F.col("contact_item.relationship.coding")[0].getField("system")
              ).otherwise(None).alias("contact_relationship_system"),
        F.when(F.col("contact_item.relationship").isNotNull() & F.col("contact_item.relationship.coding").isNotNull() & (F.size(F.col("contact_item.relationship.coding")) > 0),
               F.col("contact_item.relationship.coding")[0].getField("display")
              ).otherwise(None).alias("contact_relationship_display"),
        F.col("contact_item.name.text").alias("contact_name_text"),  # text field should be available in contact name struct
        F.col("contact_item.name.family").alias("contact_name_family"),
        convert_to_json_udf(F.col("contact_item.name.given")).alias("contact_name_given"),
        F.col("contact_item.telecom.system").alias("contact_telecom_system"),
        F.col("contact_item.telecom.value").alias("contact_telecom_value"),
        F.col("contact_item.telecom.use").alias("contact_telecom_use"),
        F.lit(None).alias("contact_gender"),  # gender field not available in contact struct
        F.lit(None).alias("contact_organization_id"),  # organization field not available in contact struct
        F.lit(None).alias("period_start"),  # period field not available in contact struct
        F.lit(None).alias("period_end")     # period field not available in contact struct
    ).filter(
        F.col("contact_name_family").isNotNull() | F.col("contact_name_text").isNotNull()  # filter on either family name or text
    )
    
    return contacts_final

def transform_patient_communications(df):
    """Transform patient communication preferences (languages)"""
    logger.info("Transforming patient communications...")
    
    # Check if communication column exists
    if "communication" not in df.columns:
        logger.warning("communication column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("patient_id"),
            F.lit("").alias("language_code"),
            F.lit("").alias("language_system"),
            F.lit("").alias("language_display"),
            F.lit(False).cast(BooleanType()).alias("preferred"),
            F.lit("").alias("extensions")
        ).filter(F.lit(False))
    
    # Use Spark's native column operations to handle the nested structure
    # communication: array -> element: struct -> {language, preferred, extension}
    
    # First explode the communication array
    communications_df = df.select(
        F.col("id").alias("patient_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode(F.col("communication")).alias("communication_item")
    ).filter(
        F.col("communication_item").isNotNull()
    )
    
    # Extract communication details
    communications_final = communications_df.select(
        F.col("patient_id"),
        F.col("meta_last_updated"),
        F.when(F.col("communication_item.language").isNotNull() & F.col("communication_item.language.coding").isNotNull() & (F.size(F.col("communication_item.language.coding")) > 0),
               F.col("communication_item.language.coding")[0].getField("code")
              ).otherwise(None).alias("language_code"),
        F.when(F.col("communication_item.language").isNotNull() & F.col("communication_item.language.coding").isNotNull() & (F.size(F.col("communication_item.language.coding")) > 0),
               F.col("communication_item.language.coding")[0].getField("system")
              ).otherwise(None).alias("language_system"),
        F.when(F.col("communication_item.language").isNotNull() & F.col("communication_item.language.coding").isNotNull() & (F.size(F.col("communication_item.language.coding")) > 0),
               F.col("communication_item.language.coding")[0].getField("display")
              ).otherwise(None).alias("language_display"),
        F.lit(False).alias("preferred"),  # preferred field not available in communication struct
        F.lit(None).alias("extensions")  # extension field not available in communication struct
    ).filter(
        F.col("language_code").isNotNull()
    )
    
    return communications_final

def transform_patient_practitioners(df):
    """Transform patient general practitioners"""
    logger.info("Transforming patient practitioners...")
    
    # Check if generalPractitioner column exists
    if "generalPractitioner" not in df.columns:
        logger.warning("generalPractitioner column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("patient_id"),
            F.lit("").alias("practitioner_id"),
            F.lit("").alias("practitioner_role_id"),
            F.lit("").alias("organization_id"),
            F.lit("").alias("reference_type")
        ).filter(F.lit(False))
    
    # Use Spark's native column operations to handle the nested structure
    # generalPractitioner: array -> element: Reference -> {reference, display}
    
    # First explode the generalPractitioner array
    practitioners_df = df.select(
        F.col("id").alias("patient_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode(F.col("generalPractitioner")).alias("practitioner_item")
    ).filter(
        F.col("practitioner_item").isNotNull()
    )
    
    # Extract practitioner details
    practitioners_final = practitioners_df.select(
        F.col("patient_id"),
        F.col("meta_last_updated"),
        F.when(F.col("practitioner_item.reference").rlike("^Practitioner/"),
               F.regexp_extract(F.col("practitioner_item.reference"), r"Practitioner/(.+)", 1)
              ).otherwise(None).alias("practitioner_id"),
        F.when(F.col("practitioner_item.reference").rlike("^PractitionerRole/"),
               F.regexp_extract(F.col("practitioner_item.reference"), r"PractitionerRole/(.+)", 1)
              ).otherwise(None).alias("practitioner_role_id"),
        F.when(F.col("practitioner_item.reference").rlike("^Organization/"),
               F.regexp_extract(F.col("practitioner_item.reference"), r"Organization/(.+)", 1)
              ).otherwise(None).alias("organization_id"),
        F.when(F.col("practitioner_item.reference").rlike("^Practitioner/"), "Practitioner")
        .when(F.col("practitioner_item.reference").rlike("^PractitionerRole/"), "PractitionerRole")
        .when(F.col("practitioner_item.reference").rlike("^Organization/"), "Organization")
        .otherwise("Unknown").alias("reference_type")
    ).filter(
        F.col("practitioner_id").isNotNull() | F.col("practitioner_role_id").isNotNull() | F.col("organization_id").isNotNull()
    )
    
    return practitioners_final

def transform_patient_links(df):
    """Transform patient links (links to other patient resources)"""
    logger.info("Transforming patient links...")
    
    # Check if link column exists
    if "link" not in df.columns:
        logger.warning("link column not found in data, returning empty DataFrame")
        # Return empty DataFrame with expected schema
        return df.select(
            F.col("id").alias("patient_id"),
            F.lit("").alias("other_patient_id"),
            F.lit("").alias("link_type_code"),
            F.lit("").alias("link_type_system"),
            F.lit("").alias("link_type_display")
        ).filter(F.lit(False))
    
    # Use Spark's native column operations to handle the nested structure
    # link: array -> element: struct -> {other, type}
    
    # First explode the link array
    links_df = df.select(
        F.col("id").alias("patient_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode(F.col("link")).alias("link_item")
    ).filter(
        F.col("link_item").isNotNull()
    )
    
    # Extract link details
    links_final = links_df.select(
        F.col("patient_id"),
        F.col("meta_last_updated"),
        F.when(F.col("link_item.other").isNotNull(),
               F.regexp_extract(F.col("link_item.other").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("other_patient_id"),
        F.col("link_item.type").alias("link_type_code"),  # type is a simple string, not a complex struct
        F.lit(None).alias("link_type_system"),  # system field not available in link type
        F.lit(None).alias("link_type_display")  # display field not available in link type
    ).filter(
        F.col("other_patient_id").isNotNull()
    )
    
    return links_final

def create_redshift_tables_sql():
    """Generate SQL for creating main patients table in Redshift with proper syntax"""
    return """
    -- Main patients table
    CREATE TABLE IF NOT EXISTS public.patients (
        patient_id VARCHAR(255) PRIMARY KEY,
        active BOOLEAN,
        gender VARCHAR(10),
        birth_date DATE,
        deceased BOOLEAN,
        deceased_date TIMESTAMP,
        resourcetype VARCHAR(50),
        marital_status_code VARCHAR(50),
        marital_status_display VARCHAR(255),
        marital_status_system VARCHAR(255),
        multiple_birth BOOLEAN,
        birth_order INTEGER,
        managing_organization_id VARCHAR(255),
        photos TEXT,
        meta_last_updated TIMESTAMP,
        meta_source VARCHAR(255),
        meta_security TEXT,
        meta_tag TEXT,
        extensions TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTKEY (patient_id) SORTKEY (patient_id, birth_date)
    """

def create_patient_names_table_sql():
    """Generate SQL for creating patient_names table"""
    return """
    CREATE TABLE IF NOT EXISTS public.patient_names (
        patient_id VARCHAR(255),
        name_use VARCHAR(50),
        name_text VARCHAR(255),
        family_name VARCHAR(255),
        given_names TEXT,
        prefix VARCHAR(50),
        suffix VARCHAR(50),
        period_start DATE,
        period_end DATE
    ) SORTKEY (patient_id, name_use)
    """

def create_patient_telecoms_table_sql():
    """Generate SQL for creating patient_telecoms table"""
    return """
    CREATE TABLE IF NOT EXISTS public.patient_telecoms (
        patient_id VARCHAR(255),
        telecom_system VARCHAR(50),
        telecom_value VARCHAR(255),
        telecom_use VARCHAR(50),
        telecom_rank INTEGER,
        period_start DATE,
        period_end DATE
    ) SORTKEY (patient_id, telecom_system)
    """

def create_patient_addresses_table_sql():
    """Generate SQL for creating patient_addresses table"""
    return """
    CREATE TABLE IF NOT EXISTS public.patient_addresses (
        patient_id VARCHAR(255),
        address_use VARCHAR(50),
        address_type VARCHAR(50),
        address_text VARCHAR(500),
        address_line TEXT,
        city VARCHAR(100),
        district VARCHAR(100),
        state VARCHAR(100),
        postal_code VARCHAR(20),
        country VARCHAR(100),
        period_start DATE,
        period_end DATE
    ) SORTKEY (patient_id, address_use)
    """

def create_patient_contacts_table_sql():
    """Generate SQL for creating patient_contacts table"""
    return """
    CREATE TABLE IF NOT EXISTS public.patient_contacts (
        patient_id VARCHAR(255),
        contact_relationship_code VARCHAR(50),
        contact_relationship_system VARCHAR(255),
        contact_relationship_display VARCHAR(255),
        contact_name_text VARCHAR(255),
        contact_name_family VARCHAR(255),
        contact_name_given TEXT,
        contact_telecom_system VARCHAR(50),
        contact_telecom_value VARCHAR(255),
        contact_telecom_use VARCHAR(50),
        contact_gender VARCHAR(10),
        contact_organization_id VARCHAR(255),
        period_start DATE,
        period_end DATE
    ) SORTKEY (patient_id, contact_relationship_code)
    """

def create_patient_communications_table_sql():
    """Generate SQL for creating patient_communications table"""
    return """
    CREATE TABLE IF NOT EXISTS public.patient_communications (
        patient_id VARCHAR(255),
        language_code VARCHAR(10),
        language_system VARCHAR(255),
        language_display VARCHAR(255),
        preferred BOOLEAN,
        extensions TEXT
    ) SORTKEY (patient_id, language_code)
    """

def create_patient_practitioners_table_sql():
    """Generate SQL for creating patient_practitioners table"""
    return """
    CREATE TABLE IF NOT EXISTS public.patient_practitioners (
        patient_id VARCHAR(255),
        practitioner_id VARCHAR(255),
        practitioner_role_id VARCHAR(255),
        organization_id VARCHAR(255),
        reference_type VARCHAR(50)
    ) SORTKEY (patient_id, reference_type)
    """

def create_patient_links_table_sql():
    """Generate SQL for creating patient_links table"""
    return """
    CREATE TABLE IF NOT EXISTS public.patient_links (
        patient_id VARCHAR(255),
        other_patient_id VARCHAR(255),
        link_type_code VARCHAR(50),
        link_type_system VARCHAR(255),
        link_type_display VARCHAR(255)
    ) SORTKEY (patient_id, other_patient_id)
    """


def get_existing_versions_from_redshift(table_name, id_column):
    """Query Redshift to get existing entity timestamps for comparison"""
    logger.info(f"Fetching existing timestamps from {table_name}...")

    try:
        # First check if table exists by trying to read it directly
        # This prevents malformed query errors when table doesn't exist
        existing_versions_df = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "redshiftTmpDir": S3_TEMP_DIR,
                "useConnectionProperties": "true",
                "dbtable": f"public.{table_name}",
                "connectionName": REDSHIFT_CONNECTION
            },
            transformation_ctx=f"read_existing_versions_{table_name}"
        )

        # Convert to Spark DataFrame for easier processing
        existing_df = existing_versions_df.toDF()

        # Select only the columns we need if table exists
        if id_column in existing_df.columns and 'meta_last_updated' in existing_df.columns:
            existing_df = existing_df.select(id_column, 'meta_last_updated')
        else:
            logger.warning(f"Table {table_name} exists but missing required columns: {id_column} or meta_last_updated")
            return {}

        # Collect as dictionary: {entity_id: timestamp}
        timestamp_map = {}
        if existing_df.count() > 0:
            rows = existing_df.collect()
            for row in rows:
                entity_id = row[id_column]
                timestamp = row['meta_last_updated']
                if entity_id and timestamp:
                    timestamp_map[entity_id] = timestamp

        logger.info(f"Found {len(timestamp_map)} existing entities with timestamps in {table_name}")
        return timestamp_map

    except Exception as e:
        logger.info(f"Table {table_name} does not exist or is empty - treating all records as new")
        logger.debug(f"Details: {str(e)}")
        return {}

def filter_dataframe_by_version(df, existing_versions, id_column):
    """Filter DataFrame based on version comparison"""
    logger.info("Filtering data based on version comparison...")

    if not existing_versions:
        # No existing data, all records are new
        total_count = df.count()
        logger.info(f"No existing versions found - treating all {total_count} records as new")
        return df, total_count, 0

    # Add a column to mark records that need processing
    def needs_processing(entity_id, last_updated):
        """Check if record needs processing based on timestamp comparison"""
        if entity_id is None or last_updated is None:
            return True  # Process records with missing IDs/timestamps

        existing_timestamp = existing_versions.get(entity_id)
        if existing_timestamp is None:
            return True  # New entity

        # Convert timestamps to comparable format if needed
        # If timestamps are already datetime objects, direct comparison works
        if existing_timestamp == last_updated:
            return False  # Same timestamp, skip

        # Process if incoming timestamp is newer than existing
        # Note: This handles the case where timestamps might be different
        # In production, you may want to add tolerance for small time differences
        try:
            return last_updated > existing_timestamp
        except TypeError:
            # If comparison fails (e.g., different types), process the record
            return True

    # Create UDF for timestamp comparison
    from pyspark.sql.types import BooleanType
    needs_processing_udf = F.udf(needs_processing, BooleanType())

    # Add processing flag
    df_with_flag = df.withColumn(
        "needs_processing",
        needs_processing_udf(F.col(id_column), F.col("meta_last_updated"))
    )

    # Split into processing needed and skipped
    to_process_df = df_with_flag.filter(F.col("needs_processing") == True).drop("needs_processing")
    skipped_count = df_with_flag.filter(F.col("needs_processing") == False).count()

    to_process_count = to_process_df.count()
    total_count = df.count()

    logger.info(f"Version comparison results:")
    logger.info(f"  Total incoming records: {total_count}")
    logger.info(f"  Records to process (new/updated): {to_process_count}")
    logger.info(f"  Records to skip (same version): {skipped_count}")

    return to_process_df, to_process_count, skipped_count

def get_entities_to_delete(df, existing_versions, id_column):
    """Get list of entity IDs that need their old versions deleted"""
    logger.info("Identifying entities that need old version cleanup...")

    if not existing_versions:
        return []

    # Get list of entity IDs from incoming data
    incoming_entity_ids = set()
    if df.count() > 0:
        entity_rows = df.select(id_column).distinct().collect()
        incoming_entity_ids = {row[id_column] for row in entity_rows if row[id_column]}

    # Find entities that exist in both incoming data and Redshift
    entities_to_delete = []
    for entity_id in incoming_entity_ids:
        if entity_id in existing_versions:
            entities_to_delete.append(entity_id)

    logger.info(f"Found {len(entities_to_delete)} entities that need old version cleanup")
    return entities_to_delete

def write_to_redshift_versioned(dynamic_frame, table_name, id_column, preactions=""):
    """Version-aware write to Redshift - only processes new/updated entities"""
    logger.info(f"Writing {table_name} to Redshift with version checking...")

    try:
        # Convert dynamic frame to DataFrame for processing
        df = dynamic_frame.toDF()
        total_records = df.count()

        if total_records == 0:
            logger.info(f"No records to process for {table_name}")
            return

        # Step 1: Get existing versions from Redshift
        existing_versions = get_existing_versions_from_redshift(table_name, id_column)

        # Step 2: Filter incoming data based on version comparison
        filtered_df, to_process_count, skipped_count = filter_dataframe_by_version(
            df, existing_versions, id_column
        )

        if to_process_count == 0:
            logger.info(f"✅ All {total_records} records in {table_name} are up to date - no changes needed")
            return

        # Step 3: Get entities that need old version cleanup
        entities_to_delete = get_entities_to_delete(filtered_df, existing_versions, id_column)

        # Step 4: Build preactions for selective deletion
        selective_preactions = preactions
        if entities_to_delete:
            # Create DELETE statements for specific entity IDs
            entity_ids_str = "', '".join(entities_to_delete)
            delete_clause = f"DELETE FROM public.{table_name} WHERE {id_column} IN ('{entity_ids_str}');"

            if selective_preactions:
                selective_preactions = delete_clause + " " + selective_preactions
            else:
                selective_preactions = delete_clause

            logger.info(f"Will delete {len(entities_to_delete)} existing entities before inserting updated versions")

        # Step 5: Convert filtered DataFrame back to DynamicFrame
        filtered_dynamic_frame = DynamicFrame.fromDF(filtered_df, glueContext, f"filtered_{table_name}")

        # Step 6: Write only the new/updated records
        logger.info(f"Writing {to_process_count} new/updated records to {table_name}")

        glueContext.write_dynamic_frame.from_options(
            frame=filtered_dynamic_frame,
            connection_type="redshift",
            connection_options={
                "redshiftTmpDir": S3_TEMP_DIR,
                "useConnectionProperties": "true",
                "dbtable": f"public.{table_name}",
                "connectionName": REDSHIFT_CONNECTION,
                "preactions": selective_preactions or ""
            },
            transformation_ctx=f"write_{table_name}_versioned_to_redshift"
        )

        logger.info(f"✅ Successfully wrote {to_process_count} records to {table_name} in Redshift")
        logger.info(f"📊 Version summary: {to_process_count} processed, {skipped_count} skipped (same version)")

    except Exception as e:
        logger.error(f"❌ Failed to write {table_name} to Redshift with versioning: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        raise

def main():
    """Main ETL process"""
    start_time = datetime.now()

    try:
        logger.info("=" * 80)
        logger.info("🚀 STARTING ENHANCED FHIR PATIENT ETL PROCESS")
        logger.info("=" * 80)
        logger.info(f"⏰ Job started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📊 Source: {DATABASE_NAME}.{TABLE_NAME}")
        logger.info(f"🎯 Target: Redshift (8 tables)")
        logger.info("🔄 Process: 7 steps (Read → Transform → Convert → Resolve → Validate → Write)")
        
        # Step 1: Read data from S3 using Iceberg catalog
        logger.info("\n" + "=" * 50)
        logger.info("📥 STEP 1: READING DATA FROM S3 ICEBERG CATALOG")
        logger.info("=" * 50)
        logger.info(f"Database: {DATABASE_NAME}")
        logger.info(f"Table: {TABLE_NAME}")
        logger.info(f"Catalog: {catalog_nm}")
        
        # Use Iceberg to read patient data from S3
        table_name_full = f"{catalog_nm}.{DATABASE_NAME}.{TABLE_NAME}"
        logger.info(f"Reading from table: {table_name_full}")

        patient_df_raw = spark.table(table_name_full)
        
        print("=== PATIENT DATA PREVIEW (Iceberg) ===")
        patient_df_raw.show(5, truncate=False)  # Show 5 rows without truncating
        print(f"Total records: {patient_df_raw.count()}")
        print("Available columns:", patient_df_raw.columns)
        print("Schema:")
        patient_df_raw.printSchema()

        # Convert to DynamicFrame for compatibility with existing code
        # patient_dynamic_frame = DynamicFrame.fromDF(patient_df_raw, glueContext, "patient_dynamic_frame")

        # TESTING MODE: Sample data for quick testing
        # Set to True to process only a sample of records

        USE_SAMPLE = False  # Set to True for testing with limited data
        SAMPLE_SIZE = 1000

        if USE_SAMPLE:
            logger.info(f"⚠️  TESTING MODE: Sampling {SAMPLE_SIZE} records for quick testing")
            logger.info("⚠️  Set USE_SAMPLE = False for production runs")
            patient_df = patient_df_raw.limit(SAMPLE_SIZE)

        else:
            logger.info("✅ Processing full dataset")
            patient_df = patient_df_raw
            
     
        available_columns = patient_df_raw.columns
        logger.info(f"📋 Available columns in source: {available_columns}")
        
        # Use all available columns (don't filter based on COLUMNS_TO_READ)
        logger.info(f"✅ Using all {len(available_columns)} available columns")
        patient_df = patient_df_raw
        
        logger.info("✅ Successfully read data using S3 Iceberg Catalog")
        
        total_records = patient_df.count()
        logger.info(f"📊 Read {total_records:,} raw patient records")
        
        # Debug: Show sample of raw data and schema
        if total_records > 0:
            logger.info("\n🔍 DATA QUALITY CHECKS:")
            logger.info("Sample of raw patient data:")
            patient_df.show(3, truncate=False)
            logger.info("Raw data schema:")
            patient_df.printSchema()
            
            # Check for NULL values in key fields using efficient aggregation
            from pyspark.sql.functions import sum as spark_sum, when

            null_check_df = patient_df.agg(
                spark_sum(when(F.col("id").isNull(), 1).otherwise(0)).alias("id_nulls"),
                spark_sum(when(F.col("active").isNull(), 1).otherwise(0)).alias("active_nulls"),
                spark_sum(when(F.col("gender").isNull(), 1).otherwise(0)).alias("gender_nulls"),
                spark_sum(when(F.col("birthDate").isNull(), 1).otherwise(0)).alias("birthdate_nulls")
            ).collect()[0]

            logger.info("NULL value analysis in key fields:")
            for field, alias in [("id", "id_nulls"), ("active", "active_nulls"),
                               ("gender", "gender_nulls"), ("birthDate", "birthdate_nulls")]:
                null_count = null_check_df[alias] or 0
                percentage = (null_count / total_records) * 100 if total_records > 0 else 0
                logger.info(f"  {field}: {null_count:,} NULLs ({percentage:.1f}%)")
        else:
            logger.error("❌ No raw data found! Check the data source.")
            return
        
        # Step 2: Transform main patient data
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 2: TRANSFORMING MAIN PATIENT DATA")
        logger.info("=" * 50)
        
        main_patient_df = transform_main_patient_data(patient_df)
        main_count = main_patient_df.count()
        logger.info(f"✅ Transformed {main_count:,} main patient records")
        
        if main_count == 0:
            logger.error("❌ No main patient records after transformation! Check filtering criteria.")
            return
        
        # Debug: Show sample of transformed main data
        logger.info("Sample of transformed main patient data:")
        main_patient_df.show(3, truncate=False)
        
        # Step 3: Transform multi-valued data (all supporting tables)
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 3: TRANSFORMING MULTI-VALUED DATA")
        logger.info("=" * 50)
        
        patient_names_df = transform_patient_names(patient_df)
        names_count = patient_names_df.count()
        logger.info(f"✅ Transformed {names_count:,} patient name records")
        
        patient_telecoms_df = transform_patient_telecoms(patient_df)
        telecoms_count = patient_telecoms_df.count()
        logger.info(f"✅ Transformed {telecoms_count:,} telecom records")
        
        patient_addresses_df = transform_patient_addresses(patient_df)
        addresses_count = patient_addresses_df.count()
        logger.info(f"✅ Transformed {addresses_count:,} address records")
        
        patient_contacts_df = transform_patient_contacts(patient_df)
        contacts_count = patient_contacts_df.count()
        logger.info(f"✅ Transformed {contacts_count:,} contact records")
        
        patient_communications_df = transform_patient_communications(patient_df)
        communications_count = patient_communications_df.count()
        logger.info(f"✅ Transformed {communications_count:,} communication records")
        
        patient_practitioners_df = transform_patient_practitioners(patient_df)
        practitioners_count = patient_practitioners_df.count()
        logger.info(f"✅ Transformed {practitioners_count:,} practitioner records")
        
        patient_links_df = transform_patient_links(patient_df)
        links_count = patient_links_df.count()
        logger.info(f"✅ Transformed {links_count:,} link records")
        
        # Debug: Show samples of multi-valued data if available
        if names_count > 0:
            logger.info("Sample of patient names data:")
            patient_names_df.show(3, truncate=False)
        
        if telecoms_count > 0:
            logger.info("Sample of patient telecoms data:")
            patient_telecoms_df.show(3, truncate=False)
        
        if addresses_count > 0:
            logger.info("Sample of patient addresses data:")
            patient_addresses_df.show(3, truncate=False)
        
        if contacts_count > 0:
            logger.info("Sample of patient contacts data:")
            patient_contacts_df.show(3, truncate=False)
        
        if communications_count > 0:
            logger.info("Sample of patient communications data:")
            patient_communications_df.show(3, truncate=False)
        
        if practitioners_count > 0:
            logger.info("Sample of patient practitioners data:")
            patient_practitioners_df.show(3, truncate=False)
        
        if links_count > 0:
            logger.info("Sample of patient links data:")
            patient_links_df.show(3, truncate=False)
        
        # Step 4: Convert to DynamicFrames and ensure data is flat for Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 4: CONVERTING TO DYNAMICFRAMES")
        logger.info("=" * 50)
        logger.info("Converting to DynamicFrames and ensuring Redshift compatibility...")
        
        # Convert main patients DataFrame and ensure flat structure
        main_flat_df = main_patient_df.select(
            F.col("patient_id").cast(StringType()).alias("patient_id"),
            F.col("active").cast(BooleanType()).alias("active"),
            F.col("gender").cast(StringType()).alias("gender"),
            F.col("birth_date").cast(DateType()).alias("birth_date"),
            F.col("deceased").cast(BooleanType()).alias("deceased"),
            F.col("deceased_date").cast(TimestampType()).alias("deceased_date"),
            F.col("resourcetype").cast(StringType()).alias("resourcetype"),
            F.col("marital_status_code").cast(StringType()).alias("marital_status_code"),
            F.col("marital_status_display").cast(StringType()).alias("marital_status_display"),
            F.col("marital_status_system").cast(StringType()).alias("marital_status_system"),
            F.col("multiple_birth").cast(BooleanType()).alias("multiple_birth"),
            F.col("birth_order").cast(IntegerType()).alias("birth_order"),
            F.col("managing_organization_id").cast(StringType()).alias("managing_organization_id"),
            F.col("photos").cast(StringType()).alias("photos"),
            F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),
            F.col("meta_source").cast(StringType()).alias("meta_source"),
            F.col("meta_security").cast(StringType()).alias("meta_security"),
            F.col("meta_tag").cast(StringType()).alias("meta_tag"),
            F.col("extensions").cast(StringType()).alias("extensions"),
            F.col("created_at").cast(TimestampType()).alias("created_at"),
            F.col("updated_at").cast(TimestampType()).alias("updated_at")
        )
        
        main_dynamic_frame = DynamicFrame.fromDF(main_flat_df, glueContext, "main_patient_dynamic_frame")
        
        # Convert other DataFrames with type casting
        names_flat_df = patient_names_df.select(
            F.col("patient_id").cast(StringType()).alias("patient_id"),
            F.col("name_use").cast(StringType()).alias("name_use"),
            F.col("name_text").cast(StringType()).alias("name_text"),
            F.col("family_name").cast(StringType()).alias("family_name"),
            F.col("given_names").cast(StringType()).alias("given_names"),
            F.col("prefix").cast(StringType()).alias("prefix"),
            F.col("suffix").cast(StringType()).alias("suffix"),
            F.col("period_start").cast(DateType()).alias("period_start"),
            F.col("period_end").cast(DateType()).alias("period_end")
        )
        names_dynamic_frame = DynamicFrame.fromDF(names_flat_df, glueContext, "names_dynamic_frame")
        
        telecoms_flat_df = patient_telecoms_df.select(
            F.col("patient_id").cast(StringType()).alias("patient_id"),
            F.col("telecom_system").cast(StringType()).alias("telecom_system"),
            F.col("telecom_value").cast(StringType()).alias("telecom_value"),
            F.col("telecom_use").cast(StringType()).alias("telecom_use"),
            F.col("telecom_rank").cast(IntegerType()).alias("telecom_rank"),
            F.col("period_start").cast(DateType()).alias("period_start"),
            F.col("period_end").cast(DateType()).alias("period_end")
        )
        telecoms_dynamic_frame = DynamicFrame.fromDF(telecoms_flat_df, glueContext, "telecoms_dynamic_frame")
        
        addresses_flat_df = patient_addresses_df.select(
            F.col("patient_id").cast(StringType()).alias("patient_id"),
            F.col("address_use").cast(StringType()).alias("address_use"),
            F.col("address_type").cast(StringType()).alias("address_type"),
            F.col("address_text").cast(StringType()).alias("address_text"),
            F.col("address_line").cast(StringType()).alias("address_line"),
            F.col("city").cast(StringType()).alias("city"),
            F.col("district").cast(StringType()).alias("district"),
            F.col("state").cast(StringType()).alias("state"),
            F.col("postal_code").cast(StringType()).alias("postal_code"),
            F.col("country").cast(StringType()).alias("country"),
            F.col("period_start").cast(DateType()).alias("period_start"),
            F.col("period_end").cast(DateType()).alias("period_end")
        )
        addresses_dynamic_frame = DynamicFrame.fromDF(addresses_flat_df, glueContext, "addresses_dynamic_frame")
        
        contacts_flat_df = patient_contacts_df.select(
            F.col("patient_id").cast(StringType()).alias("patient_id"),
            F.col("contact_relationship_code").cast(StringType()).alias("contact_relationship_code"),
            F.col("contact_relationship_system").cast(StringType()).alias("contact_relationship_system"),
            F.col("contact_relationship_display").cast(StringType()).alias("contact_relationship_display"),
            F.col("contact_name_text").cast(StringType()).alias("contact_name_text"),
            F.col("contact_name_family").cast(StringType()).alias("contact_name_family"),
            F.col("contact_name_given").cast(StringType()).alias("contact_name_given"),
            F.col("contact_telecom_system").cast(StringType()).alias("contact_telecom_system"),
            F.col("contact_telecom_value").cast(StringType()).alias("contact_telecom_value"),
            F.col("contact_telecom_use").cast(StringType()).alias("contact_telecom_use"),
            F.col("contact_gender").cast(StringType()).alias("contact_gender"),
            F.col("contact_organization_id").cast(StringType()).alias("contact_organization_id"),
            F.col("period_start").cast(DateType()).alias("period_start"),
            F.col("period_end").cast(DateType()).alias("period_end")
        )
        contacts_dynamic_frame = DynamicFrame.fromDF(contacts_flat_df, glueContext, "contacts_dynamic_frame")
        
        communications_flat_df = patient_communications_df.select(
            F.col("patient_id").cast(StringType()).alias("patient_id"),
            F.col("language_code").cast(StringType()).alias("language_code"),
            F.col("language_system").cast(StringType()).alias("language_system"),
            F.col("language_display").cast(StringType()).alias("language_display"),
            F.col("preferred").cast(BooleanType()).alias("preferred"),
            F.col("extensions").cast(StringType()).alias("extensions")
        )
        communications_dynamic_frame = DynamicFrame.fromDF(communications_flat_df, glueContext, "communications_dynamic_frame")
        
        practitioners_flat_df = patient_practitioners_df.select(
            F.col("patient_id").cast(StringType()).alias("patient_id"),
            F.col("practitioner_id").cast(StringType()).alias("practitioner_id"),
            F.col("practitioner_role_id").cast(StringType()).alias("practitioner_role_id"),
            F.col("organization_id").cast(StringType()).alias("organization_id"),
            F.col("reference_type").cast(StringType()).alias("reference_type")
        )
        practitioners_dynamic_frame = DynamicFrame.fromDF(practitioners_flat_df, glueContext, "practitioners_dynamic_frame")
        
        links_flat_df = patient_links_df.select(
            F.col("patient_id").cast(StringType()).alias("patient_id"),
            F.col("other_patient_id").cast(StringType()).alias("other_patient_id"),
            F.col("link_type_code").cast(StringType()).alias("link_type_code"),
            F.col("link_type_system").cast(StringType()).alias("link_type_system"),
            F.col("link_type_display").cast(StringType()).alias("link_type_display")
        )
        links_dynamic_frame = DynamicFrame.fromDF(links_flat_df, glueContext, "links_dynamic_frame")
        
        # Step 5: Resolve any remaining choice types to ensure Redshift compatibility
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 5: RESOLVING CHOICE TYPES")
        logger.info("=" * 50)
        logger.info("Resolving choice types for Redshift compatibility with Glue 4 optimizations...")
        
        main_resolved_frame = main_dynamic_frame.resolveChoice(
            specs=[
                ("patient_id", "cast:string"),
                ("active", "cast:boolean"),
                ("gender", "cast:string"),
                ("birth_date", "cast:date"),
                ("deceased", "cast:boolean"),
                ("deceased_date", "cast:timestamp"),
                ("resourcetype", "cast:string"),
                ("marital_status_code", "cast:string"),
                ("marital_status_display", "cast:string"),
                ("marital_status_system", "cast:string"),
                ("multiple_birth", "cast:boolean"),
                ("birth_order", "cast:int"),
                ("managing_organization_id", "cast:string"),
                ("photos", "cast:string"),
                ("meta_last_updated", "cast:timestamp"),
                ("meta_source", "cast:string"),
                ("meta_security", "cast:string"),
                ("meta_tag", "cast:string"),
                ("extensions", "cast:string"),
                ("created_at", "cast:timestamp"),
                ("updated_at", "cast:timestamp")
            ]
        )
        
        names_resolved_frame = names_dynamic_frame.resolveChoice(
            specs=[
                ("patient_id", "cast:string"),
                ("name_use", "cast:string"),
                ("name_text", "cast:string"),
                ("family_name", "cast:string"),
                ("given_names", "cast:string"),
                ("prefix", "cast:string"),
                ("suffix", "cast:string"),
                ("period_start", "cast:date"),
                ("period_end", "cast:date")
            ]
        )
        
        telecoms_resolved_frame = telecoms_dynamic_frame.resolveChoice(
            specs=[
                ("patient_id", "cast:string"),
                ("telecom_system", "cast:string"),
                ("telecom_value", "cast:string"),
                ("telecom_use", "cast:string"),
                ("telecom_rank", "cast:int"),
                ("period_start", "cast:date"),
                ("period_end", "cast:date")
            ]
        )
        
        addresses_resolved_frame = addresses_dynamic_frame.resolveChoice(
            specs=[
                ("patient_id", "cast:string"),
                ("address_use", "cast:string"),
                ("address_type", "cast:string"),
                ("address_text", "cast:string"),
                ("address_line", "cast:string"),
                ("city", "cast:string"),
                ("district", "cast:string"),
                ("state", "cast:string"),
                ("postal_code", "cast:string"),
                ("country", "cast:string"),
                ("period_start", "cast:date"),
                ("period_end", "cast:date")
            ]
        )
        
        contacts_resolved_frame = contacts_dynamic_frame.resolveChoice(
            specs=[
                ("patient_id", "cast:string"),
                ("contact_relationship_code", "cast:string"),
                ("contact_relationship_system", "cast:string"),
                ("contact_relationship_display", "cast:string"),
                ("contact_name_text", "cast:string"),
                ("contact_name_family", "cast:string"),
                ("contact_name_given", "cast:string"),
                ("contact_telecom_system", "cast:string"),
                ("contact_telecom_value", "cast:string"),
                ("contact_telecom_use", "cast:string"),
                ("contact_gender", "cast:string"),
                ("contact_organization_id", "cast:string"),
                ("period_start", "cast:date"),
                ("period_end", "cast:date")
            ]
        )
        
        communications_resolved_frame = communications_dynamic_frame.resolveChoice(
            specs=[
                ("patient_id", "cast:string"),
                ("language_code", "cast:string"),
                ("language_system", "cast:string"),
                ("language_display", "cast:string"),
                ("preferred", "cast:boolean"),
                ("extensions", "cast:string")
            ]
        )
        
        practitioners_resolved_frame = practitioners_dynamic_frame.resolveChoice(
            specs=[
                ("patient_id", "cast:string"),
                ("practitioner_id", "cast:string"),
                ("practitioner_role_id", "cast:string"),
                ("organization_id", "cast:string"),
                ("reference_type", "cast:string")
            ]
        )
        
        links_resolved_frame = links_dynamic_frame.resolveChoice(
            specs=[
                ("patient_id", "cast:string"),
                ("other_patient_id", "cast:string"),
                ("link_type_code", "cast:string"),
                ("link_type_system", "cast:string"),
                ("link_type_display", "cast:string")
            ]
        )
        
        # Step 6: Final validation before writing
        logger.info("\n" + "=" * 50)
        logger.info("🔄 STEP 6: FINAL VALIDATION")
        logger.info("=" * 50)
        logger.info("Performing final validation before writing to Redshift...")
        
        # Validate main patients data
        main_final_df = main_resolved_frame.toDF()
        main_final_count = main_final_df.count()
        logger.info(f"Final main patients count: {main_final_count}")
        
        if main_final_count == 0:
            logger.error("No main patient records to write to Redshift! Stopping the process.")
            return
        
        # Validate other tables
        names_final_count = names_resolved_frame.toDF().count()
        telecoms_final_count = telecoms_resolved_frame.toDF().count()
        addresses_final_count = addresses_resolved_frame.toDF().count()
        contacts_final_count = contacts_resolved_frame.toDF().count()
        communications_final_count = communications_resolved_frame.toDF().count()
        practitioners_final_count = practitioners_resolved_frame.toDF().count()
        links_final_count = links_resolved_frame.toDF().count()
        
        logger.info(f"Final counts - Names: {names_final_count}, Telecoms: {telecoms_final_count}, Addresses: {addresses_final_count}, Contacts: {contacts_final_count}, Communications: {communications_final_count}, Practitioners: {practitioners_final_count}, Links: {links_final_count}")
        
        # Debug: Show final sample data being written
        logger.info("Final sample data being written to Redshift (main patients):")
        main_final_df.show(3, truncate=False)
        
        # Step 7: Create tables and write to Redshift
        logger.info("\n" + "=" * 50)
        logger.info("💾 STEP 7: WRITING DATA TO REDSHIFT")
        logger.info("=" * 50)
        logger.info(f"🔗 Using connection: {REDSHIFT_CONNECTION}")
        logger.info(f"📁 S3 temp directory: {S3_TEMP_DIR}")
        
        # Create all tables individually with version-aware processing
        # Note: Each write_to_redshift_versioned call only processes new/updated entities
        logger.info("📝 Creating main patients table with version checking...")
        patients_table_sql = create_redshift_tables_sql()
        write_to_redshift_versioned(main_resolved_frame, "patients", "patient_id", patients_table_sql)
        logger.info("✅ Main patients table created and written successfully")

        logger.info("📝 Creating patient names table with version checking...")
        names_table_sql = create_patient_names_table_sql()
        write_to_redshift_versioned(names_resolved_frame, "patient_names", "patient_id", names_table_sql)
        logger.info("✅ Patient names table created and written successfully")

        logger.info("📝 Creating patient telecoms table with version checking...")
        telecoms_table_sql = create_patient_telecoms_table_sql()
        write_to_redshift_versioned(telecoms_resolved_frame, "patient_telecoms", "patient_id", telecoms_table_sql)
        logger.info("✅ Patient telecoms table created and written successfully")

        logger.info("📝 Creating patient addresses table with version checking...")
        addresses_table_sql = create_patient_addresses_table_sql()
        write_to_redshift_versioned(addresses_resolved_frame, "patient_addresses", "patient_id", addresses_table_sql)
        logger.info("✅ Patient addresses table created and written successfully")

        logger.info("📝 Creating patient contacts table with version checking...")
        contacts_table_sql = create_patient_contacts_table_sql()
        write_to_redshift_versioned(contacts_resolved_frame, "patient_contacts", "patient_id", contacts_table_sql)
        logger.info("✅ Patient contacts table created and written successfully")

        logger.info("📝 Creating patient communications table with version checking...")
        communications_table_sql = create_patient_communications_table_sql()
        write_to_redshift_versioned(communications_resolved_frame, "patient_communications", "patient_id", communications_table_sql)
        logger.info("✅ Patient communications table created and written successfully")

        logger.info("📝 Creating patient practitioners table with version checking...")
        practitioners_table_sql = create_patient_practitioners_table_sql()
        write_to_redshift_versioned(practitioners_resolved_frame, "patient_practitioners", "patient_id", practitioners_table_sql)
        logger.info("✅ Patient practitioners table created and written successfully")

        logger.info("📝 Creating patient links table with version checking...")
        links_table_sql = create_patient_links_table_sql()
        write_to_redshift_versioned(links_resolved_frame, "patient_links", "patient_id", links_table_sql)
        logger.info("✅ Patient links table created and written successfully")
        
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
        logger.info("  ✅ public.patients (main patient data)")
        logger.info("  ✅ public.patient_names (patient names)")
        logger.info("  ✅ public.patient_telecoms (phone, email, etc.)")
        logger.info("  ✅ public.patient_addresses (patient addresses)")
        logger.info("  ✅ public.patient_contacts (emergency contacts)")
        logger.info("  ✅ public.patient_communications (language preferences)")
        logger.info("  ✅ public.patient_practitioners (general practitioners)")
        logger.info("  ✅ public.patient_links (patient links)")
        
        logger.info("\n📊 FINAL ETL STATISTICS:")
        logger.info(f"  📥 Total raw records processed: {total_records:,}")
        logger.info(f"  👤 Main patient records: {main_count:,}")
        logger.info(f"  📝 Name records: {names_count:,}")
        logger.info(f"  📞 Telecom records: {telecoms_count:,}")
        logger.info(f"  🏠 Address records: {addresses_count:,}")
        logger.info(f"  👥 Contact records: {contacts_count:,}")
        logger.info(f"  🗣️  Communication records: {communications_count:,}")
        logger.info(f"  👨‍⚕️ Practitioner records: {practitioners_count:,}")
        logger.info(f"  🔗 Link records: {links_count:,}")
        
        # Calculate data expansion ratio
        total_output_records = main_count + names_count + telecoms_count + addresses_count + contacts_count + communications_count + practitioners_count + links_count
        expansion_ratio = total_output_records / total_records if total_records > 0 else 0
        logger.info(f"  📈 Data expansion ratio: {expansion_ratio:.2f}x (output records / input records)")
        
        logger.info("\n" + "=" * 80)
        
        if USE_SAMPLE:
            logger.info("⚠️  WARNING: THIS WAS A TEST RUN WITH SAMPLED DATA")
            logger.info(f"⚠️  Only {SAMPLE_SIZE} records were processed")
            logger.info("⚠️  Set USE_SAMPLE = False for production runs")
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
        logger.error(f"🚨 Error type: {type(e).__name__}")
        logger.error("=" * 80)
        raise

if __name__ == "__main__":
    main()
    job.commit()
