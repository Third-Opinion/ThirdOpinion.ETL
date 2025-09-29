import sys 
from awsglue.transforms import * 
from awsglue.utils import getResolvedOptions 
from pyspark.sql import SparkSession 
from pyspark.context import SparkContext 
from awsglue.context import GlueContext 
from awsglue.job import Job 
from awsglue import DynamicFrame 

args = getResolvedOptions(sys.argv, ["JOB_NAME"]) 

catalog_nm = "glue_catalog" 
s3_bucket = "s3://7df690fd40c734f8937daf02f39b2ec3-457560472834-group/datalake/hmu_fhir_data_store_836e877666cebf177ce6370ec1478a92_healthlake_view/" 
ahl_database = "hmu_fhir_data_store_836e877666cebf177ce6370ec1478a92_healthlake_view" 
tableCatalogId = "457560472834" #AHL service account 
s3_output_bucket = "s3://healthlake-glue-output-2025" 

spark = SparkSession.builder.config(f"spark.sql.extensions" 
"org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions").config(f"spark.sql.catalog.{catalog_nm}",  
"org.apache.iceberg.spark.SparkCatalog").config(f"spark.sql.catalog.{catalog_nm}.warehouse", \ s3_bucket).config(f"spark.sql.catalog.{catalog_nm}.catalog-impl", \ "org.apache.iceberg.aws.glue.GlueCatalog").config(f"spark.sql.catalog.{catalog_nm}.io-impl", \ "org.apache.iceberg.aws.s3.S3FileIO").config(f"spark.sql.catalog.glue_catalog.glue.lakeformation-enabled", \ "true")  .config(f"spark.sql.catalog.glue_catalog.glue.id", \ f"{tableCatalogId}").getOrCreate() 

sc = spark.sparkContext 
glueContext = GlueContext(sc) 
spark = glueContext.spark_session 
job = Job(glueContext) 
job.init(args["JOB_NAME"], args) 

# Script generated for node AWS Glue Data Catalog AWSGlueDataCatalog_node1707491436959_df = glueContext.create_data_frame_from_catalog( 
database=ahl_database, 
table_name="patient", 
) 
AWSGlueDataCatalog_node1707491436959 = DynamicFrame.fromDF( 
AWSGlueDataCatalog_node1707491436959_df, 
glueContext, 
"AWSGlueDataCatalog_node1707491436959", 
) 

# Script generated for node Change Schema 
ChangeSchema_node1707491584061 = ApplyMapping.apply( 
frame=AWSGlueDataCatalog_node1707491436959, 
mappings=[ 
("resourcetype", "string", "resourcetype", "string"), 
("id", "string", "id", "string"), 
("meta.id", "string", "meta.id", "string"), 
("meta.extension", "array", "meta.extension", "array"), 
("meta.versionId", "string", "meta.versionId", "string"), 
("meta._versionId.id", "string", "meta._versionId.id", "string"), 
("meta._versionId.extension", "array", "meta._versionId.extension", "array"), 
("meta.lastUpdated", "string", "meta.lastUpdated", "string"), 
("meta._lastUpdated.id", "string", "meta._lastUpdated.id", "string"), 
( 
"meta._lastUpdated.extension", 
"array", 
"meta._lastUpdated.extension", 
"array", 
), 
("meta.source", "string", "meta.source", "string"), 
("meta._source.id", "string", "meta._source.id", "string"), 
("meta._source.extension", "array", "meta._source.extension", "array"), 
("meta.profile", "array", "meta.profile", "array"), 
("meta.security", "array", "meta.security", "array"), 
("meta.tag", "array", "meta.tag", "array"), 
("extension", "array", "extension", "array"), 
("modifierextension", "array", "modifierextension", "array"), 
("active", "boolean", "active", "boolean"), 
("_active.id", "string", "_active.id", "string"), 
("_active.extension", "array", "_active.extension", "array"), 
("gender", "string", "gender", "string"), 
("_gender.id", "string", "_gender.id", "string"), 
("_gender.extension", "array", "_gender.extension", "array"), 
("_birthdate.id", "string", "_birthdate.id", "string"), 
("_birthdate.extension", "array", "_birthdate.extension", "array"), 
("deceasedboolean", "boolean", "deceasedboolean", "boolean"), 
("_deceasedboolean.id", "string", "_deceasedboolean.id", "string"), 
("_deceasedboolean.extension", "array", "_deceasedboolean.extension", "array"), 
("deceaseddatetime", "string", "deceaseddatetime", "string"), 
("_deceaseddatetime.id", "string", "_deceaseddatetime.id", "string"), 
( 
"_deceaseddatetime.extension", 
"array", 
"_deceaseddatetime.extension", 
"array", 
), 
("maritalstatus.id", "string", "maritalstatus.id", "string"), 
("maritalstatus.extension", "array", "maritalstatus.extension", "array"), 
("maritalstatus.coding", "array", "maritalstatus.coding", "array"), 
("maritalstatus.text", "string", "maritalstatus.text", "string"), 
("maritalstatus._text.id", "string", "maritalstatus._text.id", "string"), 
( 
"maritalstatus._text.extension", 
"array", 
"maritalstatus._text.extension", 
"array", 
), 
("multiplebirthboolean", "boolean", "multiplebirthboolean", "boolean"), 
("_multiplebirthboolean.id", "string", "_multiplebirthboolean.id", "string"), 
( 
"_multiplebirthboolean.extension", 
"array", 
"_multiplebirthboolean.extension", 
"array", 
), 
("multiplebirthinteger", "decimal", "multiplebirthinteger", "decimal"), 
("_multiplebirthinteger.id", "string", "_multiplebirthinteger.id", "string"), 
( 
"_multiplebirthinteger.extension", 
"array", 
"_multiplebirthinteger.extension", 
"array", 
), 
("link", "array", "link", "array"), 
], 
transformation_ctx="ChangeSchema_node1707491584061", 
) 

# Script generated for node Amazon S3 
AmazonS3_node1707491718060 = glueContext.write_dynamic_frame.from_options( 
frame=ChangeSchema_node1707491584061, 
connection_type="s3", 
format="json", 
connection_options={"path": s3_output_bucket, "partitionKeys": []}, 
transformation_ctx="AmazonS3_node1707491718060", 
) 

job.commit()
