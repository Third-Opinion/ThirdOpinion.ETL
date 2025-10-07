#!/usr/bin/env python3
"""
Check what the code field looks like in the Glue table
"""
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

# Initialize Spark/Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Configuration
catalog_nm = "glue_catalog"
s3_bucket = "s3://7df690fd40c734f8937daf02f39b2ec3-457560472834-group/datalake/hmu_fhir_data_store_836e877666cebf177ce6370ec1478a92_healthlake_view/"
ahl_database = "hmu_fhir_data_store_836e877666cebf177ce6370ec1478a92_healthlake_view"
tableCatalogId = "457560472834"

# Load condition data
print("Loading condition data...")
condition_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=ahl_database,
    table_name="condition",
    additional_options={"catalogId": tableCatalogId}
)

condition_df = condition_dynamic_frame.toDF()
print(f"Total records: {condition_df.count()}")

# Check the schema
print("\nSchema:")
condition_df.printSchema()

# Check code column data type
print("\n" + "=" * 80)
print("CODE COLUMN ANALYSIS")
print("=" * 80)

code_dtype = str(condition_df.schema["code"].dataType)
print(f"Code column data type: {code_dtype}")

# Sample some code values
print("\nSample code values (first 5):")
condition_df.select("id", "code").show(5, truncate=False)

# Check if code.coding exists and has data
if "StructType" in code_dtype:
    print("\nCode is a struct - checking coding array...")
    condition_df.select(
        "id",
        "code.coding"
    ).show(5, truncate=False)

    # Count rows with non-null coding arrays
    from pyspark.sql import functions as F
    coding_stats = condition_df.select(
        F.count("*").alias("total"),
        F.count(F.col("code")).alias("code_not_null"),
        F.count(F.col("code.coding")).alias("coding_not_null"),
        F.sum(F.when(F.size(F.col("code.coding")) > 0, 1).otherwise(0)).alias("coding_has_elements")
    ).collect()[0]

    print(f"\nCoding statistics:")
    print(f"  Total rows: {coding_stats['total']}")
    print(f"  Rows with non-null code: {coding_stats['code_not_null']}")
    print(f"  Rows with non-null coding: {coding_stats['coding_not_null']}")
    print(f"  Rows with coding elements: {coding_stats['coding_has_elements']}")

print("\n" + "=" * 80)
print("META COLUMN ANALYSIS")
print("=" * 80)

meta_dtype = str(condition_df.schema["meta"].dataType)
print(f"Meta column data type: {meta_dtype}")

condition_df.select("id", "meta").show(5, truncate=False)

if "StructType" in meta_dtype:
    condition_df.select("id", "meta.lastUpdated").show(5, truncate=False)
