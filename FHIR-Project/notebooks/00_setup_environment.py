# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.types import *
import json

# COMMAND ----------

# Load configuration
with open("/Volumes/workspace/default/fhir_lakehouse/config/config.json", "r") as f:
    config = json.load(f)

DATABASE = config["storage"]["database_name"]
VOLUME_PATH = "/Volumes/workspace/default/fhir_lakehouse"

print(f"Database: {DATABASE}")
print(f"Volume: {VOLUME_PATH}")

# COMMAND ----------

# Create database
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABASE}")
spark.sql(f"USE {DATABASE}")
print(f"✓ Database created: {DATABASE}")

# COMMAND ----------

# Drop existing tables if they exist (to avoid schema conflicts)
tables_to_drop = [
    "bronze_patient", "bronze_encounter", "bronze_observation", "bronze_condition",
    "silver_patient", "silver_encounter", "silver_observation", "silver_condition",
    "pipeline_metadata_log"
]

for table in tables_to_drop:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {DATABASE}.{table}")
    except:
        pass

print("✓ Existing tables dropped")

# COMMAND ----------

# Create folders
for folder in ["raw", "bronze", "silver", "gold"]:
    try:
        dbutils.fs.mkdirs(f"{VOLUME_PATH}/{folder}")
    except:
        pass

for resource in ["patient", "encounter", "observation", "condition"]:
    for layer in ["raw", "bronze", "silver"]:
        try:
            dbutils.fs.mkdirs(f"{VOLUME_PATH}/{layer}/{resource}")
        except:
            pass

print("✓ Folders created")

# COMMAND ----------

# Bronze Patient
bronze_patient_schema = StructType([
    StructField("id", StringType(), False),
    StructField("resource_type", StringType(), True),
    StructField("name_family", StringType(), True),
    StructField("name_given", ArrayType(StringType()), True),
    StructField("gender", StringType(), True),
    StructField("birth_date", StringType(), True),
    StructField("address_city", StringType(), True),
    StructField("address_state", StringType(), True),
    StructField("address_country", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("extraction_timestamp", TimestampType(), True),
    StructField("api_url", StringType(), True),
    StructField("ingestion_date", StringType(), True),
])

spark.createDataFrame([], bronze_patient_schema) \
    .write.format("delta").mode("overwrite") \
    .saveAsTable(f"{DATABASE}.bronze_patient")

print("✓ bronze_patient")

# COMMAND ----------

# Bronze Encounter
bronze_encounter_schema = StructType([
    StructField("id", StringType(), False),
    StructField("resource_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("class_code", StringType(), True),
    StructField("class_display", StringType(), True),
    StructField("subject_reference", StringType(), True),
    StructField("period_start", TimestampType(), True),
    StructField("period_end", TimestampType(), True),
    StructField("raw_json", StringType(), True),
    StructField("extraction_timestamp", TimestampType(), True),
    StructField("api_url", StringType(), True),
    StructField("ingestion_date", StringType(), True),
])

spark.createDataFrame([], bronze_encounter_schema) \
    .write.format("delta").mode("overwrite") \
    .saveAsTable(f"{DATABASE}.bronze_encounter")

print("✓ bronze_encounter")

# COMMAND ----------

# Bronze Observation
bronze_observation_schema = StructType([
    StructField("id", StringType(), False),
    StructField("resource_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("code_code", StringType(), True),
    StructField("code_display", StringType(), True),
    StructField("subject_reference", StringType(), True),
    StructField("effective_datetime", TimestampType(), True),
    StructField("value_quantity", StringType(), True),
    StructField("value_unit", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("extraction_timestamp", TimestampType(), True),
    StructField("api_url", StringType(), True),
    StructField("ingestion_date", StringType(), True),
])

spark.createDataFrame([], bronze_observation_schema) \
    .write.format("delta").mode("overwrite") \
    .saveAsTable(f"{DATABASE}.bronze_observation")

print("✓ bronze_observation")

# COMMAND ----------

# Bronze Condition
bronze_condition_schema = StructType([
    StructField("id", StringType(), False),
    StructField("resource_type", StringType(), True),
    StructField("clinical_status", StringType(), True),
    StructField("code_code", StringType(), True),
    StructField("code_display", StringType(), True),
    StructField("subject_reference", StringType(), True),
    StructField("onset_datetime", TimestampType(), True),
    StructField("raw_json", StringType(), True),
    StructField("extraction_timestamp", TimestampType(), True),
    StructField("api_url", StringType(), True),
    StructField("ingestion_date", StringType(), True),
])

spark.createDataFrame([], bronze_condition_schema) \
    .write.format("delta").mode("overwrite") \
    .saveAsTable(f"{DATABASE}.bronze_condition")

print("✓ bronze_condition")

# COMMAND ----------

# Silver Patient
silver_patient_schema = StructType(
    list(bronze_patient_schema.fields) + [
        StructField("record_hash", StringType(), True),
        StructField("is_current", BooleanType(), True),
        StructField("valid_from", DateType(), True),
        StructField("valid_to", DateType(), True)
    ]
)

spark.createDataFrame([], silver_patient_schema) \
    .write.format("delta").mode("overwrite") \
    .saveAsTable(f"{DATABASE}.silver_patient")

print("✓ silver_patient")

# COMMAND ----------

# Silver Encounter
silver_encounter_schema = StructType(
    list(bronze_encounter_schema.fields) + [
        StructField("record_hash", StringType(), True),
        StructField("is_current", BooleanType(), True),
        StructField("valid_from", DateType(), True),
        StructField("valid_to", DateType(), True)
    ]
)

spark.createDataFrame([], silver_encounter_schema) \
    .write.format("delta").mode("overwrite") \
    .saveAsTable(f"{DATABASE}.silver_encounter")

print("✓ silver_encounter")

# COMMAND ----------

# Silver Observation
silver_observation_schema = StructType(
    list(bronze_observation_schema.fields) + [
        StructField("record_hash", StringType(), True),
        StructField("is_current", BooleanType(), True),
        StructField("valid_from", DateType(), True),
        StructField("valid_to", DateType(), True)
    ]
)

spark.createDataFrame([], silver_observation_schema) \
    .write.format("delta").mode("overwrite") \
    .saveAsTable(f"{DATABASE}.silver_observation")

print("✓ silver_observation")

# COMMAND ----------

# Silver Condition
silver_condition_schema = StructType(
    list(bronze_condition_schema.fields) + [
        StructField("record_hash", StringType(), True),
        StructField("is_current", BooleanType(), True),
        StructField("valid_from", DateType(), True),
        StructField("valid_to", DateType(), True)
    ]
)

spark.createDataFrame([], silver_condition_schema) \
    .write.format("delta").mode("overwrite") \
    .saveAsTable(f"{DATABASE}.silver_condition")

print("✓ silver_condition")

# COMMAND ----------

# Metadata log
metadata_schema = StructType([
    StructField("log_id", StringType(), True),
    StructField("pipeline", StringType(), True),
    StructField("resource", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("record_count", IntegerType(), True)
])

spark.createDataFrame([], metadata_schema) \
    .write.format("delta").mode("overwrite") \
    .saveAsTable(f"{DATABASE}.pipeline_metadata_log")

print("✓ pipeline_metadata_log")

# COMMAND ----------

print("""
╔═══════════════════════════════════════╗
║  SETUP COMPLETED ✓                    ║
╠═══════════════════════════════════════╣
║  Database: fhir_lakehouse             ║
║  Bronze tables: 4                     ║
║  Silver tables: 4                     ║
║  Metadata table: 1                    ║
║                                       ║
║  Next: Run 01_raw_ingestion           ║
╚═══════════════════════════════════════╝
""")

spark.sql(f"SHOW TABLES IN '{DATABASE}'").show(truncate=False)