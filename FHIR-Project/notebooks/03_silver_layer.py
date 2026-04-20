# Databricks notebook source
import sys
sys.path.append("/Workspace/Users/vijayaayushi1@gmail.com/FHIR-Project/utils")

from utils_scd_handler import SCDType2Handler
from pyspark.sql.functions import col

DATABASE = "fhir_lakehouse"
spark.sql(f"USE {DATABASE}")

scd_handler = SCDType2Handler(spark)

# COMMAND ----------

# Process Patient with SCD Type 2
bronze_patient = spark.table(f"{DATABASE}.bronze_patient")

stats = scd_handler.merge_scd_type2(
    target_table="silver_patient",
    source_df=bronze_patient,
    key_columns=["id"],
    hash_columns=["name_family", "gender", "birth_date", "address_city", "address_state"],
    database=DATABASE
)

print(f"✓ silver_patient: {stats}")

# COMMAND ----------

# Process Encounter with SCD Type 2
bronze_encounter = spark.table(f"{DATABASE}.bronze_encounter")

stats = scd_handler.merge_scd_type2(
    target_table="silver_encounter",
    source_df=bronze_encounter,
    key_columns=["id"],
    hash_columns=["status", "class_code", "subject_reference", "period_start", "period_end"],
    database=DATABASE
)

print(f"✓ silver_encounter: {stats}")

# COMMAND ----------

# Process Observation with SCD Type 2
bronze_observation = spark.table(f"{DATABASE}.bronze_observation")

stats = scd_handler.merge_scd_type2(
    target_table="silver_observation",
    source_df=bronze_observation,
    key_columns=["id"],
    hash_columns=["status", "code_code", "subject_reference", "value_quantity", "value_unit"],
    database=DATABASE
)

print(f"✓ silver_observation: {stats}")

# COMMAND ----------

# Process Condition with SCD Type 2
bronze_condition = spark.table(f"{DATABASE}.bronze_condition")

stats = scd_handler.merge_scd_type2(
    target_table="silver_condition",
    source_df=bronze_condition,
    key_columns=["id"],
    hash_columns=["clinical_status", "code_code", "subject_reference", "onset_datetime"],
    database=DATABASE
)

print(f"✓ silver_condition: {stats}")

# COMMAND ----------

print(" Silver layer with SCD Type 2 completed")

# COMMAND ----------

#for testing scd type 2 is working
# Adding this as a new cell in 03_silver_layer notebook
from pyspark.sql.functions import when, col
bronze_patient_updated = spark.table("fhir_lakehouse.bronze_patient") \
    .withColumn("gender", when(col("id") == bronze_patient.select("id").first()[0], "male").otherwise(col("gender"))) \
    .withColumn("address_city", when(col("id") == bronze_patient.select("id").first()[0], "New York").otherwise(col("address_city")))
stats = scd_handler.merge_scd_type2(
    target_table="silver_patient",
    source_df=bronze_patient_updated,
    key_columns=["id"],
    hash_columns=["name_family", "gender", "birth_date", "address_city", "address_state"],
    database="fhir_lakehouse"
)

print(f"Second run stats: {stats}")