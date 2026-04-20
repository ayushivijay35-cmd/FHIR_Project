# Databricks notebook source
DATABASE = "fhir_lakehouse"
spark.sql(f"USE {DATABASE}")

spark.sql(f"""
CREATE OR REPLACE VIEW {DATABASE}.gold_patient_demographics AS
SELECT 
    id as patient_id,
    name_family,
    name_given[0] as first_name,
    gender,
    birth_date,
    YEAR(CURRENT_DATE()) - YEAR(TO_DATE(birth_date)) as age,
    address_city,
    address_state,
    address_country
FROM {DATABASE}.silver_patient
WHERE is_current = TRUE
""")

print("✓ gold_patient_demographics")

# COMMAND ----------

# Gold Encounter Summary
spark.sql(f"""
CREATE OR REPLACE VIEW {DATABASE}.gold_encounter_summary AS
SELECT 
    e.id as encounter_id,
    e.status,
    e.class_code,
    e.class_display,
    e.subject_reference,
    SPLIT(e.subject_reference, '/')[1] as patient_id,
    e.period_start,
    e.period_end,
    DATEDIFF(e.period_end, e.period_start) as duration_days
FROM {DATABASE}.silver_encounter e
WHERE e.is_current = TRUE
""")

print("✓ gold_encounter_summary")

# COMMAND ----------

# Gold Patient Encounter Facts
spark.sql(f"""
CREATE OR REPLACE VIEW {DATABASE}.gold_patient_encounter_facts AS
SELECT 
    p.id as patient_id,
    p.name_family,
    p.gender,
    p.birth_date,
    COUNT(DISTINCT e.id) as total_encounters,
    MIN(e.period_start) as first_encounter,
    MAX(e.period_start) as last_encounter
FROM {DATABASE}.silver_patient p
LEFT JOIN {DATABASE}.silver_encounter e 
    ON CONCAT('Patient/', p.id) = e.subject_reference
WHERE p.is_current = TRUE
GROUP BY p.id, p.name_family, p.gender, p.birth_date
""")

print("✓ gold_patient_encounter_facts")

# COMMAND ----------

# Gold Observation Summary
spark.sql(f"""
CREATE OR REPLACE VIEW {DATABASE}.gold_observation_summary AS
SELECT 
    code_code,
    code_display,
    COUNT(*) as observation_count,
    COUNT(DISTINCT subject_reference) as unique_patients,
    AVG(CAST(value_quantity AS DOUBLE)) as avg_value,
    MIN(CAST(value_quantity AS DOUBLE)) as min_value,
    MAX(CAST(value_quantity AS DOUBLE)) as max_value,
    value_unit
FROM {DATABASE}.silver_observation
WHERE is_current = TRUE AND value_quantity IS NOT NULL
GROUP BY code_code, code_display, value_unit
ORDER BY observation_count DESC
""")

print("✓ gold_observation_summary")

# COMMAND ----------

# Gold Condition Analysis
spark.sql(f"""
CREATE OR REPLACE VIEW {DATABASE}.gold_condition_analysis AS
SELECT 
    code_code,
    code_display,
    clinical_status,
    COUNT(*) as condition_count,
    COUNT(DISTINCT subject_reference) as unique_patients
FROM {DATABASE}.silver_condition
WHERE is_current = TRUE
GROUP BY code_code, code_display, clinical_status
ORDER BY condition_count DESC
""")

print(" gold_condition_analysis")

print(" Gold layer analytics views completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Patient demographics
# MAGIC SELECT * FROM fhir_lakehouse.gold_patient_demographics LIMIT 10;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Encounter summary
# MAGIC SELECT * FROM fhir_lakehouse.gold_encounter_summary LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Patient encounter facts (joined data)
# MAGIC SELECT * FROM fhir_lakehouse.gold_patient_encounter_facts LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top observations
# MAGIC SELECT * FROM fhir_lakehouse.gold_observation_summary LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top conditions
# MAGIC SELECT * FROM fhir_lakehouse.gold_condition_analysis LIMIT 10;