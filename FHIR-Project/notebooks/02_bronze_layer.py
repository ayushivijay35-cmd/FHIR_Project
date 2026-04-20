# Databricks notebook source
import json
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration
VOLUME_PATH = "/Volumes/workspace/default/fhir_lakehouse"
DATABASE = "fhir_lakehouse"

spark.sql(f"USE {DATABASE}")
def parse_timestamp(ts_string):
    """Convert FHIR datetime string to timestamp"""
    if not ts_string:
        return None
    try:
        # Handle different FHIR datetime formats
        if 'T' in ts_string:
            return datetime.fromisoformat(ts_string.replace('Z', '+00:00'))
        else:
            return datetime.strptime(ts_string, '%Y-%m-%d')
    except:
        return None

patient_path = f"{VOLUME_PATH}/raw/patient"
patient_files = dbutils.fs.ls(patient_path)
patient_records = []

for date_folder in patient_files:
    if date_folder.isDir():
        json_files = dbutils.fs.ls(date_folder.path)
        ingestion_date = date_folder.name.rstrip('/')
        
        for json_file in json_files:
            if json_file.name.endswith('.json'):
                content = dbutils.fs.head(json_file.path, 10000000)
                data = json.loads(content)
                
                if 'entry' in data:
                    for entry in data['entry']:
                        resource = entry.get('resource', {})
                        if resource.get('resourceType') == 'Patient':
                            names = resource.get('name', [{}])
                            first_name = names[0] if names else {}
                            addresses = resource.get('address', [{}])
                            address = addresses[0] if addresses else {}
                            
                            patient_records.append({
                                'id': resource.get('id'),
                                'resource_type': resource.get('resourceType'),
                                'name_family': first_name.get('family'),
                                'name_given': first_name.get('given', []),
                                'gender': resource.get('gender'),
                                'birth_date': resource.get('birthDate'),
                                'address_city': address.get('city'),
                                'address_state': address.get('state'),
                                'address_country': address.get('country'),
                                'raw_json': json.dumps(resource),
                                'extraction_timestamp': datetime.now(),
                                'api_url': 'https://hapi.fhir.org/baseR4/Patient',
                                'ingestion_date': ingestion_date
                            })

if patient_records:
    patient_df = spark.createDataFrame([Row(**r) for r in patient_records])
    patient_df.write.format("delta").mode("overwrite").saveAsTable(f"{DATABASE}.bronze_patient")
    print(f"✓ bronze_patient: {patient_df.count()} records")


# Process Encounter
encounter_path = f"{VOLUME_PATH}/raw/encounter"
encounter_files = dbutils.fs.ls(encounter_path)
encounter_records = []

for date_folder in encounter_files:
    if date_folder.isDir():
        json_files = dbutils.fs.ls(date_folder.path)
        ingestion_date = date_folder.name.rstrip('/')
        
        for json_file in json_files:
            if json_file.name.endswith('.json'):
                content = dbutils.fs.head(json_file.path, 10000000)
                data = json.loads(content)
                
                if 'entry' in data:
                    for entry in data['entry']:
                        resource = entry.get('resource', {})
                        if resource.get('resourceType') == 'Encounter':
                            class_info = resource.get('class', {})
                            period = resource.get('period', {})
                            subject = resource.get('subject', {})
                            
                            encounter_records.append({
                                'id': resource.get('id'),
                                'resource_type': resource.get('resourceType'),
                                'status': resource.get('status'),
                                'class_code': class_info.get('code'),
                                'class_display': class_info.get('display'),
                                'subject_reference': subject.get('reference'),
                                'period_start': parse_timestamp(period.get('start')),
                                'period_end': parse_timestamp(period.get('end')),
                                'raw_json': json.dumps(resource),
                                'extraction_timestamp': datetime.now(),
                                'api_url': 'https://hapi.fhir.org/baseR4/Encounter',
                                'ingestion_date': ingestion_date
                            })

if encounter_records:
    encounter_df = spark.createDataFrame([Row(**r) for r in encounter_records])
    encounter_df.write.format("delta").mode("overwrite").saveAsTable(f"{DATABASE}.bronze_encounter")
    print(f"✓ bronze_encounter: {encounter_df.count()} records")

# Process Observation
observation_path = f"{VOLUME_PATH}/raw/observation"
observation_files = dbutils.fs.ls(observation_path)
observation_records = []

for date_folder in observation_files:
    if date_folder.isDir():
        json_files = dbutils.fs.ls(date_folder.path)
        ingestion_date = date_folder.name.rstrip('/')
        
        for json_file in json_files:
            if json_file.name.endswith('.json'):
                content = dbutils.fs.head(json_file.path, 10000000)
                data = json.loads(content)
                
                if 'entry' in data:
                    for entry in data['entry']:
                        resource = entry.get('resource', {})
                        if resource.get('resourceType') == 'Observation':
                            code = resource.get('code', {})
                            codings = code.get('coding', [{}])
                            code_info = codings[0] if codings else {}
                            subject = resource.get('subject', {})
                            value = resource.get('valueQuantity', {})
                            
                            observation_records.append({
                                'id': resource.get('id'),
                                'resource_type': resource.get('resourceType'),
                                'status': resource.get('status'),
                                'code_code': code_info.get('code'),
                                'code_display': code_info.get('display'),
                                'subject_reference': subject.get('reference'),
                                'effective_datetime': parse_timestamp(resource.get('effectiveDateTime')),
                                'value_quantity': str(value.get('value')) if value.get('value') else None,
                                'value_unit': value.get('unit'),
                                'raw_json': json.dumps(resource),
                                'extraction_timestamp': datetime.now(),
                                'api_url': 'https://hapi.fhir.org/baseR4/Observation',
                                'ingestion_date': ingestion_date
                            })

if observation_records:
    observation_df = spark.createDataFrame([Row(**r) for r in observation_records])
    observation_df.write.format("delta").mode("overwrite").saveAsTable(f"{DATABASE}.bronze_observation")
    print(f"✓ bronze_observation: {observation_df.count()} records")


# Process Condition
condition_path = f"{VOLUME_PATH}/raw/condition"
condition_files = dbutils.fs.ls(condition_path)
condition_records = []

for date_folder in condition_files:
    if date_folder.isDir():
        json_files = dbutils.fs.ls(date_folder.path)
        ingestion_date = date_folder.name.rstrip('/')
        
        for json_file in json_files:
            if json_file.name.endswith('.json'):
                content = dbutils.fs.head(json_file.path, 10000000)
                data = json.loads(content)
                
                if 'entry' in data:
                    for entry in data['entry']:
                        resource = entry.get('resource', {})
                        if resource.get('resourceType') == 'Condition':
                            clinical_status = resource.get('clinicalStatus', {})
                            codings = clinical_status.get('coding', [{}])
                            status_code = codings[0].get('code') if codings else None
                            
                            code = resource.get('code', {})
                            code_codings = code.get('coding', [{}])
                            code_info = code_codings[0] if code_codings else {}
                            subject = resource.get('subject', {})
                            
                            condition_records.append({
                                'id': resource.get('id'),
                                'resource_type': resource.get('resourceType'),
                                'clinical_status': status_code,
                                'code_code': code_info.get('code'),
                                'code_display': code_info.get('display'),
                                'subject_reference': subject.get('reference'),
                                'onset_datetime': parse_timestamp(resource.get('onsetDateTime')),
                                'raw_json': json.dumps(resource),
                                'extraction_timestamp': datetime.now(),
                                'api_url': 'https://hapi.fhir.org/baseR4/Condition',
                                'ingestion_date': ingestion_date
                            })

if condition_records:
    condition_df = spark.createDataFrame([Row(**r) for r in condition_records])
    condition_df.write.format("delta").mode("overwrite").saveAsTable(f"{DATABASE}.bronze_condition")
    print(f"✓ bronze_condition: {condition_df.count()} records")
print(" Bronze layer completed")
