dbutils.library.restartPython()
# Importing libraries
import sys
sys.path.append("/Workspace/Users/vijayaayushi1@gmail.com/FHIR-Project/utils")

import json
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Load configuration
with open("/Volumes/workspace/default/fhir_lakehouse/config/config.json", "r") as f:
    config = json.load(f)

API_BASE_URL = config["api"]["base_url"]
RESOURCES = config["api"]["resources"]
PAGE_SIZE = config["api"]["pagination"]["page_size"]
MAX_PAGES = config["api"]["pagination"]["max_pages_per_run"]
VOLUME_PATH = "/Volumes/workspace/default/fhir_lakehouse"
DATABASE = config["storage"]["database_name"]

# Initialize API client
from api_client import FHIRAPIClient

client = FHIRAPIClient(API_BASE_URL)

# Fetch data from FHIR API
all_data = {}

for resource in RESOURCES:
    results = client.fetch_with_pagination(resource, PAGE_SIZE, MAX_PAGES)
    all_data[resource] = results

# Save JSON files to volume
storage_summary = []
partition_date = datetime.now().strftime('%Y-%m-%d')

for resource_type, pages_data in all_data.items():
    for page_idx, (json_data, api_url, timestamp) in enumerate(pages_data):
        # Save file
        file_path = f"{VOLUME_PATH}/raw/{resource_type.lower()}/{partition_date}/page_{page_idx + 1}.json"
        dbutils.fs.put(file_path, json.dumps(json_data, indent=2), overwrite=True)
        
        # Track metadata
        storage_summary.append({
            "resource_type": resource_type,
            "file_path": file_path,
            "partition_date": partition_date,
            "page_number": page_idx + 1,
            "api_url": api_url,
            "extraction_timestamp": timestamp,
            "entry_count": len(json_data.get('entry', []))
        })

# Create metadata DataFrame
metadata_df = spark.createDataFrame([Row(**item) for item in storage_summary])

# Log to metadata table
log_schema = StructType([
    StructField("log_id", StringType(), True),
    StructField("pipeline", StringType(), True),
    StructField("resource", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("record_count", IntegerType(), True)
])

for resource in RESOURCES:
    count = int(metadata_df.filter(col("resource_type") == resource).agg({"entry_count": "sum"}).collect()[0][0] or 0)
    
    log_df = spark.createDataFrame([(
        f"raw_{resource}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "raw_ingestion",
        resource,
        "raw",
        datetime.now(),
        "SUCCESS",
        count
    )], schema=log_schema)
    
    log_df.write.format("delta").mode("append").saveAsTable(f"{DATABASE}.pipeline_metadata_log")

print(" Raw ingestion completed")
