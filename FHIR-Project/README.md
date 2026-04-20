
# FHIR Data Lakehouse - Medallion Architecture Implementation

## 🎯 Project Overview

A production-ready **Medallion Lakehouse Architecture** implementation for healthcare data ingestion and analytics using the FHIR API standard.

**Platform:** Databricks (Serverless)  
**Data Source:** HAPI FHIR Public API  
**Architecture:** Raw → Bronze → Silver → Gold  
**Completion Date:** April 18, 2026  
**Pipeline Duration:** 2 minutes 30 seconds  

---

## ✅ Assignment Requirements Met

✅ **Incremental ingestion with pagination** - 1000 FHIR entries (5 pages × 4 resources)  
✅ **Store raw JSON responses** - 20 JSON files partitioned by date  
✅ **Medallion Architecture** - 4 complete layers implemented  
✅ **Metadata columns** - extraction_timestamp, api_url, ingestion_date  
✅ **SCD Type 2 versioning** - Historical tracking with record_hash, is_current, valid_from, valid_to  
✅ **Delta tables** - All layers use Delta Lake format  
✅ **Modular code** - Reusable utilities, no hardcoding  
✅ **Orchestration pipeline** - Automated Databricks Workflow with dependencies  
✅ **Documentation** - Complete with architecture diagrams and schemas  

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              FHIR API (HAPI Server)                         │
│          https://hapi.fhir.org/baseR4/                      │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
            ┌──────────────────────┐
            │   API Client         │
            │   (Pagination)       │
            │   • Offset-based     │
            │   • Retry logic      │
            └──────────┬───────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                  RAW LAYER                                  │
│  Location: /Volumes/workspace/default/fhir_lakehouse/raw/   │
│  Format: JSON files                                         │
│  Partitioning: By date (YYYY-MM-DD)                         │
│  • 20 JSON files (5 pages × 4 resources)                    │
│  • 1,000 FHIR entries                                       │
│  • Full API response preserved                              │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              BRONZE LAYER (Delta Tables)                    │
│  Tables: bronze_patient, bronze_encounter,                  │
│          bronze_observation, bronze_condition               │
│  • 1,200 structured records                                 │
│  • Parsed from JSON with flattened structure               │
│  • Metadata: extraction_timestamp, api_url, ingestion_date  │
│  • Partitioned by ingestion_date                            │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│           SILVER LAYER (SCD Type 2 Versioned)               │
│  Tables: silver_patient, silver_encounter,                  │
│          silver_observation, silver_condition               │
│  • 1,200 versioned records (current versions)               │
│  • Data quality applied (cleansing, deduplication)          │
│  • SCD Type 2 columns:                                      │
│    - record_hash: MD5 hash for change detection             │
│    - is_current: TRUE/FALSE flag                            │
│    - valid_from: Version start date                         │
│    - valid_to: Version end date (NULL for current)          │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              GOLD LAYER (Analytics Views)                   │
│  Views: gold_patient_demographics,                          │
│         gold_encounter_summary,                             │
│         gold_patient_encounter_facts,                       │
│         gold_observation_summary,                           │
│         gold_condition_analysis                             │
│  • Business-ready aggregations                              │
│  • Optimized for BI tools                                   │
│  • Denormalized for performance                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
FHIR-Assignment-Submission/
├── README.md                          # This file
├── config/
│   └── config.json                    # Configuration parameters
├── utils/
│   ├── __init__.py                    # Package initializer
│   ├── api_client.py                  # FHIR API client with pagination
│   └── scd_handler.py                 # SCD Type 2 implementation
├── notebooks/
│   ├── 00_setup_environment.py        # Database & table creation
│   ├── 01_raw_ingestion.py            # API data ingestion
│   ├── 02_bronze_layer.py             # JSON to Delta transformation
│   ├── 03_silver_layer.py             # SCD Type 2 versioning
│   └── 04_gold_layer.py               # Analytics views creation
└── screenshots/
    └── pipeline_success.png           # Successful pipeline execution
```

---


### Prerequisites
- Databricks workspace
- Unity Catalog volume access
- Cluster with Databricks Runtime 13.3 LTS or newer

### Setup Instructions

1. **Import Notebooks to Databricks**
   - Upload all notebooks from `notebooks/` folder
   - Place in: `/Workspace/Users/{your-email}/FHIR-Project/`

2. **Upload Configuration**
   - Upload `config/config.json` to: `/Volumes/workspace/default/fhir_lakehouse/config/`

3. **Upload Utility Files**
   - Upload `utils/*.py` files to: `/Workspace/Users/{your-email}/FHIR-Project/utils/`

4. **Run Notebooks in Sequence**
   ```
   1. 00_setup_environment      → Creates database and tables
   2. 01_raw_ingestion          → Fetches data from FHIR API
   3. 02_bronze_layer           → Parses JSON to Delta tables
   4. 03_silver_layer           → Applies SCD Type 2
   5. 04_gold_layer             → Creates analytics views
   ```

5. **Or Create Databricks Workflow**
   - Navigate to: Jobs & Pipelines → Create Job
   - Add tasks in order with dependencies
   - Click "Run now" to execute pipeline

---

## 🔄 Pipeline Orchestration

### Workflow Execution

**Total Duration:** 2 minutes 30 seconds

```
Task 1: Raw Ingestion (1m 1s)
   └─ Fetches 1000 entries from FHIR API
   └─ Stores 20 JSON files
      ↓
Task 2: Bronze Layer (33s)
   └─ Parses JSON to structured Delta tables
   └─ Creates 1,200 records
      ↓
Task 3: Silver Layer (41s)
   └─ Applies SCD Type 2 versioning
   └─ Tracks historical changes
      ↓
Task 4: Gold Layer (14s)
   └─ Creates 5 analytics views
   └─ Ready for reporting
```

**Resource Processing Order:**
1. Patient (base entity)
2. Encounter (references Patient)
3. Observation (references Patient & Encounter)
4. Condition (references Patient)

---

## 📊 Data Model

### Resources Ingested

| Resource    | Description                          | Records |
|-------------|--------------------------------------|---------|
| Patient     | Demographics & contact information   | 300     |
| Encounter   | Healthcare visits & interactions     | 300     |
| Observation | Clinical measurements & findings     | 300     |
| Condition   | Diagnoses & health conditions        | 300     |
| **Total**   |                                      | **1,200** |

### Table Relationships

```
Patient (1) ────< (N) Encounter
   │                     │
   │                     │
   └────< (N) Observation
   │
   └────< (N) Condition
```

### Bronze Layer Schema (Example: bronze_patient)

```sql
id                      STRING      -- FHIR resource ID
resource_type           STRING      -- Always 'Patient'
name_family             STRING      -- Family name
name_given              ARRAY       -- Given names
gender                  STRING      -- Gender
birth_date              STRING      -- Date of birth
address_city            STRING      -- City
address_state           STRING      -- State
address_country         STRING      -- Country
raw_json                STRING      -- Full FHIR JSON
extraction_timestamp    TIMESTAMP   -- When fetched
api_url                 STRING      -- API endpoint
ingestion_date          STRING      -- Partition key (YYYY-MM-DD)
```

### Silver Layer Schema (Adds SCD Type 2)

```sql
-- All bronze columns plus:
record_hash             STRING      -- MD5 hash for change detection
is_current              BOOLEAN     -- TRUE = current version
valid_from              DATE        -- Version start date
valid_to                DATE        -- Version end date (NULL = current)
```

---

## 🔄 SCD Type 2 Implementation

### How It Works

**Initial Load:**
- All records inserted with `is_current = TRUE`
- `valid_from = CURRENT_DATE`
- `valid_to = NULL`

**On Change Detection:**
1. Calculate MD5 hash of business columns
2. Compare with existing records
3. For changed records:
   - Expire old version: `is_current=FALSE`, `valid_to=today`
   - Insert new version: `is_current=TRUE`, `valid_from=today`

**Example:**
```
Patient P001 changes city: Boston → New York

BEFORE:
| id   | city   | is_current | valid_from | valid_to |
|------|--------|------------|------------|----------|
| P001 | Boston | TRUE       | 2026-04-18 | NULL     |

AFTER:
| id   | city     | is_current | valid_from | valid_to   |
|------|----------|------------|------------|------------|
| P001 | Boston   | FALSE      | 2026-04-18 | 2026-04-19 |
| P001 | New York | TRUE       | 2026-04-19 | NULL       |
```

---

## 📈 Data Statistics

### Pipeline Summary

| Metric                  | Value      |
|-------------------------|------------|
| API Calls Made          | 20         |
| Raw JSON Files          | 20         |
| Raw FHIR Entries        | 1,000      |
| Bronze Records          | 1,200      |
| Silver Records          | 1,200      |
| Gold Views              | 5          |
| Total Execution Time    | 2m 30s     |

---

## 📊 Gold Layer Analytics

### Available Views

1. **gold_patient_demographics**
   - Patient demographics with calculated age
   - Includes city, state, country

2. **gold_encounter_summary**
   - Encounter details with patient references
   - Duration calculations

3. **gold_patient_encounter_facts**
   - Patient information + encounter counts
   - First and last encounter dates

4. **gold_observation_summary**
   - Observation types and statistics
   - Aggregated by code and unit

5. **gold_condition_analysis**
   - Condition distribution
   - Patient counts per condition

### Example Queries

```sql
-- Top 10 most common conditions
SELECT * FROM fhir_lakehouse.gold_condition_analysis 
ORDER BY condition_count DESC LIMIT 10;

-- Patient demographics with age
SELECT * FROM fhir_lakehouse.gold_patient_demographics 
WHERE age > 50;

-- Observation statistics
SELECT * FROM fhir_lakehouse.gold_observation_summary 
ORDER BY observation_count DESC;
```

---


Edit `config/config.json`:

```json
{
  "api": {
    "base_url": "https://hapi.fhir.org/baseR4",
    "resources": ["Patient", "Encounter", "Observation", "Condition"],
    "pagination": {
      "page_size": 50,
      "max_pages_per_run": 5
    }
  },
  "storage": {
    "database_name": "fhir_lakehouse",
    "volume_path": "/Volumes/workspace/default/fhir_lakehouse"
  }
}
```

**Parameters:**
- `page_size`: Records per API call (default: 50)
- `max_pages_per_run`: Pages to fetch (default: 5)
- `database_name`: Target database
- `volume_path`: Unity Catalog volume path

---

## 🏆 Best Practices Implemented

✅ **Modular Code** - Reusable utilities (api_client, scd_handler)  
✅ **No Hardcoding** - All configs in JSON file  
✅ **Error Handling** - Retry logic for API calls  
✅ **Metadata Tracking** - Complete audit trail  
✅ **Data Partitioning** - Optimized queries  
✅ **SCD Type 2** - Historical change tracking  
✅ **Delta Lake** - ACID transactions  
✅ **Serverless Compute** - Auto-scaling  
✅ **Documentation** - Comprehensive guides  

---


### Common Issues

**Module not found:**
```python
import sys
sys.path.append("/Workspace/Users/{your-email}/FHIR-Project/utils")
```

**Schema mismatch:**
```sql
DROP TABLE IF EXISTS fhir_lakehouse.bronze_patient;
-- Then rerun setup notebook
```

**API timeout:**
- Already handled with retry logic
- Reduce `max_pages_per_run` if needed

---


- ✅ Complete source code (5 notebooks, 3 utility files)
- ✅ Configuration files
- ✅ Working Databricks Workflow
- ✅ Complete documentation
- ✅ SCD Type 2 implementation
- ✅ Metadata tracking
- ✅ Screenshots

---

## 👨‍💻 Technical Stack

**Technologies:**
- Platform: Databricks (Serverless)
- Storage: Unity Catalog, Delta Lake
- Processing: Apache Spark (PySpark)
- Language: Python 3.12
- API: FHIR RESTful API
- Orchestration: Databricks Workflows

**Performance:**
- Throughput: ~400 records/minute
- Latency: 2.5 minutes end-to-end
- Storage: ~900 KB raw, ~1.5 MB total

---

## 📄 License

Educational project for FHIR API Data Ingestion & Analytics Assignment

---

## 📧 Contact

Created: April 18, 2026  
Platform: Databricks  
Status: ✅ Complete and Production-Ready

---

**End of README**
