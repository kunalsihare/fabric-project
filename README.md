# P3H POC - Healthcare Data Platform

A Healthcare Data Platform built on **Microsoft Fabric** using the **Medallion Architecture** (Bronze → Silver → Gold → Warehouse). This project implements a complete ETL pipeline for healthcare data including members, providers, claims, encounters, and payments.

---

## Architecture Overview

```
Source Files (CSV/Parquet/JSON)
        │
        ▼
┌─────────────────┐
│   Bronze Layer  │  Raw ingestion with audit columns + PII masking
│   (Lakehouse)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Silver Layer  │  Cleansed, conformed, SCD Type 2, DQ validated
│   (Lakehouse)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Gold Layer   │  Star schema (Dimensions + Facts)
│   (Lakehouse)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Warehouse    │  Copy Job transfers gold tables for reporting
│   (Warehouse)   │
└─────────────────┘
         │
         ▼
┌─────────────────┐
│     Report      │  Power BI Healthcare Report
└─────────────────┘
```

---

## Project Structure

| Folder | Description |
|--------|-------------|
| `activator/` | Reflex activator for pipeline monitoring |
| `copy job/` | Copy Job to transfer data from Lakehouse to Warehouse |
| `Dataflow Gen2/` | Dataflow for date dimension generation |
| `Lakehouse/` | Lakehouse configuration (`lh_p3h_p3hp`) |
| `notebooks/` | All PySpark notebooks (bronze, silver, gold, common utilities, DDLs) |
| `Pipelines/` | Data Factory pipelines for orchestration |
| `report/` | Power BI healthcare report |
| `semantic model/` | Semantic model for self-serve analytics |
| `Variable Library/` | Shared configuration variables |
| `warehouse/` | Warehouse configuration (`wh_p3h_p3h_warehouse`) |

---

## Prerequisites

Before running any pipeline, ensure the following are in place:

1. **Microsoft Fabric Workspace** is provisioned
2. **Lakehouse** `lh_p3h_p3hp` is created and available
3. **Warehouse** `wh_p3h_p3h_warehouse` is created and available
4. **Variable Library** `vl_p3h_p3h_variables` is deployed with correct values:
   - `LH_Name` = `lh_p3h_p3hp`
   - `WH_NAME` = `wh_p3h_p3h_warehouse`
   - `config_schema_name` = `cfg`
   - `log_schema_name` = `logs`
   - `bronze_schema_name` = `bronze`
   - `silver_schema_name` = `silver`
   - `dimension_schema_name` = `dimension`
   - `fact_schema_name` = `fact`
5. **Config CSV files** are placed in `Files/cfg_files/` folder in the Lakehouse:
   - `cfg_source_details.csv`
   - `cfg_table_schema.csv`
   - `cfg_column_schema.csv`
   - `cfg_scd_type2_metadata.csv`
   - `cfg_maintenance_config.csv`
   - `cfg_dq_rules.csv`
6. **Source data files** are placed in the appropriate folders referenced in `cfg_source_details.folder_path`

---

## First-Time Setup (Run Once - In Order)

Execute these pipelines **sequentially in the exact order below**. Each step depends on the previous one.

### Step 1: Create Config Schema & Tables

| Pipeline | Notebook | What it creates |
|----------|----------|-----------------|
| `pl_p3h_config_ddl` | `nb_p3h_config_tables_ddl` | Schema `cfg` + tables: `cfg_source_details`, `cfg_table_schema`, `cfg_column_schema`, `cfg_scd_type2_metadata`, `cfg_maintenance_config`, `cfg_dq_rules` |

### Step 2: Create Log Schema & Tables

| Pipeline | Notebook | What it creates |
|----------|----------|-----------------|
| `pl_p3h_logs_ddl` | `nb_p3h_log_tables_ddl` | Schema `logs` + tables: `run_logs`, `processed_file_logs`, `maintenance_logs`, `data_quality_exceptions` |

### Step 3: Create Bronze Schema & Tables

| Pipeline | Notebook | What it creates |
|----------|----------|-----------------|
| `pl_p3h_bronze_ddl` | `nb_p3h_bronze_layer_ddl` | Schema `bronze` + tables: `member_master`, `member_enrollment`, `provider_master`, `provider_contract`, `claim_header`, `claim_line`, `encounter`, `capitation_payment` |

### Step 4: Create Silver Schema & Tables

| Pipeline | Notebook | What it creates |
|----------|----------|-----------------|
| `pl_p3h_silver_ddl` | `nb_p3h_silver_layer_ddl` | Schema `silver` + tables: `member`, `provider`, `claim`, `encounter`, `payment` |

### Step 5: Create Gold Schema & Tables

| Pipeline | Notebook | What it creates |
|----------|----------|-----------------|
| `pl_p3h_gold_ddl` | `nb_p3h_gold_layer_ddl` | Schemas `dimension` + `fact` + tables: `dim_member`, `dim_provider`, `dim_plan`, `dim_diagnosis`, `dim_procedure`, `fact_healthcare` |

### Step 6: Load Metadata Configuration

| Pipeline | Notebook | What it does |
|----------|----------|--------------|
| `pl_p3h_metadata_config_load` | `nb_p3h_config_load` | Loads CSV config files into `cfg_*` Delta tables (merge upsert by primary key) |

### Step 7: Generate Date Dimension

| Pipeline | Dataflow | What it does |
|----------|----------|--------------|
| `pl_p3h_date_dimension` | `df_p3h_date_dimension` | Generates `dimension.dimdate` table (2014-01-01 to 2035-12-31) in the warehouse |

### Step 8: Setup Row-Level Security (Optional)

| Pipeline | What it does |
|----------|--------------|
| `pl_p3h_wh_rls_security` | Copies `cfg_user_mapping_security.csv` into `security.UserSecurityMapping` table in the warehouse for Row-Level Security |

---

## Recurring Data Load (Scheduled Execution)

After the first-time setup is complete, run the **master pipeline** for regular data loads:

```
pl_p3h_master_main_data_load
```

This master pipeline orchestrates the entire ETL process in sequence:

```
pl_p3h_master_main_data_load
  │
  ├── 1. Bronze Layer (pl_p3h_bronze_main)
  │       └── ForEach active source → pl_p3h_bronze_layer_table_load
  │             ├── nb_p3h_bronze_file_update  (detect new/modified files)
  │             └── nb_p3h_bronze_layer_load   (ingest + audit columns + PII masking)
  │
  ├── 2. Silver Layer (pl_p3h_silver_main)  [runs after Bronze succeeds]
  │       └── ForEach silver table → pl_p3h_silver_child_pipeline
  │             └── Switch by table_id:
  │                   9  → nb_p3h_silver_member_load
  │                   10 → nb_p3h_silver_provider_load
  │                   11 → nb_p3h_silver_claim_load
  │                   12 → nb_p3h_silver_encounter_load
  │                   13 → nb_p3h_silver_payment_load
  │
  ├── 3. Gold Layer (pl_p3h_Gold_main)  [runs after Silver succeeds]
  │       ├── ForEach dimension → pl_p3h_Gold_dimension_child_pipeline
  │       │     └── Switch by table_id:
  │       │           14 → nb_p3h_gold_dimdiagnosis_load
  │       │           15 → nb_p3h_gold_dimmember_load
  │       │           16 → nb_p3h_gold_dimplan_load
  │       │           17 → nb_p3h_gold_dimprocedure_load
  │       │           18 → nb_p3h_gold_dimprovider_load
  │       └── ForEach fact → pl_p3h_gold_fact_child_pipeline
  │             └── nb_p3h_gold_facthealthcare_load_01
  │
  ├── 4. Copy Job (cj_p3h_lh_to_wh)  [runs after Gold succeeds]
  │       └── Copies gold tables from Lakehouse to Warehouse:
  │             - dimension.dim_diagnosis
  │             - dimension.dim_member
  │             - dimension.dim_plan
  │             - dimension.dim_procedure
  │             - dimension.dim_provider
  │             - fact.fact_healthcare
  │
  └── 5. Email Notifications (after each layer completes)
```

---

## Maintenance (Periodic)

Run periodically to optimize Delta table performance:

| Pipeline | Notebook | What it does |
|----------|----------|--------------|
| `pl_p3h_maintenance` | `nb_p3h_table_maintenance` | Performs OPTIMIZE, VACUUM, V-ORDER, and compute statistics on Delta tables based on `cfg_maintenance_config` |

---

## Data Flow Details

### Bronze Layer (Raw Ingestion)

- **File Detection**: Scans source folders, compares file modification dates against watermarks in `logs.processed_file_logs`
- **Ingestion**: Reads files (CSV/Parquet/JSON), enforces schema from `cfg_column_schema`, adds audit columns (`load_ts`, `file_name`, `file_modification_time`), masks PII columns, writes full/incremental load
- **Logging**: Records results to `logs.run_logs`

### Silver Layer (Cleansed & Conformed)

- **Member**: Joins `bronze.member_master` + `bronze.member_enrollment` (latest enrollment per member), applies SCD Type 2
- **Provider**: Joins `bronze.provider_master` + `bronze.provider_contract` (latest contract per provider), applies SCD Type 2
- **Claim**: Joins `bronze.claim_header` + `bronze.claim_line` (with line numbering)
- **Encounter**: Direct load from `bronze.encounter`
- **Payment**: Direct load from `bronze.capitation_payment`
- **Data Quality**: Applies DQ rules from `cfg_dq_rules`, routes failures to `logs.data_quality_exceptions`

### Gold Layer (Star Schema)

- **Dimensions**: Extracts current records from silver, assigns surrogate keys, merges into dimension tables
  - `dim_member` ← `silver.member` (is_current = 'Y')
  - `dim_provider` ← `silver.provider` (is_current = 'Y')
  - `dim_plan` ← extracted from member enrollment data
  - `dim_diagnosis` ← from encounter/claim diagnosis codes
  - `dim_procedure` ← from claim procedure codes
- **Fact**: `fact_healthcare` — unified fact table combining claims, encounters, and payments, joined to all dimension surrogate keys + date_key

### Lakehouse to Warehouse

- **Copy Job** (`cj_p3h_lh_to_wh`): Copies all 6 gold tables from Lakehouse to Warehouse for reporting consumption

---

## Common Utility Notebooks

| Notebook | Purpose |
|----------|---------|
| `nb_p3h_schema_utils` | Reads schema definitions from `cfg_column_schema`, returns `StructType` + column order |
| `nb_p3h_audit_columns` | Adds `load_ts`, `file_name`, `file_modification_time` audit columns |
| `nb_p3h_read_data` | File discovery from processed_file_logs, table details lookup, file reading |
| `nb_p3h_write_data` | Full load, incremental load (partition overwrite), maintenance log writes |
| `nb_p3h_log_runs` | Appends execution records to `logs.run_logs` with auto-increment ID |
| `nb_p3h_dq_checks` | Data quality validation — null checks, unique keys, regex, numeric range |
| `nb_p3h_table_maintenance` | OPTIMIZE, VACUUM, V-ORDER, compute stats based on config |
| `nb_p3h_pii_masking` | Masks PII columns — STRING→"XXXX-XXXX", DATE→1900-01-01 |

---

## Schemas & Tables Summary

| Schema | Tables |
|--------|--------|
| `cfg` | `cfg_source_details`, `cfg_table_schema`, `cfg_column_schema`, `cfg_scd_type2_metadata`, `cfg_maintenance_config`, `cfg_dq_rules` |
| `logs` | `run_logs`, `processed_file_logs`, `maintenance_logs`, `data_quality_exceptions` |
| `bronze` | `member_master`, `member_enrollment`, `provider_master`, `provider_contract`, `claim_header`, `claim_line`, `encounter`, `capitation_payment` |
| `silver` | `member`, `provider`, `claim`, `encounter`, `payment` |
| `dimension` | `dim_member`, `dim_provider`, `dim_plan`, `dim_diagnosis`, `dim_procedure`, `dimdate` |
| `fact` | `fact_healthcare` |
| `security` | `UserSecurityMapping` (in Warehouse) |

---

## Quick Start Summary

```
┌─────────────────────────────────────────────────────────────┐
│                    FIRST-TIME SETUP                          │
├─────────────────────────────────────────────────────────────┤
│  1. pl_p3h_config_ddl           (Config tables)             │
│  2. pl_p3h_logs_ddl             (Log tables)                │
│  3. pl_p3h_bronze_ddl           (Bronze tables)             │
│  4. pl_p3h_silver_ddl           (Silver tables)             │
│  5. pl_p3h_gold_ddl             (Gold tables)               │
│  6. pl_p3h_metadata_config_load (Load config data)          │
│  7. pl_p3h_date_dimension       (Date dimension)            │
│  8. pl_p3h_wh_rls_security      (RLS security - optional)   │
├─────────────────────────────────────────────────────────────┤
│                    RECURRING LOADS                           │
├─────────────────────────────────────────────────────────────┤
│  pl_p3h_master_main_data_load   (Full ETL orchestration)    │
├─────────────────────────────────────────────────────────────┤
│                    MAINTENANCE                               │
├─────────────────────────────────────────────────────────────┤
│  pl_p3h_maintenance             (Delta table optimization)  │
└─────────────────────────────────────────────────────────────┘
```

---

## Monitoring

- **Reflex Activator** (`av_p3h_pipeline_monitoring`): Monitors pipeline execution and triggers alerts
- **Run Logs**: Check `logs.run_logs` for execution history
- **DQ Exceptions**: Check `logs.data_quality_exceptions` for data quality failures
- **Processed Files**: Check `logs.processed_file_logs` for file ingestion tracking

---

## Report

- **Power BI Report**: `pb_p3h_healthcare_report` — Connected to semantic model `sm_p3h_self_serve_model` for self-serve healthcare analytics
