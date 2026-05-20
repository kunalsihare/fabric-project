# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# MAGIC %%configure -f
# MAGIC {
# MAGIC    "defaultLakehouse": {
# MAGIC     "name": {
# MAGIC       "variableName": "$(/**/vl_p3h_p3h_variables/LH_Name)" 
# MAGIC     },
# MAGIC     "id": {
# MAGIC       "variableName": "$(/**/vl_p3h_p3h_variables/LH_ID)"
# MAGIC     },
# MAGIC     "workspaceId": {
# MAGIC       "variableName": "$(/**/vl_p3h_p3h_variables/WS_ID)"
# MAGIC     }
# MAGIC   }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

var_lib = notebookutils.variableLibrary.getLibrary("vl_p3h_p3h_variables")
log_schema = var_lib.getVariable("log_schema_name")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"create schema if not exists {log_schema}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {log_schema}.run_logs (
    id                   INT        ,
    pipeline_run_id       STRING     ,
    pipeline_name         STRING     ,
    layer                STRING     ,   -- BRONZE / SILVER / GOLD
    table_id              INT        ,
    status               STRING     ,   -- SUCCESS / FAILED / RUNNING
    source_count          BIGINT     ,   -- 0 if not applicable
    insert_count          BIgINT     ,
    error                 STRING     ,                -- populated on failure
    pipeline_start_time   TIMESTAMP  ,
    file_details STRING 
)
USING DELTA;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {log_schema}.processed_file_logs (
    id           INT        ,
    table_id     INT        ,
    file_name    STRING     ,
    watermark    DATE     ,   -- NONE if not applicable
    is_active    BOOLEAN        
)
USING DELTA;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {log_schema}.maintenance_logs (
    table_id        INT        ,
    operation_type  STRING     ,   -- OPTIMIZE / VACUUM / STATS
    run_timestamp   TIMESTAMP  ,
    status          STRING     ,   -- SUCCESS / FAILED
    duration_ms     BIGINT     
)
USING DELTA;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {log_schema}.data_quality_exceptions (
    table_id INT COMMENT 'Silver table where DQ failed',
    failure_reason STRING COMMENT 'Reason for DQ failure',
    record_json STRING COMMENT 'Full failed record in JSON format',
    run_id STRING COMMENT 'Pipeline run identifier',
    inserted_ts TIMESTAMP COMMENT 'DQ failure insertion timestamp'
)
USING DELTA
PARTITIONED BY (table_id);
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
