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
config_schema = var_lib.getVariable("config_schema_name")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"create schema if not exists {config_schema}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS  {config_schema}.cfg_source_details (
    table_id              INT        ,
    domain                STRING     ,
    dataset_name          STRING     ,
    folder_path           STRING     ,
    file_format           STRING     ,   -- parquet / csv / json
    is_active             BOOLEAN     ,  
    is_full_load          BOOLEAN     ,  
    last_watermark_value  Date     ,   -- NONE if not applicable
    delimiter             STRING         -- , | ; or NONE
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
CREATE TABLE IF NOT EXISTS  {config_schema}.cfg_table_schema (
    table_id            INT        ,
    layer               STRING     ,   -- BRONZE / SILVER / GOLD
    database_name       STRING     ,
    schema_name         STRING     ,
    table_name          STRING     ,
    partition_columns   STRING     ,   -- comma-separated or NONE
    primary_key         STRING     ,   -- comma-separated or NONE
    surrogate_key       STRING     ,   -- NONE for Bronze
    scd_type2           BOOLEAN   , 
    is_active           BOOLEAN     
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
CREATE TABLE IF NOT EXISTS {config_schema}.cfg_column_schema (
    column_id          INT        NOT NULL,
    table_id           INT        NOT NULL,
    column_name        STRING     NOT NULL,
    data_type          STRING     NOT NULL,
    sequence_number    INT        NOT NULL,
    constraint         STRING     NOT NULL,   -- PK / NOT NULL / NONE
    default_value      STRING     NOT NULL,   -- NONE or literal
    is_pii             BOOLEAN 
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
CREATE TABLE IF NOT EXISTS {config_schema}.cfg_scd_type2_metadata (
    table_id              INT        ,
    business_key_columns  STRING     ,   -- comma-separated
    start_date_column     STRING     ,
    end_date_column       STRING     ,
    is_current_column     STRING     
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
CREATE TABLE IF NOT EXISTS {config_schema}.cfg_maintenance_config (
    table_id                  INT        ,
    enable_vorder             BOOLEAN    ,   
    perform_optimize          BOOLEAN     ,  
    perform_vacuum            BOOLEAN     ,  
    perform_stats             BOOLEAN     ,  
    zorder_columns            STRING     ,  
    vacuum_retention_hours    INT        ,    
    is_active                 BOOLEAN      
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
CREATE TABLE IF NOT EXISTS {config_schema}.cfg_dq_rules (
    rule_id          INT        ,
    column_id        INT        ,
    rule_type        STRING     ,   -- NOT_NULL / RANGE / REGEX / CUSTOM
    rule_value       STRING     ,   -- threshold, regex, NONE
    is_active        BOOLEAN     , 
    created_at       TIMESTAMP  
)
USING DELTA;

""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
