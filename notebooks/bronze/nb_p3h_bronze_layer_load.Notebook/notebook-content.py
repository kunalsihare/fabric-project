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

%run nb_p3h_bronze_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime
import traceback

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

pipeline_run_id = ""
pipeline_name = ""
table_id = 0
pipeline_start_time = ""
is_full_load = ""
last_watermark_value = ""
file_format = ""
delimiter = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

var_lib = notebookutils.variableLibrary.getLibrary("vl_p3h_p3h_variables")
config_schema  = var_lib.getVariable("config_schema_name")
bronze_schema = var_lib.getVariable("bronze_schema_name")
log_schema = var_lib.getVariable("log_schema_name")

process_log_table = f"{log_schema}.processed_file_logs"
pipeline_start_time = datetime.fromisoformat(pipeline_start_time.replace("Z", "+00:00"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def deactivate_processed_files(table_id):
    print("Updating Flag for Processed Files...")
    spark.sql(f"update {process_log_table} set is_active = false where table_id = cast({table_id} as int)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def bronze_ingestion_with_logging(
    table_id,
    file_format,
    delimiter,
    is_full_load,
    pipeline_run_id,
    pipeline_name
):
    start_time = datetime.now()
    files = []

    try:
        schema, columns = get_required_schema(table_id, config_schema)
        files = get_files_to_process(table_id, is_full_load, log_schema)

        if files is None:
            log_run(pipeline_run_id, pipeline_name, "BRONZE", table_id,
                "Skip", 0, 0, "No File To Process", pipeline_start_time, "")
            return
            
        log_files = ','.join(files)

        df = read_files(files, file_format, delimiter)
        df = add_audit_columns(df)
        df = mask_pii_columns(table_id, df)

        df = enforce_schema(df, schema, columns)
    
        source_count = df.count()

        target_details = get_table_details(table_id, config_schema)
        target_table = f"""{bronze_schema}.{target_details["table_name"]}"""

        initial_count = spark.table(target_table).count()

        if is_full_load:
            write_full_load(df, target_table)
        else:
            write_incremental_load(df, target_table)

        print("Data Successfully Loaded...")

        final_count = spark.table(target_table).count()
        deactivate_processed_files(table_id)

        log_run(pipeline_run_id, pipeline_name, "BRONZE", table_id,
                "SUCCESS", source_count, final_count - initial_count,  None, pipeline_start_time, log_files)

    except Exception as e:
        log_run(pipeline_run_id, pipeline_name, "BRONZE", table_id,
                "FAILED", 0, 0, str(e), pipeline_start_time, "")
        raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_ingestion_with_logging(table_id, file_format, delimiter, is_full_load, pipeline_run_id, pipeline_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
