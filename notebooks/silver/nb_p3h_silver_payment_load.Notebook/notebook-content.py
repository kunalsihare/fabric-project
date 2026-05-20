# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
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

%run nb_p3h_silver_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
table_id = 13
pipeline_start_time = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

var_lib = notebookutils.variableLibrary.getLibrary("vl_p3h_p3h_variables")
config_schema  = var_lib.getVariable("config_schema_name")
bronze_schema  = var_lib.getVariable("bronze_schema_name")
silver_schema  = var_lib.getVariable("silver_schema_name")
log_schema     = var_lib.getVariable("log_schema_name")

pipeline_start_time = datetime.fromisoformat(pipeline_start_time.replace("Z", "+00:00"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def prepare_silver_payment():
    print("Preparing silver.payment from bronze.capitation_payment...")

    df = (
        spark.table(f"{bronze_schema}.capitation_payment")
        .select(
            F.col("payment_id"),
            F.col("provider_id"),
            F.to_date(
                F.concat(F.col("payment_month"), F.lit("-01")), "yyyy-MM-dd"
            ).alias("payment_date"),
            F.col("total_payment").alias("amount"),
            F.col("file_name"),
            F.col("file_modification_time"),
            F.current_timestamp().alias("load_ts")
        )
    )
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def silver_payment_ingestion_with_logging(table_id, pipeline_run_id, pipeline_name):
    start_time = datetime.now()

    try:
        target_table = f"{silver_schema}.payment"

        df = prepare_silver_payment()
        source_count = df.count()
        print(f"Source record count: {source_count}")

        print(f"Running DQ checks for table_id={table_id}...")
        df = run_metadata_dq(df, table_id, pipeline_run_id,target_table)
        clean_count = df.count()
        print(f"Clean record count after DQ: {clean_count}")

        initial_count = spark.table(target_table).count()

        df.write.mode("append").saveAsTable(target_table)
        print(f"Append load complete for {target_table}.")

        final_count = spark.table(target_table).count()

        log_run(
            pipeline_run_id, pipeline_name, "SILVER", table_id,
            "SUCCESS", source_count, final_count - initial_count,
            None, pipeline_start_time, ""
        )

    except Exception as e:
        print(traceback.format_exc())
        log_run(
            pipeline_run_id, pipeline_name, "SILVER", table_id,
            "FAILED", 0, 0, str(e), pipeline_start_time, ""
        )
        raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_payment_ingestion_with_logging(table_id, pipeline_run_id, pipeline_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
