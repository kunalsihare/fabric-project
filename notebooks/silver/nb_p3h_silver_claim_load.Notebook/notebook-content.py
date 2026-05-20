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
table_id = 11
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

def prepare_silver_claim():
    print("Preparing silver.claim from bronze.claim_header + bronze.claim_line...")

    header_df = spark.table(f"{bronze_schema}.claim_header")
    line_df   = spark.table(f"{bronze_schema}.claim_line")
    #apply trim
    header_df = header_df.withColumn("claim_id", F.trim(F.col("claim_id")))
    line_df = line_df.withColumn("claim_id", F.trim(F.col("claim_id")))
    # Deduplicate - keep latest record by file_modification_time
    header_clean = (
        header_df
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("claim_id").orderBy(F.col("file_modification_time").desc())
        ))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    line_clean = (
        line_df
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("claim_id", "claim_line_id").orderBy(F.col("file_modification_time").desc())
        ))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    # Since there's no common join key, use row position as join key
    # This assumes both datasets are ordered similarly
    print("Creating row-based join...")
    header_with_row = header_clean.withColumn("row_num", F.monotonically_increasing_id())
    line_with_row = line_clean.withColumn("row_num", F.monotonically_increasing_id())
    df = (
        header_with_row.alias("h")
        .join(line_with_row.alias("l"), F.col("h.row_num") == F.col("l.row_num"), "inner")
        .drop("row_num")
    )
    # Add line numbers based on claim_line_id ordering
    w = Window.partitionBy("h.claim_id").orderBy("l.claim_line_id")
    df = df.withColumn("line_number", F.row_number().over(w))

    # Create final result
    result = df.select(
        F.col("h.claim_id"),
        F.col("h.member_id"),
        F.col("h.provider_id"),
        F.col("h.claim_date"),
        F.col("line_number").cast("int"),
        F.col("l.procedure_code"),
        F.col("l.line_amount").alias("amount"),
        F.col("l.file_name"),
        F.col("l.file_modification_time"),
        F.current_timestamp().alias("load_ts")
    )
    
    print(f"Row-based join completed. Result count: {result.count()}")
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def silver_claim_ingestion_with_logging(table_id, pipeline_run_id, pipeline_name):
    start_time = datetime.now()

    try:
        target_table = f"{silver_schema}.claim"

        df = prepare_silver_claim()
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

silver_claim_ingestion_with_logging(table_id, pipeline_run_id, pipeline_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
