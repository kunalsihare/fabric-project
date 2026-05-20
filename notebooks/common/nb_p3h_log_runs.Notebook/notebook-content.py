# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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

from pyspark.sql.types import (
    StructType, StructField,
    LongType, IntegerType, StringType, TimestampType
)

def log_run(run_id, run_name, layer, table_id, status,
            source_count, insert_count, error, pipeline_start_time, file_details):

    print("logging run details...")

    log_table = f"{log_schema}.run_logs"

    max_id = spark.table(log_table).selectExpr("max(id) as max_id").collect()[0]["max_id"]
    max_id = (max_id if max_id is not None else 0) + 1

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("pipeline_run_id", StringType(), True),
        StructField("pipeline_name", StringType(), True),
        StructField("layer", StringType(), True),
        StructField("table_id", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("source_count", LongType(), True),
        StructField("insert_count", LongType(), True),
        StructField("error", StringType(), True),
        StructField("pipeline_start_time", TimestampType(), True),
        StructField("file_details", StringType(),True)
    ])

    df = spark.createDataFrame(
        [(max_id, run_id, run_name, layer, table_id,
          status, source_count, insert_count, error, pipeline_start_time, file_details)],
        schema=schema
    )

    df.write.mode("append").saveAsTable(log_table)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
