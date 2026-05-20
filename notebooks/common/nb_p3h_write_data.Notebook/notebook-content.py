# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType
from datetime import datetime


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_full_load(df, target_table):
    print("Trigger Full load...")
    (
        df.write.mode("overwrite")
          .partitionBy("file_modification_time")
          .saveAsTable(target_table)
    )

def write_incremental_load(df, target_table):

    print("Trigger Incremental Load...")
    partitions = [r[0] for r in df.select("file_modification_time").distinct().collect()]

    replace_where = "file_modification_time IN ({})".format(
        ",".join([f"'{p}'" for p in partitions])
    )

    (
        df.write.mode("overwrite")
          .option("replaceWhere", replace_where)
          .partitionBy("file_modification_time")
          .saveAsTable(target_table)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_maintenance_logs(
    table_id: int,
    operation_type: str,
    run_timestamp,
    status: str,
    duration_ms: int,
    log_table: str
):
    
    schema = StructType([
        StructField("table_id", IntegerType(), False),
        StructField("operation_type", StringType(), False),
        StructField("run_timestamp", TimestampType(), False),
        StructField("status", StringType(), False),
        StructField("duration_ms", LongType(), False)
    ])

    data = [
        (table_id, operation_type, run_timestamp, status, duration_ms)
    ]

    df = spark.createDataFrame(data, schema=schema)

    df.write \
      .format("delta") \
      .mode("append") \
      .saveAsTable(log_table)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
