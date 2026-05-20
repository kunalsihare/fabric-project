# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def add_audit_columns(df):
    return (
        df.withColumn("load_ts", F.expr("current_timestamp()"))
        .withColumn("file_modification_time",F.col("_metadata.file_modification_time"))
        .withColumn("file_name", F.col("_metadata.file_path"))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
