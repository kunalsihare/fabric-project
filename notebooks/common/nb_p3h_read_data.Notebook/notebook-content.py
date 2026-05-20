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

def get_files_to_process(table_id, is_full_load, log_schema):
    print("Getting unprocessed Files....")
    pfl = (
        spark.table(f"{log_schema}.processed_file_logs")
        .filter(
            (F.col("table_id") == table_id) &
            (F.col("is_active") == True)
        )
    )

    if pfl.count() == 0:
        return None

    if is_full_load:
        return [
            pfl.orderBy(F.col("watermark").desc())
               .limit(1)
               .collect()[0]["file_name"]
        ]

    return [
        r.file_name
        for r in pfl.select("file_name")
                    .distinct()
                    .collect()
    ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_table_details(table_id, config_schema):
    print("Getting Table Name...")
    df = (
        spark.table(f"{config_schema}.cfg_table_schema")
            .filter(F.col("table_id") == F.lit(table_id))
            .select("database_name","schema_name", "table_name")
            .first()
        )
    return {
        "database_name" : df.database_name,
        "schema_name" : df.schema_name,
        "table_name" : df.table_name
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_files(files, file_format, delimiter):
    paths = [f for f in files]
    print(f"Reading files from path : {paths}...")
    reader = spark.read
    if file_format == "csv":
        reader = reader.option("header", "true").option("delimiter", delimiter)

    df = reader.format(file_format).load(paths)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
