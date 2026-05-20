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

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

folder_path = ""
table_id = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

var_lib = notebookutils.variableLibrary.getLibrary("vl_p3h_p3h_variables")
log_schema = var_lib.getVariable("log_schema_name")
LH_ID = var_lib.getVariable("LH_ID")
WS_ID = var_lib.getVariable("WS_ID")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Define your inputs
LOG_TABLE = f"{log_schema}.processed_file_logs"
print(LOG_TABLE)
LAKEHOUSE_PATH = f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}/Files{folder_path}"
print(LAKEHOUSE_PATH)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_latest_watermark(table_id, log_table):
    """
    Fetches the max watermark for the given table_id.
    Returns Jan 1, 1900 if no records exist.
    """
    try:
        max_date = spark.table(log_table) \
            .filter(F.col("table_id") == table_id) \
            .select(F.max("watermark")) \
            .collect()[0][0]
        return max_date if max_date else datetime(1900, 1, 1).date()
    except Exception as e:
        print(f"Exception Occured : {e}")
        return datetime(1900, 1, 1).date()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_new_files_metadata(folder_path, table_id, watermark):
    """
    Scans the Lakehouse folder and returns metadata for files 
    modified after the watermark.
    """
    # Use Fabric's built-in file system utility
    all_files = notebookutils.fs.ls(folder_path)
    
    new_files = []
    for f in all_files:
        # modifyTime is in milliseconds
        file_mod_date = datetime.fromtimestamp(f.modifyTime / 1000).date()
        
        if file_mod_date > watermark:
            new_files.append({
                "file_name": f.path,
                "watermark": file_mod_date,
                "is_active": True
            })
    return new_files

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def update_processed_log(log_data, log_table):
    """
    Increments the ID and appends new file records to the log table.
    """
    if not log_data:
        print("No new files found. Log table remains unchanged.")
        return

    # 1. Convert new data to DataFrame
    new_records_df = spark.createDataFrame(log_data)

    # 2. Calculate next starting ID
    try:
        current_max_id = spark.table(log_table).select(F.max("id")).collect()[0][0]
        start_id = current_max_id if current_max_id else 0
    except Exception:
        start_id = 0

    # 3. Assign sequential IDs
    window_spec = Window.orderBy("file_name")
    final_df = (
        new_records_df.withColumn("id", F.row_number().over(window_spec) + start_id)
                        .withColumn("table_id", F.lit(table_id))
    )

    # 4. Append to Delta Table
    display(final_df)
    final_df.select("id", "table_id", "file_name", "watermark", "is_active") \
        .write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(log_table)
    
    print(f"Successfully updated {len(log_data)} records in {log_table}.")

# --- Main Execution Workflow ---

# 2. Run the pipeline
try:
    current_watermark = get_latest_watermark(table_id, LOG_TABLE)
    new_metadata = fetch_new_files_metadata(LAKEHOUSE_PATH, table_id, current_watermark)
    update_processed_log(new_metadata, LOG_TABLE)

except Exception as e:
    print(e)
    raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
