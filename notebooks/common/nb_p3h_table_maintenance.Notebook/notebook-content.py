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

%run nb_p3h_read_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_p3h_write_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

var_lib = notebookutils.variableLibrary.getLibrary("vl_p3h_p3h_variables")
config_schema = var_lib.getVariable("config_schema_name")
log_schema = var_lib.getVariable("log_schema_name")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CONFIG_TABLE = f"{config_schema}.cfg_maintenance_config"
LOG_TABLE = f"{log_schema}.maintenance_logs"

# Optional: Disable the retention duration check if you plan to vacuum with less than 7 days (168 hours)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def check_vorder_enabled(full_table_name):
    """Parses DESC EXTENDED to see if V-Order is already true."""
    try:
        desc_df = spark.sql(f"DESCRIBE EXTENDED {full_table_name}")
        tbl_props = desc_df.filter(col("col_name") == "Table Properties").collect()
        if tbl_props:
            prop_str = tbl_props[0]["data_type"]
            return "delta.parquet.vorder.enabled=true" in prop_str.replace(" ", "")
    except:
        return False
    return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_maintenance():
    print("--- Starting Delta Maintenance Engine ---")
    
    # 1. Fetch only active table configurations
    try:
        configs = spark.read.table(CONFIG_TABLE).filter(col("is_active") == True).collect()
    except Exception as e:
        print(f"Error reading config table: {e}")
        return

    for row in configs:
        start_time = datetime.now()
        table_id = row['table_id']
        table_details = get_table_details(table_id, config_schema)

        db = table_details['database_name']
        schema = table_details['schema_name']
        tbl = table_details['table_name']
        full_name = f"{db}.{schema}.{tbl}"
        run_log = []
        
        print(f"\nTarget: {full_name}")

        try:
            # --- STEP A: V-Order ---
            if row['enable_vorder']:
                print("Enabling v-order...")
                if not check_vorder_enabled(full_name):
                    spark.sql(f"ALTER TABLE {full_name} SET TBLPROPERTIES ('delta.parquet.vorder.enabled' = 'true')")
                    run_log.append("V-ORDER ENABLED")
                else:
                    run_log.append("V-ORDER ALREADY ON")

            # --- STEP B: Optimize / Z-Order ---
            if row['perform_optimize']:
                print("Performing Optimize...")
                z_cols = row['zorder_columns']
                if z_cols and z_cols.strip():
                    spark.sql(f"OPTIMIZE {full_name} ZORDER BY ({z_cols})")
                    run_log.append(f"OPTIMIZE ZORDER({z_cols})")
                else:
                    spark.sql(f"OPTIMIZE {full_name}")
                    run_log.append("OPTIMIZE")

            # --- STEP C: Vacuum ---
            if row['perform_vacuum']:
                print("Performing Vacuum...")
                retention = row['vacuum_retention_hours'] if row['vacuum_retention_hours'] else 168
                spark.sql(f"VACUUM {full_name} RETAIN {retention} HOURS")
                run_log.append(f"VACUUM({retention}h)")

            # --- STEP D: Compute Statistics ---
            if row['perform_stats']:
                print("Performing Statistics Computation...")
                spark.sql(f"ANALYZE TABLE {full_name} COMPUTE STATISTICS FOR ALL COLUMNS")
                run_log.append("STATS COMPUTED")

            # --- STEP E: Update Audit Metadata ---
            final_ops = ", ".join(run_log) if run_log else "NO OPS PERFORMED"
            
            # Update the config table so you can monitor progress
            end_time = datetime.now()

            duration_ms = int((end_time - start_time).total_seconds() * 1000)
            
            write_maintenance_logs(table_id, final_ops, start_time, 'Success', duration_ms, LOG_TABLE)
            print(f"  -> Success: {final_ops}")

        except Exception as e:
            print(f"  [!] Error on {full_name}: {str(e)}")
            write_maintenance_logs(table_id, "", start_time, 'Fail', 0, LOG_TABLE)
            raise e

    print("\n--- Maintenance Cycle Complete ---")





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

run_maintenance()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
