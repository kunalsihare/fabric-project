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

%run nb_p3h_log_runs

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

pipeline_run_id     = ""
pipeline_name       = ""
table_id = 25
pipeline_start_time = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

var_lib          = notebookutils.variableLibrary.getLibrary("vl_p3h_p3h_variables")
silver_schema    = var_lib.getVariable("silver_schema_name")
dimension_schema = var_lib.getVariable("dimension_schema_name")
log_schema       = var_lib.getVariable("log_schema_name")

pipeline_start_time = datetime.fromisoformat(pipeline_start_time.replace("Z", "+00:00")) if pipeline_start_time else datetime.now()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def gold_dimprocedure_ingestion_with_logging(table_id, pipeline_run_id, pipeline_name):
    start_time = datetime.now()

    try:
        target_table = f"{dimension_schema}.dim_procedure"
        print(f"Loading {target_table} from silver.claim...")

        # CORRECTED: Extract unique procedure codes from silver.claim (only 1 unique code expected)
        source_df = (
            spark.table(f"{silver_schema}.claim")
            .select(F.col("procedure_code"))
            .filter(F.col("procedure_code").isNotNull() & (F.col("procedure_code") != ""))
            .distinct()
            .withColumn("procedure_desc", F.concat(F.lit("Procedure - "), F.col("procedure_code")))
            .withColumn("category", F.lit("CPT"))
            .withColumn("cost_category", F.lit("Standard"))
            .withColumn("load_ts", F.current_timestamp())
        )

        print(f"Found {source_df.count()} unique procedure codes")
        
        # VALIDATION: Should be 1 row based on data analysis
        if source_df.count() == 1:
            print("✅ EXPECTED: Only 1 unique procedure code found (matches data analysis)")
        elif source_df.count() == 0:
            print("No procedure codes found in silver.claim")
            log_run(
                pipeline_run_id, pipeline_name, "GOLD", table_id,
                "SUCCESS", 0, 0, None, pipeline_start_time, "No procedure codes found"
            )
            return
        else:
            print(f"⚠️  UNEXPECTED: Found {source_df.count()} procedure codes (expected 1)")

        # Check if target table exists
        target_exists = True
        try:
            spark.read.format("delta").table(target_table)
        except:
            target_exists = False

        if not target_exists:
            # Create new table with surrogate keys
            print(f"Creating new table {target_table}...")
            
            w = Window.orderBy(F.lit(1))
            source_df = source_df.withColumn(
                "procedure_sk",
                F.row_number().over(w).cast("long")
            )
            
            # Reorder columns
            source_df = source_df.select(
                "procedure_sk", "procedure_code", "procedure_desc", 
                "category", "cost_category", "load_ts"
            )
            
            source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
            print(f"Created {target_table} with {source_df.count()} procedure codes")
            
        else:
            # Use SQL logic to extract and insert only new procedure codes
            print(f"Adding new procedure codes to existing table...")
            
            # Get existing procedure codes to avoid duplicates
            existing_codes = (
                spark.table(target_table)
                .select("procedure_code")
                .distinct()
            )
            
            # Filter out existing codes
            new_procedures = source_df.join(
                existing_codes, 
                "procedure_code", 
                "left_anti"
            )
            
            if new_procedures.count() > 0:
                # Add surrogate keys for new procedures
                max_sk = (
                    spark.table(target_table)
                    .selectExpr("coalesce(max(procedure_sk), 0) as max_sk")
                    .collect()[0]["max_sk"]
                )
                
                w = Window.orderBy(F.lit(1))
                new_procedures = new_procedures.withColumn(
                    "procedure_sk",
                    (F.row_number().over(w) + F.lit(max_sk)).cast("long")
                )
                
                # Reorder columns
                new_procedures = new_procedures.select(
                    "procedure_sk", "procedure_code", "procedure_desc", 
                    "category", "cost_category", "load_ts"
                )
                
                # Insert new records
                new_procedures.write.format("delta").mode("append").saveAsTable(target_table)
                print(f"Added {new_procedures.count()} new procedure codes")
            else:
                print("No new procedure codes to add")

        final_count = spark.table(target_table).count()
        print(f"{target_table} row count: {final_count}")
        
        # VALIDATION: Should be 1 row based on actual data
        if final_count == 1:
            print("✅ SUCCESS: dim_procedure has correct row count (1 - matches source data)")
        else:
            print(f"⚠️  INFO: dim_procedure has {final_count} rows (source data has only 1 unique procedure code)")

        log_run(
            pipeline_run_id, pipeline_name, "GOLD", table_id,
            "SUCCESS", final_count, final_count, None, pipeline_start_time, ""
        )

    except Exception as e:
        print(traceback.format_exc())
        log_run(
            pipeline_run_id, pipeline_name, "GOLD", table_id,
            "FAILED", 0, 0, str(e), pipeline_start_time, ""
        )
        raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_dimprocedure_ingestion_with_logging(table_id, pipeline_run_id, pipeline_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
