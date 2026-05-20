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
table_id = 22
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

def gold_dimprovider_ingestion_with_logging(table_id, pipeline_run_id, pipeline_name):
    start_time = datetime.now()

    try:
        target_table = f"{dimension_schema}.dim_provider"
        print(f"Loading {target_table} from silver.provider (current records)...")

        # CORRECTED: Extract provider data from silver.provider (1000 rows expected)
        source_df = (
            spark.table(f"{silver_schema}.provider")
            .filter(F.col("is_current") == "Y")
            .select(
                F.col("provider_id"),
                F.col("npi"),
                F.col("provider_name"),
                F.col("specialty"),
                F.col("city"),
                F.col("state"),
                F.col("contract_type"),
                F.col("payment_model"),
                F.col("status"),
                F.current_timestamp().alias("load_ts")
            )
        )

        print(f"Found {source_df.count()} current providers")
        
        if source_df.count() == 0:
            print("No current providers found in silver.provider")
            log_run(
                pipeline_run_id, pipeline_name, "GOLD", table_id,
                "SUCCESS", 0, 0, None, pipeline_start_time, "No providers found"
            )
            return

        # Check if target table exists
        target_exists = True
        try:
            spark.read.format("delta").table(target_table)
        except:
            target_exists = False

        if not target_exists:
            # Create new table with surrogate keys
            print(f"Creating new table {target_table}...")
            
            # Add surrogate keys
            w = Window.orderBy(F.lit(1))
            source_df = source_df.withColumn(
                "provider_sk",
                F.row_number().over(w).cast("long")
            )
            
            # Reorder columns
            source_df = source_df.select(
                "provider_sk", "provider_id", "npi", "provider_name",
                "specialty", "city", "state", "contract_type",
                "payment_model", "status", "load_ts"
            )
            
            source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
            print(f"Created {target_table} with {source_df.count()} providers")
            
        else:
            # CORRECTED: Use natural key merge (provider_id) - this should create 1000 rows
            print(f"Merging into existing table {target_table} using natural key...")
            
            target_delta = DeltaTable.forName(spark, target_table)
            
            # Get max provider_sk for new records
            max_sk = (
                spark.table(target_table)
                .selectExpr("coalesce(max(provider_sk), 0) as max_sk")
                .collect()[0]["max_sk"]
            )
            
            # Find new providers (not in target)
            existing_providers = spark.table(target_table).select("provider_id")
            new_providers = source_df.join(existing_providers, "provider_id", "left_anti")
            
            if new_providers.count() > 0:
                # Add surrogate keys to new providers
                w = Window.orderBy(F.lit(1))
                new_providers = new_providers.withColumn(
                    "provider_sk",
                    (F.row_number().over(w) + F.lit(max_sk)).cast("long")
                )
                
                # Reorder columns
                new_providers = new_providers.select(
                    "provider_sk", "provider_id", "npi", "provider_name",
                    "specialty", "city", "state", "contract_type",
                    "payment_model", "status", "load_ts"
                )
                
                # Insert new providers
                new_providers.write.format("delta").mode("append").saveAsTable(target_table)
                print(f"Added {new_providers.count()} new providers")
                
                # Update existing providers (if needed)
                target_delta.alias("tgt").merge(
                    source_df.alias("src"),
                    "tgt.provider_id = src.provider_id"  # Natural key merge
                ).whenMatchedUpdate(set={
                    "npi": "src.npi",
                    "provider_name": "src.provider_name",
                    "specialty": "src.specialty",
                    "city": "src.city",
                    "state": "src.state",
                    "contract_type": "src.contract_type",
                    "payment_model": "src.payment_model",
                    "status": "src.status",
                    "load_ts": "src.load_ts"
                }).execute()
            else:
                print("No new providers to add")

        final_count = spark.table(target_table).count()
        print(f"{target_table} row count: {final_count}")
        
        # VALIDATION: Should be 1000 rows
        if final_count == 1000:
            print("✅ SUCCESS: dim_provider has correct row count (1000)")
        else:
            print(f"⚠️  WARNING: Expected 1000 rows, got {final_count}")

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

gold_dimprovider_ingestion_with_logging(table_id, pipeline_run_id, pipeline_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
