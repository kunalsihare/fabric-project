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

pipeline_run_id      = ""
pipeline_name        = ""
table_id = 23
pipeline_start_time  = ""

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

def gold_dimplan_ingestion_with_logging(table_id, pipeline_run_id, pipeline_name):
    start_time = datetime.now()

    try:
        target_table = f"{dimension_schema}.dim_plan"
        print(f"Loading {target_table} from silver.member...")

        # CORRECTED: Extract unique plans from silver.member (should be 3 unique plans)
        source_df = (
            spark.table(f"{silver_schema}.member")
            .filter(F.col("is_current") == "Y")
            .select(
                F.col("plan_id"),
                F.col("effective_date"),
                F.col("termination_date")
            )
            .filter(F.col("plan_id").isNotNull())
            .distinct()
            .withColumn("plan_name", F.concat(F.lit("Plan "), F.col("plan_id")))
            .withColumn("plan_type", F.lit("Health Insurance"))
            .withColumn("coverage_type", F.lit("Medical"))
            .withColumn("load_ts", F.current_timestamp())
        )

        print(f"Found {source_df.count()} unique plans")
        
        # VALIDATION: Should be 3 rows based on data analysis
        if source_df.count() == 3:
            print("✅ EXPECTED: Found 3 unique plans (matches data analysis)")
        elif source_df.count() == 0:
            print("No plans found in silver.member")
            log_run(
                pipeline_run_id, pipeline_name, "GOLD", table_id,
                "SUCCESS", 0, 0, None, pipeline_start_time, "No plans found"
            )
            return
        else:
            print(f"⚠️  INFO: Found {source_df.count()} plans (expected 3)")

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
                "plan_sk",
                F.row_number().over(w).cast("long")
            )
            
            # Reorder columns
            source_df = source_df.select(
                "plan_sk", "plan_id", "plan_name", "plan_type",
                "coverage_type", "effective_date", "termination_date", "load_ts"
            )
            
            source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
            print(f"Created {target_table} with {source_df.count()} plans")
            
        else:
            # CORRECTED: Use natural key merge (plan_id) instead of surrogate key
            print(f"Merging into existing table {target_table} using natural key...")
            
            target_delta = DeltaTable.forName(spark, target_table)
            
            # Get existing plans to avoid duplicates
            existing_plans = (
                spark.table(target_table)
                .select("plan_id")
                .distinct()
            )
            
            # Filter out existing plans
            new_plans = source_df.join(
                existing_plans, 
                "plan_id", 
                "left_anti"
            )
            
            if new_plans.count() > 0:
                # Add surrogate keys for new plans
                max_sk = (
                    spark.table(target_table)
                    .selectExpr("coalesce(max(plan_sk), 0) as max_sk")
                    .collect()[0]["max_sk"]
                )
                
                w = Window.orderBy(F.lit(1))
                new_plans = new_plans.withColumn(
                    "plan_sk",
                    (F.row_number().over(w) + F.lit(max_sk)).cast("long")
                )
                
                # Reorder columns
                new_plans = new_plans.select(
                    "plan_sk", "plan_id", "plan_name", "plan_type",
                    "coverage_type", "effective_date", "termination_date", "load_ts"
                )
                
                # Insert new plans
                new_plans.write.format("delta").mode("append").saveAsTable(target_table)
                print(f"Added {new_plans.count()} new plans")
                
                # Update existing plans (if needed)
                target_delta.alias("tgt").merge(
                    source_df.alias("src"),
                    "tgt.plan_id = src.plan_id"  # CORRECTED: Natural key merge
                ).whenMatchedUpdate(set={
                    "plan_name": "src.plan_name",
                    "plan_type": "src.plan_type",
                    "coverage_type": "src.coverage_type",
                    "effective_date": "src.effective_date",
                    "termination_date": "src.termination_date",
                    "load_ts": "src.load_ts"
                }).execute()
            else:
                print("No new plans to add")

        final_count = spark.table(target_table).count()
        print(f"{target_table} row count: {final_count}")
        
        # VALIDATION: Should be 3 rows based on actual data
        if final_count == 3:
            print("SUCCESS: dim_plan has correct row count (3 - matches source data)")
        else:
            print(f"INFO: dim_plan has {final_count} rows (source data has 3 unique plans)")

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

gold_dimplan_ingestion_with_logging(table_id, pipeline_run_id, pipeline_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
