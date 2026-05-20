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
table_id = 21
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

def gold_dimmember_ingestion_with_logging(table_id, pipeline_run_id, pipeline_name):
    start_time = datetime.now()

    try:
        target_table = f"{dimension_schema}.dim_member"
        print(f"Loading {target_table} from silver.member (current records)...")

        # CORRECTED: Extract current members from silver.member (should be 1000 rows)
        source_df = (
            spark.table(f"{silver_schema}.member")
            .filter(F.col("is_current") == "Y")
            .select(
                F.col("member_id"),
                F.col("member_number"),
                F.col("first_name"),
                F.col("last_name"),
                F.col("gender"),
                F.col("dob"),
                F.col("city"),
                F.col("state"),
                F.col("plan_id"),
                F.col("enrollment_status"),
                F.col("effective_date"),
                F.col("termination_date"),
                F.current_timestamp().alias("load_ts")
            )
        )

        print(f"Found {source_df.count()} current members")
        
        # VALIDATION: Should be 1000 rows based on data analysis
        if source_df.count() == 1000:
            print("✅ EXPECTED: Found 1000 current members (matches data analysis)")
        elif source_df.count() == 0:
            print("No current members found in silver.member")
            log_run(
                pipeline_run_id, pipeline_name, "GOLD", table_id,
                "SUCCESS", 0, 0, None, pipeline_start_time, "No members found"
            )
            return
        else:
            print(f"⚠️  INFO: Found {source_df.count()} members (expected 1000)")

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
                "member_sk",
                F.row_number().over(w).cast("long")
            )
            
            # Reorder columns
            source_df = source_df.select(
                "member_sk", "member_id", "member_number", "first_name",
                "last_name", "gender", "dob", "city", "state",
                "plan_id", "enrollment_status", "effective_date",
                "termination_date", "load_ts"
            )
            
            source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
            print(f"Created {target_table} with {source_df.count()} members")
            
        else:
            # CORRECTED: Use natural key merge (member_id) instead of surrogate key
            print(f"Merging into existing table {target_table} using natural key...")
            
            target_delta = DeltaTable.forName(spark, target_table)
            
            # Get existing members to avoid duplicates
            existing_members = (
                spark.table(target_table)
                .select("member_id")
                .distinct()
            )
            
            # Filter out existing members
            new_members = source_df.join(
                existing_members, 
                "member_id", 
                "left_anti"
            )
            
            if new_members.count() > 0:
                # Add surrogate keys for new members
                max_sk = (
                    spark.table(target_table)
                    .selectExpr("coalesce(max(member_sk), 0) as max_sk")
                    .collect()[0]["max_sk"]
                )
                
                w = Window.orderBy(F.lit(1))
                new_members = new_members.withColumn(
                    "member_sk",
                    (F.row_number().over(w) + F.lit(max_sk)).cast("long")
                )
                
                # Reorder columns
                new_members = new_members.select(
                    "member_sk", "member_id", "member_number", "first_name",
                    "last_name", "gender", "dob", "city", "state",
                    "plan_id", "enrollment_status", "effective_date",
                    "termination_date", "load_ts"
                )
                
                # Insert new members
                new_members.write.format("delta").mode("append").saveAsTable(target_table)
                print(f"Added {new_members.count()} new members")
                
                # Update existing members (if needed)
                target_delta.alias("tgt").merge(
                    source_df.alias("src"),
                    "tgt.member_id = src.member_id"  # CORRECTED: Natural key merge
                ).whenMatchedUpdate(set={
                    "member_number": "src.member_number",
                    "first_name": "src.first_name", 
                    "last_name": "src.last_name",
                    "gender": "src.gender",
                    "dob": "src.dob",
                    "city": "src.city",
                    "state": "src.state",
                    "plan_id": "src.plan_id",
                    "enrollment_status": "src.enrollment_status",
                    "effective_date": "src.effective_date",
                    "termination_date": "src.termination_date",
                    "load_ts": "src.load_ts"
                    }).execute()
            else:
                print("No new members to add")

        final_count = spark.table(target_table).count()
        print(f"{target_table} row count: {final_count}")
        
        # VALIDATION: Should be 1000 rows based on actual data
        if final_count == 1000:
            print("SUCCESS: dim_member has correct row count (1000 - matches source data)")
        else:
            print(f"INFO: dim_member has {final_count} rows (source data has 1000 unique members)")

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

gold_dimmember_ingestion_with_logging(table_id, pipeline_run_id, pipeline_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
