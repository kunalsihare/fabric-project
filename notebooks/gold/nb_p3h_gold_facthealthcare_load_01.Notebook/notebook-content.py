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
table_id = 40
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
fact_schema      = var_lib.getVariable("fact_schema_name")
log_schema       = var_lib.getVariable("log_schema_name")

pipeline_start_time = datetime.fromisoformat(pipeline_start_time.replace("Z", "+00:00")) if pipeline_start_time else datetime.now()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_dimdate():
    # Use shortcut to warehouse dimdate with correct column names and types
    return (
        spark.table("dimension_1.dimdate")
        .select(
            F.col("datekey").cast("int").alias("date_key"),  # Ensure integer type
            F.col("Date").alias("full_date")                # Keep date type
        )
    )


def get_max_sk(target_table, sk_col):
    return (
        spark.table(target_table)
        .selectExpr(f"coalesce(max({sk_col}), 0) as max_sk")
        .collect()[0]["max_sk"]
    )


def add_surrogate_key(df, sk_col, max_sk):
    w = Window.orderBy(F.lit(1))
    return df.withColumn(
        sk_col,
        (F.row_number().over(w) + F.lit(max_sk)).cast("long")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def gold_facthealthcare_ingestion_with_logging(table_id, pipeline_run_id, pipeline_name):
    start_time = datetime.now()

    try:
        target_table = f"{fact_schema}.fact_healthcare"
        print(f"Loading {target_table} from all silver tables...")

        # Get dimension mappings
        dim_member_df = (
            spark.table(f"{dimension_schema}.dim_member")
            .select("member_sk", "member_id")
        )
        dim_provider_df = (
            spark.table(f"{dimension_schema}.dim_provider")
            .select("provider_sk", "provider_id")
        )
        dim_plan_df = (
            spark.table(f"{dimension_schema}.dim_plan")
            .select("plan_sk", "plan_id")
        )
        dim_diagnosis_df = (
            spark.table(f"{dimension_schema}.dim_diagnosis")
            .select("diagnosis_sk", "diagnosis_code")
        )
        dim_procedure_df = (
            spark.table(f"{dimension_schema}.dim_procedure")
            .select("procedure_sk", "procedure_code")
        )
        dimdate_df = get_dimdate()

        # Get plan_id from member table (Alternative Join Strategy)
        print("Using Alternative Join Strategy: Getting plan_id from member table")
        member_with_plan = (
            spark.table(f"{silver_schema}.member")
            .filter(F.col("is_current") == "Y")
            .select("member_id", "plan_id")
        )

        # CORRECTED: Process claims with proper join logic (should fix 100% null provider_sk)
        print("Processing claims with corrected join logic...")
        claim_df = (
            spark.table(f"{silver_schema}.claim")
            # Join on natural keys first, then get surrogate keys
            .join(dim_member_df, "member_id", "left")
            .join(dim_provider_df, "provider_id", "left")
            .join(member_with_plan, "member_id", "left")  # Get plan_id from member
            .join(dim_plan_df, "plan_id", "left")
            .join(dim_procedure_df, "procedure_code", "left")
            .join(
                dimdate_df.withColumnRenamed("full_date", "claim_date"),
                "claim_date",
                "left"
            )
            .select(
                F.col("claim_id"),
                F.lit(None).alias("encounter_id"),
                F.lit(None).alias("payment_id"),
                F.col("member_sk"),
                F.col("provider_sk"),
                F.col("plan_sk"),
                F.lit(None).alias("diagnosis_sk"),
                F.col("procedure_sk"),
                F.col("date_key").cast("int"),  # Ensure integer type
                F.col("line_number").cast("int"),
                F.col("amount").cast("double"),
                F.lit("CLAIM").alias("transaction_type"),
                F.current_timestamp().alias("load_ts")
            )
        )

        # CORRECTED: Process encounters (should add 1000 encounter rows)
        print("Processing encounters...")
        encounter_df = (
            spark.table(f"{silver_schema}.encounter")
            .join(dim_member_df,   "member_id",   "left")
            .join(dim_provider_df, "provider_id", "left")
            .join(dim_diagnosis_df, "diagnosis_code", "left")
            .join(
                dimdate_df.withColumnRenamed("full_date", "encounter_date"),
                "encounter_date",
                "left"
            )
            .select(
                F.lit(None).alias("claim_id"),
                F.col("encounter_id"),
                F.lit(None).alias("payment_id"),
                F.col("member_sk"),
                F.col("provider_sk"),
                F.lit(None).alias("plan_sk"),
                F.col("diagnosis_sk"),
                F.lit(None).alias("procedure_sk"),
                F.col("date_key").cast("int"),  # Ensure integer type
                F.lit(None).alias("line_number"),
                F.lit(0.0).cast("double").alias("amount"),
                F.lit("ENCOUNTER").alias("transaction_type"),
                F.current_timestamp().alias("load_ts")
            )
        )

        # CORRECTED: Process payments (should add 1000 payment rows)
        print("Processing payments...")
        payment_df = (
            spark.table(f"{silver_schema}.payment")
            .join(dim_provider_df, "provider_id", "left")
            .join(
                dimdate_df.withColumnRenamed("full_date", "payment_date"),
                "payment_date",
                "left"
            )
            .select(
                F.lit(None).alias("claim_id"),
                F.lit(None).alias("encounter_id"),
                F.col("payment_id"),
                F.lit(None).alias("member_sk"),
                F.col("provider_sk"),
                F.lit(None).alias("plan_sk"),
                F.lit(None).alias("diagnosis_sk"),
                F.lit(None).alias("procedure_sk"),
                F.col("date_key").cast("int"),  # Ensure integer type
                F.lit(None).alias("line_number"),
                F.col("amount").cast("double"),
                F.lit("PAYMENT").alias("transaction_type"),
                F.current_timestamp().alias("load_ts")
            )
        )

        # Union all fact records
        combined_df = claim_df.unionByName(encounter_df).unionByName(payment_df)

        # Filter out records with no business keys
        combined_df = combined_df.filter(
            (F.col("claim_id").isNotNull()) |
            (F.col("encounter_id").isNotNull()) |
            (F.col("payment_id").isNotNull())
        )

        # DEBUG: Show data quality metrics
        print("=== DATA QUALITY METRICS ===")
        print(f"Combined rows: {combined_df.count()}")
        print(f"Claims: {claim_df.count()}")
        print(f"Encounters: {encounter_df.count()}")
        print(f"Payments: {payment_df.count()}")
        
        # Check null percentages
        total_rows = combined_df.count()
        for col_name in ["member_sk", "provider_sk", "plan_sk", "diagnosis_sk", "procedure_sk", "date_key"]:
            null_count = combined_df.filter(F.col(col_name).isNull()).count()
            null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
            print(f"{col_name}: {null_count} nulls ({null_pct:.1f}%)")

        # VALIDATION: Expected results based on data analysis
        expected_claims = 1000
        expected_encounters = 1000
        expected_payments = 1000
        expected_total = expected_claims + expected_encounters + expected_payments
        
        print(f"\n=== VALIDATION ===")
        print(f"Expected: {expected_total} total rows (Claims: {expected_claims}, Encounters: {expected_encounters}, Payments: {expected_payments})")
        print(f"Actual: {combined_df.count()} total rows (Claims: {claim_df.count()}, Encounters: {encounter_df.count()}, Payments: {payment_df.count()})")
        
        if combined_df.count() == expected_total:
            print("✅ SUCCESS: Fact table has expected row count")
        else:
            print(f"⚠️  WARNING: Expected {expected_total} rows, got {combined_df.count()}")

        # HASH-BASED DEDUPLICATION
        print("Applying hash-based deduplication...")
        
        # Create hash column for comprehensive duplicate detection
        key_columns = [
            "claim_id", "encounter_id", "payment_id", "line_number",
            "member_sk", "provider_sk", "plan_sk", "diagnosis_sk", 
            "procedure_sk", "date_key", "transaction_type"
        ]
        
        # Convert all key columns to strings for consistent hashing
        for col_name in key_columns:
            combined_df = combined_df.withColumn(
                col_name + "_str", 
                F.coalesce(F.col(col_name).cast("string"), F.lit(""))
            )
        
        string_columns = [col_name + "_str" for col_name in key_columns]
        
        # Create SHA-256 hash of concatenated key columns
        combined_df = combined_df.withColumn(
            "record_hash",
            F.sha2(F.concat_ws("|", *string_columns), 256)
        )
        
        # Remove temporary string columns
        for col_name in string_columns:
            combined_df = combined_df.drop(col_name)
        
        # Get existing hashes from target table
        if spark.table(target_table).count() > 0:
            existing_hashes = (
                spark.table(target_table)
                .withColumn(
                    "record_hash",
                    F.sha2(
                        F.concat_ws("|", 
                            F.coalesce(F.col("claim_id").cast("string"), F.lit("")),
                            F.coalesce(F.col("encounter_id").cast("string"), F.lit("")),
                            F.coalesce(F.col("payment_id").cast("string"), F.lit("")),
                            F.coalesce(F.col("line_number").cast("string"), F.lit("")),
                            F.coalesce(F.col("member_sk").cast("string"), F.lit("")),
                            F.coalesce(F.col("provider_sk").cast("string"), F.lit("")),
                            F.coalesce(F.col("plan_sk").cast("string"), F.lit("")),
                            F.coalesce(F.col("diagnosis_sk").cast("string"), F.lit("")),
                            F.coalesce(F.col("procedure_sk").cast("string"), F.lit("")),
                            F.coalesce(F.col("date_key").cast("string"), F.lit("")),
                            F.coalesce(F.col("transaction_type").cast("string"), F.lit(""))
                        ), 
                        256
                    )
                )
                .select("record_hash")
            )
            
            # Filter out records that already exist
            new_rows = combined_df.join(
                existing_hashes, 
                "record_hash", 
                "left_anti"
            )
        else:
            # If target table is empty, all records are new
            new_rows = combined_df
        
        # Remove hash column before insertion
        new_rows = new_rows.drop("record_hash")

        insert_count = new_rows.count()
        if insert_count > 0:
            max_sk   = get_max_sk(target_table, "fact_healthcare_sk")
            new_rows = add_surrogate_key(new_rows, "fact_healthcare_sk", max_sk)
            new_rows.write.mode("append").saveAsTable(target_table)
            print(f"Inserted {insert_count} rows into {target_table}.")
        else:
            print(f"No new rows for {target_table}.")

        log_run(
            pipeline_run_id, pipeline_name, "GOLD", table_id,
            "SUCCESS", insert_count, insert_count, None, pipeline_start_time, ""
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

gold_facthealthcare_ingestion_with_logging(table_id, pipeline_run_id, pipeline_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
