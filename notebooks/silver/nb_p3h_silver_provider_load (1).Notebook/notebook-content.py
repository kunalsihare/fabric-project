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

%run nb_p3h_silver_config

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

pipeline_run_id = ""
pipeline_name = ""
table_id = 10
pipeline_start_time = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

var_lib = notebookutils.variableLibrary.getLibrary("vl_p3h_p3h_variables")
config_schema  = var_lib.getVariable("config_schema_name")
bronze_schema  = var_lib.getVariable("bronze_schema_name")
silver_schema  = var_lib.getVariable("silver_schema_name")
log_schema     = var_lib.getVariable("log_schema_name")

pipeline_start_time = datetime.fromisoformat(pipeline_start_time.replace("Z", "+00:00"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def prepare_silver_provider():
    print("Preparing silver.provider from bronze.provider_master + bronze.provider_contract...")

    provider_df = spark.table(f"{bronze_schema}.provider_master")
    contract_df = spark.table(f"{bronze_schema}.provider_contract")

    w = Window.partitionBy("provider_id").orderBy(F.col("start_date").desc())
    latest_contract = (
        contract_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    df = (
        provider_df.alias("p")
        .join(latest_contract.alias("c"), "provider_id", "left")
        .select(
            F.col("p.provider_id"),
            F.col("p.npi"),
            F.col("p.provider_name"),
            F.col("p.specialty"),
            F.col("p.city"),
            F.col("p.state"),
            F.col("c.contract_type"),
            F.col("c.payment_model"),
            F.col("c.status"),
            F.col("p.file_name"),
            F.col("p.file_modification_time"),
            F.current_timestamp().alias("load_ts")
        )
    )
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def silver_provider_ingestion_with_logging(table_id, pipeline_run_id, pipeline_name):
    start_time = datetime.now()

    try:
        target_table = f"{silver_schema}.provider"
        business_key = "provider_id"
        surrogate_key = "provider_sk"

        df = prepare_silver_provider()
        source_count = df.count()
        print(f"Source record count: {source_count}")

        # Apply SCD2 Type-2 logic FIRST
        current_date = datetime.now().date()

        hash_cols = [
            c for c in df.columns
            if c not in ["file_name", "file_modification_time", "load_ts"]
        ]

        df = (
            df
            .withColumn("hash_col",   F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in hash_cols])))
            .withColumn("start_date", F.lit(str(current_date)).cast("date"))
            .withColumn("end_date",   F.lit("9999-12-31").cast("date"))
            .withColumn("is_current", F.lit("Y"))
        )

        # Filter to only NEW or CHANGED records before DQ
        # This prevents duplicate business key DQ failures
        if spark.table(target_table).count() > 0:
            existing_records = (
                spark.table(target_table)
                .filter(F.col("is_current") == "Y")
                .select(business_key, F.col("hash_col").alias("hash_col_right"))
            )
            
            # Only keep records that are new or have changed data
            df_for_dq = (
                df
                .join(existing_records, business_key, "left")
                .filter(
                    (F.col("hash_col_right").isNull()) |  # New provider
                    (F.col("hash_col") != F.col("hash_col_right"))  # Changed provider
                )
                .drop("hash_col_right")
            )
        else:
            df_for_dq = df  # First load - all records are new

        dq_count = df_for_dq.count()
        print(f"Records for DQ check (new/changed only): {dq_count}")

        if dq_count > 0:
            print(f"Running DQ checks for table_id={table_id} on NEW/CHANGED records...")
            df_for_dq = run_metadata_dq(df_for_dq, table_id, pipeline_run_id, target_table)
            clean_count = df_for_dq.count()
            print(f"Clean record count after DQ: {clean_count}")
        else:
            print("No new or changed records - skipping DQ check")
            clean_count = 0

        initial_count = spark.table(target_table).count()

        # Use the full dataframe for MERGE (not just DQ-filtered one)
        target_delta = DeltaTable.forName(spark, target_table)

        target_delta.alias("tgt").merge(
            df.alias("src"),
            f"tgt.{business_key} = src.{business_key} AND tgt.is_current = 'Y' AND tgt.hash_col != src.hash_col"
        ).whenMatchedUpdate(set={
            "is_current": F.lit("N"),
            "end_date":   F.lit(str(current_date)).cast("date"),
            "load_ts":    F.current_timestamp()
        }).execute()

        existing_current = (
            spark.table(target_table)
            .filter(F.col("is_current") == "Y")
            .select(business_key, "hash_col")
        )

        records_to_insert = (
            df
            .join(existing_current.withColumnRenamed("hash_col", "_existing_hash"), business_key, "left")
            .filter(
                F.col("_existing_hash").isNull() |
                (F.col("hash_col") != F.col("_existing_hash"))
            )
            .drop("_existing_hash")
        )

        insert_count = records_to_insert.count()
        print(f"Records to insert into {target_table}: {insert_count}")

        if insert_count > 0:
            max_sk = (
                spark.table(target_table)
                .selectExpr(f"coalesce(max({surrogate_key}), 0) as max_sk")
                .collect()[0]["max_sk"]
            )
            w_sk = Window.orderBy(F.lit(1))
            records_to_insert = records_to_insert.withColumn(
                surrogate_key,
                (F.row_number().over(w_sk) + F.lit(max_sk)).cast("long")
            )
            records_to_insert.write.mode("append").saveAsTable(target_table)

        print(f"SCD2 load complete for {target_table}.")
        final_count = spark.table(target_table).count()

        log_run(
            pipeline_run_id, pipeline_name, "SILVER", table_id,
            "SUCCESS", source_count, final_count - initial_count,
            None, pipeline_start_time, ""
        )

    except Exception as e:
        print(traceback.format_exc())
        log_run(
            pipeline_run_id, pipeline_name, "SILVER", table_id,
            "FAILED", 0, 0, str(e), pipeline_start_time, ""
        )
        raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# def silver_provider_ingestion_with_logging(table_id, pipeline_run_id, pipeline_name):
#     start_time = datetime.now()

#     try:
#         target_table = f"{silver_schema}.provider"
#         business_key = "provider_id"
#         surrogate_key = "provider_sk"

#         df = prepare_silver_provider()
#         source_count = df.count()
#         print(f"Source record count: {source_count}")

#         print(f"Running DQ checks for table_id={table_id}...")
#         df = run_metadata_dq(df, table_id, pipeline_run_id,target_table)
#         clean_count = df.count()
#         print(f"Clean record count after DQ: {clean_count}")

#         initial_count = spark.table(target_table).count()

#         # Apply SCD2 Type-2 logic
#         current_date = datetime.now().date()

#         hash_cols = [
#             c for c in df.columns
#             if c not in ["file_name", "file_modification_time", "load_ts"]
#         ]

#         df = (
#             df
#             .withColumn("hash_col",   F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in hash_cols])))
#             .withColumn("start_date", F.lit(str(current_date)).cast("date"))
#             .withColumn("end_date",   F.lit("9999-12-31").cast("date"))
#             .withColumn("is_current", F.lit("Y"))
#         )

#         target_delta = DeltaTable.forName(spark, target_table)

#         target_delta.alias("tgt").merge(
#             df.alias("src"),
#             f"tgt.{business_key} = src.{business_key} AND tgt.is_current = 'Y' AND tgt.hash_col != src.hash_col"
#         ).whenMatchedUpdate(set={
#             "is_current": F.lit("N"),
#             "end_date":   F.lit(str(current_date)).cast("date"),
#             "load_ts":    F.current_timestamp()
#         }).execute()

#         existing_current = (
#             spark.table(target_table)
#             .filter(F.col("is_current") == "Y")
#             .select(business_key, "hash_col")
#         )

#         records_to_insert = (
#             df
#             .join(existing_current.withColumnRenamed("hash_col", "_existing_hash"), business_key, "left")
#             .filter(
#                 F.col("_existing_hash").isNull() |
#                 (F.col("hash_col") != F.col("_existing_hash"))
#             )
#             .drop("_existing_hash")
#         )

#         insert_count = records_to_insert.count()
#         print(f"Records to insert into {target_table}: {insert_count}")

#         if insert_count > 0:
#             max_sk = (
#                 spark.table(target_table)
#                 .selectExpr(f"coalesce(max({surrogate_key}), 0) as max_sk")
#                 .collect()[0]["max_sk"]
#             )
#             w_sk = Window.orderBy(F.lit(1))
#             records_to_insert = records_to_insert.withColumn(
#                 surrogate_key,
#                 (F.row_number().over(w_sk) + F.lit(max_sk)).cast("long")
#             )
#             records_to_insert.write.mode("append").saveAsTable(target_table)

#         print(f"SCD2 load complete for {target_table}.")
#         final_count = spark.table(target_table).count()

#         log_run(
#             pipeline_run_id, pipeline_name, "SILVER", table_id,
#             "SUCCESS", source_count, final_count - initial_count,
#             None, pipeline_start_time, ""
#         )

#     except Exception as e:
#         print(traceback.format_exc())
#         log_run(
#             pipeline_run_id, pipeline_name, "SILVER", table_id,
#             "FAILED", 0, 0, str(e), pipeline_start_time, ""
#         )
#         raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_provider_ingestion_with_logging(table_id, pipeline_run_id, pipeline_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
