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
table_id = 0
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

def prepare_silver_member():
    print("Preparing silver.member from bronze.member_master + bronze.member_enrollment...")

    member_df = spark.table(f"{bronze_schema}.member_master")
    enrollment_df = spark.table(f"{bronze_schema}.member_enrollment")

    w = Window.partitionBy("member_id").orderBy(F.col("effective_date").desc())
    latest_enrollment = (
        enrollment_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    df = (
        member_df.alias("m")
        .join(latest_enrollment.alias("e"), "member_id", "left")
        .select(
            F.col("m.member_id"),
            F.col("m.member_number"),
            F.col("m.first_name"),
            F.col("m.last_name"),
            F.col("m.gender"),
            F.col("m.dob"),
            F.col("m.city"),
            F.col("m.state"),
            F.col("e.plan_id"),
            F.col("e.enrollment_status"),
            F.col("e.effective_date"),
            F.col("e.termination_date"),
            F.col("m.file_name"),
            F.col("m.file_modification_time"),
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

def prepare_silver_claim():
    print("Preparing silver.claim from bronze.claim_header + bronze.claim_line...")

    header_df = spark.table(f"{bronze_schema}.claim_header")
    line_df   = spark.table(f"{bronze_schema}.claim_line")

    w = Window.partitionBy("claim_id").orderBy("claim_line_id")
    line_with_num = line_df.withColumn("line_number", F.row_number().over(w))

    df = (
        header_df.alias("h")
        .join(line_with_num.alias("l"), F.col("h.claim_id") == F.col("l.claim_id"), "inner")
        .select(
            F.col("h.claim_id"),
            F.col("h.member_id"),
            F.col("h.provider_id"),
            F.col("h.claim_date"),
            F.col("l.line_number").cast("int"),
            F.col("l.procedure_code"),
            F.col("l.line_amount").alias("amount"),
            F.col("l.file_name"),
            F.col("l.file_modification_time"),
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

def prepare_silver_encounter():
    print("Preparing silver.encounter from bronze.encounter...")

    df = (
        spark.table(f"{bronze_schema}.encounter")
        .select(
            F.col("encounter_id"),
            F.col("member_id"),
            F.col("provider_id"),
            F.col("encounter_date"),
            F.col("diagnosis_code"),
            F.col("file_name"),
            F.col("file_modification_time"),
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

def prepare_silver_payment():
    print("Preparing silver.payment from bronze.capitation_payment...")

    df = (
        spark.table(f"{bronze_schema}.capitation_payment")
        .select(
            F.col("payment_id"),
            F.col("provider_id"),
            F.to_date(
                F.concat(F.col("payment_month"), F.lit("-01")), "yyyy-MM-dd"
            ).alias("payment_date"),
            F.col("total_payment").alias("amount"),
            F.col("file_name"),
            F.col("file_modification_time"),
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

SILVER_TABLE_MAP = {
    9:  {"table_name": "member",    "prepare_fn": prepare_silver_member,    "is_scd2": True,  "business_key": "member_id",   "surrogate_key": "member_sk"},
    10: {"table_name": "provider",  "prepare_fn": prepare_silver_provider,  "is_scd2": True,  "business_key": "provider_id", "surrogate_key": "provider_sk"},
    11: {"table_name": "claim",     "prepare_fn": prepare_silver_claim,     "is_scd2": False, "business_key": "claim_id",    "surrogate_key": None},
    12: {"table_name": "encounter", "prepare_fn": prepare_silver_encounter, "is_scd2": False, "business_key": "encounter_id", "surrogate_key": None},
    13: {"table_name": "payment",   "prepare_fn": prepare_silver_payment,   "is_scd2": False, "business_key": "payment_id",  "surrogate_key": None},
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def apply_scd2_load(new_df, target_table, business_key, surrogate_key):
    print(f"Applying SCD Type-2 merge on {target_table} (business_key={business_key})...")

    current_date = datetime.now().date()

    hash_cols = [
        c for c in new_df.columns
        if c not in ["file_name", "file_modification_time", "load_ts"]
    ]

    new_df = (
        new_df
        .withColumn("hash_col",   F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in hash_cols])))
        .withColumn("start_date", F.lit(str(current_date)).cast("date"))
        .withColumn("end_date",   F.lit("9999-12-31").cast("date"))
        .withColumn("is_current", F.lit("Y"))
    )

    target_delta = DeltaTable.forName(spark, target_table)

    target_delta.alias("tgt").merge(
        new_df.alias("src"),
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
        new_df
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def silver_ingestion_with_logging(table_id, pipeline_run_id, pipeline_name):
    start_time = datetime.now()

    try:
        if table_id not in SILVER_TABLE_MAP:
            raise Exception(f"No silver transformation defined for table_id={table_id}")

        cfg          = SILVER_TABLE_MAP[table_id]
        table_name   = cfg["table_name"]
        prepare_fn   = cfg["prepare_fn"]
        is_scd2      = cfg["is_scd2"]
        business_key = cfg["business_key"]
        surrogate_key = cfg["surrogate_key"]

        target_table = f"{silver_schema}.{table_name}"

        df = prepare_fn()
        source_count = df.count()
        print(f"Source record count: {source_count}")

        print(f"Running DQ checks for table_id={table_id}...")
        df = run_metadata_dq(df, table_id, pipeline_run_id,target_table)
        clean_count = df.count()
        print(f"Clean record count after DQ: {clean_count}")

        initial_count = spark.table(target_table).count()

        if is_scd2:
            apply_scd2_load(df, target_table, business_key, surrogate_key)
        else:
            df.write.mode("append").saveAsTable(target_table)
            print(f"Append load complete for {target_table}.")

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

silver_ingestion_with_logging(table_id, pipeline_run_id, pipeline_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
