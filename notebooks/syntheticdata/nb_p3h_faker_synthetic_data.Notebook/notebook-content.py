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

var_lib = notebookutils.variableLibrary.getLibrary("vl_p3h_p3h_variables")
LH_ID = var_lib.getVariable("LH_ID")
WS_ID = var_lib.getVariable("WS_ID")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pip install faker

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType
from datetime import date, timedelta
from faker import Faker

# Initialize Faker
faker = Faker("en_IN")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

udf_first_name = F.udf(lambda: faker.first_name(), StringType())
udf_last_name = F.udf(lambda: faker.last_name(), StringType())
udf_dob = F.udf(lambda: faker.date_of_birth(minimum_age=18, maximum_age=85), DateType())
udf_city = F.udf(lambda: faker.city(), StringType())
udf_state = F.udf(lambda: faker.state(), StringType())
udf_company = F.udf(lambda: faker.company(), StringType()) # For realistic provider names

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def deterministic_id(*cols):
    """
    Creates a stable deterministic hash ID.
    Same input -> same output across runs & days
    """
    return F.sha2(F.concat_ws("||", *cols), 256)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BASE_PATH = f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}/Files/raw"
DAYS = 1
RECORDS = 12000
START_DATE = date.today()

DATASET_CONFIG = {
    ("member", "member_master"): {"load_type": "FULL", "format": "parquet"},
    ("member", "member_enrollment"): {"load_type": "INCREMENTAL", "format": "csv"},
    ("provider", "provider_master"): {"load_type": "FULL", "format": "parquet"},
    ("provider", "provider_contract"): {"load_type": "FULL", "format": "csv"},
    ("claims", "claim_header"): {"load_type": "INCREMENTAL", "format": "parquet"},
    ("claims", "claim_line"): {"load_type": "INCREMENTAL", "format": "parquet"},
    ("clinical", "encounter"): {"load_type": "INCREMENTAL", "format": "parquet"},
    ("finance", "capitation_payment"): {"load_type": "INCREMENTAL", "format": "csv"},
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def generate_member_master(spark, rows, run_date):
    df = spark.range(rows)

    return (
        df.withColumn("member_number", F.col("id") + 100000)
          .withColumn("member_id", deterministic_id(F.col("member_number").cast("string")))
          .withColumn("first_name", udf_first_name())
          .withColumn("last_name", udf_last_name())
          # Using Spark rand() for row-by-row dynamic gender
          .withColumn("gender", F.expr("CASE WHEN rand() > 0.5 THEN 'M' ELSE 'F' END"))
          .withColumn("dob", udf_dob())
          .withColumn("city", udf_city())
          .withColumn("state", udf_state())
          .withColumn("created_date", F.lit(run_date))
          .drop("id")
    )



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def generate_member_enrollment(spark, rows, run_date):
    df = spark.range(rows)

    return (
        df.withColumn("member_number", (F.col("id") % rows) + 100000)
          .withColumn("member_id", deterministic_id(F.col("member_number").cast("string")))
          .withColumn(
              "enrollment_id",
              deterministic_id(F.col("member_number").cast("string"), F.lit(str(run_date)))
          )
          .withColumn("plan_id", F.concat(F.lit("PLAN_"), (F.col("id") % 3)))
          .withColumn("enrollment_status", F.lit("ACTIVE"))
          .withColumn("effective_date", F.lit(run_date))
          .withColumn("termination_date", F.lit(None).cast("date"))
          # Replaced python random with Spark SQL array selection for row-by-row randomness
          .withColumn("coverage_type", F.expr("element_at(array('MA', 'HMO', 'PPO'), cast(rand() * 3 + 1 as int))"))
          .withColumn("load_date", F.lit(run_date))
          .withColumn("source_system", F.lit("FABRIC_SYNTH"))
          .drop("id")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def generate_provider_master(spark, rows, run_date):
    df = spark.range(rows)

    return (
        df.withColumn("npi", F.col("id") + 1000000000)
          .withColumn("provider_id", deterministic_id(F.col("npi").cast("string")))
          # Replaced static PROV_ string with realistic Faker Company/Hospital names
          .withColumn("provider_name", udf_company())
          .withColumn("specialty", F.expr("CASE WHEN rand() > 0.5 THEN 'IM' ELSE 'FAM' END"))
          .withColumn("entity_type", F.expr("CASE WHEN rand() > 0.5 THEN 'INDIVIDUAL' ELSE 'ORGANIZATION' END"))
          .withColumn("city", udf_city())
          .withColumn("state", udf_state())
          .withColumn("active_flag", F.lit("Y"))
          .withColumn("created_date", F.lit(run_date))
          .drop("id")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def generate_provider_contract(spark, rows, run_date):
    df = spark.range(rows)

    return (
        df.withColumn("npi", (F.col("id") % rows) + 1000000000)
          .withColumn("provider_id", deterministic_id(F.col("npi").cast("string")))
          .withColumn(
              "contract_id",
              deterministic_id(F.col("npi").cast("string"), F.lit("CAP"))
          )
          .withColumn("contract_type", F.lit("CAPITATION"))
          .withColumn("start_date", F.lit(run_date.replace(day=1)))
          .withColumn("end_date", F.lit(None).cast("date"))
          .withColumn("payment_model", F.lit("PMPM"))
          .withColumn("status", F.lit("ACTIVE"))
          .withColumn("load_date", F.lit(run_date))
          .drop("id")
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def generate_claim_header(spark, rows, run_date):
    df = spark.range(rows)

    return (
        df.withColumn("claim_number", F.col("id") + run_date.toordinal() * 100000)
          .withColumn("claim_id", deterministic_id(F.col("claim_number").cast("string")))
          .withColumn("member_number", (F.col("id") % rows) + 100000)
          .withColumn("member_id", deterministic_id(F.col("member_number").cast("string")))
          .withColumn("provider_npi", (F.col("id") % rows) + 1000000000)
          .withColumn("provider_id", deterministic_id(F.col("provider_npi").cast("string")))
          .withColumn("claim_date", F.lit(run_date))
          .withColumn("claim_status", F.lit("PAID"))
          .withColumn("total_amount", (F.col("id") % 5000 + 500))
          .withColumn("paid_amount", (F.col("id") % 4000 + 400))
          .withColumn("load_date", F.lit(run_date))
          .drop("id")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def generate_claim_line(spark, rows, run_date):
    df = spark.range(rows)

    return (
        df.withColumn("claim_number", (F.col("id") % rows) + 900000)
          .withColumn("claim_id", deterministic_id(F.col("claim_number").cast("string")))
          .withColumn(
              "claim_line_id",
              deterministic_id(F.col("claim_number").cast("string"), F.col("id").cast("string"))
          )
          .withColumn("procedure_code", F.lit("99213"))
          .withColumn("diagnosis_code", F.lit("E11.9"))
          .withColumn("line_amount", (F.col("id") % 1200 + 100))
          .withColumn("units", F.lit(1))
          .withColumn("service_date", F.lit(run_date))
          .withColumn("line_status", F.lit("PAID"))
          .withColumn("load_date", F.lit(run_date))
          .drop("id")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def generate_encounter(spark, rows, run_date):
    df = spark.range(rows)

    return (
        df.withColumn("member_number", (F.col("id") % rows) + 100000)
          .withColumn("member_id", deterministic_id(F.col("member_number").cast("string")))
          .withColumn("provider_npi", (F.col("id") % rows) + 1000000000)
          .withColumn("provider_id", deterministic_id(F.col("provider_npi").cast("string")))
          .withColumn(
              "encounter_id",
              deterministic_id(F.col("member_number").cast("string"), F.lit(str(run_date)))
          )
          .withColumn("encounter_date", F.lit(run_date))
          .withColumn("encounter_type", F.lit("OUTPATIENT"))
          .withColumn("diagnosis_code", F.lit("E11.9"))
          .withColumn("procedure_code", F.lit("99213"))
          .withColumn("encounter_cost", (F.col("id") % 1500 + 200))
          .withColumn("load_date", F.lit(run_date))
          .drop("id")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def generate_capitation_payment(spark, rows, run_date):
    df = spark.range(rows)

    return (
        df.withColumn("provider_npi", (F.col("id") % rows) + 1000000000)
          .withColumn("provider_id", deterministic_id(F.col("provider_npi").cast("string")))
          .withColumn(
              "payment_id",
              deterministic_id(F.col("provider_npi").cast("string"), F.lit(str(run_date)))
          )
          .withColumn("payment_month", F.date_format(F.lit(run_date), "yyyyMM"))
          .withColumn("member_count", (F.col("id") % 300 + 50))
          .withColumn("rate_pmpm", F.lit(800))
          .withColumn("total_payment", F.col("member_count") * F.col("rate_pmpm"))
          .withColumn("payment_status", F.lit("GENERATED"))
          .withColumn("load_date", F.lit(run_date))
          .drop("id")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_data(df, domain, dataset, day):
    cfg = DATASET_CONFIG[(domain, dataset)]
    path = f"{BASE_PATH}/{domain}/{dataset}/"

    writer = df.coalesce(1).write.mode("append")

    if cfg["format"] == "parquet":
        writer.parquet(path)
    else:
        writer.option("header", "true").csv(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for day in range(1, DAYS + 1):
    run_date = START_DATE + timedelta(days=day - 1)

    # MEMBER
    write_data(generate_member_master(spark, RECORDS, run_date), "member", "member_master", day)
    write_data(generate_member_enrollment(spark, RECORDS, run_date), "member", "member_enrollment", day)

    # PROVIDER
    write_data(generate_provider_master(spark, RECORDS, run_date), "provider", "provider_master", day)
    write_data(generate_provider_contract(spark, RECORDS, run_date), "provider", "provider_contract", day)

    # CLAIMS
    write_data(generate_claim_header(spark, RECORDS, run_date), "claims", "claim_header", day)
    write_data(generate_claim_line(spark, RECORDS, run_date), "claims", "claim_line", day)

    # CLINICAL
    write_data(generate_encounter(spark, RECORDS, run_date), "clinical", "encounter", day)

    # FINANCE
    write_data(generate_capitation_payment(spark, RECORDS, run_date), "finance", "capitation_payment", day)

    print(f"✅ Day {day} generated successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
