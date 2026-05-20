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

var_lib = notebookutils.variableLibrary.getLibrary("vl_p3h_p3h_variables")
silver_schema = var_lib.getVariable("silver_schema_name")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_schema}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_schema}.member (
    member_sk              BIGINT,
    member_id              STRING,
    member_number          BIGINT,
    first_name             STRING,
    last_name              STRING,
    gender                 STRING,
    dob                    DATE,
    city                   STRING,
    state                  STRING,
    plan_id                STRING,
    enrollment_status      STRING,
    effective_date         DATE,
    termination_date       DATE,
    file_name              STRING,
    file_modification_time DATE,
    load_ts                TIMESTAMP,
    hash_col               STRING,
    start_date             DATE,
    end_date               DATE,
    is_current             STRING
)
USING DELTA;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_schema}.provider (
    provider_sk            BIGINT,
    provider_id            STRING,
    npi                    BIGINT,
    provider_name          STRING,
    specialty              STRING,
    city                   STRING,
    state                  STRING,
    contract_type          STRING,
    payment_model          STRING,
    status                 STRING,
    file_name              STRING,
    file_modification_time DATE,
    load_ts                TIMESTAMP,
    hash_col               STRING,
    start_date             DATE,
    end_date               DATE,
    is_current             STRING
)
USING DELTA;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_schema}.claim (
    claim_id               STRING,
    member_id              STRING,
    provider_id            STRING,
    claim_date             DATE,
    line_number            INT,
    procedure_code         STRING,
    amount                 DOUBLE,
    file_name              STRING,
    file_modification_time DATE,
    load_ts                TIMESTAMP
)
USING DELTA;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_schema}.encounter (
    encounter_id           STRING,
    member_id              STRING,
    provider_id            STRING,
    encounter_date         DATE,
    diagnosis_code         STRING,
    file_name              STRING,
    file_modification_time DATE,
    load_ts                TIMESTAMP
)
USING DELTA;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_schema}.payment (
    payment_id             STRING,
    provider_id            STRING,
    payment_date           DATE,
    amount                 DOUBLE,
    file_name              STRING,
    file_modification_time DATE,
    load_ts                TIMESTAMP
)
USING DELTA;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
