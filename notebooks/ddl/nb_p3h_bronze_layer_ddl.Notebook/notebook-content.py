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
bronze_schema = var_lib.getVariable("bronze_schema_name")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


spark.sql(f"""
CREATE  TABLE IF NOT EXISTS {bronze_schema}.member_master (
    member_id        STRING,
    member_number    BIGINT,
    first_name       STRING,
    last_name        STRING,
    gender           STRING,
    dob              DATE,
    city             STRING,
    state            STRING,
    created_date     DATE,
    -- Audit columns
    load_ts                TIMESTAMP,
    file_name              STRING,
    file_modification_time DATE
)
USING DELTA
PARTITIONED BY (file_modification_time);
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {bronze_schema}.member_enrollment (
    enrollment_id     STRING,
    member_id         STRING,
    plan_id           STRING,
    enrollment_status STRING,
    effective_date    DATE,
    termination_date  DATE,
    coverage_type     STRING,
    load_date         DATE,
    source_system     STRING,

    -- Audit columns
    load_ts                TIMESTAMP,
    file_name              STRING,
    file_modification_time DATE
)
USING DELTA
PARTITIONED BY (file_modification_time);
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS TABLE {bronze_schema}.provider_master (
    provider_id   STRING,
    npi           BIGINT,
    provider_name STRING,
    specialty     STRING,
    entity_type   STRING,
    city          STRING,
    state         STRING,
    active_flag   STRING,
    created_date  DATE,

    -- Audit columns
    load_ts                TIMESTAMP,
    file_name              STRING,
    file_modification_time DATE
)
USING DELTA
PARTITIONED BY (file_modification_time);
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {bronze_schema}.provider_contract (
    contract_id   STRING,
    provider_id   STRING,
    contract_type STRING,
    start_date    DATE,
    end_date      DATE,
    payment_model STRING,
    status        STRING,
    load_date     DATE,

    -- Audit columns
    load_ts                TIMESTAMP,
    file_name              STRING,
    file_modification_time DATE
)
USING DELTA
PARTITIONED BY (file_modification_time);
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {bronze_schema}.claim_header (
    claim_id      STRING,
    claim_number  BIGINT,
    member_id     STRING,
    provider_id   STRING,
    claim_date    DATE,
    claim_status  STRING,
    total_amount  DOUBLE,
    paid_amount   DOUBLE,
    load_date     DATE,

    -- Audit columns
    load_ts                TIMESTAMP,
    file_name              STRING,
    file_modification_time DATE
)
USING DELTA
PARTITIONED BY (file_modification_time);
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


spark.sql(f"""
CREATE TABLE IF NOT EXISTS {bronze_schema}.claim_line (
    claim_line_id  STRING,
    claim_id       STRING,
    procedure_code STRING,
    diagnosis_code STRING,
    line_amount    DOUBLE,
    units           INT,
    service_date   DATE,
    line_status    STRING,
    load_date      DATE,

    -- Audit columns
    load_ts                TIMESTAMP,
    file_name              STRING,
    file_modification_time DATE
)
USING DELTA
PARTITIONED BY (file_modification_time);
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {bronze_schema}.encounter (
    encounter_id    STRING,
    member_id       STRING,
    provider_id     STRING,
    encounter_date  DATE,
    encounter_type  STRING,
    diagnosis_code  STRING,
    procedure_code  STRING,
    encounter_cost  DOUBLE,
    load_date       DATE,

    -- Audit columns
    load_ts                TIMESTAMP,
    file_name              STRING,
    file_modification_time DATE
)
USING DELTA
PARTITIONED BY (file_modification_time);
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {bronze_schema}.capitation_payment (
    payment_id      STRING,
    provider_id     STRING,
    payment_month   STRING,
    member_count    INT,
    rate_pmpm       DOUBLE,
    total_payment   DOUBLE,
    payment_status  STRING,
    load_date       DATE,

    -- Audit columns
    load_ts                TIMESTAMP,
    file_name              STRING,
    file_modification_time DATE
)
USING DELTA
PARTITIONED BY (file_modification_time);
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
