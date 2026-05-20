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

var_lib           = notebookutils.variableLibrary.getLibrary("vl_p3h_p3h_variables")
dimension_schema  = var_lib.getVariable("dimension_schema_name")
fact_schema       = var_lib.getVariable("fact_schema_name")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create Schemas

# CELL ********************

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {dimension_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fact_schema}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dimension Tables

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {dimension_schema}.dim_member (
    member_sk         BIGINT    NOT NULL,
    member_id         STRING    NOT NULL,
    member_number     BIGINT,
    first_name        STRING,
    last_name         STRING,
    gender            STRING,
    dob               DATE,
    city              STRING,
    state             STRING,
    plan_id           STRING,
    enrollment_status STRING,
    effective_date    DATE,
    termination_date  DATE,
    load_ts           TIMESTAMP
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
CREATE TABLE IF NOT EXISTS {dimension_schema}.dim_provider (
    provider_sk    BIGINT  NOT NULL,
    provider_id    STRING  NOT NULL,
    npi            BIGINT,
    provider_name  STRING,
    specialty      STRING,
    city           STRING,
    state          STRING,
    contract_type  STRING,
    payment_model  STRING,
    status         STRING,
    load_ts        TIMESTAMP
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
CREATE TABLE IF NOT EXISTS {dimension_schema}.dim_plan (
    plan_sk         BIGINT    NOT NULL,
    plan_id         STRING    NOT NULL,
    plan_name       STRING,
    plan_type       STRING,
    coverage_type   STRING,
    effective_date  DATE,
    termination_date DATE,
    load_ts         TIMESTAMP
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
CREATE TABLE IF NOT EXISTS {dimension_schema}.dim_diagnosis (
    diagnosis_sk     BIGINT  NOT NULL,
    diagnosis_code   STRING  NOT NULL,
    diagnosis_desc   STRING,
    category         STRING,
    severity         STRING,
    load_ts          TIMESTAMP
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
CREATE TABLE IF NOT EXISTS {dimension_schema}.dim_procedure (
    procedure_sk     BIGINT  NOT NULL,
    procedure_code   STRING  NOT NULL,
    procedure_desc   STRING,
    category         STRING,
    cost_category    STRING,
    load_ts          TIMESTAMP
)
USING DELTA;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fact Table

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fact_schema}.fact_healthcare (
    fact_healthcare_sk   BIGINT    NOT NULL,
    claim_id             STRING,
    encounter_id         STRING,
    payment_id           STRING,
    member_sk            BIGINT,
    provider_sk          BIGINT,
    plan_sk              BIGINT,
    diagnosis_sk         BIGINT,
    procedure_sk         BIGINT,
    date_key             INT,
    line_number          INT,
    amount               DOUBLE,
    transaction_type     STRING,
    load_ts              TIMESTAMP
)
USING DELTA;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
