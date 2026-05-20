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

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

var_lib = notebookutils.variableLibrary.getLibrary("vl_p3h_p3h_variables")
config_schema  = var_lib.getVariable("config_schema_name")
WS_ID = var_lib.getVariable("WS_ID")
LH_ID = var_lib.getVariable("LH_ID")
CFG_CSV_BASE_PATH = f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}/Files/cfg_files"
print(CFG_CSV_BASE_PATH)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CFG_PK_MAP = {
    "cfg_source_details": ["table_id"],
    "cfg_table_schema": ["table_id"],
    "cfg_column_schema": ["column_id"],
    "cfg_maintenance_config": ["table_id"],
    "cfg_scd_type2_metadata": ["table_id"],
    "cfg_dq_rules": ["rule_id"] 
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def cast_df_to_delta_schema(
    source_df: DataFrame,
    target_df: DataFrame
) -> DataFrame:
    """
    Casts all source columns to match target Delta table schema.
    Extra columns are ignored; missing columns are added as NULLs.
    """

    target_schema = {field.name: field.dataType for field in target_df.schema}

    df = source_df
    for col_name, data_type in target_schema.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(data_type))
        else:
            print(col_name)
            df = df.withColumn(col_name, F.lit(None).cast(data_type))

    # Reorder columns exactly like Delta table
    df = df.select(list(target_schema.keys()))
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def merge_cfg_csv_to_delta(
    table_name: str,
    csv_base_path: str
):
    """
    Reads cfg CSV file, casts to Delta schema, and merges into Delta table.
    """

    if table_name not in CFG_PK_MAP:
        raise Exception(f"Primary key not defined for {table_name}")

    pk_cols = CFG_PK_MAP[table_name]
    csv_path = f"{csv_base_path}/{table_name}.csv"

    print(f"📥 Reading CSV: {csv_path}")

    # Read CSV WITHOUT trusting schema
    src_raw_df = (
        spark.read
             .option("header", "true")
             .csv(csv_path)
    )

    # Read target Delta table
    table_name = f"{config_schema}.{table_name}"
    delta_table = DeltaTable.forName(spark, table_name)
    tgt_df = delta_table.toDF()

    # ✅ Cast source data to Delta schema
    src_df = cast_df_to_delta_schema(src_raw_df, tgt_df)

    # Build merge condition
    merge_condition = " AND ".join(
        [f"t.{c} = s.{c}" for c in pk_cols]
    )

    update_expr = {c: f"s.{c}" for c in src_df.columns}
    insert_expr = {c: f"s.{c}" for c in src_df.columns}

    (
        delta_table.alias("t")
        .merge(
            src_df.alias("s"),
            merge_condition
        )
        .whenMatchedUpdate(set=update_expr)
        .whenNotMatchedInsert(values=insert_expr)
        .execute()
    )

    print(f"✅ {table_name} merged successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for cfg_table in CFG_PK_MAP.keys():
    merge_cfg_csv_to_delta(
        table_name=cfg_table,
        csv_base_path=CFG_CSV_BASE_PATH
    )



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#For Single Table
'''merge_cfg_csv_to_delta(
    spark,
    table_name="cfg_source_details",
    csv_base_path=CFG_CSV_BASE_PATH
)'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
