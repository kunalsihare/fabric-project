# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

var_lib = notebookutils.variableLibrary.getLibrary("vl_p3h_p3h_variables")
config_schema = var_lib.getVariable("config_schema_name")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def mask_pii_columns(
    table_id: int,
    input_df: DataFrame,
    pii_mask_string: str = "XXXX-XXXX",
    default_mask_date: str = "1900-01-01"
) -> DataFrame:
    """
    Masks PII columns dynamically based on cfg_column_schema metadata.

    Parameters
    ----------
    table_id : int
        Table identifier from cfg_table_schema / cfg_column_schema
    input_df : DataFrame
        Input DataFrame to be masked
    pii_mask_string : str
        Mask value for STRING PII columns
    default_mask_date : str
        Mask value for DATE PII columns

    Returns
    -------
    DataFrame
        Masked DataFrame
    """
    print("Masking pii data ...")
    # Read column metadata
    cfg_df = spark.table(f"{config_schema}.cfg_column_schema")

    pii_columns = (
        cfg_df
        .filter(
            (F.col("table_id") == table_id) &
            (F.col("is_pii") == "Y")
        )
        .select("column_name", "data_type")
        .collect()
    )

    masked_df = input_df

    for row in pii_columns:
        col_name = row["column_name"]
        data_type = row["data_type"].upper()

        if col_name not in masked_df.columns:
            continue

        # STRING masking
        if data_type == "STRING":
            masked_df = masked_df.withColumn(
                col_name,
                F.lit(pii_mask_string)
            )

        # DATE masking
        elif data_type == "DATE":
            masked_df = masked_df.withColumn(
                col_name,
                F.lit(default_mask_date).cast("date")
            )

        # NUMERIC masking
        elif data_type in ("INT", "BIGINT"):
            masked_df = masked_df.withColumn(
                col_name,
                (F.rand() * 100000).cast("int")
            )

        elif data_type.startswith("DECIMAL"):
            masked_df = masked_df.withColumn(
                col_name,
                (F.rand() * 100000).cast(masked_df.schema[col_name].dataType)
            )

        else:
            # Unsupported types are left unchanged
            pass

    return masked_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
