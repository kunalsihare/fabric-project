# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_required_schema(table_id: int, config_schema: str):
    print("reading Schema from cfg table...")
    cfg_df = (
        spark.table(f"{config_schema}.cfg_column_schema")
        .filter(F.col("table_id") == table_id)
        .orderBy("sequence_number")
    )

    if cfg_df.count() == 0:
        raise Exception(f"No schema defined for table_id={table_id}")

    fields = []
    column_order = []

    for r in cfg_df.collect():
        dtype = r.data_type.upper()
        col = r.column_name

        if dtype.startswith("DECIMAL"):
            p = int(dtype.split("(")[1].split(",")[0])
            s = int(dtype.split(",")[1].replace(")", ""))
            spark_type = DecimalType(p, s)
        else:
            spark_type = {
                "STRING": StringType(),
                "INT": IntegerType(),
                "BIGINT": LongType(),
                "DATE": DateType(),
                "TIMESTAMP": TimestampType(),
                "BOOLEAN": BooleanType(),
                "DOUBLE" : DoubleType()
            }[dtype]

        fields.append(StructField(col, spark_type))
        column_order.append(col)

    return StructType(fields), column_order

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def enforce_schema(df, schema, ordered_columns):
    missing = set(ordered_columns) - set(df.columns)
    if missing:
        raise Exception(f"Missing columns in source file: {missing}")

    df = df.select(*ordered_columns)

    for f in schema.fields:
        df = df.withColumn(f.name, F.col(f.name).cast(f.dataType))

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
