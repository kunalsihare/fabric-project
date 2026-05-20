# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e0a74b03-30b7-4d36-ad44-c5ff0c61603b",
# META       "default_lakehouse_name": "config_db",
# META       "default_lakehouse_workspace_id": "e135354a-4493-4192-ae7f-bdc6072f9746",
# META       "known_lakehouses": [
# META         {
# META           "id": "e0a74b03-30b7-4d36-ad44-c5ff0c61603b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# %%configure -f
# {
#    "defaultLakehouse": {
#     "name": {
#       "variableName": "$(/**/vl_p3h_p3h_variables/LH_Name)" 
#     },
#     "id": {
#       "variableName": "$(/**/vl_p3h_p3h_variables/LH_ID)"
#     },
#     "workspaceId": {
#       "variableName": "$(/**/vl_p3h_p3h_variables/WS_ID)"
#     }
#   }
# }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql import functions as F
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

var_lib = notebookutils.variableLibrary.getLibrary("vl_p3h_p3h_variables")
config_schema  = var_lib.getVariable("config_schema_name")
log_schema = var_lib.getVariable("log_schema_name")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def load_dq_rules(table_id: int):
    """
    Reads DQ rules for the given target table from cfg_dq_rules.
    Returns a structured dictionary for the DQ engine.
    """
    rules_df = spark.sql(f"""
        SELECT A.column_name, B.*
        FROM {config_schema}.cfg_column_schema as A 
        INNER JOIN {config_schema}.cfg_dq_rules as B 
        ON A.column_id = B.column_id
        WHERE A.table_id = {table_id}
          AND B.is_active = TRUE
    """)

    rules = {
        "null_checks": [],
        "unique_key": None,
        "regex_checks": {},
        "numeric_rules": {}  # numeric rules stored as {"min": x, "max": y}
    }

    for r in rules_df.collect():

        if r.rule_type == "null_check":
            rules["null_checks"].append(r.column_name)

        elif r.rule_type == "unique_key":
            rules["unique_key"] = r.column_name

        elif r.rule_type == "regex":
            rules["regex_checks"][r.column_name] = r.rule_value

        elif r.rule_type == "numeric":
            rules["numeric_rules"][r.column_name] = json.loads(r.rule_value)

    return rules

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def apply_dq_rules(df, rules, table_id, run_id):
    """
    Applies metadata DQ rules on a dataframe.
    Returns (clean_df, bad_df_with_metadata)
    """

    failure_conditions = []
    failure_reason_expr = []

    # --------------------------
    # 1. NULL CHECKS
    # --------------------------
    for col_name in rules.get("null_checks", []):
        cond = F.col(col_name).isNull()
        failure_conditions.append(cond)
        failure_reason_expr.append(
            F.when(cond, F.lit(f"{col_name} IS NULL"))
        )

    # --------------------------
    # 2. UNIQUE KEY CHECK
    # --------------------------
    if rules.get("unique_key"):
        bk = rules["unique_key"]
        w = Window.partitionBy(bk)

        df = df.withColumn("_dup_count", F.count("*").over(w))

        cond = F.col("_dup_count") > 1
        failure_conditions.append(cond)
        failure_reason_expr.append(
            F.when(cond, F.lit(f"Duplicate Business Key: {bk}"))
        )

    # --------------------------
    # 3. REGEX CHECK
    # --------------------------
    for col_name, pattern in rules.get("regex_checks", {}).items():
        cond = ~F.col(col_name).rlike(pattern)
        failure_conditions.append(cond)
        failure_reason_expr.append(
            F.when(cond, F.lit(f"{col_name} failed regex: {pattern}"))
        )

    # --------------------------
    # 4. NUMERIC CHECK
    # --------------------------
    for col_name, rule in rules.get("numeric_rules", {}).items():
        local_cond = None
        min_v = rule.get("min")
        max_v = rule.get("max")

        if min_v is not None:
            min_cond = F.col(col_name).cast("double") < float(min_v)
            local_cond = min_cond if local_cond is None else (local_cond | min_cond)

        if max_v is not None:
            max_cond = F.col(col_name).cast("double") > float(max_v)
            local_cond = max_cond if local_cond is None else (local_cond | max_cond)

        if local_cond is not None:
            failure_conditions.append(local_cond)
            failure_reason_expr.append(
                F.when(local_cond, F.lit(f"{col_name} out of bounds {rule}"))
            )

    # If no rules → return entire df as clean
    if not failure_conditions:
        return df, None

    # Combine all failure conditions
    combined_condition = failure_conditions[0]
    for cond in failure_conditions[1:]:
        combined_condition = combined_condition | cond

    # Build failure reason column
    failure_reason = F.coalesce(*failure_reason_expr)

    bad_df = (
        df.filter(combined_condition)
          .withColumn("failure_reason", failure_reason)
          .withColumn("table_id", F.lit(table_id))
          .withColumn("record_json", F.to_json(F.struct("*")))
          .withColumn("run_id", F.lit(run_id))
          .select("table_id", "failure_reason", "record_json", "run_id")
    )

    # Clean dataframe
    clean_df = df.filter(~combined_condition).drop("_dup_count")

    return clean_df, bad_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def run_metadata_dq(source_df, table_id, run_id,target_table=None):
    """
    Applies metadata-driven DQ rules and:
      ✔ Inserts BAD records into logs.data_quality_exceptions
      ✔ Returns GOOD dataframe only
    """

    # Load rules
    rules = load_dq_rules(table_id)

    # Apply rules
    clean_df, bad_df = apply_dq_rules(
        df=source_df,
        rules=rules,
        table_id=table_id,
        run_id=run_id
    )

    # --------------------------------------
    # 1. Write BAD records to Delta table
    # --------------------------------------
    if bad_df is not None and bad_df.count() > 0:

        # Insert bad records into DQ exception table
        bad_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(f"{log_schema}.data_quality_exceptions")
        
        
        table_name = target_table if target_table else f"table_id={table_id}"
        print(f"[DQ] {bad_df.count()} bad records inserted into {log_schema}.data_quality_exceptions for {table_name}")

    else:
        table_name = target_table if target_table else f"table_id={table_id}"
        print(f"[DQ] No DQ failures for {table_name}")

    # --------------------------------------
    # 2. Return ONLY good dataframe
    # --------------------------------------
    return clean_df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
