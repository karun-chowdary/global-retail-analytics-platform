"""Reusable PySpark helpers for the Silver layer.

Kept in plain Python (no Fabric-specific code) so the same functions run in a
Fabric notebook and in local pytest.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def snake_case_columns(df: DataFrame) -> DataFrame:
    """Lower-case every column name. Bronze keeps Oracle's upper-case names;
    Silver standardises them ("conformed")."""
    return df.toDF(*[c.lower() for c in df.columns])


def dedupe_latest(df: DataFrame, keys: list, order_col: str) -> DataFrame:
    """Keep exactly one row per key: the row with the newest order_col.

    Same idea as ROW_NUMBER() OVER (PARTITION BY keys ORDER BY order_col DESC) = 1.
    """
    window = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def split_valid_invalid(df: DataFrame, rules: dict):
    """Split a DataFrame into (valid_rows, quarantine_rows).

    rules: {reason_code: Column} where the Column is True when the row is BAD.
    Rules are checked in the order given, and the first failing rule becomes
    the row's reason_code in the quarantine DataFrame.
    A rule that evaluates to NULL (for example a comparison with a NULL value)
    counts as "not failed", so handle NULLs with an explicit rule if needed.
    """
    reason = F.lit(None).cast("string")
    for code, is_bad in reversed(list(rules.items())):
        reason = F.when(is_bad, F.lit(code)).otherwise(reason)
    flagged = df.withColumn("_reason", reason)
    valid = flagged.filter(F.col("_reason").isNull()).drop("_reason")
    quarantine = (
        flagged.filter(F.col("_reason").isNotNull())
        .withColumnRenamed("_reason", "reason_code")
    )
    return valid, quarantine
