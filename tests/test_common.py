import os
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "transformation"))
from common import dedupe_latest, snake_case_columns, split_valid_invalid  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("silver-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_snake_case_columns(spark):
    df = spark.createDataFrame([(1, "a")], ["ORDER_ID", "Order_Status"])
    assert snake_case_columns(df).columns == ["order_id", "order_status"]


def test_dedupe_latest_keeps_newest_row_per_key(spark):
    df = spark.createDataFrame(
        [("o1", "created", 1), ("o1", "shipped", 3), ("o1", "approved", 2), ("o2", "created", 1)],
        ["order_id", "order_status", "version"],
    )
    result = {r["order_id"]: r["order_status"] for r in dedupe_latest(df, ["order_id"], "version").collect()}
    assert result == {"o1": "shipped", "o2": "created"}


def test_split_valid_invalid_uses_first_failing_rule(spark):
    df = spark.createDataFrame(
        [("p1", 10.0, "credit_card"), ("p2", -5.0, "credit_card"), ("p3", -1.0, "bitcoin"), ("p4", 3.0, "bitcoin")],
        ["payment_id", "payment_value", "payment_type"],
    )
    rules = {
        "NEGATIVE_VALUE": F.col("payment_value") < 0,
        "UNKNOWN_TYPE": ~F.col("payment_type").isin("credit_card", "boleto"),
    }
    valid, quarantine = split_valid_invalid(df, rules)
    assert [r["payment_id"] for r in valid.collect()] == ["p1"]
    reasons = {r["payment_id"]: r["reason_code"] for r in quarantine.collect()}
    assert reasons == {"p2": "NEGATIVE_VALUE", "p3": "NEGATIVE_VALUE", "p4": "UNKNOWN_TYPE"}
    assert "reason_code" not in valid.columns
