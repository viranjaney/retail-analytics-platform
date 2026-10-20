from datetime import datetime

import pytest
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from src.transformations.silver.clean_orders import transform_orders

SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", TimestampType()),
    StructField("order_approved_at", TimestampType()),
    StructField("order_delivered_carrier_date", TimestampType()),
    StructField("order_delivered_customer_date", TimestampType()),
    StructField("order_estimated_delivery_date", TimestampType()),
    StructField("_ingested_at", TimestampType()),
])

PURCHASED = datetime(2017, 10, 2, 10, 0, 0)
ESTIMATED = datetime(2017, 10, 15, 0, 0, 0)
INGESTED = datetime(2026, 10, 19, 12, 0, 0)


def make_row(order_id, status, delivered=None):
    return (order_id, "cust_1", status, PURCHASED, PURCHASED, PURCHASED,
            delivered, ESTIMATED, INGESTED)


def run(spark, *rows):
    clean, quarantine = transform_orders(spark.createDataFrame(list(rows), SCHEMA))
    return (
        {r["order_id"]: r for r in clean.collect()},
        {r["order_id"]: r for r in quarantine.collect()},
    )


def test_late_delivered_order_is_clean_with_derived_columns(spark):
    clean, quarantine = run(spark, make_row("o1", "delivered", datetime(2017, 10, 20, 9, 0, 0)))
    assert "o1" in clean and not quarantine
    assert clean["o1"]["delivery_days"] == 18
    assert clean["o1"]["is_late"] is True


def test_on_time_delivery_is_not_late(spark):
    clean, _ = run(spark, make_row("o2", "delivered", datetime(2017, 10, 10, 9, 0, 0)))
    assert clean["o2"]["is_late"] is False


def test_invalid_status_is_quarantined(spark):
    clean, quarantine = run(spark, make_row("o3", "teleported"))
    assert "o3" in quarantine and not clean
    assert quarantine["o3"]["_dq_reason"] == "invalid_status"


def test_delivered_without_delivery_date_is_quarantined(spark):
    _, quarantine = run(spark, make_row("o4", "delivered", None))
    assert quarantine["o4"]["_dq_reason"] == "delivered_without_delivery_date"


def test_delivered_before_purchase_is_quarantined(spark):
    _, quarantine = run(spark, make_row("o5", "delivered", datetime(2017, 9, 1, 9, 0, 0)))
    assert quarantine["o5"]["_dq_reason"] == "delivered_before_purchase"


def test_status_is_trimmed_and_lowercased(spark):
    clean, _ = run(spark, make_row("o6", "  Shipped "))
    assert clean["o6"]["order_status"] == "shipped"


def test_undelivered_order_stays_clean_with_null_derived_columns(spark):
    clean, _ = run(spark, make_row("o7", "shipped", None))
    assert clean["o7"]["delivery_days"] is None
    assert clean["o7"]["is_late"] is None