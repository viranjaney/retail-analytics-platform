from datetime import datetime
from decimal import Decimal

from pyspark.sql.types import (DecimalType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

from src.transformations.silver.clean_order_items import transform_order_items

SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("order_item_id", IntegerType()),
    StructField("product_id", StringType()),
    StructField("seller_id", StringType()),
    StructField("shipping_limit_date", TimestampType()),
    StructField("price", DecimalType(10, 2)),
    StructField("freight_value", DecimalType(10, 2)),
    StructField("_ingested_at", TimestampType()),
])

T1 = datetime(2026, 10, 1, 12, 0, 0)
T2 = datetime(2026, 10, 2, 12, 0, 0)
SHIPPING = datetime(2017, 10, 5, 12, 0, 0)


def item(order_id="o1", item_no=1, product_id="p1", seller_id="s1",
         price="29.90", freight="8.72", ingested=T1):
    return (order_id, item_no, product_id, seller_id, SHIPPING,
            Decimal(price), Decimal(freight), ingested)


def run(spark, *rows):
    clean, quarantine = transform_order_items(
        spark.createDataFrame(list(rows), SCHEMA),
        spark.createDataFrame([("o1",), ("o2",)], ["order_id"]),
        spark.createDataFrame([("p1",)], ["product_id"]),
        spark.createDataFrame([("s1",)], ["seller_id"]),
    )
    return clean.collect(), quarantine.collect()


def test_valid_item_is_clean_with_item_total(spark):
    clean, quarantine = run(spark, item())
    assert not quarantine
    assert clean[0]["item_total"] == Decimal("38.62")


def test_item_with_unknown_order_is_quarantined(spark):
    clean, quarantine = run(spark, item(order_id="o9"))
    assert not clean
    assert quarantine[0]["_dq_reason"] == "orphan_order"


def test_item_with_unknown_product_is_quarantined(spark):
    _, quarantine = run(spark, item(product_id="p9"))
    assert quarantine[0]["_dq_reason"] == "orphan_product"


def test_item_with_unknown_seller_is_quarantined(spark):
    _, quarantine = run(spark, item(seller_id="s9"))
    assert quarantine[0]["_dq_reason"] == "orphan_seller"


def test_negative_price_is_quarantined(spark):
    _, quarantine = run(spark, item(price="-1.00"))
    assert quarantine[0]["_dq_reason"] == "negative_amount"


def test_missing_key_is_quarantined(spark):
    _, quarantine = run(spark, item(product_id=None))
    assert quarantine[0]["_dq_reason"] == "missing_key"


def test_duplicate_line_item_keeps_latest_ingested_row(spark):
    clean, _ = run(spark, item(price="10.00", ingested=T1), item(price="12.00", ingested=T2))
    assert len(clean) == 1
    assert clean[0]["price"] == Decimal("12.00")


def test_different_line_items_of_same_order_are_both_kept(spark):
    clean, _ = run(spark, item(item_no=1), item(item_no=2))
    assert len(clean) == 2