from datetime import datetime
from decimal import Decimal

from pyspark.sql.types import (DecimalType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

from src.transformations.silver.clean_order_payments import transform_order_payments

SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("payment_sequential", IntegerType()),
    StructField("payment_type", StringType()),
    StructField("payment_installments", IntegerType()),
    StructField("payment_value", DecimalType(10, 2)),
    StructField("_ingested_at", TimestampType()),
])

T1 = datetime(2026, 11, 1, 12, 0, 0)
T2 = datetime(2026, 11, 2, 12, 0, 0)


def payment(order_id="o1", seq=1, ptype="credit_card", installments=3, value="50.00", ingested=T1):
    return (order_id, seq, ptype, installments,
            None if value is None else Decimal(value), ingested)


def run(spark, *rows):
    clean, quarantine = transform_order_payments(
        spark.createDataFrame(list(rows), SCHEMA),
        spark.createDataFrame([("o1",), ("o2",)], ["order_id"]),
    )
    return clean.collect(), quarantine.collect()


def test_valid_payment_is_clean_with_flags(spark):
    clean, quarantine = run(spark, payment())
    assert not quarantine
    assert clean[0]["is_installment_plan"] is True
    assert clean[0]["is_zero_value"] is False


def test_payment_type_is_trimmed_and_lowercased(spark):
    clean, _ = run(spark, payment(ptype=" Credit_Card "))
    assert clean[0]["payment_type"] == "credit_card"


def test_unknown_payment_type_is_quarantined(spark):
    clean, quarantine = run(spark, payment(ptype="bitcoin"))
    assert not clean
    assert quarantine[0]["_dq_reason"] == "invalid_payment_type"


def test_null_payment_type_is_quarantined(spark):
    _, quarantine = run(spark, payment(ptype=None))
    assert quarantine[0]["_dq_reason"] == "invalid_payment_type"


def test_negative_amount_is_quarantined(spark):
    _, quarantine = run(spark, payment(value="-5.00"))
    assert quarantine[0]["_dq_reason"] == "negative_amount"


def test_missing_amount_is_quarantined(spark):
    _, quarantine = run(spark, payment(value=None))
    assert quarantine[0]["_dq_reason"] == "missing_amount"


def test_payment_for_unknown_order_is_quarantined(spark):
    _, quarantine = run(spark, payment(order_id="o9"))
    assert quarantine[0]["_dq_reason"] == "orphan_order"


def test_zero_value_payment_is_kept_and_flagged(spark):
    clean, quarantine = run(spark, payment(ptype="voucher", installments=1, value="0.00"))
    assert not quarantine
    assert clean[0]["is_zero_value"] is True


def test_multiple_payments_for_one_order_are_all_kept(spark):
    clean, _ = run(spark, payment(seq=1, ptype="voucher", installments=1),
                          payment(seq=2, ptype="credit_card"))
    assert len(clean) == 2


def test_duplicate_payment_keeps_latest_ingested_row(spark):
    clean, _ = run(spark, payment(value="10.00", ingested=T1), payment(value="12.00", ingested=T2))
    assert len(clean) == 1
    assert clean[0]["amount"] == Decimal("12.00")