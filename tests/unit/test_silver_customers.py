from datetime import datetime

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from src.transformations.silver.clean_customers import transform_customers

SCHEMA = StructType(
    [StructField(n, StringType()) for n in [
        "customer_id", "customer_unique_id", "customer_zip_code_prefix",
        "customer_city", "customer_state"
    ]] + [StructField("_ingested_at", TimestampType())]
)

T1 = datetime(2026, 10, 1, 12, 0, 0)
T2 = datetime(2026, 10, 2, 12, 0, 0)

def make_row(customer_id, state="SP", zip_code="01046", city="Sao Paulo", ingested=T1):
    return (customer_id, "person_1", zip_code, city, state, ingested)

def run(spark, *rows):
    clean, quarantine = transform_customers(spark.createDataFrame(list(rows),SCHEMA))
    return clean.collect(), quarantine.collect()

def test_values_are_standardized_and_columns_renamed(spark):
    clean, quarantine = run(spark, make_row("c1", state= " sp", city="  Sao Paulo "))
    assert not quarantine
    assert clean[0]["state"] == "SP"
    assert clean[0]["city"] == "sao paulo"
    assert clean[0]["zip_code_prefix"] == "01046"

def test_invalid_state_isquarantined(spark):
    clean, quarantine = run(spark, make_row("c2", state="SAO"))
    assert not clean
    assert quarantine[0]["_dq_reason"] == "invalid_state"

def test_zip_that_lost_its_leading_zero_is_quarantined(spark):
    _, quarantine = run(spark, make_row("c3", zip_code="1046"))
    assert quarantine[0]["_dq_reason"] == "invalid_zip_code_prefix"


def test_missing_customer_id_is_quarantined(spark):
    _, quarantine = run(spark, make_row(None))
    assert quarantine[0]["_dq_reason"] == "missing_customer_id"


def test_duplicate_customer_keeps_latest_ingested_row(spark):
    clean, _ = run(
        spark,
        make_row("c5", city="Old City", ingested=T1),
        make_row("c5", city="New City", ingested=T2),
    )
    assert len(clean) == 1
    assert clean[0]["city"] == "new city"