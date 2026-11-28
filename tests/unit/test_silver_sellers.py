from datetime import datetime

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from src.transformations.silver.clean_sellers import transform_sellers

SCHEMA = StructType(
    [StructField(n, StringType()) for n in [
        "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state",
    ]] + [StructField("_ingested_at", TimestampType())]
)

T1 = datetime(2026, 11, 1, 12, 0, 0)
T2 = datetime(2026, 11, 2, 12, 0, 0)


def make_row(seller_id, state="SP", zip_code="13023", city="Campinas", ingested=T1):
    return (seller_id, zip_code, city, state, ingested)


def run(spark, *rows):
    clean, quarantine = transform_sellers(spark.createDataFrame(list(rows), SCHEMA))
    return clean.collect(), quarantine.collect()


def test_values_are_standardized_and_columns_renamed(spark):
    clean, quarantine = run(spark, make_row("s1", state=" sp ", city="  Campinas "))
    assert not quarantine
    assert clean[0]["state"] == "SP"
    assert clean[0]["city"] == "campinas"
    assert clean[0]["zip_code_prefix"] == "13023"


def test_short_zip_is_left_padded_with_zeros(spark):
    clean, quarantine = run(spark, make_row("s2", zip_code="3023"))
    assert not quarantine
    assert clean[0]["zip_code_prefix"] == "03023"


def test_zip_longer_than_five_digits_is_quarantined_not_truncated(spark):
    clean, quarantine = run(spark, make_row("s3", zip_code="130231"))
    assert not clean
    assert quarantine[0]["_dq_reason"] == "invalid_zip_code_prefix"


def test_invalid_state_is_quarantined(spark):
    _, quarantine = run(spark, make_row("s4", state="SAO"))
    assert quarantine[0]["_dq_reason"] == "invalid_state"


def test_missing_seller_id_is_quarantined(spark):
    _, quarantine = run(spark, make_row(None))
    assert quarantine[0]["_dq_reason"] == "missing_seller_id"


def test_duplicate_seller_keeps_latest_ingested_row(spark):
    clean, _ = run(spark, make_row("s5", city="Old City", ingested=T1),
                          make_row("s5", city="New City", ingested=T2))
    assert len(clean) == 1
    assert clean[0]["city"] == "new city"
    