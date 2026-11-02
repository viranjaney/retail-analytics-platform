from datetime import datetime

from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from src.transformations.silver.clean_products import transform_products

PRODUCT_SCHEMA = StructType([
    StructField("product_id", StringType()),
    StructField("product_category_name", StringType()),
    StructField("product_name_length", IntegerType()),
    StructField("product_description_lenght", IntegerType()),
    StructField("product_photos_qty", IntegerType()),
    StructField("product_weight_g", IntegerType()),
    StructField("product_length_cm", IntegerType()),
    StructField("product_height_cm", IntegerType()),
    StructField("product_width_cm", IntegerType()),
    StructField("_ingested_at", TimestampType()),
])
TRANSLATION_SCHEMA = StructType([
    StructField("product_category_name", StringType()),
    StructField("product_category_name_english", StringType()),
])
TRANSLATIONS = [("perfumaria", "perfumery"), ("beleza_saude", "health_beauty")]

T1 = datetime(2026, 10, 1, 12, 0, 0)
T2 = datetime(2026, 10, 2, 12, 0, 0)


def product(product_id, category="perfumaria", weight=500, ingested=T1):
    return (product_id, category, 40, 300, 2, weight, 20, 10, 15, ingested)


def run(spark, *rows):
    clean, quarantine = transform_products(
        spark.createDataFrame(list(rows), PRODUCT_SCHEMA),
        spark.createDataFrame(TRANSLATIONS, TRANSLATION_SCHEMA),
    )
    return (
        {r["product_id"]: r for r in clean.collect()},
        {r["product_id"]: r for r in quarantine.collect()},
    )


def test_category_is_translated(spark):
    clean, quarantine = run(spark, product("p1"))
    assert not quarantine
    assert clean["p1"]["category_name_pt"] == "perfumaria"
    assert clean["p1"]["category_name_en"] == "perfumery"
    assert clean["p1"]["is_category_missing"] is False


def test_missing_category_becomes_unknown_and_is_flagged(spark):
    clean, _ = run(spark, product("p2", category=None))
    assert clean["p2"]["category_name_pt"] == "unknown"
    assert clean["p2"]["category_name_en"] == "unknown"
    assert clean["p2"]["is_category_missing"] is True


def test_untranslated_category_falls_back_to_original_name(spark):
    clean, _ = run(spark, product("p3", category="pc_gamer"))
    assert clean["p3"]["category_name_en"] == "pc_gamer"
    assert clean["p3"]["is_category_missing"] is False


def test_null_measurement_is_kept_and_flagged_not_quarantined(spark):
    clean, quarantine = run(spark, product("p4", weight=None))
    assert not quarantine
    assert clean["p4"]["weight_g"] is None
    assert clean["p4"]["has_complete_dimensions"] is False


def test_negative_measurement_is_quarantined(spark):
    clean, quarantine = run(spark, product("p5", weight=-5))
    assert not clean
    assert quarantine["p5"]["_dq_reason"] == "negative_measurement"


def test_missing_product_id_is_quarantined(spark):
    _, quarantine = run(spark, product(None))
    assert quarantine[None]["_dq_reason"] == "missing_product_id"


def test_duplicate_product_keeps_latest_ingested_row(spark):
    clean, _ = run(spark, product("p6", weight=100, ingested=T1), product("p6", weight=200, ingested=T2))
    assert len(clean) == 1
    assert clean["p6"]["weight_g"] == 200


def test_misspelled_source_columns_are_renamed(spark):
    clean, _ = run(spark, product("p7"))
    columns = clean["p7"].asDict().keys()
    assert "name_length" in columns and "description_length" in columns
    assert "product_description_lenght" not in columns