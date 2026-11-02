"""Silver products: rename, validate, dedupe, join English category names."""
import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.delta_utils import overwrite_table
from src.common.spark_session import get_spark
from src.common.transform_utils import dedupe_latest

MEASURES = ["weight_g", "length_cm", "height_cm", "width_cm"]


def transform_products(products_df: DataFrame, translation_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (clean, quarantine). Pure function of its two inputs."""
    translation = (
        translation_df.select(
            F.lower(F.trim("product_category_name")).alias("_cat_key"),
            F.trim("product_category_name_english").alias("_cat_english"),
        )
        .dropDuplicates(["_cat_key"])
    )

    df = products_df.select(
        F.trim("product_id").alias("product_id"),
        F.lower(F.trim("product_category_name")).alias("category_name_pt"),
        F.col("product_name_length").alias("name_length"),            # misspelling fixed
        F.col("product_description_lenght").alias("description_length"),
        F.col("product_photos_qty").alias("photos_qty"),
        F.col("product_weight_g").alias("weight_g"),
        F.col("product_length_cm").alias("length_cm"),
        F.col("product_height_cm").alias("height_cm"),
        F.col("product_width_cm").alias("width_cm"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
    )

    negative_measure = F.lit(False)
    for m in MEASURES:
        negative_measure = negative_measure | (F.col(m) < 0)

    dq_reason = (
        F.when(F.col("product_id").isNull() | (F.col("product_id") == ""), "missing_product_id")
         .when(negative_measure, "negative_measurement")
    )
    df = df.withColumn("_dq_reason", dq_reason)

    quarantine = df.filter(F.col("_dq_reason").isNotNull())

    deduped = dedupe_latest(
        df.filter(F.col("_dq_reason").isNull()).drop("_dq_reason"), ["product_id"]
    )

    complete = F.lit(True)
    for m in MEASURES:
        complete = complete & F.col(m).isNotNull()

    clean = (
        deduped.join(F.broadcast(translation), deduped["category_name_pt"] == translation["_cat_key"], "left")
        .withColumn("is_category_missing", F.col("category_name_pt").isNull())
        .withColumn("category_name_en",
                    F.coalesce("_cat_english", "category_name_pt", F.lit("unknown")))
        .withColumn("category_name_pt", F.coalesce("category_name_pt", F.lit("unknown")))
        .withColumn("has_complete_dimensions", complete)
        .drop("_cat_key", "_cat_english")
        .withColumn("_processed_at", F.current_timestamp())
    )
    return clean, quarantine


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="dev")
    args = parser.parse_args()

    spark = get_spark()
    clean, quarantine = transform_products(
        spark.table(f"{args.catalog}.bronze.products"),
        spark.table(f"{args.catalog}.bronze.product_category_translation"),
    )
    n_clean = overwrite_table(clean, f"{args.catalog}.silver.products")
    n_quar = overwrite_table(quarantine, f"{args.catalog}.silver.products_quarantine")
    print(f"silver.products: {n_clean:,} rows")
    print(f"silver.products_quarantine: {n_quar:,} rows")


if __name__ == "__main__":
    main()