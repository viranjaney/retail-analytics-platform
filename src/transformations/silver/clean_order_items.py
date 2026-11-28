"""Silver order_items: validate keys and amounts, check references, dedupe."""
import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.delta_utils import overwrite_table
from src.common.spark_session import get_spark
from src.common.transform_utils import dedupe_latest, is_blank, with_known_flag

KNOWN_FLAGS = ["_known_order", "_known_product", "_known_seller"]


def transform_order_items(
    items_df: DataFrame,
    order_keys: DataFrame,
    product_keys: DataFrame,
    seller_keys: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Return (clean, quarantine). The *_keys inputs only need order_id / product_id / seller_id."""
    df = items_df.select(
        F.trim("order_id").alias("order_id"),
        F.col("order_item_id"),
        F.trim("product_id").alias("product_id"),
        F.trim("seller_id").alias("seller_id"),
        F.col("shipping_limit_date").alias("shipping_limit_at"),
        F.col("price"),
        F.col("freight_value"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
    )
    df = with_known_flag(df, order_keys, "order_id", "_known_order")
    df = with_known_flag(df, product_keys, "product_id", "_known_product")
    df = with_known_flag(df, seller_keys, "seller_id", "_known_seller")

    dq_reason = (
        F.when(is_blank("order_id") | is_blank("product_id") | is_blank("seller_id")
               | F.col("order_item_id").isNull(), "missing_key")
         .when((F.col("price") < 0) | (F.col("freight_value") < 0), "negative_amount")
         .when(F.col("_known_order").isNull(), "orphan_order")
         .when(F.col("_known_product").isNull(), "orphan_product")
         .when(F.col("_known_seller").isNull(), "orphan_seller")
    )
    df = df.withColumn("_dq_reason", dq_reason)

    quarantine = df.filter(F.col("_dq_reason").isNotNull()).drop(*KNOWN_FLAGS)
    clean = (
        dedupe_latest(
            df.filter(F.col("_dq_reason").isNull()).drop("_dq_reason", *KNOWN_FLAGS),
            ["order_id", "order_item_id"],
        )
        .withColumn("item_total", F.col("price") + F.col("freight_value"))
        .withColumn("_processed_at", F.current_timestamp())
    )
    return clean, quarantine


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="dev")
    args = parser.parse_args()
    c = args.catalog

    spark = get_spark()
    clean, quarantine = transform_order_items(
        spark.table(f"{c}.bronze.order_items"),
        spark.table(f"{c}.silver.orders"),
        spark.table(f"{c}.silver.products"),
        spark.table(f"{c}.silver.sellers")
    )
    n_clean = overwrite_table(clean, f"{c}.silver.order_items")
    n_quar = overwrite_table(quarantine, f"{c}.silver.order_items_quarantine")
    print(f"silver.order_items: {n_clean:,} rows")
    print(f"silver.order_items_quarantine: {n_quar:,} rows")


if __name__ == "__main__":
    main()