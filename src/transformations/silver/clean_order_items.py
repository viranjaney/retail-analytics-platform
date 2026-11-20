"""Silver order_items: validate keys and amounts, check references, dedupe."""
import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.delta_utils import overwrite_table
from src.common.spark_session import get_spark
from src.common.transform_utils import dedupe_latest

KNOWN_FLAGS = ["_known_order", "_known_product", "_known_seller"]


def _blank(col: str):
    return F.col(col).isNull() | (F.col(col) == "")


def _with_known_flag(df: DataFrame, keys_df: DataFrame, key_col: str, flag_col: str) -> DataFrame:
    """Left join to the set of valid keys; flag_col is NULL when the key has no match."""
    keys = (
        keys_df.select(F.trim(F.col(key_col)).alias(key_col))
        .distinct()
        .withColumn(flag_col, F.lit(True))
    )
    return df.join(keys, on=key_col, how="left")


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
    df = _with_known_flag(df, order_keys, "order_id", "_known_order")
    df = _with_known_flag(df, product_keys, "product_id", "_known_product")
    df = _with_known_flag(df, seller_keys, "seller_id", "_known_seller")

    dq_reason = (
        F.when(_blank("order_id") | _blank("product_id") | _blank("seller_id")
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