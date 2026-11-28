"""Silver order_payments: validate type and amount, check the order exists, dedupe."""
import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.delta_utils import overwrite_table
from src.common.spark_session import get_spark
from src.common.transform_utils import dedupe_latest, is_blank, with_known_flag

ALLOWED_PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card", "not_defined"]


def transform_order_payments(payments_df: DataFrame, order_keys: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (clean, quarantine). order_keys only needs an order_id column."""
    df = payments_df.select(
        F.trim("order_id").alias("order_id"),
        F.col("payment_sequential"),
        F.lower(F.trim("payment_type")).alias("payment_type"),
        F.col("payment_installments").alias("installments"),
        F.col("payment_value").alias("amount"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
    )
    df = with_known_flag(df, order_keys, "order_id", "_known_order")

    # coalesce so a NULL payment_type fails the check instead of slipping past it
    valid_type = F.coalesce(F.col("payment_type").isin(ALLOWED_PAYMENT_TYPES), F.lit(False))

    dq_reason = (
        F.when(is_blank("order_id") | F.col("payment_sequential").isNull(), "missing_key")
         .when(~valid_type, "invalid_payment_type")
         .when(F.col("amount").isNull(), "missing_amount")
         .when((F.col("amount") < 0) | (F.col("installments") < 0), "negative_amount")
         .when(F.col("_known_order").isNull(), "orphan_order")
    )
    df = df.withColumn("_dq_reason", dq_reason)

    quarantine = df.filter(F.col("_dq_reason").isNotNull()).drop("_known_order")
    clean = (
        dedupe_latest(
            df.filter(F.col("_dq_reason").isNull()).drop("_dq_reason", "_known_order"),
            ["order_id", "payment_sequential"],
        )
        .withColumn("is_zero_value", F.col("amount") == 0)
        .withColumn("is_installment_plan", F.col("installments") > 1)
        .withColumn("_processed_at", F.current_timestamp())
    )
    return clean, quarantine


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="dev")
    args = parser.parse_args()
    c = args.catalog

    spark = get_spark()
    clean, quarantine = transform_order_payments(
        spark.table(f"{c}.bronze.order_payments"),
        spark.table(f"{c}.silver.orders"),
    )
    n_clean = overwrite_table(clean, f"{c}.silver.order_payments")
    n_quar = overwrite_table(quarantine, f"{c}.silver.order_payments_quarantine")
    print(f"silver.order_payments: {n_clean:,} rows")
    print(f"silver.order_payments_quarantine: {n_quar:,} rows")


if __name__ == "__main__":
    main()