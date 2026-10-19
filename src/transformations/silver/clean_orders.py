"""Silver orders: standardize, validate, quarantine bad rows, add derived columns."""
import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.spark_session import get_spark

ALLOWED_STATUSES = [
    "created", "approved", "invoiced", "processing",
    "shipped", "delivered", "unavailable", "canceled",
]


def transform_orders(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (clean, quarantine). Pure function of the input, so it can be unit tested."""
    df = (
        bronze_df
        .select(
            "order_id",
            "customer_id",
            F.lower(F.trim("order_status")).alias("order_status"),
            F.col("order_purchase_timestamp").alias("purchased_at"),
            F.col("order_approved_at").alias("approved_at"),
            F.col("order_delivered_carrier_date").alias("delivered_to_carrier_at"),
            F.col("order_delivered_customer_date").alias("delivered_to_customer_at"),
            F.col("order_estimated_delivery_date").alias("estimated_delivery_at"),
            F.col("_ingested_at").alias("_bronze_ingested_at"),
        )
    )

    dq_reason = (
        F.when(~F.col("order_status").isin(ALLOWED_STATUSES), "invalid_status")
         .when((F.col("order_status") == "delivered")
               & F.col("delivered_to_customer_at").isNull(), "delivered_without_delivery_date")
         .when(F.col("delivered_to_customer_at") < F.col("purchased_at"), "delivered_before_purchase")
    )
    df = df.withColumn("_dq_reason", dq_reason)

    quarantine = df.filter(F.col("_dq_reason").isNotNull())
    clean = (
        df.filter(F.col("_dq_reason").isNull())
          .drop("_dq_reason")
          .withColumn("delivery_days",
                      F.datediff("delivered_to_customer_at", "purchased_at"))
          .withColumn("is_late",
                      F.col("delivered_to_customer_at") > F.col("estimated_delivery_at"))
          .withColumn("_processed_at", F.current_timestamp())
    )
    return clean, quarantine


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="dev")
    args = parser.parse_args()

    spark = get_spark()
    bronze = spark.table(f"{args.catalog}.bronze.orders")
    clean, quarantine = transform_orders(bronze)

    clean.write.format("delta").mode("overwrite") \
         .option("overwriteSchema", "true").saveAsTable(f"{args.catalog}.silver.orders")
    quarantine.write.format("delta").mode("overwrite") \
         .option("overwriteSchema", "true").saveAsTable(f"{args.catalog}.silver.orders_quarantine")

    print(f"silver.orders: {spark.table(f'{args.catalog}.silver.orders').count():,} rows")
    print(f"silver.orders_quarantine: {spark.table(f'{args.catalog}.silver.orders_quarantine').count():,} rows")


if __name__ == "__main__":
    main()