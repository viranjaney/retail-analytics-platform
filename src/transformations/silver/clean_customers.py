"""Silver customers: standardize, validate, deduplicate."""
import argparse

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.common.delta_utils import overwrite_table
from src.common.spark_session import get_spark

def transform_customers(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (clean, quarantine). Pure function so it can be unit tested"""
    df = bronze_df.select(
       F.trim("customer_id").alias("customer_id"),
        F.trim("customer_unique_id").alias("customer_unique_id"),
        F.trim("customer_zip_code_prefix").alias("zip_code_prefix"),
        F.lower(F.trim("customer_city")).alias("city"),
        F.upper(F.trim("customer_state")).alias("state"),
        F.col("_ingested_at").alias("_bronze_ingested_at")
    )
# coalesce(..., False) so a NULL value fails the check instead of slipping past it
    def matches(column: str, pattern: str):
        return F.coalesce(F.col(column).rlike(pattern), F.lit(False))

    dq_reason = (
        F.when(F.col("customer_id").isNull() | (F.col("customer_id") == ""), "missing_customer_id")
        .when(~matches("state", "^[A-Z]{2}$"), "invalid_state")
        .when(~matches("zip_code_prefix", "^[0-9]{5}$"), "invalid_zip_code_prefix")
    )

    df = df.withColumn("_dq_reason", dq_reason)

    quarantine = df.filter(F.col("_dq_reason").isNotNull())

    latest_first = Window.partitionBy("customer_id").orderBy(F.col("_bronze_ingested_at").desc())

    clean = (
        df.filter(F.col("_dq_reason").isNull())
        .drop("_dq_reason")
        .withColumn("_rn", F.row_number().over(latest_first))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("_processed_at", F.current_timestamp())
    )
    return clean, quarantine

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="dev")
    args = parser.parse_args()

    spark = get_spark()
    clean, quarantine = transform_customers(spark.table(f"{args.catalog}.bronze.customers"))

    n_clean = overwrite_table(clean, f"{args.catalog}.silver.customers")
    n_quar = overwrite_table(quarantine, f"{args.catalog}.silver.customers_quaratine")
    print(f"silver.customers: {n_clean:,} rows")
    print(f"silver.customers_quaratine: {n_quar:,} rows")

if __name__ == "__main__":
    main()