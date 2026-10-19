"""Bronze batch load: Parquet files landed by ADF -> Delta tables in Unity Catalog."""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SOURCE_SYSTEM = "oltp_retail"

TABLES = [
    "customers", "sellers", "product_category_translation", "products",
    "orders", "order_items", "order_payments", "geolocation", "order_reviews",
]


def get_spark() -> SparkSession:
    """Databricks Connect when run locally, the cluster's session when run on Databricks."""
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()


def load_table(spark, catalog: str, storage_account: str, table: str) -> int:
    path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/olist/{table}"
    df = (
        spark.read.parquet(path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_system", F.lit(SOURCE_SYSTEM))
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    target = f"{catalog}.bronze.{table}"
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(target))
    return spark.table(target).count()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Olist Parquet into Bronze Delta tables")
    parser.add_argument("--storage-account", required=True)
    parser.add_argument("--catalog", default="dev")
    parser.add_argument("--tables", nargs="*", default=TABLES,
                        help="Subset of tables to load (default: all)")
    args = parser.parse_args()

    spark = get_spark()
    for table in args.tables:
        rows = load_table(spark, args.catalog, args.storage_account, table)
        print(f"{args.catalog}.bronze.{table}: {rows:,} rows")


if __name__ == "__main__":
    main()