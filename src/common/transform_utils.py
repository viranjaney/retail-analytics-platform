from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def dedupe_latest(df: DataFrame, key_cols: list[str], order_col: str = "_bronze_ingested_at") -> DataFrame:
    """Keep one row per key: the one with the latest order_col."""
    latest_first = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(latest_first))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )