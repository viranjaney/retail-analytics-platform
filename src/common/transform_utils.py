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

def matches(column: str, pattern: str):
    """True only when the column is non-null and matches the regex; NULL counts as a failed check."""
    return F.coalesce(F.col(column).rlike(pattern), F.lit(False))


def pad_zip(column: str):
    """Left-pad short zips to 5 digits. Longer values are left alone so the validity check rejects them."""
    z = F.trim(F.col(column))
    return F.when(F.length(z) < 5, F.lpad(z, 5, "0")).otherwise(z)

