from pyspark.sql import DataFrame 

def overwrite_table(df: DataFrame, table: str) -> int:
    """Full overwrite of a Unity Catalog Delta table. Returns the row count."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )
    return df.sparkSession.table(table).count()
