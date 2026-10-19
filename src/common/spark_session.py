from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Databricks Connect when run locally, the cluster's session when run on Databricks."""
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()