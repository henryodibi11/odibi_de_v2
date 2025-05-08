from pyspark.sql import SparkSession

def get_active_spark() -> SparkSession:
    """
    Returns the currently active SparkSession or creates a new one if none exists.

    This utility ensures SparkSession access in transformers or utilities
    where no input DataFrame is provided.
    """
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
