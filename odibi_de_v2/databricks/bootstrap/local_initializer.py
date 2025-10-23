from odibi_de_v2.connector import LocalConnection
from odibi_de_v2.core.enums import Framework
from pyspark.sql import SparkSession

def init_local_connection(
    app_name: str = "Local_Ingestion",
    base_path: str = "."
) -> tuple[SparkSession, LocalConnection]:
    """
    Initializes a Spark session and a LocalConnection for reading files
    from DBFS or local filesystem paths.

    Args:
        app_name (str): Name of the Spark application.
        base_path (str): Root path where files are stored (e.g., '/dbfs/FileStore').

    Returns:
        tuple[SparkSession, LocalConnection]: A Spark session and LocalConnection.
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    connector = LocalConnection()
    return spark, connector
