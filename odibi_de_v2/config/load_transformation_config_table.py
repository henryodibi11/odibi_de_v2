from odibi_de_v2.ingestion import ReaderProvider
from odibi_de_v2.connector import SQLDatabaseConnection
from odibi_de_v2.core.enums import Framework, DataType
import pandas as pd
from pyspark.sql import SparkSession


def load_transformation_config_table(
    host: str,
    database: str,
    user: str,
    password: str,
    project: str,
    transformation_id: str,
    spark: SparkSession
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Loads configuration data for transformation from a SQL Server and returns them as Pandas DataFrames.

    This function queries the SQL Server based on provided transformation id. It retrieves
    configuration data for the specified transformation id which is then converted to Pandas DataFrames for
    further processing.

    Args:
        host (str): The hostname or IP address of the SQL Server.
        database (str): The name of the database from which to fetch the configuration data.
        user (str): The username used to authenticate with SQL Server.
        password (str): The password used to authenticate with SQL Server.
        project (str): The name of the project for which configuration data is being fetched.
        transformation_id (str): The identifier for the source configuration to be fetched.
        spark (SparkSession): An active SparkSession to handle data operations.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing two Pandas DataFrames:
            - The first DataFrame contains the source configuration data.
            - The second DataFrame contains the target configuration data.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.appName("ConfigLoader").getOrCreate()
        >>> host = "sqlserver.example.com"
        >>> database = "config_db"
        >>> user = "admin"
        >>> password = "securepassword"
        >>> project = "data_ingestion"
        >>> transformation_id = "tr-1223"
        >>> transformation_df, targets_df = load_config_tables_azure(
        ...     host=host,
        ...     database=database,
        ...     user=user,
        ...     password=password,
        ...     project=project,
        ...     transformation_id=source_id,
        ...     target_id=target_id,
        ...     spark=spark
        ... )
    """
    connection = SQLDatabaseConnection(
        host=host,
        database=database,
        user=user,
        password=password,
        framework=Framework.SPARK)

    provider = ReaderProvider(connector=connection,local_engine=Framework.SPARK)

    source_query = f"""
        SELECT
            *
        FROM [dbo].[TransformationConfig]
        WHERE transformation_id = '{transformation_id}' and project = '{project}'
            """

    transformation_df = provider.read(
        data_type=DataType.SQL,
        container="",  # Not used for SQL
        path_prefix="",  # Not used for SQL
        object_name=source_query,
        spark=spark  # ReaderProvider handles Spark internally
    ).toPandas()

    return transformation_df