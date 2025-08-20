from odibi_de_v2.ingestion import ReaderProvider
from odibi_de_v2.connector import SQLDatabaseConnection
from odibi_de_v2.core.enums import Framework, DataType
import pandas as pd
from pyspark.sql import SparkSession


def load_ingestion_config_tables(
    host: str,
    database: str,
    user: str,
    password: str,
    project: str,
    enviornment: str = 'qat',
    source_id: str,
    target_id: str,
    spark: SparkSession
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Loads configuration data for source and target from a SQL Server and returns them as Pandas DataFrames.

    This function queries the SQL Server based on provided source and target IDs, and project details. It retrieves
    configuration data for both ingestion sources and targets, which are then converted to Pandas DataFrames for
    further processing.

    Args:
        host (str): The hostname or IP address of the SQL Server.
        database (str): The name of the database from which to fetch the configuration data.
        user (str): The username used to authenticate with SQL Server.
        password (str): The password used to authenticate with SQL Server.
        project (str): The name of the project for which configuration data is being fetched.
        enviornment (str): The environment for which the configuration data is being fetched. Defaults to 'qat'.
        source_id (str): The identifier for the source configuration to be fetched.
        target_id (str): The identifier for the target configuration to be fetched.
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
        >>> source_id = "source123"
        >>> target_id = "target456"
        >>> sources_df, targets_df = load_config_tables_azure(
        ...     host=host,
        ...     database=database,
        ...     user=user,
        ...     password=password,
        ...     project=project,
        ...     source_id=source_id,
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
            isc.source_id, isc.project, isc.source_name, isc.source_type,
            isc.source_path_or_query, isc.file_format, isc.is_autoloader,
            isc.source_options, isc.connection_config,
            sc.secret_scope, sc.identifier_key, sc.credential_key,
            sc.server, sc.connection_string_key, sc.auth_type,
            sc.token_header_name, sc.[description]
        FROM IngestionSourceConfig isc
        LEFT JOIN SecretsConfig sc ON isc.secret_config_id = sc.secret_config_id
        WHERE isc.project = '{project}' AND isc.source_id = '{source_id}'
        AND (isc.environment = {enviornment} OR isc.environment IS NULL)
    """

    target_query = f"""
        SELECT
            itc.target_id, itc.project, itc.target_name, itc.target_type,
            itc.target_path_or_table, itc.write_mode, itc.target_options,
            itc.connection_config, itc.merge_config,
            sc.secret_scope, sc.identifier_key, sc.credential_key,
            sc.server, sc.connection_string_key, sc.auth_type,
            sc.token_header_name, sc.[description]
        FROM IngestionTargetConfig itc
        LEFT JOIN SecretsConfig sc ON itc.secret_config_id = sc.secret_config_id
        WHERE itc.project = '{project}' AND itc.target_id = '{target_id}'
        AND (itc.environment = {enviornment} OR itc.environment IS NULL)
    """

    sources_df = provider.read(
        data_type=DataType.SQL,
        container="",  # Not used for SQL
        path_prefix="",  # Not used for SQL
        object_name=source_query,
        spark=spark  # ReaderProvider handles Spark internally
    ).toPandas()

    targets_df = provider.read(
        data_type=DataType.SQL,
        container="",
        path_prefix="",
        object_name=target_query,
        spark=spark
    ).toPandas()

    return sources_df, targets_df

