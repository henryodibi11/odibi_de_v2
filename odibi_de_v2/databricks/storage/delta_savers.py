from odibi_de_v2.core.enums import DataType, ErrorType
from odibi_de_v2.logger import log_and_optionally_raise
from odibi_de_v2.storage import SaverProvider
from odibi_de_v2.databricks.utils.metadata_helpers import (
    add_ingestion_metadata, add_hash_columns)
from odibi_de_v2.databricks.config import TargetOptionsResolver
from odibi_de_v2.databricks.delta.delta_table_manager import DeltaTableManager
from odibi_de_v2.databricks.delta.delta_merge_manager import DeltaMergeManager
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

def save_static_data_from_config(df, config: dict, spark, connector):
    """
    Saves a static Spark DataFrame based on configuration settings using a specified connector.

    This function handles the saving of a Spark DataFrame by applying ingestion metadata,
    resolving batch save options from the configuration, and using a SaverProvider to persist
    the data. It also logs the process and registers the saved data as a table in a Spark SQL database.

    Args:
        df (DataFrame): The Spark DataFrame to be saved.
        config (dict): Configuration dictionary containing all necessary settings, including `connection_config`
        and `target_options`.
        spark (SparkSession): The active Spark session instance.
        connector (BaseConnection): The connector instance used for saving the data, such as `AzureBlobConnection`.

    Raises:
        RuntimeError: If any error occurs during the saving process, encapsulating the original exception.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> from your_connector_module import YourConnector
        >>> spark = SparkSession.builder.appName("ExampleApp").getOrCreate()
        >>> df = spark.createDataFrame([{"id": 1, "value": "A"}])
        >>> config = {
            "connection_config": {"storage_unit": "your_container", "object_name": "your_object"},
            "target_path_or_table": "database.table"
        }
        >>> connector = YourConnector()
        >>> save_static_data_from_config(df, config, spark, connector)
    """

    try:
        log_and_optionally_raise(
            module="STORAGE",
            component="save_static_data_from_config",
            method="save",
            error_type=ErrorType.NO_ERROR,
            message="Saving static data using config and SaverProvider...",
            level="INFO"
        )

        # Add standard metadata
        df = add_ingestion_metadata(df)

        # Resolve dynamic batch options
        batch_options = TargetOptionsResolver(config, mode="batch").resolve()

        # Extract path info
        target_path_or_table = config["target_path_or_table"]
        conn_cfg = config["connection_config"]
        container = conn_cfg["storage_unit"]
        object_name = conn_cfg["object_name"]
        file_path = connector.get_file_path(container, object_name, "")

        # Delegate save logic
        SaverProvider(connector=connector).save(
            df=df,
            data_type=DataType.DELTA,
            container=container,
            path_prefix="",
            object_name=object_name,
            spark=spark,
            is_stream=False,
            **batch_options
        )
        # Register the table using SQL
        register_list= target_path_or_table.split(".")
        table = register_list[1]
        database = register_list[0]
        DeltaTableManager(spark, file_path, is_path=True).register_table(
            table_name=table,
            database=database
        )
        log_and_optionally_raise(
            module="STORAGE",
            component="save_static_data_from_config",
            method="save",
            error_type=ErrorType.NO_ERROR,
            message=f"Data successfully saved to: {object_name}",
            level="INFO"
        )

    except Exception as e:
        raise RuntimeError(f"[save_static_data_from_config] Save failed: {e}") from e




def _table_exists(spark, table_name: str) -> bool:
    """
    Checks if a specified table exists in the Spark session.

    This function attempts to describe a table using Spark SQL to
    determine if it exists in the current Spark session's catalog.

    Args:
        spark (SparkSession): The Spark session instance to use for querying the database.
        table_name (str): The name of the table to check for existence.

    Returns:
        bool: True if the table exists, False otherwise.

    Raises:
        AnalysisException: If an error occurs during the execution of
            the SQL query, typically indicating the table does not exist.

    Example:
        >>> spark = SparkSession.builder.appName("Example").getOrCreate()
        >>> _table_exists(spark, "users")
        True
    """
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except AnalysisException:
        return False


def save_or_merge_delta(df, config: dict, spark, connector):
    """
    Saves or merges a DataFrame to a Delta table based on configuration settings.

    This function checks if a Delta table specified in the configuration exists.
    If it does, it performs a merge operation using specified keys and columns.If the
    table does not exist, it bootstraps a new Delta table and registers it in the catalog.

    Args:
        df (DataFrame): The Spark DataFrame to save or merge.
            config (dict): Configuration dictionary containing settings for connection, merge,
            and target path or table. Expected keys are 'connection_config', 'merge_config',
            and 'target_path_or_table'.
        spark (SparkSession): The active Spark session instance.
        connector (Connector): An instance of a connector class, which provides methods
            to interact with storage systems.

    Raises:
        RuntimeError: If any error occurs during the save or merge process.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.appName("ExampleApp").getOrCreate()
        >>> df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        >>> config = {
            "connection_config": {"storage_unit": "s3", "object_name": "people"},
            "merge_config": {
                "merge_keys": ["id"],
                "change_columns": ["name"],
                "catalog_table": "people_table",
                "catalog_database": "default"
            },
            "target_path_or_table": "s3://data-lake/people_table"
        }
        >>> connector = SomeConnector()
        >>> save_or_merge_delta(df, config, spark, connector)
    """
    try:
        log_and_optionally_raise(
            module="STORAGE",
            component="save_or_merge_delta",
            method="save",
            error_type=ErrorType.NO_ERROR,
            message="Saving or merging Delta table...",
            level="INFO"
        )

        conn_cfg = config["connection_config"]
        storage_unit = conn_cfg["storage_unit"]
        object_name = conn_cfg["object_name"]
        file_path = connector.get_file_path(storage_unit, "", object_name)

        spark_config = connector.get_storage_options()
        for key, value in spark_config.items():
            spark.conf.set(key, value)

        merge_cfg = config["merge_config"]
        merge_keys = merge_cfg["merge_keys"]
        change_columns = merge_cfg["change_columns"]
        table = merge_cfg["catalog_table"]
        database = merge_cfg["catalog_database"]
        full_table_name = config["target_path_or_table"]

        df = df.dropDuplicates(merge_keys)
        df = add_hash_columns(df, merge_keys, change_columns)
        df = add_ingestion_metadata(df)

        if not _table_exists(spark, full_table_name):
            print(f"[Delta] Bootstrapping new table: {full_table_name}")

            # Save using SaverProvider
            batch_options = TargetOptionsResolver(config, mode="batch").resolve()

            SaverProvider(connector=connector).save(
                df=df,
                data_type=DataType.DELTA,
                container=storage_unit,
                path_prefix="",
                object_name=object_name,
                spark=spark,
                is_stream=False,
                **batch_options
            )

            # Register the table using SQL
            DeltaTableManager(spark, file_path, is_path=True).register_table(
                table_name=table,
                database=database
            )
        else:
            print(f"[Delta] Performing MERGE on existing table: {full_table_name}")
            DeltaMergeManager(spark, full_table_name, is_path=False).merge(
                source_df=df,
                merge_keys=merge_keys,
                change_columns=change_columns
            )

        log_and_optionally_raise(
            module="STORAGE",
            component="save_or_merge_delta",
            method="save",
            error_type=ErrorType.NO_ERROR,
            message=f"Successfully saved or merged Delta table: {full_table_name}",
            level="INFO"
        )

    except Exception as e:
        raise RuntimeError(f"[save_or_merge_delta] Failed to save or merge: {e}") from e
