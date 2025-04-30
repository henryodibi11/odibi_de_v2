import copy
from pyspark.sql import DataFrame, SparkSession
from odibi_de_v2.core.enums import DataType, ErrorType
from odibi_de_v2.logger import log_and_optionally_raise
from odibi_de_v2.databricks.config import TargetOptionsResolver
from odibi_de_v2.databricks.utils.metadata_helpers import (
    add_ingestion_metadata, add_hash_columns
)
from odibi_de_v2.storage import SaverProvider
from odibi_de_v2.databricks.storage.helpers import wrap_for_foreach_batch_from_registry
from odibi_de_v2.databricks.storage.function_registry import get_function_registry
from delta import DeltaTable
from odibi_de_v2.databricks.delta.delta_table_manager import DeltaTableManager



class SparkDataSaverFromConfig:
    """
    Executes the appropriate save method based on the DataFrame's streaming status.

    This method determines whether the provided DataFrame is a streaming or batch DataFrame
    and calls the respective private method (`_save_batch` or `_save_stream`) to handle the
    saving process according to the configuration provided during the initialization of the
    class instance.

    Args:
        df (DataFrame): The Spark DataFrame to be saved. This can either be a batch or
            streaming DataFrame.

    Raises:
        ValueError: If the batch save method specified in the configuration is not found
            in the function registry.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> from pyspark.sql import DataFrame
        >>> spark = SparkSession.builder.appName("ExampleApp").getOrCreate()
        >>> df = spark.read.format("csv").option("header", "true").load("path/to/csv")
        >>> config = {
            "connection_config": {
                "storage_unit": "my_storage",
                "object_name": "my_data_object"
            },
            "target_path_or_table": "database.table_name"
        }
        >>> connector = SomeConnector(config)
        >>> saver = SparkDataSaverFromConfig(spark, config, connector)
        >>> saver.save(df)
    """

    def __init__(self, spark: SparkSession, config: dict, connector):
        """
        Initializes a new instance of the data processing class, setting up the necessary configurations and paths for data handling.

        Args:
            spark (SparkSession): An active SparkSession instance to handle Spark operations.
            config (dict): Configuration dictionary containing necessary parameters such as connection configurations and target paths or tables.
            connector: An object responsible for connecting to data sources and handling file paths.

        Attributes:
            spark (SparkSession): The SparkSession used for data operations.
            config (dict): A deep copy of the configuration dictionary provided at initialization.
            connector: The connector object used for accessing file paths.
            storage_unit (str): The storage unit name extracted from the connection configuration in `config`.
            object_name (str): The object name extracted from the connection configuration in `config`.
            file_path (str): The full file path for the data object, obtained using the connector.
            target_path_or_table (str): The target path or table where data will be stored or manipulated.
            data_type (DataType): The type of data format, initialized as `DataType.DELTA`.
            function_registry: A registry of functions available for data manipulation, obtained from a global function registry.

        Raises:
            KeyError: If the necessary keys are missing in the `config` dictionary.

        Example:
            # Assuming `spark_session`, `config_dict`, and `data_connector` are predefined:
            processor = DataProcessor(spark_session, config_dict, data_connector)
        """
        self.spark = spark
        self.config = copy.deepcopy(config)
        self.connector = connector
        self.storage_unit = config["connection_config"]["storage_unit"]
        self.object_name = config["connection_config"]["object_name"]
        self.file_path = connector.get_file_path(self.storage_unit, self.object_name, "")
        self.target_path_or_table = config["target_path_or_table"]
        self.data_type = DataType.DELTA
        self.function_registry = get_function_registry()

    def save(self, df: DataFrame):
        """
        Saves a DataFrame to a storage system, handling both streaming and batch data.

        This method checks if the DataFrame is streaming and calls the appropriate private method
        to handle the save operation based on the DataFrame's type.

        Args:
            df (DataFrame): The Spark DataFrame to be saved. This can either be a streaming or a batch DataFrame.

        Returns:
            None

        Raises:
            AttributeError: If the DataFrame does not have the 'isStreaming' attribute.

        Example:
            >>> df = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
            >>> saver = DataSaver()
            >>> saver.save(df)
        """
        if df.isStreaming:
            self._save_stream(df)
        else:
            self._save_batch(df)


    def _save_batch(self, df: DataFrame):
        """
        Saves a DataFrame based on configuration-driven batch logic.

        This method processes a DataFrame by adding ingestion metadata, resolving save options from
        the configuration, and delegating the save operation to a SaverProvider. It supports dynamic
        selection of the save method through configuration and handles the registration of the table
        in a SQL database.

        Args:
            df (DataFrame): The DataFrame to be saved.

        Returns:
            None

        Raises:
            ValueError: If the specified batch save method is not found in the function registry.

        Example:
            Assuming `df` is a DataFrame object, `config` is set up with necessary parameters, and all required
            objects and methods (`spark`, `connector`, `function_registry`, etc.) are defined:

            self._save_batch(df)


        Note:
            This method is intended to be used internally within the class and should be accessed through
            public interfaces that ensure all dependencies are properly configured.
        """
        method_name = (
            self.config.get("target_options", {})
            .get("databricks", {})
            .get("method", "")
            .strip()
            )

        if method_name:
            save_fn = self.function_registry.get(method_name)
            if not save_fn:
                raise ValueError(f"Batch save method '{method_name}' not found in registry.")
            return save_fn(df=df, spark=self.spark, connector=self.connector, config=self.config)


        df = add_ingestion_metadata(df)
        options = TargetOptionsResolver(self.config, mode="batch").resolve()

        SaverProvider(self.connector).save(
            df=df,
            data_type=self.data_type,
            container=self.storage_unit,
            path_prefix=self.object_name,
            object_name="",
            spark=self.spark,
            is_stream=False,
            **options)

        # Register the table using SQL
        register_list= self.target_path_or_table.split(".")
        table = register_list[1]
        database = register_list[0]
        DeltaTableManager(self.spark, self.file_path, is_path=True).register_table(
            table_name=table,
            database=database
        )

    def _save_stream(self, df: DataFrame):
        """
        Saves a streaming DataFrame based on configuration settings.

        This method configures and initiates the saving of a streaming DataFrame using options
        specified in the configuration. It resolves the checkpoint location, potentially wraps
        a custom foreachBatch handler, and starts the streaming write process using the resolved settings.

        Args:
            df (DataFrame): The streaming DataFrame to be saved.

        Raises:
            KeyError: If essential configuration options are missing.
            Exception: If the DataFrame cannot be saved due to an operational error.

        Example:
            Assuming `self.config`, `self.connector`, `self.storage_unit`, `self.data_type`, and `self.object_name`
            are predefined and `df` is a streaming DataFrame:

            self._save_stream(df)


        Note:
            This method is intended to be used internally and handles specific low-level operations
            tailored to the system's architecture.
        """
        options = TargetOptionsResolver(self.config, mode="streaming").resolve()
        checkpoint_rel_path = options.get("options", {}).get("checkpointLocation", "")
        resolved_checkpoint = self.connector.get_file_path(self.storage_unit, checkpoint_rel_path, "")

        options["options"]["checkpointLocation"] = resolved_checkpoint

        # Resolve and wrap handler if provided
        foreach_fn_name = options.get("foreachBatch")
        if foreach_fn_name:
            handler = wrap_for_foreach_batch_from_registry(
                function_name=foreach_fn_name,
                spark=self.spark,
                connector=self.connector,
                config=self.config)
            options["foreachBatch"] = handler

        SaverProvider(self.connector).save(
            df=df,
            data_type=self.data_type,
            container=self.storage_unit,
            path_prefix="",
            object_name=self.object_name,
            spark=self.spark,
            is_stream=True,
            **options)
