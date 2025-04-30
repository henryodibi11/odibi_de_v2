import copy
from pyspark.sql import SparkSession, DataFrame
from odibi_de_v2.core import DataType, Framework
from odibi_de_v2.ingestion import ReaderProvider
from odibi_de_v2.databricks import (
    SourceOptionsResolver,
    BuildConnectionFromConfig
)
from odibi_de_v2.databricks.utils.api_ingestion import prepare_api_reader_kwargs_from_config
from odibi_de_v2.connector import LocalConnection

class SparkDataReaderFromConfig:
    """
    Initializes a SparkDataReaderFromConfig instance to read data from various sources based on configuration.

    This class constructor prepares the necessary attributes and connections to read data from different sources
    like ADLS, SQL databases, and API endpoints. It supports both batch and streaming data ingestion, determined
    by the configuration provided.

    Args:
        spark (SparkSession): An active Spark session to use for reading data.
        config (dict): Configuration dictionary specifying details about the data source, format, and other
            necessary parameters.
        dbutils (optional): Database utilities object, required primarily for API authentication and other
            database interactions.

    Attributes:
        spark (SparkSession): Active Spark session.
        config (dict): Deep copy of the source configuration.
        dbutils (optional): Database utilities for API interactions.
        file_format (str): Data format (e.g., 'csv', 'json', 'delta') extracted and converted to lower case from config.
        data_type (DataType): Enum representing the data format.
        is_streaming (bool): Boolean flag indicating if the data source is streaming, derived from config.
        connection (Connection): Connection object to interact with the data source.
        storage_unit (str): Storage unit or container name, applicable for file and database sources.
        object_name (str): Object or file name to read from, applicable for file sources.
        options (dict): Options resolved for reading data, specific to the mode (streaming or batch).

    Raises:
        ValueError: If 'file_format' is not specified in the config.
        KeyError: If essential keys like 'connection_config' are missing in the config.

    Example:
        >>> spark_session = SparkSession.builder.appName("Example").getOrCreate()
        >>> config = {
            "file_format": "csv",
            "is_autoloader": False,
            "connection_config": {
                "storage_unit": "my-container",
                "object_name": "data.csv"
            }
        }
        >>> reader = SparkDataReaderFromConfig(spark_session, config)
        >>> dataframe = reader.read_data()
        >>> dataframe.show()
    """

    def __init__(self, spark: SparkSession, config: dict, dbutils=None):
        """
        Initializes a new instance of the data processing class, setting up the environment
        based on the provided Spark session and configuration.

        This constructor prepares the necessary connections and configurations for data processing,
        either from a local or remote source, depending on the specified file format and other
        configuration settings. It supports both batch and streaming data operations.

        Args:
            spark (SparkSession): The Spark session to be used for data operations.
            config (dict): A dictionary containing configuration settings such as file format,connection details,
                and mode of data loading (batch or streaming).
            dbutils (optional): A utility object provided by Databricks, used primarily for API authentication.
                Default is None.

        Attributes:
            spark (SparkSession): Stores the provided Spark session.
            config (dict): A deep copy of the configuration settings.
            dbutils: Stores the dbutils object if provided.
            file_format (str): The file format extracted and converted to lower case from the config.
            data_type (DataType): An enum representing the type of data source based on the file format.
            is_streaming (bool): A flag determining if the data should be processed as a stream.
            connection: The connection object created based on the data type.
            storage_unit (str): The storage unit identifier from the connection configuration.
            object_name (str): The object name from the connection configuration.
            options (dict): Options for data processing, resolved based on the mode (streaming or batch).

        Raises:
            KeyError: If essential keys are missing in the `config` dictionary.
            ValueError: If the provided file format is not supported.

        Example:
            >>> spark_session = SparkSession.builder.appName("Example").getOrCreate()
            >>> config = {
                "file_format": "json",
                "connection_config": {
                    "storage_unit": "s3_bucket",
                    "object_name": "data_file"
                },
                "is_autoloader": True
            }
            >>> processor = DataProcessor(spark_session, config)
        """
        self.spark = spark
        self.config = copy.deepcopy(config)
        self.dbutils = dbutils  # Only needed for API auth
        self.file_format = self.config["file_format"].lower()
        self.data_type = DataType(self.file_format)
        self.is_streaming = self.config.get("is_autoloader", False)

        if self.data_type != DataType.API:
            self.connection = BuildConnectionFromConfig(self.config).get_connection()
            self.storage_unit = self.config["connection_config"]["storage_unit"]
            self.object_name = self.config["connection_config"]["object_name"]
            self.options = self._resolve_options("streaming" if self.is_streaming else "batch")
        else:
            self.connection = LocalConnection()
            self.storage_unit = ""
            self.object_name = ""
            self.options = {}  # Will be constructed inside read_data()

    def _resolve_options(self, mode: str) -> dict:
        """
        Resolves and returns options for a data source based on the specified mode.

        This method initializes a `SourceOptionsResolver` with the current connection,
        configuration, and mode. It then calls the `resolve` method on the resolver instance
        to get the appropriate options for the data source.

        Args:
            mode (str): The mode of operation which can influence how options are resolved.
                Typical modes might include 'read', 'write', etc.

        Returns:
            dict: A dictionary containing resolved options based on the provided mode.

        Example:
            >>> _resolve_options('read')
            {'timeout': 120, 'retry': 3}

        Note:
            This method is intended to be used internally, hence the leading underscore in its name.
        """
        return SourceOptionsResolver(self.connection, self.config, mode).resolve()

    def read_data(self) -> DataFrame:
        """
        Reads data from various sources and formats using a specified provider.

        This method determines the source type from the instance's attributes and configures the appropriate
        reader from the `ReaderProvider`. It supports reading from APIs, files, and SQL databases. The method
        dynamically adjusts the reading parameters based on the source type and additional configuration provided
        in the instance's attributes.

        Returns:
            pyspark.sql.DataFrame: A DataFrame containing the data read from the specified source.

        Raises:
            ValueError: If the configuration is invalid or insufficient for the specified data type.
            ConnectionError: If there is a failure in connecting to the data source.

        Example:
            Assuming `data_reader` is an instance of a class containing this method and properly initialized:

            df = data_reader.read_data()
            print(df.show())


        Note:
            This method requires the instance to have `connection`, `data_type`, `config`, `dbutils`, `spark`,
            `storage_unit`, `is_streaming`, and `options` attributes properly set.
        """
        provider = ReaderProvider(
            connector=self.connection,
            local_engine=Framework.SPARK
        )

        if self.data_type == DataType.API:
            api_kwargs = prepare_api_reader_kwargs_from_config(self.config, self.dbutils)
            return provider.read(
                data_type=DataType.API,
                container="", path_prefix="", object_name="",
                spark=self.spark,
                **api_kwargs
            )

        # For file- or SQL-based sources
        object_name = (
            self.config["connection_config"]["object_name"]
            if self.config["source_type"] != "sql"
            else self.config["source_path_or_query"]
        )

        return provider.read(
            data_type=self.data_type,
            container=self.storage_unit,
            path_prefix="",
            object_name=object_name,
            spark=self.spark,
            is_stream=self.is_streaming,
            **self.options
        )
