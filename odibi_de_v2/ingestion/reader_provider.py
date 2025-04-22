from typing import  Union
import pandas as pd
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame

from odibi_de_v2.core import (
    DataType, Framework, ErrorType,BaseConnection)
from odibi_de_v2.connector import LocalConnection
from odibi_de_v2.ingestion import (
    SparkDataReader, SparkStreamingDataReader, PandasDataReader)
from odibi_de_v2.utils import (
    enforce_types, log_call, validate_non_empty)
from odibi_de_v2.logger import log_exceptions

class ReaderProvider:
    """
    Reads data from local or cloud sources using the appropriate reader engine.

    This method:
    - Resolves the file path using the connector
    - Applies any required Spark config (if using Spark)
    - Dispatches to the correct reader (Pandas, Spark, Spark Streaming)
    - Returns a DataFrame using the delegated reader logic

    Args:
        data_type (DataType): File format enum (CSV, JSON, PARQUET, etc.).
        container (str): Top-level storage container (e.g., "bronze").
        path_prefix (str): Sub-directory path (e.g., "raw/events").
        object_name (str): File name (e.g., "sales.csv").
        spark (SparkSession): Spark session (required for Spark readers).
        is_stream (bool, optional): If True, use SparkStreamingDataReader.
        **kwargs: Additional reader options (e.g., sep, header, multiline).

    Returns:
        Union[pd.DataFrame, pyspark.sql.DataFrame]: Loaded dataset

    Raises:
        ValueError: If Spark is required but not provided
        NotImplementedError: If an unsupported framework is specified
        RuntimeError: If any internal method fails (via decorated exception handler)
    Example:
        >>> provider = ReaderProvider()
        >>> df = provider.read(
        ...     data_type=DataType.CSV,
        ...     container="bronze",
        ...     path_prefix="raw",
        ...     object_name="sales.csv",
        ...     sep=","
        ... )
    """


    @log_call(module="INGESTION", component="ReaderProvider")
    @enforce_types()
    @log_exceptions(
        module="INGESTION",
        component="ReaderProvider",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def __init__(
        self,
        connector: BaseConnection = LocalConnection(),
        local_engine: Framework = Framework.PANDAS):
        """
        Initializes a new instance of the ReaderProvider class with specified connection and data processing settings.

        This constructor sets up a data connection and a local data processing engine for the ReaderProvider instance.
        It allows customization of the data connection through the `connector` parameter, which must be an instance of
        a class derived from `BaseConnection`. If no connector is specified, it defaults to `LocalConnection`. The
        `local_engine` parameter determines the framework used for local data processing, defaulting to Pandas unless
        specified otherwise.

        Args:
            connector (BaseConnection, optional): The connection handler for accessing data sources. It should provide
                methods like `get_file_path()` and `get_storage_options()`. Defaults to `LocalConnection`.
            local_engine (Framework, optional): The data processing framework to use locally. Options include
                ['7]`Framework.PANDAS` and `Framework.SPARK`, with a default of `Framework.PANDAS`.

        Returns:
            None: Constructor does not return any value.

        Raises:
            TypeError: If `connector` is not an instance of a class that inherits from `BaseConnection` or does not
            implement the required methods.

        Example:
            # Creating a ReaderProvider with default settings
            reader_provider = ReaderProvider()

            # Using a custom connector and specifying Spark as the local processing engine
            custom_connector = CustomConnection()
            reader_provider = ReaderProvider(connector=custom_connector, local_engine=Framework.SPARK)
        """

        self.connector = connector
        self.local_engine = local_engine


    @log_call(module="INGESTION", component="ReaderProvider")
    @enforce_types(strict=False)
    @log_exceptions(
        module="INGESTION",
        component="ReaderProvider",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def read(
        self,
        data_type: DataType,
        container: str,
        path_prefix: str,
        object_name: str,
        spark: SparkSession = None,
        is_stream: bool = False,
        **kwargs
        ) -> Union[pd.DataFrame, SparkDataFrame]:
        """
        Reads data from specified storage using a configured reader based on the data type and execution context.

        This method dynamically selects the appropriate data reading mechanism (Pandas, Spark, or Spark Streaming) based
        on the provided parameters and the environment configuration. It supports reading from various data formats and
        handles different storage frameworks (local or cloud-based).

        Args:
            data_type (DataType): The format of the data file to be read (e.g., CSV, JSON, PARQUET).
            container (str): The name of the top-level storage container where data files are located.
            path_prefix (str): The path prefix within the container where data files are stored.
            object_name (str): The specific name of the data file to be read.
            spark (SparkSession, optional): An instance of SparkSession if Spark-based operations are required.
                Defaults to None.
            is_stream (bool, optional): Flag to determine if Spark streaming should be used for reading data.
                Defaults to False.
            **kwargs: Arbitrary keyword arguments that are passed to the underlying data reader. These could include
                options like 'sep' for CSV files, 'header' presence, etc.

        Returns:
            Union[pd.DataFrame, pyspark.sql.DataFrame]: A DataFrame object containing the loaded data, which could
                either be a pandas DataFrame or a Spark DataFrame depending on the execution context.

        Raises:
            ValueError: If a Spark session is required but not provided, or if other necessary parameters are missing.
            NotImplementedError: If the specified framework or data type is not supported by the implementation.
            RuntimeError: If there is an error during the execution of the data reading process.

        Example:
            >>> read(DataType.CSV, "data_bucket", "2021/data", "sales.csv", spark=my_spark_session)
            Returns a DataFrame containing data from the 'sales.csv' file located in '2021/data' within 'data_bucket'.
        """
        if data_type == DataType.API:
            if spark:
                from odibi_de_v2.ingestion import SparkAPIReader
                return SparkAPIReader(spark=spark, **kwargs).read_data()
            else:
                from odibi_de_v2.ingestion import PandasAPIReader
                return PandasAPIReader(**kwargs).read_data()


        if not self.connector:
            raise ValueError("Connector is required to resolve paths.")

        file_path = self.connector.get_file_path(
            container=container,
            path_prefix=path_prefix,
            object_name=object_name)

        options = self.connector.get_storage_options()
        framework = self.connector.framework
        if framework == Framework.LOCAL:
            framework = self.local_engine

        match framework:
            case Framework.PANDAS:
                reader = PandasDataReader()
                return reader.read_data(
                    data_type=data_type,
                    file_path=file_path,
                    storage_options=options,
                    **kwargs
                )

            case Framework.SPARK:
                if spark is None:
                    raise ValueError("Spark session is required for Spark reads.")

                if options and data_type != DataType.SQL:
                    for key, value in options.items():
                        spark.conf.set(key, value)
                    options = {}

                reader = SparkStreamingDataReader() if is_stream else SparkDataReader()
                return reader.read_data(
                    data_type=data_type,
                    file_path=file_path,
                    spark=spark,
                    storage_options=options,
                    **kwargs
                )
            case _:
                raise NotImplementedError(f"Framework {self.connector.framework} is not supported.")
