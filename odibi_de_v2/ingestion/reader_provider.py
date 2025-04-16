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
    Unified provider for reading structured data using Pandas, Spark, or Spark Streaming.

    This class dynamically selects the appropriate reader based on the framework specified
    in the connector. If the connector is local, it defaults to a configurable engine
    (PANDAS or SPARK). It supports cloud and local paths using the same interface.

    Responsibilities:
    - Resolve file path and storage options using the connector
    - Automatically apply Spark config options (if applicable)
    - Select the correct reader (Pandas, Spark, or Spark Streaming)
    - Delegate actual read logic to the reader class

    Decorators:
        - @log_call: Logs method entry and exit
        - @enforce_types: Enforces type safety on init and read()
        - @log_exceptions: Catches and raises decorated runtime errors

    Note:
        This class is intentionally lightweight. It delegates validation, benchmarking,
        and logging of the read logic to the concrete reader classes themselves.

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
        connector: BaseConnection = None,
        local_engine: Framework = Framework.PANDAS):
        """
        Initialize the ReaderProvider with an optional connector and local engine fallback.

        Args:
            connector (BaseConnection): A cloud or local connection implementing
                `get_file_path()` and `get_storage_options()`. If not provided, defaults to LocalConnection.
            local_engine (Framework): If the connector is local, this determines whether
                to read using Pandas or Spark. Defaults to Framework.PANDAS.
        """

        self.connector = connector or LocalConnection()
        self.local_engine = local_engine


    @log_call(module="INGESTION", component="ReaderProvider")
    @enforce_types()
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
        """


        if not self.connector:
            raise ValueError("Connector is required to resolve paths.")

        file_path = self.connector.get_file_path(
            container=container,
            path_prefix=path_prefix,
            object_name=object_name
        )

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

                if options:
                    for key, value in options.items():
                        spark.conf.set(key, value)

                reader = SparkStreamingDataReader() if is_stream else SparkDataReader()
                return reader.read_data(
                    data_type=data_type,
                    file_path=file_path,
                    spark=spark,
                    **kwargs
                )
            case _:
                raise NotImplementedError(f"Framework {self.connector.framework} is not supported.")
