from typing import Union
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame, SparkSession

from odibi_de_v2.core import DataType, Framework, ErrorType, BaseConnection
from odibi_de_v2.connector import LocalConnection
from odibi_de_v2.storage import (
    PandasDataSaver,
    SparkDataSaver,
    SparkStreamingDataSaver
)
from odibi_de_v2.utils import (
    enforce_types, log_call
)
from odibi_de_v2.logger import log_exceptions


class SaverProvider:
    """
    Unified provider for saving data using Pandas, Spark, or Spark Streaming.

    This class selects the appropriate saver class based on the connector’s framework.
    It supports cloud and local saves using a consistent interface, automatically applies
    Spark config options (if applicable), and handles streaming logic via a flag.

    Responsibilities:
    - Resolve file path and storage options using the connector
    - Apply authentication options to Spark if needed
    - Delegate the actual save logic to the correct saver implementation

    Decorators:
        - @log_call: Logs method entry/exit
        - @enforce_types: Ensures input validation
        - @log_exceptions: Catches and raises structured runtime errors

    Example:
        >>> provider = SaverProvider()
        >>> provider.save(
        ...     df=pd.DataFrame({"a": [1, 2]}),
        ...     data_type=DataType.CSV,
        ...     container="local",
        ...     path_prefix="/dbfs/tmp",
        ...     object_name="example.csv",
        ...     index=False
        ... )

        >>> connector = AzureBlobConnection(
        ...     account_name="myacct",
        ...     account_key="secret",
        ...     framework=Framework.SPARK
        ... )
        >>> provider = SaverProvider(connector)
        >>> provider.save(
        ...     df=spark_df,
        ...     data_type=DataType.PARQUET,
        ...     container="bronze",
        ...     path_prefix="curated/metrics",
        ...     object_name="metrics.parquet",
        ...     spark=spark,
        ...     mode="overwrite",
        ...     option={"compression": "snappy"}
        ... )
    """


    @log_call(module="STORAGE", component="SaverProvider")
    @enforce_types()
    @log_exceptions(
        module="STORAGE",
        component="SaverProvider",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def __init__(
        self,
        connector: BaseConnection = None,
        local_engine: Framework = Framework.PANDAS
    ):
        """
        Initialize the SaverProvider with an optional connector and local fallback engine.

        Args:
            connector (BaseConnection): A connector for local or cloud access. If None,
                defaults to LocalConnection.
            local_engine (Framework): The engine to use for local writes (PANDAS or SPARK).
                Defaults to Framework.PANDAS.
        """
        self.connector = connector or LocalConnection()
        self.local_engine = local_engine

    @log_call(module="STORAGE", component="SaverProvider")
    @enforce_types()
    @log_exceptions(
        module="STORAGE",
        component="SaverProvider",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def save(
        self,
        df: Union[pd.DataFrame, SparkDataFrame],
        data_type: DataType,
        container: str,
        path_prefix: str,
        object_name: str,
        spark: SparkSession = None,
        is_stream: bool = False,
        **kwargs
    ) -> None:
        """
        Saves the DataFrame using the appropriate framework and save strategy.

        Args:
            df (pd.DataFrame | Spark DataFrame): The dataset to save.
            data_type (DataType): Output format (CSV, JSON, PARQUET, etc.).
            container (str): Cloud container or local base path (e.g., "bronze" or "local").
            path_prefix (str): Folder or logical grouping inside the container.
            object_name (str): File name or object key to write.
            spark (SparkSession): Required for Spark and streaming saves.
            is_stream (bool): If True, uses SparkStreamingDataSaver. Defaults to False.
            **kwargs: Additional arguments passed to the underlying save method.

        Returns:
            None

        Raises:
            ValueError: If Spark session is missing for Spark-based saves.
            NotImplementedError: If framework is not supported.
            RuntimeError: If an internal save operation fails.

        Examples:
            >>> provider = SaverProvider()
            >>> df = pd.DataFrame({"id": [1, 2]})
            >>> provider.save(
            ...     df=df,
            ...     data_type=DataType.CSV,
            ...     container="local",
            ...     path_prefix="/dbfs/tmp/output",
            ...     object_name="data.csv",
            ...     index=False
            ... )

            >>> azure_connector = AzureBlobConnection(
            ...     account_name="youracct",
            ...     account_key="yourkey",
            ...     framework=Framework.SPARK
            ... )
            >>> provider = SaverProvider(azure_connector)
            >>> provider.save(
            ...     df=spark_df,
            ...     data_type=DataType.PARQUET,
            ...     container="bronze",
            ...     path_prefix="curated/daily",
            ...     object_name="snapshot.parquet",
            ...     spark=spark,
            ...     mode="overwrite"
            ... )

            >>> provider.save(
            ...     df=streaming_df,
            ...     data_type=DataType.DELTA,
            ...     container="bronze",
            ...     path_prefix="raw/streaming",
            ...     object_name="energy_stream",
            ...     spark=spark,
            ...     is_stream=True,
            ...     outputMode="append",
            ...     options={"checkpointLocation": "/mnt/bronze/_checkpoints"}
            ... )
        """
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
                saver = PandasDataSaver()
                return saver.save_data(
                    df=df,
                    data_type=data_type,
                    file_path=file_path,
                    storage_options=options,
                    **kwargs
                )

            case Framework.SPARK:
                if spark is None:
                    raise ValueError("Spark session is required for Spark saves.")

                if options:
                    for key, value in options.items():
                        spark.conf.set(key, value)

                saver = SparkStreamingDataSaver() if is_stream else SparkDataSaver()
                return saver.save_data(
                    df=df,
                    data_type=data_type,
                    file_path=file_path,
                    **kwargs
                )

            case _:
                raise NotImplementedError(f"Unsupported framework: {self.connector.framework}")
