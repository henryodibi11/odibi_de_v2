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
    Unified provider for persisting datasets using Pandas, Spark, or Spark Streaming.

    This class automatically selects the correct saver implementation
    (`PandasDataSaver`, `SparkDataSaver`, or `SparkStreamingDataSaver`)
    based on the configured connector framework.

    Key Features:
    -------------
    - **Connector-aware**: Resolves file paths or SQL table names using the
      connector’s ``get_file_path`` method.
    - **Authentication handling**: Injects storage or JDBC connection options
      from the connector’s ``get_storage_options`` output.
    - **SQL support**: Transparently handles Pandas (via SQLAlchemy) and Spark
      (via JDBC) SQL writes when ``data_type == DataType.SQL``.
    - **Streaming support**: Delegates to `SparkStreamingDataSaver` if the
      ``is_stream`` flag is enabled.

    Responsibilities:
    -----------------
    - Normalize connector framework: fall back to local_engine if connector
      specifies ``Framework.LOCAL``.
    - Apply Spark-specific configs (e.g., credentials) when writing to cloud
      storage, but skip these for SQL writes.
    - Dispatch to the appropriate saver class and return its result.

    Decorators:
    -----------
    - ``@log_call``: Logs method entry/exit for debugging.
    - ``@enforce_types``: Ensures runtime type validation of arguments.
    - ``@log_exceptions``: Captures and raises structured runtime errors.

    Examples:
    ---------
    **Pandas CSV Save**
    >>> provider = SaverProvider()
    >>> df = pd.DataFrame({"id": [1, 2]})
    >>> provider.save(
    ...     df=df,
    ...     data_type=DataType.CSV,
    ...     container="local",
    ...     path_prefix="/dbfs/tmp",
    ...     object_name="data.csv",
    ...     index=False
    ... )

    **Pandas SQL Save**
    >>> sql_connector = SQLDatabaseConnection(
    ...     host="myserver.database.windows.net",
    ...     database="sales_db",
    ...     user="admin",
    ...     password="secret",
    ...     framework=Framework.PANDAS
    ... )
    >>> provider = SaverProvider(connector=sql_connector)
    >>> provider.save(
    ...     df=pd.DataFrame({"id": [1, 2]}),
    ...     data_type=DataType.SQL,
    ...     container="",    # ignored for SQL
    ...     path_prefix="",  # ignored for SQL
    ...     object_name="FactSales",  # table name
    ...     mode="overwrite"
    ... )

    **Spark Parquet Save**
    >>> spark = SparkSession.builder.getOrCreate()
    >>> azure_connector = AzureBlobConnection(
    ...     account_name="acct",
    ...     account_key="key",
    ...     framework=Framework.SPARK
    ... )
    >>> provider = SaverProvider(connector=azure_connector)
    >>> provider.save(
    ...     df=spark_df,
    ...     data_type=DataType.PARQUET,
    ...     container="bronze",
    ...     path_prefix="curated/daily",
    ...     object_name="snapshot.parquet",
    ...     spark=spark,
    ...     mode="overwrite"
    ... )

    **Spark SQL Save**
    >>> sql_connector = SQLDatabaseConnection(
    ...     host="myserver.database.windows.net",
    ...     database="sales_db",
    ...     user="admin",
    ...     password="secret",
    ...     framework=Framework.SPARK
    ... )
    >>> provider = SaverProvider(connector=sql_connector)
    >>> provider.save(
    ...     df=spark_df,
    ...     data_type=DataType.SQL,
    ...     container="",    # ignored for SQL
    ...     path_prefix="",  # ignored
    ...     object_name="FactSales",  # table name
    ...     spark=spark,
    ...     mode="append"
    ... )
    """

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

        Parameters
        ----------
        connector : BaseConnection, optional
            Connector responsible for path resolution and authentication.
            If None, defaults to a `LocalConnection`.
        local_engine : Framework, default=Framework.PANDAS
            Local execution framework to use if connector specifies
            `Framework.LOCAL`. Can be `Framework.PANDAS` or `Framework.SPARK`.
        """
        self.connector = connector or LocalConnection()
        self.local_engine = local_engine

    @log_call(module="STORAGE", component="SaverProvider")
    @enforce_types(strict=False)
    @log_exceptions(
        module="STORAGE",
        component="SaverProvider",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    @log_call(module="STORAGE", component="SaverProvider")
    @enforce_types(strict=False)
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
        Persist a dataset using the correct saver implementation.

        Parameters
        ----------
        df : Union[pd.DataFrame, pyspark.sql.DataFrame]
            DataFrame to be saved.
        data_type : DataType
            Target format (CSV, JSON, PARQUET, AVRO, SQL, etc.).
        container : str
            Logical container for the destination (e.g., `bronze`, `silver`, `gold`).
            Ignored for SQL writes.
        path_prefix : str
            Path within the container. Ignored for SQL writes.
        object_name : str
            Target file name (for file-based writes) or table name (for SQL).
        spark : SparkSession, optional
            Active Spark session, required for Spark- and streaming-based saves.
        is_stream : bool, default=False
            If True, use `SparkStreamingDataSaver`.
        **kwargs : dict
            Additional options passed to the underlying saver:
            - For Pandas SQL: `mode` ("append" or "overwrite")
            - For Spark SQL: `mode` + JDBC `storage_options`
            - For file-based saves: Spark writer options like `partitionBy`, `option`.

        Raises
        ------
        ValueError
            If Spark is required but not provided.
        NotImplementedError
            If the framework is unsupported.
        RuntimeError
            If the underlying save fails (handled by decorators).
        """
        # 1. Resolve "file_path" or "table_name"
        file_path = self.connector.get_file_path(
            container=container,
            path_prefix=path_prefix,
            object_name=object_name
        )

        # 2. Resolve storage options (e.g., SQL DSN or Spark options)
        options = self.connector.get_storage_options()
        framework = self.connector.framework
        if framework == Framework.LOCAL:
            framework = self.local_engine

        # 3. Dispatch based on framework
        match framework:
            case Framework.PANDAS:
                saver = PandasDataSaver()

                # SQL needs table_name and connection_string passed explicitly
                if data_type == DataType.SQL:
                    return saver.save_data(
                        df=df,
                        data_type=data_type,
                        file_path=object_name,   # table name
                        storage_options=options, # contains {"connection_string": "..."}
                        **kwargs
                    )

                # Non-SQL formats: just use file_path as before
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

                # Apply connector auth to Spark if needed (skip for SQL)
                if options and data_type != DataType.SQL:
                    for key, value in options.items():
                        spark.conf.set(key, value)

                saver = SparkStreamingDataSaver() if is_stream else SparkDataSaver()
                return saver.save_data(
                    df=df,
                    data_type=data_type,
                    file_path=file_path if data_type != DataType.SQL else object_name,
                    storage_options=options,
                    spark=spark,
                    **kwargs
                )

            case _:
                raise NotImplementedError(f"Unsupported framework: {self.connector.framework}")
