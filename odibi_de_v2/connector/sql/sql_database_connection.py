
from typing import Optional
from odibi_de_v2.core.enums import Framework
from odibi_de_v2.core import BaseConnection
from odibi_de_v2.utils import enforce_types, log_call
from odibi_de_v2.logger import log_exceptions
from odibi_de_v2.core.enums import ErrorType
from odibi_de_v2.logger import log_and_optionally_raise


class SQLDatabaseConnection(BaseConnection):
    """
    Connector for accessing SQL databases via Pandas or Spark.

    This class dynamically generates framework-specific connection options
    and resolves the appropriate query or table string for ingestion.

    Supported Frameworks:
        - Framework.PANDAS: Uses pyodbc-compatible connection strings.
        - Framework.SPARK: Uses Spark `.read.format("jdbc")` with JDBC options.

    Designed to work seamlessly with ReaderProvider and any DataReader that supports
    SQL-based ingestion workflows.

    Example (Pandas):
        >>> from pyspark.sql import SparkSession
        >>> from odibi_de_v2.core.enums import Framework
        >>> from odibi_de_v2.connector import SQLDatabaseConnection
        >>> from odibi_de_v2.ingestion import ReaderProvider
        >>> from odibi_de_v2.core import DataType

        >>> connector = SQLDatabaseConnection(
        ...     host="myserver.database.windows.net",
        ...     database="sales_db",
        ...     user="admin",
        ...     password="secret",
        ...     framework=Framework.PANDAS
        ... )
        >>> provider = ReaderProvider(connector)
        >>> df = provider.read(
        ...     data_type=DataType.SQL,
        ...     container="",  # Ignored for SQL, required only for signature compatibility
        ...     path_prefix="",  # Also igonored
        ...     object_name="SELECT * FROM customers"  # Can be full query or table name
        ... )

    Example (Spark):
        >>> from pyspark.sql import SparkSession
        >>> from odibi_de_v2.core.enums import Framework
        >>> from odibi_de_v2.connector import SQLDatabaseConnection
        >>> from odibi_de_v2.ingestion import ReaderProvider
        >>> from odibi_de_v2.core import DataType

        >>> spark = SparkSession.builder.getOrCreate()
        >>> connector = SQLDatabaseConnection(
        ...     host="myserver.database.windows.net",
        ...     database="sales_db",
        ...     user="admin",
        ...     password="secret",
        ...     framework=Framework.SPARK
        ... )
        >>> provider = ReaderProvider(connector)
        >>> df = provider.read(
        ...     data_type=DataType.SQL,
        ...     container="", # Ignored for SQL, required only for signature compatibility
        ...     path_prefix="", # Also igonored
        ...     object_name="SELECT * FROM dbo.sales_metrics",
        ...     spark=spark
        ... )
    """

    @enforce_types(strict=True)
    @log_exceptions(
        module="CONNECTOR",
        component="SQLDatabaseConnection",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        framework: Framework,
        port: int = 1433,
        driver: str = "{ODBC Driver 17 for SQL Server}"  # for Pandas only
        ):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.driver = driver
        self.framework = framework

    @log_call(module="CONNECTOR", component="SQLDatabaseConnection")
    @enforce_types(strict=True)
    @log_exceptions(
        module="CONNECTOR",
        component="SQLDatabaseConnection",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)

    def get_file_path(
        self,
        container: str,
        path_prefix: str,
        object_name: str
            ) -> str:
        """
        Returns the SQL query or table name to use for reading.

        This method is a semantic match to the connector interface.
        For SQL-based readers, the `object_name` is either:
        - a table name (e.g., "dbo.orders")
        - a full query string (e.g., "SELECT * FROM customers")

        The `container` and `path_prefix` arguments are accepted
        for compatibility but ignored unless you choose to use
        them for schema composition.

        Args:
            container (str): Optional schema name (ignored by default).
            path_prefix (str): Optional namespace/sub-schema (ignored by default).
            object_name (str): SQL table name or query string.

        Returns:
            str: The exact query or table name to be passed to the reader.

        Example:
            >>> connector.get_file_path(
            ...     container="dbo",
            ...     path_prefix="",
            ...     object_name="SELECT * FROM customers"
            ... )
            'SELECT * FROM customers'
        """
        log_and_optionally_raise(
            module="CONNECTOR",
            component="SQLDatabaseConnection",
            method="get_file_path",
            error_type=ErrorType.NO_ERROR,
            message=(
                "Attempting to resolve query..."
                "only object name is used for SQL-based ingestion."
                "All other arguments are ignored"),
            level="WARNING")
        table_or_query = object_name
        log_and_optionally_raise(
            module="CONNECTOR",
            component="SQLDatabaseConnection",
            method="get_file_path",
            error_type=ErrorType.NO_ERROR,
            message=(
                f"Successfully resolved query string: {table_or_query}"),
            level="INFO")
        return table_or_query

    @log_call(module="CONNECTOR", component="SQLDatabaseConnection")
    @enforce_types(strict=True)
    @log_exceptions(
        module="CONNECTOR",
        component="SQLDatabaseConnection",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def get_storage_options(self) -> Optional[dict]:
        """
        Returns connection options based on the framework.

        For Pandas:
            - Returns a pyodbc-style connection string suitable
            for use with SQLAlchemy or pd.read_sql.

        For Spark:
            - Returns a dictionary of JDBC options to pass via
            .option("url", ...), .option("user", ...), etc.

        Returns:
            dict: Framework-specific dictionary of credentials and connection strings.

        Raises:
            NotImplementedError: If an unsupported framework is used.

        Examples:
            # Pandas
            >>> connector.framework = Framework.PANDAS
            >>> connector.get_storage_options()
            {'connection_string': 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=...;UID=...;PWD=...'}

            # Spark
            >>> connector.framework = Framework.SPARK
            >>> connector.get_storage_options()
            {
                'url': 'jdbc:sqlserver://myhost:1433;databaseName=mydb',
                'user': 'username',
                'password': 'password',
                'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
            }
        """
        log_and_optionally_raise(
            module="CONNECTOR",
            component="AzureBlobConnector",
            method="get_file_path",
            error_type=ErrorType.NO_ERROR,
            message="Attempting to resolve sql connection parameters.",
            level="INFO")
        if self.framework == Framework.PANDAS:
            connection_string = (
                f"DRIVER={self.driver};"
                f"SERVER={self.host};"
                f"DATABASE={self.database};"
                    f"UID={self.user};PWD={self.password}")
            log_and_optionally_raise(
                module="CONNECTOR",
                component="AzureBlobConnector",
                method="get_file_path",
                error_type=ErrorType.NO_ERROR,
                message="Successfully resolved connection parameters.",
                level="INFO")
            return {"connection_string": connection_string}

        elif self.framework == Framework.SPARK:
            return {
                "url": (
                    f"jdbc:sqlserver://{self.host}:{self.port};"
                    f"databaseName={self.database}"
                ),
                "user": self.user,
                "password": self.password,
                "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            }

        else:
            raise NotImplementedError(
                f"Framework {self.framework} is not supported for SQL DB connection"
            )

