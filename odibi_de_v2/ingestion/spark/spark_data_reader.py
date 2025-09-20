from typing import Optional
from pyspark.sql import SparkSession, DataFrame

from odibi_de_v2.core import DataReader
from odibi_de_v2.core.enums import DataType
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    log_call, benchmark
)
from odibi_de_v2.utils import run_method_chain
from odibi_de_v2.utils import wrap_read_errors


class SparkDataReader(DataReader):
    """
    Reads a file into a Spark DataFrame using dynamic method chaining.

    This method facilitates the reading of structured data files into Spark DataFrames, leveraging Spark's ability
    to handle various data formats. It uses method chaining to dynamically apply read configurations and options
    based on the provided arguments.

    Args:
        data_type (DataType): Enum indicating the file format.
        file_path (str): Path to the file (used for logging context).
        spark (SparkSession, optional): Spark session instance. If not provided, a new session will be created.
            Defaults to None.
        storage_options (dict, optional): Additional storage options for file access, such as credentials and paths
            for cloud storage. Defaults to None.
        **kwargs: Arbitrary keyword arguments that are passed to the Spark read builder. These can include options
            like 'option', 'schema', and 'load' to customize the read process.

    Returns:
        DataFrame: A Spark DataFrame loaded using the specified data type and options.

    Raises:
        RuntimeError: If there is an error during the read operation, such as a failure in Spark, issues with file
            I/O, or incorrect usage of method chaining.
        PermissionError: If there is a permission issue accessing the file.
        FileNotFoundError: If the file specified does not exist.
        IsADirectoryError: If the path specified is a directory, not a file.
        ValueError: If there are issues with the input values, such as an invalid file format.
        OSError: For other I/O errors like hardware failures.
        NotImplementedError: If the data type specified is not supported.
        Py4JJavaError: For errors originating from the underlying Java VM.

    Example:
        >>> df = reader.read_data(
        ...     data_type=DataType.JSON,
        ...     file_path="/mnt/data/file.json",
        ...     spark=spark_session,
        ...     option={"multiline": "true"}
        ... )
    """

    @enforce_types(strict=True)
    @validate_non_empty(["file_path"])
    @ensure_output_type(DataFrame)
    @benchmark(module="INGESTION", component="SparkDataReader")
    @log_call(module="INGESTION", component="SparkDataReader")
    @wrap_read_errors(component="SparkDataReader")
    def read_data(
        self,
        data_type: DataType,
        file_path: str,
        spark: SparkSession = None,
        storage_options: Optional[dict] = None,
        **kwargs
        ) -> DataFrame:
        """
        Reads data from a specified file into a Spark DataFrame, supporting various file formats and configurations.

        This method dynamically chains read operations based on the provided `data_type` and additional method options
        specified in `kwargs`. It allows for flexible data ingestion into Spark from different storage systems by
        optionally using `storage_options`.

        Args:
            data_type (DataType): An enum indicating the file format, such as CSV, JSON, etc.
            file_path (str): The path to the file to be read.
            spark (SparkSession, optional): An existing Spark session to use for reading the data. If not provided, a
                new session will be created.
            storage_options (dict, optional): A dictionary of options that modify how the file is accessed
            (e.g., credentials for accessing a remote system). Defaults to None.
            **kwargs: Arbitrary keyword arguments that are passed to the Spark read builder. These can include options
                like 'schema', 'header', etc.

        Returns:
            DataFrame: The Spark DataFrame containing the data read from the file.

        Raises:
            PermissionError: If there is a permission issue accessing the file.
            FileNotFoundError: If the file does not exist at the specified path.
            IsADirectoryError: If the path points to a directory instead of a file.
            ValueError: If the file is invalid or empty.
            OSError: If an I/O error occurs during file reading.
            NotImplementedError: If the `data_type` is unsupported.
            RuntimeError: If an unexpected error occurs in Spark during the read operation.

        Example:
            >>> df = reader.read_data(
            ...     data_type=DataType.JSON,
            ...     file_path="/mnt/data/file.json",
            ...     spark=my_spark_session,
            ...     option={"multiline": "true"}
            ... )
        """
        spark = spark or SparkSession.builder.appName("SparkDataReader").getOrCreate()
        if data_type == DataType.SQL:
            return self._read_sql(file_path, spark, storage_options, **kwargs)

        method_chain = {"format": data_type.value, **kwargs}
        reader = run_method_chain(spark.read, method_chain)
        df = reader.load(file_path)
        return df

    def _read_sql(
        self,
        query_or_table: str,
        spark: SparkSession,
        storage_options: Optional[dict] = None,
        **kwargs
    ) -> DataFrame:
        """
        Reads data from a SQL database into a Spark DataFrame using JDBC.

        This internal method facilitates the extraction of data from a SQL database by executing a SQL query or reading
        from a specified table. It utilizes the JDBC connection options provided to establish the connection and
        retrieve data into a Spark DataFrame.

        Args:
            query_or_table (str): A SQL query string or a table name to fetch data from.
            spark (SparkSession): The Spark session instance used to perform data operations.
            storage_options (Optional[dict], optional): Additional storage options which can be used as fallback if
                JDBC options are not explicitly provided. Defaults to None.
            **kwargs: Arbitrary keyword arguments. Must include JDBC connection options such as 'url', 'user',
                'password', and 'driver'. Additional options can be passed to customize the read operation.

        Returns:
            DataFrame: A DataFrame containing the data fetched from the SQL database.

        Raises:
            ValueError: If JDBC connection details are not fully provided.

        Example:

            >>> spark_session = SparkSession.builder.appName("ExampleApp").getOrCreate()
            >>> jdbc_options = {
            ...    "url": "jdbc:postgresql://localhost/test",
            ...    "user": "user",
            ...    "password": "password",
            ...    "driver": "org.postgresql.Driver"
            ... }
            >>> df = _read_sql("SELECT * FROM users", spark_session, jdbc_options=jdbc_options)
        """
        jdbc_options = kwargs.pop("jdbc_options", {}) or storage_options

        if not jdbc_options or "url" not in jdbc_options:
            raise ValueError("Missing JDBC connection details: 'url', 'user', 'password', 'driver' required.")

        reader = spark.read.format("jdbc").options(**jdbc_options)

        is_query = kwargs.pop("is_query", None)
        query_lower = query_or_table.strip().lower()

        if is_query is True or query_lower.startswith(("select", "with")):
            reader = reader.option("query", query_or_table)
        else:
            reader = reader.option("dbtable", query_or_table)

        return reader.load()
