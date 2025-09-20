from typing import Any, Dict, Optional
import pandas as pd
import fsspec
from fastavro import reader

from odibi_de_v2.core import DataReader
from odibi_de_v2.core.enums import DataType, ErrorType
from odibi_de_v2.utils import (
    enforce_types, log_call, validate_non_empty,
    ensure_output_type, benchmark
)
from odibi_de_v2.logger import (
    log_and_optionally_raise, log_exceptions)
from odibi_de_v2.core.enums import ErrorType
from odibi_de_v2.utils import wrap_read_errors


class PandasDataReader(DataReader):
    """
    Reads structured data from a file using Pandas, based on the specified data type.

    Supported formats include CSV, JSON, Parquet, and Avro. The method dispatches to the appropriate Pandas function or
    internal Avro reader based on the `data_type` argument.

    Args:
        data_type (DataType): Enum value indicating the file format to read.
        file_path (str): The path to the file. Supports local and cloud file systems.
        **kwargs: Additional keyword arguments passed to the corresponding Pandas reader function
        (e.g., `sep`, `header`, `storage_options`).

    Returns:
        pd.DataFrame: A DataFrame containing the loaded data.

    Raises:
        FileNotFoundError: If the file path does not exist.
        PermissionError: If access to the file is denied.
        IsADirectoryError: If the path points to a directory instead of a file.
        ValueError: If the file is empty or malformed.
        OSError: If a low-level I/O error occurs.
        NotImplementedError: If the given `data_type` is not supported.
        RuntimeError: For all other unexpected failures.

    Example:
        >>> reader = PandasDataReader()
        >>> df = reader.read_data(
        ...     data_type=DataType.CSV,
        ...     file_path="data/sample.csv",
        ...     sep=";", encoding="utf-8")
    """
    @enforce_types(strict=True)
    @validate_non_empty(["file_path"])
    @ensure_output_type(pd.DataFrame)
    @benchmark(module="INGESTION", component="PandasDataReader")
    @log_call(module="INGESTION", component="PandasDataReader")
    @wrap_read_errors(component="PandasDataReader")
    def read_data(
        self,
        data_type: DataType,
        file_path: str,
        **kwargs
        ) -> pd.DataFrame:
        """
        Reads structured data from a file using Pandas, based on the specified data type.

        This method supports reading from various data formats including CSV, JSON, Parquet, and Avro. It utilizes the
        appropriate Pandas function or a custom internal reader depending on the `data_type` specified. The method can
        handle files from both local and cloud-based storage systems.

        Args:
            data_type (DataType): An enum value specifying the format of the file to be read.
            file_path (str): The path to the file. This can be a local path or a URL to a file in a cloud storage
                system.
            **kwargs: Arbitrary keyword arguments that are passed directly to the Pandas reading function. These can
            include parameters such as `sep` for CSV files, `encoding`, or `storage_options` for cloud services.

        Returns:
            pd.DataFrame: A DataFrame containing the data read from the file.

        Raises:
            FileNotFoundError: If the file specified in `file_path` does not exist.
            PermissionError: If the file is not accessible due to permission restrictions.
            IsADirectoryError: If the specified `file_path` is a directory, not a file.
            ValueError: If the file is empty, corrupted, or otherwise malformed.
            OSError: If there is a low-level I/O error during file reading.
            NotImplementedError: If the `data_type` provided is not supported by this method.
            RuntimeError: For all other unexpected issues that occur during the file reading process.

        Example:
            >>> reader = PandasDataReader()
            >>> df = reader.read_data(
            ...     data_type=DataType.CSV,
            ...     file_path="data/sample.csv",
            ...     sep=";", encoding="utf-8")
        """
        match data_type:
            case DataType.CSV:
                return pd.read_csv(file_path, **kwargs)
            case DataType.JSON:
                return pd.read_json(file_path, **kwargs)
            case DataType.PARQUET:
                return pd.read_parquet(file_path, **kwargs)
            case DataType.AVRO:
                return self._read_avro(file_path, **kwargs)
            case DataType.SQL:
                return self._read_sql(file_path, **kwargs)
            case _:
                raise NotImplementedError

    def _read_avro(
        self,
        file_path: str,
        **kwargs
    ) -> pd.DataFrame:
        """
        Reads an Avro file and returns its contents as a Pandas DataFrame.

        This private method leverages `fastavro` for parsing Avro files and supports both local and remote file paths
        through `fsspec`. It is typically used within a larger data processing framework where Avro file handling
        is required.

        Args:
            file_path (str): The full path to the Avro file. This can be a path to a local file or a URI to a remote
                file.
            **kwargs: Arbitrary keyword arguments. This includes optional `storage_options` which are passed to
            `fsspec.open` to handle remote file systems like blob storage, S3, GCS, or HDFS.

        Returns:
            pd.DataFrame: A DataFrame containing the data loaded from the Avro file.

        Raises:
            FileNotFoundError: If the Avro file cannot be found at the specified `file_path`.
            IOError: If there is an error reading from the file.

        Example:
            >>> reader = PandasDataReader()
            >>> df = reader._read_avro(
            ...     file_path="s3://bucket/data.avro",
            ...     storage_options={"key": "minio", "secret": "minio123", "client_kwargs": {"endpoint_url": "http://localhost:9000"}})
        """
        storage_options = kwargs.pop("storage_options", {})

        with fsspec.open(file_path, mode="rb", **storage_options) as f:
            log_and_optionally_raise(
                module="INGESTION",
                component="PandasDataReader",
                method="_read_avro",
                error_type=ErrorType.NO_ERROR,
                message=f"Reading AVRO file from: {file_path}.",
                level="INFO")
            avro_reader = reader(f)
            data = [record for record in avro_reader]
            df = pd.DataFrame(data)
            log_and_optionally_raise(
                module="INGESTION",
                component="PandasDataReader",
                method="_read_avro",
                error_type=ErrorType.NO_ERROR,
                message=f"Successfully read AVRO file from: {file_path}.",
                level="INFO")
        return df

    def _read_sql(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Reads data from a SQL database into a pandas DataFrame using a provided SQL query or table name.

        This method supports reading from a SQL database by either executing a SQL query or loading directly from a
        table. The method determines the mode (query or table) based on the content of the `file_path` parameter or
        the `is_query` flag.

        Args:
            file_path (str): The SQL query string or the table name from which to read the data.
            **kwargs: Arbitrary keyword arguments. Key options include:
                - storage_options (dict, optional): A dictionary of storage options. The only used key is
                    'connection_string'.
                - connection_string (str, optional): The database connection string. If not provided, it must be
                    included in `storage_options`.
                - is_query (bool, optional): Explicitly specifies whether `file_path` is a SQL query.
                    If `None`, the method infers it based on the content of `file_path`.

        Returns:
            pd.DataFrame: A DataFrame containing the data read from the SQL database.

        Raises:
            ValueError: If the 'connection_string' is not provided either directly or within `storage_options`.
            RuntimeError: If there is an error while reading from the SQL database, encapsulating the original exception.

        Example:
            # Example usage for reading from a SQL query
            df = instance._read_sql("SELECT * FROM users", connection_string="DSN=MyDatabase;UID=user;PWD=password")

            # Example usage for reading from a table
            df = instance._read_sql("users", connection_string="DSN=MyDatabase;UID=user;PWD=password", is_query=False)
        """
        import pyodbc
        print(kwargs)
        storage_options = kwargs.pop("storage_options", {})
        connection_string = kwargs.pop(
            "connection_string", None) or storage_options.get("connection_string")
        if not connection_string:
            raise ValueError("Missing 'connection_string' for Pandas SQL read.")

        is_query = kwargs.pop("is_query", None)
        query_lower = file_path.strip().lower()

        try:
            conn = pyodbc.connect(connection_string)

            if is_query is True or query_lower.startswith(("select", "with")):
                return pd.read_sql_query(file_path, conn)
            else:
                return pd.read_sql_table(file_path, conn)

        except Exception as e:
            raise RuntimeError(f"Failed to read SQL data: {e}") from e
