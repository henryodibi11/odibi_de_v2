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


class PandasDataReader(DataReader):
    """
    Unified reader class for reading structured files using Pandas.

    This class supports reading CSV, JSON, Parquet, and Avro files using the
    Pandas API. The `DataType` enum is used to determine the appropriate read
    function. It supports local and remote file systems (e.g., Azure, S3) via
    `fsspec` and optional `storage_options`.

    Decorators provide type enforcement, logging, benchmarking, input validation,
    and output contract checks.

    Example:
        >>> from odibi_de_v2.core.enums import DataType
        >>> from odibi_de_v2.ingestion.pandas.pandas_data_reader import(
            PandasDataReader)

        >>> reader = PandasDataReader()

        # Read a CSV file
        >>> df_csv = reader.read_data(
        ...     data_type=DataType.CSV,
        ...     file_path="/mnt/data/file.csv",
        ...     sep=",",
        ...     header=0)

        # Read a Parquet file
        >>> df_parquet = reader.read_data(
        ...     data_type=DataType.PARQUET,
        ...     file_path="abfs://container/path/file.parquet",
        ...     storage_options={
        ...         "account_name": "myaccount",
        ...         "account_key": "secret"})

        # Read an Avro file
        >>> df_avro = reader.read_data(
        ...     data_type=DataType.AVRO,
        ...     file_path="/mnt/data/file.avro",
        ...     storage_options={"anon": True})
    """
    @enforce_types(strict=True)
    @validate_non_empty(["file_path"])
    @ensure_output_type(pd.DataFrame)
    @benchmark(module="INGESTION", component="PandasDataReader")
    @log_call(module="INGESTION", component="PandasDataReader")
    @log_exceptions(
        module="INGESTION",
        component="PandasDataReader",
        error_type=ErrorType.READ_ERROR,
        raise_type=RuntimeError)
    def read_data(
        self,
        data_type: DataType,
        file_path: str,
        **kwargs
    ) -> pd.DataFrame:
        """
        Reads structured data from a file using Pandas, based on the specified
            data type.

        Supported formats include CSV, JSON, Parquet, and Avro. The method dispatches
        to the appropriate Pandas function or internal Avro reader based on the
        `data_type` argument.

        Args:
            data_type (DataType): Enum value indicating the file format to read.
            file_path (str): The path to the file. Supports local and cloud file
                systems.
            **kwargs: Additional keyword arguments passed to the corresponding Pandas
                reader function (e.g., `sep`, `header`, `storage_options`).

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
        try:
            log_and_optionally_raise(
                module="INGESTION",
                component="PandasDataReader",
                method="read_data",
                error_type=ErrorType.NO_ERROR,
                message=(
                    f"Attempting to read {data_type.value.upper()} "
                    f"file from: {file_path}."),
                level="INFO")
            match data_type:
                case DataType.CSV:
                    return pd.read_csv(file_path, **kwargs)
                case DataType.JSON:
                    return pd.read_json(file_path, **kwargs)
                case DataType.PARQUET:
                    return pd.read_parquet(file_path, **kwargs)
                case DataType.AVRO:
                    return self._read_avro(file_path, **kwargs)
                case _:
                    raise NotImplementedError
        except PermissionError as e:
            raise PermissionError(
                "Permission denied while accessing file: "
                f"{file_path} \n {e}") from e
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"{data_type.value.upper()} "
                f"file not found: {file_path} \n {e}"
                ) from e
        except IsADirectoryError as e:
            raise IsADirectoryError(
                "Expected a file but got a directory: "
                f"{file_path} \n {e}") from e
        except ValueError as e:
            raise ValueError(
                f"Invalid or empty {data_type.value.upper()} file: "
                f"{file_path} \n {e}") from e
        except OSError as e:
            raise OSError(
                f"I/O error while reading {data_type.value.upper()} "
                f"file: {file_path} \n {e}") from e
        except NotImplementedError as e:
            raise NotImplementedError(
                f"Unsupported data type: {data_type.value} \n {e}") from e
        except Exception as e:
            raise RuntimeError(
                f"Unexpected error while reading {data_type.value.upper()} "
                f"file: {file_path} \n {e}") from e
        finally:
            log_and_optionally_raise(
                module="INGESTION",
                component="PandasDataReader",
                method="read_data",
                error_type=ErrorType.NO_ERROR,
                message=(
                    f"Successfully read {data_type.value.upper()} "
                    f"file from: {file_path}."),
                level="INFO")

    def _read_avro(
        self,
        file_path: str,
        **kwargs
    ) -> pd.DataFrame:
        """
        Reads an Avro file and returns its contents as a Pandas DataFrame.

        This method uses `fastavro` to parse the file and supports cloud file access
        via `fsspec`. Avro reading is invoked automatically when `data_type` is set
        to `DataType.AVRO`.

        Args:
            file_path (str): The full path to the Avro file. Can be local or remote.
            **kwargs: Additional arguments, including optional `storage_options`
                for cloud file access.

        Returns:
            pd.DataFrame: DataFrame containing the Avro records.

        Example:
            >>> reader = PandasDataReader()
            >>> df = reader.read_data(
            ...     data_type=DataType.AVRO,
            ...     file_path="abfs://container/data.avro",
            ...     storage_options={"account_name": "acct", "account_key": "key"})
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
