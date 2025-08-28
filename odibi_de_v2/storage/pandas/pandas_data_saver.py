from typing import Any
import pandas as pd
import fsspec
from fastavro import writer, parse_schema
import urllib.parse
from sqlalchemy import create_engine

from odibi_de_v2.core import DataSaver
from odibi_de_v2.core.enums import DataType, ErrorType
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty,
    benchmark, log_call, validate_input_types)
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise


class PandasDataSaver(DataSaver):
    """
    Flexible saver for structured Pandas DataFrames.

    This class supports writing DataFrames to multiple formats including
    CSV, JSON, Parquet, Avro, and SQL. The appropriate writer is dispatched
    based on the ``DataType`` enum provided.

    Key Features
    ------------
    - **File-based formats**: Delegates to the native Pandas I/O methods.
    - **Avro**: Requires a valid schema passed via ``schema`` keyword argument.
    - **SQL**: Requires a valid connection string provided inside
      ``storage_options={"connection_string": "..."}`` and uses SQLAlchemy
      under the hood.
    - **Mode mapping**: Accepts Spark-style ``mode`` (``append``, ``overwrite``)
      and translates to Pandas ``if_exists`` semantics.

    Notes
    -----
    - For Avro writes, a schema must be explicitly provided.
    - For SQL writes, the table name is passed as ``file_path``.
    - ``mode="overwrite"`` maps to Pandas' ``if_exists="replace"``.
    - Additional keyword arguments are passed directly to Pandas or SQLAlchemy.
    """

    @enforce_types(strict=True)
    @validate_non_empty(["file_path"])
    @validate_input_types({"df": pd.DataFrame, "file_path": str})
    @benchmark(module="STORAGE", component="PandasDataSaver")
    @log_call(module="STORAGE", component="PandasDataSaver")
    @log_exceptions(
        module="STORAGE",
        component="PandasDataSaver",
        error_type=ErrorType.SAVE_ERROR,
        raise_type=RuntimeError
    )
    def save_data(
        self,
        df: pd.DataFrame,
        data_type: DataType,
        file_path: str,
        **kwargs
    ) -> None:
        """
        Persist a Pandas DataFrame to the specified format.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to persist.
        data_type : DataType
            Output format. Supported values are:
            - DataType.CSV
            - DataType.JSON
            - DataType.PARQUET
            - DataType.AVRO
            - DataType.SQL
        file_path : str
            Destination path (for file-based formats) or table name (for SQL).
        **kwargs : dict
            Additional writer-specific options:
            - CSV/JSON/Parquet: passed directly to Pandas writers.
            - AVRO: must include ``schema`` (dict) and optional ``storage_options``.
            - SQL: must include ``storage_options={"connection_string": "..."}``
              plus optional ``mode`` ("append" or "overwrite") and ``index``.

        Raises
        ------
        PermissionError
            If the process lacks permission to write to the destination.
        FileNotFoundError
            If the destination path does not exist.
        IsADirectoryError
            If the destination is a directory rather than a file.
        ValueError
            If inputs are invalid (e.g., Avro schema missing).
        OSError
            For underlying I/O errors.
        NotImplementedError
            If the specified data type is unsupported.
        RuntimeError
            For unexpected save errors not caught above.

        Examples
        --------
        >>> saver = PandasDataSaver()
        >>> df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

        Save as CSV:
        >>> saver.save_data(df, DataType.CSV, "/tmp/data.csv", index=False)

        Save as Avro:
        >>> schema = {
        ...     "name": "User",
        ...     "type": "record",
        ...     "fields": [
        ...         {"name": "id", "type": "int"},
        ...         {"name": "name", "type": "string"}
        ...     ]
        ... }
        >>> saver.save_data(df, DataType.AVRO, "/tmp/data.avro", schema=schema)

        Save to SQL:
        >>> saver.save_data(
        ...     df, DataType.SQL, "users",
        ...     storage_options={"connection_string": "DRIVER={ODBC Driver 17 for SQL Server};..."},
        ...     mode="overwrite"
        ... )
        """
        try:
            log_and_optionally_raise(
                module="STORAGE",
                component="PandasDataSaver",
                method="save_data",
                error_type=ErrorType.NO_ERROR,
                message=f"Saving DataFrame as {data_type.value.upper()} to {file_path}",
                level="INFO"
            )

            if data_type == DataType.CSV:
                df.to_csv(file_path, **kwargs)
            elif data_type == DataType.JSON:
                df.to_json(file_path, **kwargs)
            elif data_type == DataType.PARQUET:
                df.to_parquet(file_path, **kwargs)
            elif data_type == DataType.AVRO:
                self._save_avro(df, file_path, **kwargs)
            elif data_type == DataType.SQL:
                self._save_sql(df, file_path, **kwargs)
            else:
                raise NotImplementedError(f"Unsupported data type: {data_type.value}")

        except PermissionError as e:
            raise PermissionError(
                f"Permission denied while writing {data_type.value.upper()} to {file_path}\n{e}"
            ) from e
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"{data_type.value.upper()} path not found: {file_path}\n{e}"
            ) from e
        except IsADirectoryError as e:
            raise IsADirectoryError(
                f"Expected a file but got a directory: {file_path}\n{e}"
            ) from e
        except ValueError as e:
            raise ValueError(
                f"Invalid or empty {data_type.value.upper()} output at {file_path}\n{e}"
            ) from e
        except OSError as e:
            raise OSError(
                f"I/O error while writing {data_type.value.upper()} to {file_path}\n{e}"
            ) from e
        except NotImplementedError:
            raise
        except Exception as e:
            raise RuntimeError(
                f"Unexpected error while saving {data_type.value.upper()} to {file_path}\n{e}"
            ) from e
        finally:
            log_and_optionally_raise(
                module="STORAGE",
                component="PandasDataSaver",
                method="save_data",
                error_type=ErrorType.NO_ERROR,
                message=f"Finished saving {data_type.value.upper()} to {file_path}",
                level="INFO"
            )


    def _save_avro(
        self,
        df: pd.DataFrame,
        file_path: str,
        **kwargs: Any
    ) -> None:
        """
        Internal helper for saving a DataFrame in Avro format.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to persist.
        file_path : str
            Destination Avro file path.
        **kwargs : dict
            - schema (dict): Required Avro schema.
            - storage_options (dict): fsspec-compatible options for cloud writes.

        Raises
        ------
        ValueError
            If no schema is provided.
        """
        schema = kwargs.pop("schema", None)
        if not schema:
            raise ValueError("Saving to Avro requires a 'schema' argument.")

        storage_options = kwargs.pop("storage_options", {})

        with fsspec.open(file_path, mode="wb", **storage_options) as out:
            writer(out, parse_schema(schema), df.to_dict(orient="records"))

    def _save_sql(
        self,
        df: pd.DataFrame,
        table_name: str,
        **kwargs: Any
    ) -> None:
        """
        Internal helper for saving a DataFrame to a SQL table.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to persist.
        table_name : str
            Target SQL table name.
        **kwargs : dict
            - storage_options (dict): Must include ``connection_string`` (str).
            - mode (str): Spark-style mode ("append" or "overwrite"),
              mapped to Pandas' ``if_exists`` ("append" or "replace").
            - index (bool): Whether to write DataFrame index.

        Raises
        ------
        ValueError
            If connection string is missing.
        TypeError
            If connection string is not a string.
        RuntimeError
            For SQLAlchemy or execution errors.

        Examples
        --------
        >>> saver = PandasDataSaver()
        >>> saver._save_sql(
        ...     df,
        ...     table_name="sales",
        ...     storage_options={"connection_string": "DRIVER={ODBC Driver 17 for SQL Server};..."},
        ...     mode="append",
        ...     index=False
        ... )
        """
        storage_options = kwargs.pop("storage_options", {}) or {}
        # Expect storage_options to contain the DSN
        raw_conn_str = storage_options.get("connection_string")

        if not raw_conn_str:
            raise ValueError("Missing 'connection_string' inside storage_options for SQL write.")

        if not isinstance(raw_conn_str, str):
            raise TypeError(f"Expected connection string as str, got {type(raw_conn_str)}: {raw_conn_str}")

        # Wrap DSN in SQLAlchemy URL
        if "://" in raw_conn_str:
            conn_str = raw_conn_str
        else:
            params = urllib.parse.quote_plus(raw_conn_str)
            conn_str = f"mssql+pyodbc:///?odbc_connect={params}"

        # Map Spark-style mode → Pandas if_exists
        if_exists = kwargs.pop("mode", "append")
        if_exists = "replace" if if_exists == "overwrite" else if_exists
        index = kwargs.pop("index", False)

        engine = create_engine(conn_str)
        with engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists=if_exists, index=index, **kwargs)

