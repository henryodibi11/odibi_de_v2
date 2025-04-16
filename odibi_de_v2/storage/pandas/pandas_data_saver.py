from typing import Any
import pandas as pd
import fsspec
from fastavro import writer, parse_schema

from odibi_de_v2.core import DataSaver
from odibi_de_v2.core.enums import DataType, ErrorType
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty,
    benchmark, log_call, validate_input_types)
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise


class PandasDataSaver(DataSaver):
    """
    Flexible saver for structured Pandas DataFrames.

    Supports writing to CSV, JSON, Parquet, Excel, and Avro formats.
    Dispatches based on the `DataType` enum. Additional arguments are
    passed directly to the underlying writer functions.

    Note:
        - For Avro writes, you must provide a valid Avro schema via the `schema` argument.
        - You may also provide `storage_options` for cloud-based paths (e.g., abfs, s3).

    Example usage:
        >>> from odibi_de_v2.core.enums import DataType
        >>> from odibi_de_v2.storage.pandas.pandas_data_saver import PandasDataSaver

        >>> df = pd.DataFrame({"id": [1], "name": ["Alice"]})
        >>> saver = PandasDataSaver()

        # CSV
        >>> saver.save_data(
        ...     df=df,
        ...     data_type=DataType.CSV,
        ...     file_path="/dbfs/tmp/output.csv",
        ...     index=False)

        # Avro
        >>> schema = {
        ...     "name": "User",
        ...     "type": "record",
        ...     "fields": [
        ...         {"name": "id", "type": "int"},
        ...         {"name": "name", "type": "string"}
        ...     ]
        ... }
        >>> saver.save_data(
        ...     df=df,
        ...     data_type=DataType.AVRO,
        ...     file_path="/dbfs/tmp/output.avro",
        ...     schema=schema)
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
        Saves the DataFrame to the specified file in the desired format.

        Supported formats include CSV, JSON, Parquet, Excel, and Avro. 
        The file format is determined by the `data_type` enum. Additional 
        keyword arguments are passed to the appropriate Pandas writer.

        Args:
            df (pd.DataFrame): The data to write.
            data_type (DataType): Enum indicating the desired output format.
            file_path (str): Destination path for the saved file.
            **kwargs: Additional arguments to pass to the specific writer function.
                Common keys include:
                    - index (bool): Whether to write row index
                    - storage_options (dict): For cloud filesystems via fsspec
                    - schema (dict): Required for Avro writes

        Raises:
            FileNotFoundError: If the destination path is invalid or inaccessible.
            PermissionError: If the file cannot be written due to access restrictions.
            IsADirectoryError: If the provided path points to a directory.
            ValueError: If the file is empty or improperly formatted.
            OSError: For low-level I/O failures.
            NotImplementedError: If the `data_type` is unsupported.
            RuntimeError: For unexpected failures during save.
        """
        try:
            log_and_optionally_raise(
                module="STORAGE",
                component="PandasDataSaver",
                method="save_data",
                error_type=ErrorType.NO_ERROR,
                message=(
                    f"Saving DataFrame as {data_type.value.upper()} "
                    f"to {file_path}"),
                level="INFO")

            if data_type == DataType.CSV:
                df.to_csv(file_path, **kwargs)
            elif data_type == DataType.JSON:
                df.to_json(file_path, **kwargs)
            elif data_type == DataType.PARQUET:
                df.to_parquet(file_path, **kwargs)
            elif data_type == DataType.AVRO:
                self._save_avro(df, file_path, **kwargs)
            else:
                raise NotImplementedError(f"Unsupported data type: {data_type.value}")
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
                f"I/O error while saving {data_type.value.upper()} "
                f"file: {file_path} \n {e}") from e
        except NotImplementedError as e:
            raise NotImplementedError(
                f"Unsupported data type: {data_type.value} \n {e}") from e
        except Exception as e:
            raise RuntimeError(
                f"Unexpected error while saving {data_type.value.upper()} "
                f"file: {file_path} \n {e}") from e
        finally:
            log_and_optionally_raise(
                module="STORAGE",
                component="PandasDataSaver",
                method="save_data",
                error_type=ErrorType.NO_ERROR,
                message=(
                    f"Successfully saved {data_type.value.upper()} "
                    f"file to: {file_path}"),
                level="INFO")

    def _save_avro(
        self,
        df: pd.DataFrame,
        file_path: str,
        **kwargs: Any
    ) -> None:
        """
        Internal handler for saving a DataFrame in Avro format using fastavro.

        This method requires a valid Avro schema provided via the `schema` keyword.
        You can also pass `storage_options` to handle cloud paths (e.g., abfs, s3, gcs).

        Args:
            df (pd.DataFrame): The DataFrame to write.
            file_path (str): Destination Avro file path.
            **kwargs:
                schema (dict): Avro schema in JSON format (required).
                storage_options (dict): fsspec-compatible options for remote writes.

        Raises:
            ValueError: If no Avro schema is provided.
            Any: Errors raised by fastavro or fsspec are propagated.
        """
        schema = kwargs.pop("schema", None)
        if not schema:
            raise ValueError("Saving to Avro requires a 'schema' argument.")

        storage_options = kwargs.pop("storage_options", {})

        with fsspec.open(file_path, mode="wb", **storage_options) as out:
            writer(out, parse_schema(schema), df.to_dict(orient="records"))
