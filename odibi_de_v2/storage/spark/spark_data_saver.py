from typing import Any
from pyspark.sql import DataFrame
from py4j.protocol import Py4JJavaError

from odibi_de_v2.core import DataSaver
from odibi_de_v2.core.enums import DataType, ErrorType
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, validate_input_types,
    benchmark, log_call
)
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise
from odibi_de_v2.utils.method_chain import run_method_chain


class SparkDataSaver(DataSaver):
    """
    Flexible Spark DataFrame writer using method chaining.

    - For file-based formats (CSV, JSON, Parquet, Avro, etc.) it dynamically
      applies chained writer methods (`mode`, `partitionBy`, `option`) and saves
      to the given path.
    - For SQL (`DataType.SQL`), `file_path` is treated as the destination table
      name and Spark's JDBC writer is used.

    Example:
        >>> saver = SparkDataSaver()
        >>> saver.save_data(
        ...     df=df,
        ...     data_type=DataType.PARQUET,
        ...     file_path="/mnt/data/output.parquet",
        ...     mode="overwrite",
        ...     option={"compression": "snappy"}
        ... )

        >>> saver.save_data(
        ...     df=df,
        ...     data_type=DataType.SQL,
        ...     file_path="FactEnergyEfficiency",
        ...     storage_options={
        ...         "url": "jdbc:sqlserver://myserver:1433;databaseName=mydb",
        ...         "user": "admin",
        ...         "password": "secret",
        ...         "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        ...     },
        ...     mode="append"
        ... )
    """

    @enforce_types(strict=True)
    @validate_non_empty(["file_path"])
    @validate_input_types({"df": DataFrame, "file_path": str})
    @benchmark(module="STORAGE", component="SparkDataSaver")
    @log_call(module="STORAGE", component="SparkDataSaver")
    @log_exceptions(
        module="STORAGE",
        component="SparkDataSaver",
        error_type=ErrorType.SAVE_ERROR,
        raise_type=RuntimeError
    )
    def save_data(
        self,
        df: DataFrame,
        data_type: DataType,
        file_path: str,
        **kwargs
    ) -> None:
        try:
            log_and_optionally_raise(
                module="STORAGE",
                component="SparkDataSaver",
                method="save_data",
                error_type=ErrorType.NO_ERROR,
                message=f"Saving DataFrame as {data_type.value.upper()} to {file_path}",
                level="INFO"
            )

            if data_type == DataType.SQL:
                table_name = file_path
                self._save_sql(df, table_name, **kwargs)
            else:
                method_chain = {"format": data_type.value, **kwargs}
                writer = run_method_chain(df.write, method_chain)
                writer.save(file_path)

            log_and_optionally_raise(
                module="STORAGE",
                component="SparkDataSaver",
                method="save_data",
                error_type=ErrorType.NO_ERROR,
                message=f"Successfully saved {data_type.value.upper()} to {file_path}",
                level="INFO"
            )

        except PermissionError as e:
            raise PermissionError(f"Permission denied while writing to {file_path}\n{e}") from e
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Path not found: {file_path}\n{e}") from e
        except IsADirectoryError as e:
            raise IsADirectoryError(f"Expected a file but got a directory: {file_path}\n{e}") from e
        except ValueError as e:
            raise ValueError(f"Invalid or empty {data_type.value.upper()} at {file_path}\n{e}") from e
        except OSError as e:
            raise OSError(f"I/O error while saving {data_type.value.upper()} to {file_path}\n{e}") from e
        except NotImplementedError:
            raise
        except Py4JJavaError as e:
            raise RuntimeError(f"Spark save error: {str(e.java_exception)}") from e
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(
                f"Unexpected error while saving {data_type.value.upper()} to {file_path}\n{e}"
            ) from e

    # ------------------------------------------------------------------ #
    # Internal helpers                                                   #
    # ------------------------------------------------------------------ #
    def _save_sql(
        self,
        df: DataFrame,
        table_name: str,
        **kwargs
    ) -> None:
        """
        Write a DataFrame to a SQL table using Spark's JDBC writer.

        Args:
            df: Spark DataFrame
            table_name: Destination SQL table
            storage_options (dict): JDBC connection options, must include 'url'
            mode (str): Save mode, default 'append'
        """
        jdbc_options = kwargs.pop("jdbc_options", {}) or kwargs.pop("storage_options", {})
        if "url" not in jdbc_options:
            raise ValueError("storage_options/jdbc_options must include a 'url' for the JDBC connection.")

        mode = kwargs.pop("mode", "append")
        (
            df.write.format("jdbc")
            .options(**jdbc_options)
            .option("dbtable", table_name)
            .mode(mode)
            .save()
        )
