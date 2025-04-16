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

    This class supports writing data in any format supported by Spark
    (CSV, JSON, Parquet, Avro, etc.) and leverages dynamic chaining of 
    write methods. Users pass intermediate operations like `mode`, 
    `partitionBy`, and `option`. The `format` and `save` steps are 
    injected internally based on the `DataType`.

    Example usage:
        >>> from odibi_de_v2.core.enums import DataType
        >>> from odibi_de_v2.storage.spark.spark_data_saver import SparkDataSaver

        >>> saver = SparkDataSaver()
        >>> saver.save_data(
        ...     df=df,
        ...     data_type=DataType.PARQUET,
        ...     file_path="/mnt/data/output.parquet",
        ...     mode="overwrite",
        ...     option={"compression": "snappy"}
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
        """
        Saves a Spark DataFrame to the specified path using chained writer methods.

        Args:
            df (DataFrame): The Spark DataFrame to save.
            data_type (DataType): Enum indicating the file format to write.
            file_path (str): Destination path for the saved output.
            **kwargs: Chained writer methods like `mode`, `partitionBy`, `option`.

        Raises:
            RuntimeError: If the save operation fails due to Spark or filesystem issues.
        """
        try:
            log_and_optionally_raise(
                module="STORAGE",
                component="SparkDataSaver",
                method="save_data",
                error_type=ErrorType.NO_ERROR,
                message=f"Saving DataFrame as {data_type.value.upper()} to {file_path}",
                level="INFO"
            )

            method_chain = {"format": data_type.value, **kwargs}
            writer = run_method_chain(df.write, method_chain)
            writer.save(file_path)

            log_and_optionally_raise(
                module="STORAGE",
                component="SparkDataSaver",
                method="save_data",
                error_type=ErrorType.NO_ERROR,
                message=f"Successfully saved {data_type.value.upper()} file to: {file_path}",
                level="INFO"
            )
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
                f"Unsupported data type: "
                f"{data_type.value} \n {e}") from e
        except Py4JJavaError as e:
            raise RuntimeError(
                f"Spark save error: {str(e.java_exception)}"
            ) from e

        except Exception as e:
            raise RuntimeError(
                f"Unexpected error while saving {data_type.value.upper()} "
                f"file to: {file_path} \n {e}"
            ) from e
