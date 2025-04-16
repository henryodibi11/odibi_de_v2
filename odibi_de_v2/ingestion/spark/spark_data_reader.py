from typing import Optional
from pyspark.sql import SparkSession, DataFrame

from odibi_de_v2.core import DataReader
from odibi_de_v2.core.enums import DataType, ErrorType
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    log_call, benchmark
)
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise
from odibi_de_v2.utils import run_method_chain
from py4j.protocol import Py4JJavaError

class SparkDataReader(DataReader):
    """
    Flexible Spark reader for structured data using method chaining.

    This class supports reading files in any format supported by Spark,
    using dynamic method chaining via kwargs. The file format is inferred
    from the `DataType` enum and passed as the `.format()` method.

    Example usage:
        >>> from pyspark.sql import SparkSession
        >>> from odibi_de_v2.core.enums import DataType
        >>> from odibi_de_v2.ingestion.spark.spark_data_reader import SparkDataReader

        >>> spark = SparkSession.builder.getOrCreate()
        >>> reader = SparkDataReader()

        >>> method_chain = {
        ...     "option": {"header": "true", "inferSchema": "true"},
        ...     "load": "/mnt/data/file.csv"
        ... }

        >>> df = reader.read_data(
        ...     data_type=DataType.CSV,
        ...     file_path="/mnt/data/file.csv",
        ...     spark=spark,
        ...     **method_chain
        ... )
    """

    @enforce_types(strict=True)
    @validate_non_empty(["file_path"])
    @ensure_output_type(DataFrame)
    @benchmark(module="INGESTION", component="SparkDataReader")
    @log_call(module="INGESTION", component="SparkDataReader")
    @log_exceptions(
        module="INGESTION",
        component="SparkDataReader",
        error_type=ErrorType.READ_ERROR,
        raise_type=RuntimeError
    )
    def read_data(
        self,
        data_type: DataType,
        file_path: str,
        spark: Optional[SparkSession] = None,
        **kwargs
    ) -> DataFrame:
        """
        Reads a file into a Spark DataFrame using dynamic method chaining.

        Args:
            data_type (DataType): Enum indicating the file format.
            file_path (str): Path to the file (used for logging context).
            spark (SparkSession, optional): Spark session instance. Defaults to a new session.
            **kwargs: Additional chained methods like 'option', 'load', 'schema', etc.

        Returns:
            DataFrame: A Spark DataFrame loaded using the chained read methods.

        Raises:
            RuntimeError: If reading fails due to Spark, I/O, or chaining errors.

        Example:
            >>> df = reader.read_data(
            ...     data_type=DataType.JSON,
            ...     file_path="/mnt/data/file.json",
            ...     option={"multiline": "true"}
            ... )
        """
        spark = spark or SparkSession.builder.appName("SparkDataReader").getOrCreate()
        try:
            log_and_optionally_raise(
                module="INGESTION",
                component="SparkDataReader",
                method="read_data",
                error_type=ErrorType.NO_ERROR,
                message=f"Attempting to read {data_type.value.upper()} file from: {file_path}.",
                level="INFO")

            method_chain = {"format": data_type.value, **kwargs}
            reader = run_method_chain(spark.read, method_chain)
            df = reader.load(file_path)
            log_and_optionally_raise(
                module="INGESTION",
                component="SparkDataReader",
                method="read_data",
                error_type=ErrorType.NO_ERROR,
                message=f"Successfully read {data_type.value.upper()} file from: {file_path}.",
                level="INFO")
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
        except Py4JJavaError as e:
            raise RuntimeError(
                f"Spark read error while reading "
                f"{data_type.value.upper()} file: {str(e.java_exception)}") from e
        except Exception as e:
            raise RuntimeError(
                f"Unexpected error while reading {data_type.value.upper()} "
                f"file: {file_path} \n {e}") from e
        return df

