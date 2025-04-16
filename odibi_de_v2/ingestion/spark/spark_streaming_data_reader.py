from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from py4j.protocol import Py4JJavaError

from odibi_de_v2.core import DataReader
from odibi_de_v2.core.enums import DataType, ErrorType
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty,
    benchmark, log_call
)
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise
from odibi_de_v2.utils.method_chain import run_method_chain


class SparkStreamingDataReader(DataReader):
    """
    Flexible Spark streaming reader using Autoloader (cloudFiles).

    Internally sets:
        - format = "cloudFiles"
        - options["cloudFiles.format"] = data_type.value

    Additional options can be passed via kwargs and will be chained into
    the reader (e.g., options, schema, maxFilesPerTrigger, etc.).

    Example usage:
        >>> reader = SparkStreamingDataReader()
        >>> df = reader.read_data(
        ...     data_type=DataType.JSON,
        ...     file_path="/mnt/input/json",
        ...     spark=spark,
        ...     options={"schemaLocation": "/mnt/schema"}
        ... )
    """

    @enforce_types(strict=True)
    @validate_non_empty(["file_path"])
    @benchmark(module="INGESTION", component="SparkStreamingDataReader")
    @log_call(module="INGESTION", component="SparkStreamingDataReader")
    @log_exceptions(
        module="INGESTION",
        component="SparkStreamingDataReader",
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
        Reads streaming data using Spark readStream and cloudFiles format.

        Args:
            data_type (DataType): Format to read (e.g., CSV, JSON, PARQUET).
            file_path (str): Path to streaming input source.
            spark (SparkSession, optional): Spark session to use.
            **kwargs: Additional chained methods like options, schema, etc.

        Returns:
            DataFrame: A streaming Spark DataFrame ready for transformation or sink.
        """
        spark = spark or SparkSession.builder.getOrCreate()

        try:
            log_and_optionally_raise(
                module="INGESTION",
                component="SparkStreamingDataReader",
                method="read_data",
                error_type=ErrorType.NO_ERROR,
                message=f"Starting streaming read for {data_type.value.upper()} at {file_path}",
                level="INFO")

            # Prepare method chain
            method_chain = {
                "format": "cloudFiles",
                **kwargs}

            # Inject cloudFiles.format into the .options() call
            if "options" not in method_chain:
                method_chain["options"] = {}

            method_chain["options"]["cloudFiles.format"] = data_type.value.lower()

            reader = run_method_chain(spark.readStream, method_chain)
            df = reader.load(file_path)

            log_and_optionally_raise(
                module="INGESTION",
                component="SparkStreamingDataReader",
                method="read_data",
                error_type=ErrorType.NO_ERROR,
                message=f"Successfully created streaming DataFrame from {file_path}",
                level="INFO")
            return df

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
