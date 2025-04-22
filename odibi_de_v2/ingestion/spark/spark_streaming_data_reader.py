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
    Reads streaming data using Spark's readStream and cloudFiles format.

    This method initializes a Spark DataFrame for streaming by leveraging the cloudFiles format, which is particularly useful for scalable and efficient data ingestion from cloud storage. It supports various data formats such as CSV, JSON, and PARQUET by specifying the `data_type`. The method also allows for additional configurations through keyword arguments.

    Args:
        data_type (DataType): The format of the data to read (e.g., CSV, JSON, PARQUET).
        file_path (str): The path to the streaming input source.
        spark (SparkSession, optional): The Spark session to use. If not provided, a new session will be created.
        **kwargs: Additional options for the Spark readStream method (e.g., schema, maxFilesPerTrigger).

    Returns:
        DataFrame: A streaming Spark DataFrame ready for further transformations or storage.

    Raises:
        PermissionError: If there is a permission issue accessing the file.
        FileNotFoundError: If the specified file does not exist.
        IsADirectoryError: If a directory is provided when a file is expected.
        ValueError: If the provided file is invalid or empty.
        OSError: If an I/O error occurs during file reading.
        NotImplementedError: If the data type specified is not supported.
        RuntimeError: If an unexpected error occurs or if there is a Spark-related issue.

    Example:
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
        Reads streaming data from a specified file path using Spark's readStream with cloudFiles format.

        This method initializes a Spark session if not provided, configures the reader based on the specified data type, and handles various exceptions related to file access and data processing.

        Args:
            data_type (DataType): The format of the data to read (e.g., CSV, JSON, PARQUET).
            file_path (str): The path to the streaming input source.
            spark (SparkSession, optional): An existing Spark session to use. If not provided, a new session will be created.
            **kwargs: Additional keyword arguments to pass to the Spark readStream method. These can include options like schema, partitioning, etc.

        Returns:
            DataFrame: A Spark DataFrame representing the streaming data.

        Raises:
            PermissionError: If there is a permission issue accessing the file.
            FileNotFoundError: If the file specified does not exist.
            IsADirectoryError: If the path specified is a directory, not a file.
            ValueError: If the file is invalid or empty.
            OSError: If an I/O error occurs during file reading.
            NotImplementedError: If the data type specified is not supported.
            RuntimeError: If a Spark-related error occurs, or an unexpected error is encountered.

        Example:
            >>> spark_session = SparkSession.builder.appName("ExampleApp").getOrCreate()
            >>> data_frame = read_data(DataType.JSON, "/path/to/data.json", spark=spark_session)
            >>> data_frame.isStreaming
            True
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
