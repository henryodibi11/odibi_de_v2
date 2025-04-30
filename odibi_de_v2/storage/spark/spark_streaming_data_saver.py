from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from py4j.protocol import Py4JJavaError

from odibi_de_v2.core import DataSaver
from odibi_de_v2.core.enums import DataType, ErrorType
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, validate_input_types,
    benchmark, log_call
)
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise
from odibi_de_v2.utils.method_chain import run_method_chain


class SparkStreamingDataSaver(DataSaver):
    """
    Flexible Spark streaming saver using writeStream chaining.

    This class supports streaming output to any supported sink such as
    Delta, Parquet, CSV, Kafka, etc. It injects `.format(...)` and `.start(...)`,
    while allowing users to define options like `trigger`, `checkpointLocation`,
    and `outputMode`.

    Example usage:
        >>> saver = SparkStreamingDataSaver()
        >>> query = saver.save_data(
        ...     df=streaming_df,
        ...     data_type=DataType.DELTA,
        ...     file_path="/mnt/bronze/table",
        ...     outputMode="append",
        ...     options={"checkpointLocation": "/mnt/bronze/checkpoints"}
        ... )
        >>> query.awaitTermination()  # Optional
    """

    @enforce_types(strict=True)
    @validate_non_empty(["file_path"])
    @validate_input_types({"df": DataFrame, "file_path": str})
    @benchmark(module="STORAGE", component="SparkStreamingDataSaver")
    @log_call(module="STORAGE", component="SparkStreamingDataSaver")
    @log_exceptions(
        module="STORAGE",
        component="SparkStreamingDataSaver",
        error_type=ErrorType.SAVE_ERROR,
        raise_type=RuntimeError
    )
    def save_data(
        self,
        df: DataFrame,
        data_type: DataType,
        file_path: str,
        **kwargs
    ) -> StreamingQuery:
        """
        Starts a streaming write using Spark writeStream with dynamic chaining.

        Args:
            df (DataFrame): The streaming DataFrame.
            data_type (DataType): Format to write (e.g., delta, parquet, kafka).
            file_path (str): Target sink path or output location.
            **kwargs: Chained writer methods like outputMode, trigger, options.

        Returns:
            StreamingQuery: The active streaming query.

        Raises:
            RuntimeError: If writing fails due to Spark or filesystem issues.
        """
        try:
            log_and_optionally_raise(
                module="STORAGE",
                component="SparkStreamingDataSaver",
                method="save_data",
                error_type=ErrorType.NO_ERROR,
                message=(
                    f"Starting streaming write as {data_type.value.upper()} "
                    f"to {file_path}"),
                level="INFO")

            method_chain = {"format": data_type.value, **kwargs}
            writer = run_method_chain(df.writeStream, method_chain)
            query = writer.start(file_path)

            log_and_optionally_raise(
                module="STORAGE",
                component="SparkStreamingDataSaver",
                method="save_data",
                error_type=ErrorType.NO_ERROR,
                message=f"Streaming write started successfully for: {file_path}",
                level="INFO")
            return query

        except PermissionError as e:
            raise PermissionError(
                f"Permission denied while accessing: {file_path} \n {e}"
            ) from e
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"Streaming path not found: {file_path} \n {e}"
            ) from e
        except IsADirectoryError as e:
            raise IsADirectoryError(
                f"Expected a file path but got a directory: {file_path} \n {e}"
            ) from e
        except ValueError as e:
            raise ValueError(
                f"Invalid config or streaming parameter for: {file_path} \n {e}"
            ) from e
        except OSError as e:
            raise OSError(
                f"I/O error during streaming save to {file_path} \n {e}"
            ) from e
        except NotImplementedError as e:
            raise NotImplementedError(
                f"Unsupported streaming format: {data_type.value} \n {e}"
            ) from e
        except Py4JJavaError as e:
            raise RuntimeError(
                f"Spark streaming error: {str(e.java_exception)}"
            ) from e
        except Exception as e:
            raise RuntimeError(
                f"Unexpected error during streaming write to {file_path} \n {e}"
            ) from e
