"""
read_error_handler.py

Provides the `wrap_read_errors` decorator for Pandas and Spark data readers.
This decorator centralizes error handling and logging for all file reading
operations, ensuring consistent exception mapping and standardized log output.
"""

from functools import wraps
from py4j.protocol import Py4JJavaError
from odibi_de_v2.logger import log_and_optionally_raise
from odibi_de_v2.core import ErrorType


def wrap_read_errors(component: str):
    """
    Decorator factory for wrapping reader methods (Pandas/Spark)
    with standardized logging and error handling.

    The decorated function will:
      - Log the attempt to read a file
      - Execute the original reader method
      - Log success if completed without error
      - Catch common I/O and Spark errors, log them, and re-raise with
        descriptive messages

    Args:
        component (str): Component name for logging (e.g., "PandasDataReader")

    Returns:
        Callable: A wrapped function that executes the original read method
        with consistent logging and exception handling.

    Example:
        >>> from odibi_de_v2.utils.decorators.read_error_handler import wrap_read_errors
        >>>
        >>> class PandasDataReader:
        ...     @wrap_read_errors(component="PandasDataReader")
        ...     def read_data(self, data_type, file_path, **kwargs):
        ...         return pd.read_csv(file_path, **kwargs)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, data_type, file_path, *args, **kwargs):
            method_name = func.__name__

            # Log attempt
            log_and_optionally_raise(
                module="INGESTION",
                component=component,
                method=method_name,
                error_type=ErrorType.NO_ERROR,
                message=f"Attempting to read {data_type.value.upper()} file from: {file_path}.",
                level="INFO"
            )

            try:
                result = func(self, data_type, file_path, *args, **kwargs)

                # Log success
                log_and_optionally_raise(
                    module="INGESTION",
                    component=component,
                    method=method_name,
                    error_type=ErrorType.NO_ERROR,
                    message=f"Successfully read {data_type.value.upper()} file from: {file_path}.",
                    level="INFO"
                )
                return result

            except PermissionError as e:
                msg = f"Permission denied while accessing {data_type.value.upper()} file: {file_path}\n{e}"
                log_and_optionally_raise("INGESTION", component, method_name, ErrorType.PERMISSION_ERROR, msg, "ERROR")
                raise PermissionError(msg) from e

            except FileNotFoundError as e:
                msg = f"{data_type.value.upper()} file not found: {file_path}\n{e}"
                log_and_optionally_raise("INGESTION", component, method_name, ErrorType.FILE_NOT_FOUND, msg, "ERROR")
                raise FileNotFoundError(msg) from e

            except IsADirectoryError as e:
                msg = f"Expected a file but got a directory: {file_path}\n{e}"
                log_and_optionally_raise("INGESTION", component, method_name, ErrorType.INVALID_PATH, msg, "ERROR")
                raise IsADirectoryError(msg) from e

            except ValueError as e:
                msg = f"Invalid or empty {data_type.value.upper()} file: {file_path}\n{e}"
                log_and_optionally_raise("INGESTION", component, method_name, ErrorType.INVALID_DATA, msg, "ERROR")
                raise ValueError(msg) from e

            except OSError as e:
                msg = f"I/O error while reading {data_type.value.upper()} file: {file_path}\n{e}"
                log_and_optionally_raise("INGESTION", component, method_name, ErrorType.IO_ERROR, msg, "ERROR")
                raise OSError(msg) from e

            except NotImplementedError as e:
                msg = f"Unsupported data type: {data_type.value}\n{e}"
                log_and_optionally_raise("INGESTION", component, method_name, ErrorType.NOT_IMPLEMENTED, msg, "ERROR")
                raise NotImplementedError(msg) from e

            except Py4JJavaError as e:
                msg = f"Spark read error while reading {data_type.value.upper()} file: {str(e.java_exception)}"
                log_and_optionally_raise("INGESTION", component, method_name, ErrorType.Runtime_Error, msg, "ERROR")
                raise RuntimeError(msg) from e

            except Exception as e:
                msg = f"Unexpected error while reading {data_type.value.upper()} file: {file_path}\n{e}"
                log_and_optionally_raise("INGESTION", component, method_name, ErrorType.Runtime_Error, msg, "ERROR")
                raise RuntimeError(msg) from e

        return wrapper
    return decorator
