"""
validation_utils.py

Reusable validation utilities for checking dictionaries, tabular structures,
and supported formats in a framework-agnostic way.
"""
import inspect
from typing import List, Any, Callable, Dict
from odibi_de_v2.utils.file_utils import is_supported_format
from odibi_de_v2.logger.decorator import log_exceptions
from odibi_de_v2.core.enums import ErrorType
from odibi_de_v2.utils.type_checks import is_empty_dict
from odibi_de_v2.logger.log_helpers import (
    log_and_optionally_raise)


def validate_required_keys(data: dict, required_keys: List[str]) -> bool:
    """
    Checks whether all required keys exist in the provided dictionary.

    Args:
        data (dict): Dictionary to validate.
        required_keys (List[str]): List of keys that must be present.

    Returns:
        bool: True if all required keys are present, False otherwise.

    Example:
        >>> validate_required_keys({"a": 1, "b": 2}, ["a", "b"])
        True
    """
    return all(key in data for key in required_keys)


def validate_columns_exist(df_like: Any, required_columns: List[str]) -> bool:
    """
    Checks whether all required columns exist in a tabular object.

    Supports both:
    - Objects with `.columns` (Pandas, PySpark DataFrame)
    - Objects with `.schema.names` (Spark DataFrame)

    Args:
        df_like (Any): DataFrame-like object.
        required_columns (List[str]): Columns that must be present.

    Returns:
        bool: True if all required columns are present.

    Example:
        >>> class Dummy:
        ...     columns = ["id", "name", "value"]
        >>> validate_columns_exist(Dummy(), ["id", "value"])
        True
    """
    column_list = []

    if hasattr(df_like, "columns"):
        column_list = df_like.columns
    elif hasattr(df_like, "schema") and hasattr(df_like.schema, "names"):
        column_list = df_like.schema.names

    return all(col in column_list for col in required_columns)


def validate_supported_format(path: str, supported_formats: List[str]) -> bool:
    """
    Validates whether a file path has a supported extension.

    Args:
        path (str): File path to check.
        supported_formats (List[str]): List of allowed formats.

    Returns:
        bool: True if file format is supported.

    Example:
        >>> validate_supported_format("data/file.csv", ["csv", "json"])
        True
    """
    return is_supported_format(path, supported_formats)


@log_exceptions(
    module="UTILS",
    component="validation_utils",
    level="error",
    error_type=ErrorType.VALUE_ERROR,
    raise_exception=True,
    raise_type=ValueError
)
def validate_non_empty_dict(d: dict, context: str) -> None:
    """
    Validates that a dictionary is not empty. Logs and raises if it is.

    Args:
        d (dict): The dictionary to validate.
        context (str): Contextual string for identifying where validation is being run.

    Raises:
        ValueError: If the dictionary is empty.

    Example:
    >>> validate_non_empty_dict({"a": 1}, "config check")
        # No error

    >>> validate_non_empty_dict({}, "config check")
        ValueError: config check cannot be an empty dictionary.
    """
    if is_empty_dict(d):
        raise ValueError(f"{context} cannot be an empty dictionary.")


def validate_method_exists(obj: Any, method_name: str, context: str) -> None:
    """
    Validates that the object has the specified method. Logs a warning if it does not.
    Args:
        obj (Any): The object to check.
        method_name (str): Name of the method to look for.
        context (str): Context string for logging.
    Returns:
        None: Logs a warning if the method doesn't exist.
    Example:
        >>> class Dummy:
        ...     def my_method(self): pass

        >>> validate_method_exists(Dummy(), "my_method", context="test")
        # No warning

        >>> validate_method_exists(Dummy(), "nonexistent_method", context="test")
        # Logs a warning: Method 'nonexistent_method' not found on Dummy. Skipping...
    """
    if not hasattr(obj, method_name):
        msg = f"Method '{method_name}' not found on {type(obj).__name__}. Skipping..."
        log_and_optionally_raise(
            module="UTILS",
            component="validation_utils",
            method=context,
            error_type=ErrorType.NO_ERROR,
            message=msg,
            level="INFO",
            raise_exception=False)

def validate_kwargs_match_signature(
    method: Callable,
    kwargs: Dict,
    method_name: str,
    context: str,
    raise_error: bool = True
        ) -> None:
    """
    Validates that the provided kwargs match the method's signature.

    Args:
        method (Callable): The method to inspect.
        kwargs (dict): Keyword arguments to validate.
        method_name (str): Name of the method for logging context.
        context (str): Parent function or component using this validator.
        raise_error (bool): Whether to raise ValueError or just log.

    Raises:
        ValueError: If invalid keyword arguments are detected (optional).
    Example:
    >>> def greet(name, age): pass
    >>> validate_kwargs_match_signature(greet, {"name": "Alice"}, "greet", "example")
        # No error
    >>> validate_kwargs_match_signature(greet, {"foo": 123}, "greet", "example")
        ValueError: Invalid kwargs for 'greet': ['foo']
    """
    if not isinstance(kwargs, dict):
        return  # Only validate dict-type args

    sig = inspect.signature(method)
    valid_params = sig.parameters
    invalid = [k for k in kwargs if k not in valid_params]

    if invalid:
        message = f"Invalid kwargs for '{method.__name__}': {invalid}"
        log_and_optionally_raise(
            module="UTILS",
            component=context,
            method=method_name,
            message=message,
            level="error",
            error_type=ErrorType.VALUE_ERROR,
            raise_exception=raise_error,
            raise_type=ValueError
        )

