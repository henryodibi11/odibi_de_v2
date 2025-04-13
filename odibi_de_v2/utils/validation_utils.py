"""
validation_utils.py

Reusable validation utilities for checking dictionaries, tabular structures,
and supported formats in a framework-agnostic way.
"""

from typing import List, Any
from odibi_de_v2.utils.file_utils import is_supported_format


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
