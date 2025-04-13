"""
file_utils.py

Utility functions for basic file path handling, used across connectors,
readers, and savers in the odibi_de_v2 framework.
"""

import os
from typing import List


def get_file_extension(path: str) -> str:
    """
    Extracts the file extension from a path.

    Args:
        path (str): The input file path.

    Returns:
        str: The file extension (e.g., "csv", "json").

    Example:
        >>> get_file_extension("data/file.json")
        'json'
    """
    return os.path.splitext(path)[1].lstrip(".").lower()


def get_stem_name(path: str) -> str:
    """
    Returns the file name without the extension.

    Args:
        path (str): The input file path.

    Returns:
        str: File name without extension.

    Example:
        >>> get_stem_name("folder/data.csv")
        'data'
    """
    return os.path.splitext(os.path.basename(path))[0]


def extract_file_name(path: str) -> str:
    """
    Returns the full file name from the path (including extension).

    Args:
        path (str): Full file path.

    Returns:
        str: File name with extension.

    Example:
        >>> extract_file_name("data/folder/sample.parquet")
        'sample.parquet'
    """
    return os.path.basename(path)


def extract_folder_name(path: str) -> str:
    """
    Returns the parent folder name from a file path.

    Args:
        path (str): File path.

    Returns:
        str: Name of the parent folder.

    Example:
        >>> extract_folder_name("data/folder/file.csv")
        'folder'
    """
    return os.path.basename(os.path.dirname(path))


def is_valid_file_path(path: str) -> bool:
    """
    Checks if a string is a valid-looking file path (non-empty, has file name).

    Args:
        path (str): Path to check.

    Returns:
        bool: True if it's a valid-looking path.

    Example:
        >>> is_valid_file_path("folder/data.csv")
        True
    """
    return bool(path) and os.path.basename(path) != ""


def is_supported_format(path: str, supported_formats: List[str]) -> bool:
    """
    Checks if a file has one of the supported extensions.

    Args:
        path (str): File path.
        supported_formats (List[str]): List of allowed formats
            (e.g., ["csv", "json"]).

    Returns:
        bool: True if extension is in supported list.

    Example:
        >>> is_supported_format("data/file.avro", ["csv", "avro"])
        True
    """
    ext = get_file_extension(path)
    return ext in [fmt.lower() for fmt in supported_formats]
