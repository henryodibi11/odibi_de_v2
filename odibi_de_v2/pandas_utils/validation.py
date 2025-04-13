"""
pandas_utils.validation

Validation utilities specific to pandas DataFrames.
"""

import pandas as pd
from typing import List, Any
from collections import Counter


def is_pandas_dataframe(obj: Any) -> bool:
    """
    Checks whether an object is a pandas DataFrame.

    Args:
        obj (Any): Object to test.

    Returns:
        bool: True if obj is a pandas DataFrame, False otherwise.
    """
    return isinstance(obj, pd.DataFrame)


def has_columns(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """
    Checks whether all required columns exist in the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame to check.
        required_columns (List[str]): List of required column names.

    Returns:
        bool: True if all columns exist, False otherwise.
    """
    return all(col in df.columns for col in required_columns)


def is_list_of_strings(obj: Any) -> bool:
    """
    Checks if an object is a list of strings.

    Args:
        obj (Any): Object to check.

    Returns:
        bool: True if obj is a list of strings, False otherwise.
    """
    return isinstance(obj, list) and all(isinstance(item, str) for item in obj)


def is_empty(df: pd.DataFrame) -> bool:
    """
    Checks if the DataFrame is empty (no rows).

    Args:
        df (pd.DataFrame): DataFrame to check.

    Returns:
        bool: True if empty, False otherwise.
    """
    return df.empty


def has_nulls_in_columns(df: pd.DataFrame, cols: List[str]) -> bool:
    """
    Checks whether any of the given columns contain nulls.

    Args:
        df (pd.DataFrame): DataFrame to check.
        cols (List[str]): List of column names.

    Returns:
        bool: True if any column has nulls, False otherwise.
    """
    return any(df[col].isnull().any() for col in cols if col in df.columns)


def has_duplicate_columns(df: pd.DataFrame) -> bool:
    """
    Checks whether the DataFrame has any duplicate column names.

    Args:
        df (pd.DataFrame): DataFrame to check.

    Returns:
        bool: True if duplicates exist, False otherwise.
    """
    columns_count = Counter(df.columns)
    return any(count > 1 for count in columns_count.values())


def is_flat_dataframe(df: pd.DataFrame) -> bool:
    """
    Checks whether a DataFrame is flat (i.e., no columns of type 'object'
    containing dicts or lists).

    Args:
        df (pd.DataFrame): DataFrame to check.

    Returns:
        bool: True if flat, False if nested structures found.
    """
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            return False
    return True
