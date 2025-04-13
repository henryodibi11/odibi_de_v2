"""
spark_utils.validation

Validation utilities specific to PySpark DataFrames.
"""

from typing import Any, List
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, ArrayType


def is_spark_dataframe(obj: Any) -> bool:
    """
    Checks whether an object is a Spark DataFrame.

    Args:
        obj (Any): Object to test.

    Returns:
        bool: True if obj is a Spark DataFrame, False otherwise.
    """
    return isinstance(obj, DataFrame)


def has_columns(df: DataFrame, required_columns: List[str]) -> bool:
    """
    Checks whether all required columns exist in the Spark DataFrame.

    Args:
        df (DataFrame): Spark DataFrame to check.
        required_columns (List[str]): List of required column names.

    Returns:
        bool: True if all columns exist, False otherwise.
    """
    return all(col in df.columns for col in required_columns)


def is_empty(df: DataFrame) -> bool:
    """
    Checks whether the DataFrame has no rows.

    Args:
        df (DataFrame): Spark DataFrame to check.

    Returns:
        bool: True if DataFrame is empty, False otherwise.
    """
    return df.rdd.isEmpty()


def has_nulls_in_columns(df: DataFrame, cols: List[str]) -> bool:
    """
    Checks if any specified column contains null values.

    Args:
        df (DataFrame): Spark DataFrame to check.
        cols (List[str]): Column names to check.

    Returns:
        bool: True if any of the columns contain nulls.
    """
    for col in cols:
        if col in df.columns and df.filter(
            df[col].isNull()).limit(1).count(
                ) > 0:
            return True
    return False


def has_duplicate_columns(df: DataFrame) -> bool:
    """
    Checks for duplicate column names in the DataFrame.

    Args:
        df (DataFrame): Spark DataFrame to check.

    Returns:
        bool: True if there are duplicate column names, False otherwise.
    """
    return len(df.columns) != len(set(df.columns))


def is_flat_dataframe(df: DataFrame) -> bool:
    """
    Checks if the DataFrame schema is flat (no StructType or ArrayType
        columns).

    Args:
        df (DataFrame): Spark DataFrame to check.

    Returns:
        bool: True if schema is flat, False if nested types exist.
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, (StructType, ArrayType)):
            return False
    return True
