from typing import List
from pyspark.sql import DataFrame


def has_columns(df: DataFrame, required_columns: List[str]) -> bool:
    """
    Determine whether a Spark DataFrame contains all specified columns.

    This function checks if each column listed in `required_columns` is present in the DataFrame `df`. It is useful for
    validating DataFrame schemas before performing operations that assume certain columns exist.

    Args:
        df (DataFrame): The Spark DataFrame to check for column presence.
        required_columns (List[str]): A list of strings representing the names of the columns that are required in
        the DataFrame.

    Returns:
        bool: Returns True if all specified columns are present in the DataFrame, otherwise False.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.appName("Example").getOrCreate()
        >>> data = [("Alice", 1), ("Bob", 2)]
        >>> columns = ["name", "id"]
        >>> df = spark.createDataFrame(data, columns)
        >>> required_columns = ["name", "id"]
        >>> has_columns(df, required_columns)
        True
        >>> required_columns = ["name", "age"]
        >>> has_columns(df, required_columns)
        False
    """
    return all(col in df.columns for col in required_columns)


def drop_columns(df: DataFrame, columns_to_drop: List[str]) -> DataFrame:
   """
   Drop the specified columns from the Spark DataFrame if they exist.

   Args:
       df (DataFrame): Input Spark DataFrame.
       columns_to_drop (List[str]): List of column names to drop.

   Returns:
       DataFrame: Spark DataFrame without the specified columns.
   """
   existing = [col for col in columns_to_drop if col in df.columns]
   return df.drop(*existing)


def select_columns(df: DataFrame, columns_to_keep: List[str]) -> DataFrame:
   """
   Select only the specified columns from the Spark DataFrame if they exist.

   Args:
       df (DataFrame): Input Spark DataFrame.
       columns_to_keep (List[str]): List of column names to retain.

   Returns:
       DataFrame: Spark DataFrame containing only the specified columns.
   """
   existing = [col for col in columns_to_keep if col in df.columns]
   return df.select(*existing)