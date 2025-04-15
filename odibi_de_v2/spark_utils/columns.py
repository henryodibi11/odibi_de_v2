from typing import List
from pyspark.sql import DataFrame


def has_columns(df: DataFrame, required_columns: List[str]) -> bool:
   """
   Check if all required columns are present in the Spark DataFrame.

   Args:
       df (DataFrame): Input Spark DataFrame.
       required_columns (List[str]): List of required column names.

   Returns:
       bool: True if all required columns exist, False otherwise.
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