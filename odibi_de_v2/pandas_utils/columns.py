from typing import List
import pandas as pd

def has_columns(df: pd.DataFrame, required_columns: List[str]) -> bool:
   """
   Check if all required columns are present in the DataFrame.
   Args:
       df (pd.DataFrame): Input DataFrame.
       required_columns (List[str]): List of required column names.
   Returns:
       bool: True if all required columns exist, False otherwise.
   """
   return all(col in df.columns for col in required_columns)

def drop_columns(df: pd.DataFrame, columns_to_drop: List[str]) -> pd.DataFrame:
   """
   Drop the specified columns from the DataFrame if they exist.
   Args:
       df (pd.DataFrame): Input DataFrame.
       columns_to_drop (List[str]): List of column names to drop.
   Returns:
       pd.DataFrame: DataFrame without the specified columns.
   """
   existing = [col for col in columns_to_drop if col in df.columns]
   return df.drop(columns=existing)

def select_columns(df: pd.DataFrame, columns_to_keep: List[str]) -> pd.DataFrame:
   """
   Select only the specified columns from the DataFrame if they exist.
   Args:
       df (pd.DataFrame): Input DataFrame.
       columns_to_keep (List[str]): List of column names to retain.
   Returns:
       pd.DataFrame: DataFrame containing only the specified columns.
   """
   existing = [col for col in columns_to_keep if col in df.columns]
   return df[existing]