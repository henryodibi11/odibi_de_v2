import pandas as pd


def convert_to_datetime(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
   """
   Convert the specified column to datetime format.

   Args:
       df (pd.DataFrame): Input DataFrame.
       column_name (str): Name of the column to convert.

   Returns:
       pd.DataFrame: DataFrame with column converted to datetime.
   """
   df[column_name] = pd.to_datetime(df[column_name], errors="coerce")
   return df


def extract_date_parts(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
   """
   Extract year, month, and day from a datetime column.

   Args:
       df (pd.DataFrame): Input DataFrame.
       column_name (str): Datetime column name.

   Returns:
       pd.DataFrame: DataFrame with new columns for year, month, and day.
   """
   df["year"] = df[column_name].dt.year
   df["month"] = df[column_name].dt.month
   df["day"] = df[column_name].dt.day
   return df