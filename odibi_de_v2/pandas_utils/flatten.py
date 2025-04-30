import pandas as pd
from typing import Union

def flatten_json_columns(
   df: pd.DataFrame,
   json_columns: Union[list[str], None] = None,
   sep: str = "_"
) -> pd.DataFrame:
   """
   Flatten nested JSON columns in a Pandas DataFrame using json_normalize.
   Args:
       df (pd.DataFrame): Input DataFrame with one or more columns containing nested dicts.
       json_columns (list[str], optional): List of columns to flatten. If None, all object columns will be scanned.
       sep (str): Separator to use when flattening nested keys.
   Returns:
       pd.DataFrame: Flattened DataFrame with nested JSON fields expanded to top-level columns.
   """
   df = df.copy()
   # Auto-detect JSON columns if not provided
   if json_columns is None:
       json_columns = [
           col for col in df.columns
           if df[col].apply(lambda x: isinstance(x, dict)).any()
       ]
   for col in json_columns:
       expanded = pd.json_normalize(df[col].dropna(), sep=sep)
       expanded.columns = [f"{col}{sep}{c}" for c in expanded.columns]
       df = df.drop(columns=[col]).reset_index(drop=True)
       df = pd.concat([df, expanded.reset_index(drop=True)], axis=1)
   return df