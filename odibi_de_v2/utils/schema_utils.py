from typing import Any, List, Tuple
def get_numeric_and_string_columns(df: Any) -> Tuple[List[str], List[str]]:
   """
   Detect numeric and string/object columns in a Pandas or Spark DataFrame.
   Returns
   -------
   numeric_cols : list of str
       Column names with numeric dtypes.
   string_cols : list of str
       Column names with string/object dtypes.
   """
   # --- Pandas ---
   try:
       import pandas as pd
       if isinstance(df, pd.DataFrame):
           numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
           string_cols = [c for c in df.columns if pd.api.types.is_object_dtype(df[c])
                          or pd.api.types.is_string_dtype(df[c])]
           return numeric_cols, string_cols
   except ImportError:
       pass
   # --- Spark ---
   try:
       from pyspark.sql import DataFrame as SparkDF
       from pyspark.sql import types as T
       if isinstance(df, SparkDF):
           numeric_types = (T.ByteType, T.ShortType, T.IntegerType, T.LongType,
                            T.FloatType, T.DoubleType, T.DecimalType)
           numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, numeric_types)]
           string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, T.StringType)]
           return numeric_cols, string_cols
   except ImportError:
       pass
   raise TypeError(f"Unsupported dataframe type: {type(df)}")