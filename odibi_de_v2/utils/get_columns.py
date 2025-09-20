from typing import Any, List
import pandas as pd
from odibi_de_v2.core import Framework, ColumnType


def _is_pandas_boolean(series: pd.Series) -> bool:
    """
    Robust boolean detection for Pandas.

    Handles cases where boolean columns containing None/NaN
    are silently upcast to `object` dtype.

    Strategy
    --------
    1. First, check if Pandas recognizes it as a bool dtype.
    2. If not, fallback: verify all non-null values are True/False.

    Parameters
    ----------
    series : pd.Series
        Series to test.

    Returns
    -------
    bool
        True if the series should be treated as boolean, else False.
    """
    try:
        if pd.api.types.is_bool_dtype(series):
            return True
    except Exception:
        pass
    try:
        if series.dropna().isin([True, False]).all():
            return True
    except Exception:
        pass
    return False


def _safe_is_type(engine: Framework, category: str, dtype: Any) -> bool:
    """
    Safe wrapper around `is_type` to avoid Pandas dtype conversion errors.

    Parameters
    ----------
    engine : Framework
        Framework.PANDAS or Framework.SPARK.
    category : str
        One of "numeric", "string", "boolean", "datetime".
    dtype : Any
        The dtype/DataType object to check.

    Returns
    -------
    bool
        True if dtype matches the category, else False.
    """
    from odibi_de_v2.utils import is_type
    try:
        return is_type(engine, category, dtype)
    except Exception:
        return False


def get_columns_by_type(df: Any, col_type: ColumnType) -> List[str]:
    """
    Return column names matching a given ColumnType for Pandas or Spark DataFrames.

    Features
    --------
    - Supports both Pandas and PySpark DataFrames.
    - For Pandas, applies strict detection order:
        NUMERIC > BOOLEAN > DATETIME > STRING
      to avoid overlaps/misclassification.
    - Robust handling for Pandas "boolean with None" edge cases.

    Parameters
    ----------
    df : Any
        A Pandas or Spark DataFrame.
    col_type : ColumnType
        One of ColumnType.NUMERIC, ColumnType.STRING,
        ColumnType.BOOLEAN, ColumnType.DATETIME.

    Returns
    -------
    list of str
        Column names that match the requested type.

    Raises
    ------
    TypeError
        If the DataFrame engine cannot be detected or is unsupported.

    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame({
    ...     "a": [1, 2],
    ...     "b": ["x", "y"],
    ...     "c": [True, None],
    ...     "d": pd.date_range("2023-01-01", periods=2)
    ... })
    >>> get_columns_by_type(df, ColumnType.NUMERIC)
    ['a']
    >>> get_columns_by_type(df, ColumnType.STRING)
    ['b']
    >>> get_columns_by_type(df, ColumnType.BOOLEAN)
    ['c']
    >>> get_columns_by_type(df, ColumnType.DATETIME)
    ['d']

    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.master("local[1]").getOrCreate()
    >>> sdf = spark.createDataFrame([(1, "x", True)], ["a", "b", "c"])
    >>> get_columns_by_type(sdf, ColumnType.BOOLEAN)
    ['c']
    """
    from odibi_de_v2.utils import detect_engine
    engine = detect_engine(df)

    # ---------- Pandas ----------
    if engine == Framework.PANDAS.value:
        cols = []
        for c in df.columns:
            series = df[c]
            dtype = series.dtype

            if col_type == ColumnType.NUMERIC:
                if _safe_is_type(Framework.PANDAS, "numeric", dtype):
                    cols.append(c)
            elif col_type == ColumnType.BOOLEAN:
                if _is_pandas_boolean(series):
                    cols.append(c)
            elif col_type == ColumnType.DATETIME:
                if _safe_is_type(Framework.PANDAS, "datetime", dtype):
                    cols.append(c)
            elif col_type == ColumnType.STRING:
                if (
                    not _safe_is_type(Framework.PANDAS, "numeric", dtype)
                    and not _is_pandas_boolean(series)
                    and not _safe_is_type(Framework.PANDAS, "datetime", dtype)
                    and _safe_is_type(Framework.PANDAS, "string", dtype)
                ):
                    cols.append(c)
        return cols

    # ---------- Spark ----------
    if engine == Framework.SPARK.value:
        return [
            f.name for f in df.schema.fields
            if is_type(Framework.SPARK, col_type.value, f.dataType)
        ]

    raise TypeError(
        f"Unsupported DataFrame type: {type(df)} (engine={engine})"
    )
