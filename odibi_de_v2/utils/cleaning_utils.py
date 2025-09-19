from typing import Any, Iterable, Optional
from odibi_de_v2.core import Framework
from odibi_de_v2.utils.types import is_type


class NullFiller:
    """
    Simple, reusable null filler for Pandas & Spark DataFrames.

    This utility provides a unified API to fill missing values across 
    numeric, boolean, string, and datetime columns, with safe defaults 
    and the ability to override per type. It automatically detects the 
    backend (Pandas vs Spark), unless explicitly specified.

    Parameters
    ----------
    numeric_fill : Any, default=0
        Value used to fill numeric columns (int, float, decimal).
    bool_fill : bool, default=False
        Value used to fill boolean columns.
    string_fill : str, default=""
        Value used to fill string columns.
    datetime_fill : Any, default=None
        Value used to fill datetime columns.
        - Pandas default: `pd.NaT`
        - Spark default: leaves nulls as-is (requires explicit override).
    include : Iterable[str], optional
        List of column names to include (all others untouched).
    exclude : Iterable[str], optional
        List of column names to skip.
    engine : {"pandas", "spark"}, optional
        Force execution engine; otherwise auto-detects from input.
    fill_nan : bool, default=True
        For Spark: also replaces `NaN` in float/double columns.
        For Pandas: no effect (fillna handles NaN by default).

    Returns
    -------
    DataFrame
        A new DataFrame with nulls filled appropriately.

    Examples
    --------
    **Pandas Example**
    >>> import pandas as pd
    >>> from odibi_de_v2.utils.cleaning_utils import NullFiller
    >>> df = pd.DataFrame({
    ...     "num": [1, None, 3],
    ...     "flag": [True, None, False],
    ...     "text": ["a", None, "c"],
    ...     "date": [pd.NaT, pd.Timestamp("2023-01-01"), None]
    ... })
    >>> filler = NullFiller()
    >>> df_filled = filler.apply(df)
    >>> df_filled
       num   flag   text       date
    0   1.0   True      a        NaT
    1   0.0  False             2023-01-01
    2   3.0  False      c        NaT

    **Spark Example**
    >>> from pyspark.sql import SparkSession
    >>> import datetime
    >>> spark = SparkSession.builder.getOrCreate()
    >>> sdf = spark.createDataFrame([
    ...     (1, True, "a", None),
    ...     (None, None, None, datetime.date(2023, 1, 1)),
    ...     (3, False, "c", None),
    ... ], ["num", "flag", "text", "date"])
    >>> filler = NullFiller(numeric_fill=-1, string_fill="MISSING")
    >>> sdf_filled = filler.apply(sdf)
    >>> sdf_filled.show()
    +---+-----+-------+----------+
    |num| flag|   text|      date|
    +---+-----+-------+----------+
    |  1| true|      a|      null|
    | -1|false|MISSING|2023-01-01|
    |  3|false|      c|      null|
    +---+-----+-------+----------+
    """
    
    def __init__(
        self,
        numeric_fill: Any = 0,
        bool_fill: bool = False,
        string_fill: str = "",
        datetime_fill: Any = None,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        engine: Optional[str] = None,
        fill_nan: bool = True,
    ):
        self.numeric_fill = numeric_fill
        self.bool_fill = bool_fill
        self.string_fill = string_fill
        self.datetime_fill = datetime_fill

        self.include = set(include) if include else None
        self.exclude = set(exclude) if exclude else set()
        self.engine_override = engine
        self.fill_nan = fill_nan

    def __repr__(self) -> str:
        return (
            f"NullFiller(numeric_fill={self.numeric_fill!r}, "
            f"bool_fill={self.bool_fill!r}, string_fill={self.string_fill!r}, "
            f"datetime_fill={self.datetime_fill!r}, "
            f"include={sorted(self.include) if self.include else None}, "
            f"exclude={sorted(self.exclude) if self.exclude else []}, "
            f"engine={self.engine_override!r}, fill_nan={self.fill_nan!r})"
        )

    # --------- Public API ---------
    def apply(self, df: Any) -> Any:
        from odibi_de_v2.utils import detect_engine
        """
        Apply null-filling rules to the given DataFrame (Pandas or Spark).
        """
        engine = self.engine_override or detect_engine(df)
        if engine == Framework.PANDAS.value:
            return self._apply_pandas(df)
        elif engine == Framework.SPARK.value:
            return self._apply_spark(df)
        raise TypeError(
            f"Unsupported dataframe type: {type(df)}. "
            f"Engine detection result: {engine} "
            f"(module={getattr(type(df), '__module__', None)}, "
            f"name={getattr(type(df), '__name__', None)})"
        )

    # --------- Helpers ---------
    def _is_in_scope(self, col: str) -> bool:
        if self.include is not None and col not in self.include:
            return False
        if col in self.exclude:
            return False
        return True

    # --------- Pandas path ---------
    def _apply_pandas(self, df):
        import pandas as pd

        out = df.copy()
        for col in out.columns:
            if not self._is_in_scope(col):
                continue
            dtype = out[col].dtype

            if is_type(Framework.PANDAS, "numeric", dtype):
                out[col] = out[col].fillna(self.numeric_fill)
            elif is_type(Framework.PANDAS, "boolean", dtype):
                out[col] = out[col].fillna(self.bool_fill)
            elif is_type(Framework.PANDAS, "datetime", dtype):
                fill_value = self.datetime_fill if self.datetime_fill is not None else pd.NaT
                out[col] = out[col].fillna(fill_value)
            else:
                out[col] = out[col].fillna(self.string_fill)
        return out

    # --------- Spark path ---------
    def _apply_spark(self, df):
        from pyspark.sql import functions as F

        for field in df.schema.fields:
            col = field.name
            if not self._is_in_scope(col):
                continue
            dt = field.dataType

            if is_type(Framework.SPARK, "numeric", dt):
                if self.fill_nan:
                    df = df.withColumn(
                        col,
                        F.when(F.isnan(F.col(col)) | F.col(col).isNull(),
                               F.lit(self.numeric_fill).cast(dt)
                        ).otherwise(F.col(col))
                    )
                else:
                    df = df.na.fill(value=self.numeric_fill, subset=[col])

            elif is_type(Framework.SPARK, "boolean", dt):
                df = df.na.fill(value=self.bool_fill, subset=[col])

            elif is_type(Framework.SPARK, "datetime", dt):
                if self.datetime_fill is not None:
                    df = df.withColumn(
                        col,
                        F.when(F.col(col).isNull(),
                            F.lit(str(self.datetime_fill)).cast(dt)  # safe cast
                        ).otherwise(F.col(col))
                    )

            elif is_type(Framework.SPARK, "string", dt):
                df = df.na.fill(value=self.string_fill, subset=[col])

        return df
