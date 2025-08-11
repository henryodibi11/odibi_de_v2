from typing import Any, Iterable, Optional
class NullFiller:
    """
    Simple, reusable null filler for Pandas & PySpark with a unified API.

    Rules:
      - Numeric  -> numeric_fill (default 0)
      - Boolean  -> bool_fill   (default False)
      - String   -> string_fill (default "")

    Options:
      - include: columns to process (others untouched)
      - exclude: columns to skip
      - engine:  "pandas" | "spark" | None  (None = auto-detect)
      - fill_nan: bool (default True). For Spark, also replaces NaN in float/double.
                  For Pandas, no effect (fillna covers NaN by default).
    """

    def __init__(
        self,
        numeric_fill: Any = 0,
        bool_fill: bool = False,
        string_fill: str = "",
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        engine: Optional[str] = None,
        fill_nan: bool = True,
    ):
        self.numeric_fill = numeric_fill
        self.bool_fill = bool_fill
        self.string_fill = string_fill
        self.include = set(include) if include else None
        self.exclude = set(exclude) if exclude else set()
        self.engine_override = engine  # optional manual override
        self.fill_nan = fill_nan

    def __repr__(self) -> str:
        return (
            f"NullFiller(numeric_fill={self.numeric_fill!r}, "
            f"bool_fill={self.bool_fill!r}, string_fill={self.string_fill!r}, "
            f"include={sorted(self.include) if self.include else None}, "
            f"exclude={sorted(self.exclude) if self.exclude else []}, "
            f"engine={self.engine_override!r}, fill_nan={self.fill_nan!r})"
        )

    # --------- Public API ---------
    def apply(self, df: Any) -> Any:
        """
        Apply null-filling rules to the given DataFrame (Pandas or Spark).

        Parameters
        ----------
        df : Any
            A pandas.DataFrame or pyspark.sql.DataFrame.

        Returns
        -------
        Any
            A DataFrame of the same type with null-like values filled.
        """
        engine = self.engine_override or self._detect_engine(df)
        if engine == "pandas":
            return self._apply_pandas(df)
        elif engine == "spark":
            return self._apply_spark(df)
        raise TypeError(f"Unsupported dataframe type: {type(df)}")

    # --------- Robust engine detection ---------
    @staticmethod
    def _detect_engine(df: Any) -> str:
        """
        Detects Pandas vs Spark safely:
        1) Try isinstance checks if libraries are present.
        2) Fall back to duck-typing + module-name heuristics (no hard dependency).
        """
        # 1) isinstance checks (safe even when both libs are installed)
        try:
            import pandas as pd  # type: ignore
            if isinstance(df, pd.DataFrame):
                return "pandas"
        except Exception:
            pass

        try:
            from pyspark.sql import DataFrame as SparkDF  # type: ignore
            if isinstance(df, SparkDF):
                return "spark"
        except Exception:
            pass

        # 2) Heuristics if direct imports aren't available or types are proxied
        mod = getattr(type(df), "__module__", "") or ""
        name = getattr(type(df), "__name__", "") or ""

        # Common Spark DF module patterns
        if ("pyspark.sql" in mod) or ("pyspark.sql" in repr(type(df))):
            return "spark"

        # Duck-typing: Spark DF commonly has these attributes
        has_sparkish = all(
            hasattr(df, attr) for attr in ("schema", "dtypes", "columns")
        ) and hasattr(getattr(df, "sql_ctx", None), "sparkSession")
        if has_sparkish:
            return "spark"

        # Common Pandas DF module patterns
        if ("pandas.core.frame" in mod) or ("pandas" in mod and name == "DataFrame"):
            return "pandas"

        # Duck-typing: Pandas DF has these
        if all(hasattr(df, attr) for attr in ("values", "to_numpy", "columns")):
            # Avoid misclassifying Spark: Spark doesn't have 'values' property
            return "pandas"

        return "unknown"

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
            s = out[col]
            if pd.api.types.is_numeric_dtype(s):
                out[col] = s.fillna(self.numeric_fill)
            elif pd.api.types.is_bool_dtype(s):
                out[col] = s.fillna(self.bool_fill)
            else:
                # object/string/categorical treated as string-like
                out[col] = s.fillna(self.string_fill)
        return out

    # --------- Spark path ---------
    def _apply_spark(self, df):
        from pyspark.sql import functions as F
        from pyspark.sql import types as T

        numeric_types = (T.ByteType, T.ShortType, T.IntegerType, T.LongType,
                         T.FloatType, T.DoubleType, T.DecimalType)

        # Pass 1: numeric columns (handle NULL + NaN where applicable)
        for field in df.schema.fields:
            col = field.name
            if not self._is_in_scope(col):
                continue
            dt = field.dataType
            if isinstance(dt, numeric_types):
                if isinstance(dt, (T.FloatType, T.DoubleType)):
                    if self.fill_nan:
                        # Replace NaN or NULL -> numeric_fill, preserving dtype
                        df = df.withColumn(
                            col,
                            F.when(F.isnan(F.col(col)) | F.col(col).isNull(),
                                   F.lit(self.numeric_fill).cast(dt)
                            ).otherwise(F.col(col))
                        )
                    else:
                        # Only replace NULLs, leave NaN as-is
                        df = df.na.fill(value=self.numeric_fill, subset=[col])
                elif isinstance(dt, T.DecimalType):
                    # DecimalType has no NaN; handle NULL and cast back to same precision/scale
                    df = df.withColumn(
                        col,
                        F.when(F.col(col).isNull(), F.lit(self.numeric_fill))
                         .otherwise(F.col(col))
                         .cast(dt)
                    )
                else:
                    # Integers: only NULL can appear
                    df = df.na.fill(value=self.numeric_fill, subset=[col])

        # Pass 2: strings & booleans (NULLs only)
        string_cols = [f.name for f in df.schema.fields
                       if self._is_in_scope(f.name) and isinstance(f.dataType, T.StringType)]
        bool_cols = [f.name for f in df.schema.fields
                     if self._is_in_scope(f.name) and isinstance(f.dataType, T.BooleanType)]

        if string_cols:
            df = df.na.fill(value=self.string_fill, subset=string_cols)
        if bool_cols:
            df = df.na.fill(value=self.bool_fill, subset=bool_cols)

        return df