"""
Engine detection utilities for odibi_de_v2.

Provides a single entry point `detect_engine(df)` that safely detects
the execution engine for a given DataFrame-like object.

Currently supports:
  - Pandas
  - PySpark
"""

from typing import Any


def detect_engine(df: Any) -> str:
    """
    Detect Pandas vs Spark safely.

    Strategy:
      1. Try isinstance checks if libraries are present.
      2. Fall back to duck-typing + module-name heuristics (no hard dependency).

    Parameters
    ----------
    df : Any
        Object to inspect.

    Returns
    -------
    str
        "pandas" | "spark" | "unknown"
    """
    # ---- 1) Direct isinstance checks ----
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

    # ---- 2) Module / name heuristics ----
    mod = getattr(type(df), "__module__", "") or ""
    name = getattr(type(df), "__name__", "") or ""

    # Spark patterns
    if ("pyspark.sql" in mod) or ("pyspark.sql" in repr(type(df))):
        return "spark"

    has_sparkish = all(
        hasattr(df, attr) for attr in ("schema", "dtypes", "columns")
    ) and hasattr(getattr(df, "sql_ctx", None), "sparkSession")
    if has_sparkish:
        return "spark"

    # Pandas patterns
    if ("pandas.core.frame" in mod) or ("pandas" in mod and name == "DataFrame"):
        return "pandas"

    has_pandasish = all(
        hasattr(df, attr) for attr in ("values", "to_numpy", "columns")
    )
    if has_pandasish:
        return "pandas"

    return "unknown"
