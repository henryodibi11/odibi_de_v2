"""
Unified type utilities for odibi_de_v2.

Provides consistent checks for logical categories ("numeric", "string",
"boolean", "datetime") across supported frameworks.

- Pandas: delegates to pandas.api.types
- Spark:  checks pyspark.sql.types via isinstance

Relies on the Framework enum from odibi_de_v2.core.

Type Category Mapping
---------------------
Category   | Pandas (pandas.api.types)              | Spark (pyspark.sql.types)
-----------|---------------------------------------|----------------------------------------
numeric    | int64, float64, Int32/64, Float32/64  | ByteType, ShortType, IntegerType,
           | Decimal, nullable Int/Float dtypes    | LongType, FloatType, DoubleType,
           |                                       | DecimalType
string     | object, string[python], string[pyarrow]| StringType
boolean    | bool, boolean dtype                   | BooleanType
datetime   | datetime64[ns], datetime64[ns, tz]    | DateType, TimestampType
"""

from typing import Any
from odibi_de_v2.core import Framework

# ---------------------------
# Spark types
# ---------------------------
try:
    from pyspark.sql import types as T

    SPARK_TYPES = {
        "numeric": (
            T.ByteType, T.ShortType, T.IntegerType, T.LongType,
            T.FloatType, T.DoubleType, T.DecimalType,
        ),
        "string": (T.StringType,),
        "boolean": (T.BooleanType,),
        "datetime": (T.DateType, T.TimestampType),
    }
except Exception:
    SPARK_TYPES = {k: () for k in ("numeric", "string", "boolean", "datetime")}

# ---------------------------
# Pandas type checks
# ---------------------------
try:
    import pandas as pd
    from pandas.api import types as pdt

    def pandas_is_numeric(dtype) -> bool:
        return pdt.is_numeric_dtype(dtype)

    def pandas_is_string(dtype) -> bool:
        return pdt.is_string_dtype(dtype)

    def pandas_is_boolean(dtype) -> bool:
        return pdt.is_bool_dtype(dtype)

    def pandas_is_datetime(dtype) -> bool:
        return pdt.is_datetime64_any_dtype(dtype)

    PANDAS_TYPE_FUNCS = {
        "numeric": pandas_is_numeric,
        "string": pandas_is_string,
        "boolean": pandas_is_boolean,
        "datetime": pandas_is_datetime,
    }
except Exception:
    PANDAS_TYPE_FUNCS = {}

# ---------------------------
# API
# ---------------------------
def is_type(engine: Framework, category: str, dtype: Any) -> bool:
    """
    Check if a dtype belongs to a category for a given engine.

    Parameters
    ----------
    engine : Framework
        Framework.PANDAS | Framework.SPARK
    category : str
        "numeric" | "string" | "boolean" | "datetime"
    dtype : Any
        The dtype/DataType object to check.

    Returns
    -------
    bool
        True if dtype matches the category for the engine, else False.
    """
    category = category.lower()
    if engine == Framework.PANDAS:
        func = PANDAS_TYPE_FUNCS.get(category)
        return func(dtype) if func else False

    if engine == Framework.SPARK:
        candidates = SPARK_TYPES.get(category, ())
        return isinstance(dtype, candidates)

    return False
