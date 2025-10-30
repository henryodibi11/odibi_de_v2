"""ODIBI Reusable Function Framework.

This module provides a registry-based system for registering and resolving
reusable functions across different execution engines (Spark, Pandas).

Key Components:
    - FunctionRegistry: Singleton registry for managing functions
    - Decorators: @odibi_function, @spark_function, @pandas_function, @with_context
    - ExecutionContext: Runtime context container for dependency injection

Basic Usage:
    >>> from odibi_de_v2.odibi_functions import odibi_function, REGISTRY
    >>> 
    >>> # Define and auto-register a function
    >>> @odibi_function(engine="pandas", module="cleaning")
    ... def clean_nulls(df):
    ...     return df.dropna()
    >>> 
    >>> # Resolve and use function
    >>> fn = REGISTRY.resolve(None, "clean_nulls", "pandas")
    >>> result = fn(my_dataframe)

Advanced Usage:
    >>> from odibi_de_v2.odibi_functions import (
    ...     spark_function,
    ...     with_context,
    ...     ExecutionContext,
    ...     set_current_context
    ... )
    >>> 
    >>> # Spark function with context injection
    >>> @spark_function(module="ingestion")
    ... @with_context()
    ... def load_data(table_name, context=None):
    ...     if context:
    ...         return context.spark.table(table_name)
    ...     raise ValueError("Context required")
    >>> 
    >>> # Set context for execution
    >>> ctx = ExecutionContext(spark=spark_session, config=config)
    >>> set_current_context(ctx)
    >>> data = load_data("my_table")
"""

from .registry import FunctionRegistry, REGISTRY
from .decorators import (
    odibi_function,
    with_context,
    spark_function,
    pandas_function,
    universal_function,
)
from .context import (
    ExecutionContext,
    set_current_context,
    get_current_context,
    clear_context,
)


__all__ = [
    "FunctionRegistry",
    "REGISTRY",
    "odibi_function",
    "with_context",
    "spark_function",
    "pandas_function",
    "universal_function",
    "ExecutionContext",
    "set_current_context",
    "get_current_context",
    "clear_context",
]
