from typing import Callable, Optional, Any
from functools import wraps
import inspect

from .registry import REGISTRY


def odibi_function(
    engine: str = "any",
    name: Optional[str] = None,
    module: Optional[str] = None,
    auto_register: bool = True,
    **metadata
) -> Callable:
    """Decorator to register a function in the global ODIBI function registry.
    
    This decorator allows functions to be automatically registered for use across
    the ODIBI framework. It supports engine-specific implementations (spark, pandas)
    and universal functions (any engine). The decorator is backward-compatible and
    follows a pass-through pattern.
    
    Args:
        engine: Target engine ("spark", "pandas", "any"). Default: "any"
        name: Function name in registry (defaults to function.__name__)
        module: Optional module/namespace for organization
        auto_register: Whether to auto-register on decoration (default: True)
        **metadata: Additional metadata (description, author, version, tags, etc.)
    
    Returns:
        Decorated function (unchanged, backward-compatible)
    
    Examples:
        >>> from odibi_de_v2.odibi_functions import odibi_function
        >>> 
        >>> # Universal function (works with any engine)
        >>> @odibi_function(engine="any", module="cleaning")
        ... def remove_duplicates(df):
        ...     '''Remove duplicate rows.'''
        ...     return df.drop_duplicates()
        >>> 
        >>> # Engine-specific function
        >>> @odibi_function(engine="spark", module="aggregation")
        ... def aggregate_daily(spark_df):
        ...     '''Aggregate data by day.'''
        ...     return spark_df.groupBy("date").count()
        >>> 
        >>> # With metadata
        >>> @odibi_function(
        ...     engine="pandas",
        ...     module="validation",
        ...     author="data_team",
        ...     version="1.0",
        ...     tags=["quality", "validation"]
        ... )
        ... def check_nulls(df):
        ...     '''Check for null values.'''
        ...     return df.isnull().sum()
        >>> 
        >>> # Function works normally (backward-compatible)
        >>> import pandas as pd
        >>> result = remove_duplicates(pd.DataFrame({'a': [1, 1, 2]}))
        >>> len(result)
        2
    """
    def decorator(fn: Callable) -> Callable:
        func_name = name or fn.__name__
        
        if auto_register:
            desc = metadata.pop('description', None) or fn.__doc__
            REGISTRY.register(
                name=func_name,
                engine=engine,
                fn=fn,
                module=module,
                description=desc,
                **metadata
            )
        
        fn._odibi_registered = True
        fn._odibi_engine = engine
        fn._odibi_name = func_name
        fn._odibi_module = module
        
        return fn
    
    return decorator


def with_context(context_param: str = "context") -> Callable:
    """Decorator to inject execution context into function calls.
    
    This optional decorator enables functions to receive runtime context
    (e.g., spark session, configuration, logger) without requiring explicit
    parameters. It inspects the function signature and only injects if the
    context parameter is present.
    
    Args:
        context_param: Name of context parameter in function signature
    
    Returns:
        Decorated function that accepts context injection
    
    Examples:
        >>> from odibi_de_v2.odibi_functions import with_context, odibi_function
        >>> 
        >>> # Function with optional context
        >>> @odibi_function(engine="spark")
        ... @with_context()
        ... def load_table(table_name, context=None):
        ...     '''Load table using context's spark session.'''
        ...     if context and hasattr(context, 'spark'):
        ...         return context.spark.table(table_name)
        ...     raise ValueError("Spark context required")
        >>> 
        >>> # Function can be called normally
        >>> # load_table("my_table", context=my_context)
        >>> 
        >>> # Context param can be customized
        >>> @with_context(context_param="ctx")
        ... def process_data(data, ctx=None):
        ...     '''Process data with context.'''
        ...     if ctx:
        ...         print(f"Using context: {ctx}")
        ...     return data
    """
    def decorator(fn: Callable) -> Callable:
        sig = inspect.signature(fn)
        has_context = context_param in sig.parameters
        
        if not has_context:
            return fn
        
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if context_param not in kwargs:
                from .context import get_current_context
                current_context = get_current_context()
                if current_context is not None:
                    kwargs[context_param] = current_context
            
            return fn(*args, **kwargs)
        
        wrapper._odibi_context_aware = True
        wrapper._odibi_context_param = context_param
        
        return wrapper
    
    return decorator


def spark_function(
    name: Optional[str] = None,
    module: Optional[str] = None,
    **metadata
) -> Callable:
    """Convenience decorator for Spark-specific functions.
    
    Shorthand for @odibi_function(engine="spark", ...).
    
    Args:
        name: Function name in registry (defaults to function.__name__)
        module: Optional module/namespace
        **metadata: Additional metadata
    
    Returns:
        Decorated function
    
    Examples:
        >>> from odibi_de_v2.odibi_functions import spark_function
        >>> 
        >>> @spark_function(module="transformations")
        ... def repartition_data(df, num_partitions=10):
        ...     '''Repartition Spark DataFrame.'''
        ...     return df.repartition(num_partitions)
    """
    return odibi_function(
        engine="spark",
        name=name,
        module=module,
        **metadata
    )


def pandas_function(
    name: Optional[str] = None,
    module: Optional[str] = None,
    **metadata
) -> Callable:
    """Convenience decorator for Pandas-specific functions.
    
    Shorthand for @odibi_function(engine="pandas", ...).
    
    Args:
        name: Function name in registry (defaults to function.__name__)
        module: Optional module/namespace
        **metadata: Additional metadata
    
    Returns:
        Decorated function
    
    Examples:
        >>> from odibi_de_v2.odibi_functions import pandas_function
        >>> 
        >>> @pandas_function(module="cleaning")
        ... def fill_missing(df, value=0):
        ...     '''Fill missing values with default.'''
        ...     return df.fillna(value)
    """
    return odibi_function(
        engine="pandas",
        name=name,
        module=module,
        **metadata
    )


def universal_function(
    name: Optional[str] = None,
    module: Optional[str] = None,
    **metadata
) -> Callable:
    """Convenience decorator for universal functions (any engine).
    
    Shorthand for @odibi_function(engine="any", ...).
    
    Args:
        name: Function name in registry (defaults to function.__name__)
        module: Optional module/namespace
        **metadata: Additional metadata
    
    Returns:
        Decorated function
    
    Examples:
        >>> from odibi_de_v2.odibi_functions import universal_function
        >>> 
        >>> @universal_function(module="utilities")
        ... def get_column_names(df):
        ...     '''Get column names from any DataFrame.'''
        ...     return list(df.columns)
    """
    return odibi_function(
        engine="any",
        name=name,
        module=module,
        **metadata
    )
