"""Example functions demonstrating the ODIBI function framework.

This module contains working examples showing different patterns for
registering and using reusable functions across Spark and Pandas engines.

Examples:
    1. Spark-specific function
    2. Pandas-specific function  
    3. Universal function (works with both)
    4. Function with context injection
    5. Function with auto-registration and metadata
"""

from typing import Any, Optional, List
from .decorators import (
    odibi_function,
    spark_function,
    pandas_function,
    universal_function,
    with_context,
)


@spark_function(
    module="cleaning",
    description="Remove duplicate rows from Spark DataFrame",
    author="odibi_team",
    version="1.0",
)
def deduplicate_spark(df: Any, subset: Optional[List[str]] = None) -> Any:
    """Remove duplicate rows from Spark DataFrame.
    
    This function demonstrates a Spark-specific implementation that uses
    PySpark's dropDuplicates method. It's automatically registered in the
    global registry under engine="spark".
    
    Args:
        df: PySpark DataFrame
        subset: Optional list of columns to consider for deduplication
    
    Returns:
        PySpark DataFrame with duplicates removed
    
    Examples:
        >>> from pyspark.sql import SparkSession
        >>> from odibi_de_v2.odibi_functions import REGISTRY
        >>> 
        >>> # Direct usage
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame([(1, "a"), (1, "a"), (2, "b")], ["id", "val"])
        >>> clean_df = deduplicate_spark(df)
        >>> clean_df.count()
        2
        >>> 
        >>> # Via registry
        >>> fn = REGISTRY.resolve(None, "deduplicate_spark", "spark")
        >>> result = fn(df, subset=["id"])
    """
    if subset:
        return df.dropDuplicates(subset)
    return df.dropDuplicates()


@pandas_function(
    module="cleaning",
    description="Remove duplicate rows from Pandas DataFrame",
    author="odibi_team",
    version="1.0",
)
def deduplicate_pandas(df: Any, subset: Optional[List[str]] = None) -> Any:
    """Remove duplicate rows from Pandas DataFrame.
    
    This function demonstrates a Pandas-specific implementation that uses
    pandas' drop_duplicates method. It's automatically registered in the
    global registry under engine="pandas".
    
    Args:
        df: Pandas DataFrame
        subset: Optional list of columns to consider for deduplication
    
    Returns:
        Pandas DataFrame with duplicates removed
    
    Examples:
        >>> import pandas as pd
        >>> from odibi_de_v2.odibi_functions import REGISTRY
        >>> 
        >>> # Direct usage
        >>> df = pd.DataFrame({'id': [1, 1, 2], 'val': ['a', 'a', 'b']})
        >>> clean_df = deduplicate_pandas(df)
        >>> len(clean_df)
        2
        >>> 
        >>> # Via registry
        >>> fn = REGISTRY.resolve(None, "deduplicate_pandas", "pandas")
        >>> result = fn(df, subset=["id"])
    """
    if subset:
        return df.drop_duplicates(subset=subset)
    return df.drop_duplicates()


@universal_function(
    module="inspection",
    description="Get column names from any DataFrame",
    author="odibi_team",
    version="1.0",
    tags=["utility", "metadata"],
)
def get_columns(df: Any) -> List[str]:
    """Get column names from Spark or Pandas DataFrame.
    
    This function demonstrates a universal implementation that works with
    both Spark and Pandas DataFrames. It's registered under engine="any"
    and serves as a fallback for both engines.
    
    Args:
        df: Spark or Pandas DataFrame
    
    Returns:
        List of column names
    
    Examples:
        >>> import pandas as pd
        >>> from odibi_de_v2.odibi_functions import REGISTRY
        >>> 
        >>> # Works with Pandas
        >>> pdf = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        >>> cols = get_columns(pdf)
        >>> cols
        ['a', 'b']
        >>> 
        >>> # Also works with Spark (via .columns attribute)
        >>> # spark_df = spark.createDataFrame([(1, 2)], ["a", "b"])
        >>> # cols = get_columns(spark_df)
        >>> 
        >>> # Via registry (resolves for any engine)
        >>> fn = REGISTRY.resolve(None, "get_columns", "pandas")
        >>> result = fn(pdf)
    """
    return list(df.columns)


@spark_function(
    module="ingestion",
    description="Load table using context's Spark session",
    author="odibi_team",
    version="1.0",
)
@with_context(context_param="context")
def load_table(
    table_name: str,
    database: Optional[str] = None,
    context: Optional[Any] = None
) -> Any:
    """Load table from catalog using execution context.
    
    This function demonstrates context injection. The @with_context decorator
    automatically injects the current ExecutionContext if available, providing
    access to the Spark session and other runtime resources.
    
    Args:
        table_name: Name of table to load
        database: Optional database name
        context: Execution context (auto-injected by @with_context)
    
    Returns:
        PySpark DataFrame
    
    Raises:
        ValueError: If context is not available or missing spark session
    
    Examples:
        >>> from pyspark.sql import SparkSession
        >>> from odibi_de_v2.odibi_functions import (
        ...     ExecutionContext,
        ...     set_current_context,
        ...     REGISTRY
        ... )
        >>> 
        >>> # Setup context
        >>> spark = SparkSession.builder.getOrCreate()
        >>> ctx = ExecutionContext(spark=spark, config={"env": "dev"})
        >>> set_current_context(ctx)
        >>> 
        >>> # Function automatically receives context
        >>> df = load_table("my_table", database="bronze")
        >>> 
        >>> # Via registry
        >>> fn = REGISTRY.resolve(None, "load_table", "spark")
        >>> result = fn("my_table")
        >>> 
        >>> # Can also pass context explicitly
        >>> df = load_table("my_table", context=ctx)
    """
    if context is None:
        raise ValueError("Execution context required. Set via set_current_context()")
    
    if not hasattr(context, 'spark'):
        raise ValueError("Context missing 'spark' attribute")
    
    full_table = f"{database}.{table_name}" if database else table_name
    return context.spark.table(full_table)


@odibi_function(
    engine="pandas",
    name="validate_schema",
    module="validation",
    description="Validate DataFrame schema against expected columns",
    author="odibi_team",
    version="1.0",
    tags=["validation", "quality"],
)
def validate_dataframe_schema(
    df: Any,
    required_columns: List[str],
    raise_on_error: bool = True
) -> bool:
    """Validate that DataFrame contains required columns.
    
    This function demonstrates auto-registration with custom name and metadata.
    It's registered as "validate_schema" (different from function name) and
    includes rich metadata for documentation and discovery.
    
    Args:
        df: Pandas DataFrame to validate
        required_columns: List of required column names
        raise_on_error: Whether to raise exception on validation failure
    
    Returns:
        True if valid, False if invalid (when raise_on_error=False)
    
    Raises:
        ValueError: If schema is invalid and raise_on_error=True
    
    Examples:
        >>> import pandas as pd
        >>> from odibi_de_v2.odibi_functions import REGISTRY
        >>> 
        >>> df = pd.DataFrame({'id': [1, 2], 'name': ['a', 'b'], 'value': [10, 20]})
        >>> 
        >>> # Direct usage - valid schema
        >>> is_valid = validate_dataframe_schema(df, ['id', 'name'], raise_on_error=False)
        >>> is_valid
        True
        >>> 
        >>> # Invalid schema (missing column)
        >>> is_valid = validate_dataframe_schema(
        ...     df,
        ...     ['id', 'missing_col'],
        ...     raise_on_error=False
        ... )
        >>> is_valid
        False
        >>> 
        >>> # Via registry (using custom name)
        >>> fn = REGISTRY.resolve(None, "validate_schema", "pandas")
        >>> fn is not None
        True
        >>> 
        >>> # Check metadata
        >>> meta = REGISTRY.get_metadata("validate_schema", "pandas")
        >>> 'validation' in meta.get('tags', [])
        True
    """
    actual_columns = set(df.columns)
    required_set = set(required_columns)
    missing_columns = required_set - actual_columns
    
    if missing_columns:
        error_msg = f"Missing required columns: {sorted(missing_columns)}"
        if raise_on_error:
            raise ValueError(error_msg)
        return False
    
    return True


@pandas_function(
    module="cleaning",
    description="Fill missing values with strategy",
    author="odibi_team",
    version="1.0",
)
def fill_nulls(
    df: Any,
    strategy: str = "mean",
    columns: Optional[List[str]] = None,
    fill_value: Optional[Any] = None
) -> Any:
    """Fill missing values using different strategies.
    
    Demonstrates a practical data cleaning function with multiple strategies.
    
    Args:
        df: Pandas DataFrame
        strategy: Fill strategy ("mean", "median", "mode", "zero", "value")
        columns: Optional list of columns to fill (default: all numeric)
        fill_value: Value to use when strategy="value"
    
    Returns:
        Pandas DataFrame with nulls filled
    
    Examples:
        >>> import pandas as pd
        >>> import numpy as np
        >>> 
        >>> df = pd.DataFrame({
        ...     'a': [1, 2, np.nan, 4],
        ...     'b': [10, np.nan, 30, 40],
        ...     'c': ['x', 'y', 'z', None]
        ... })
        >>> 
        >>> # Fill with mean (numeric only)
        >>> result = fill_nulls(df, strategy="mean")
        >>> result['a'].isnull().sum()
        0
        >>> 
        >>> # Fill specific columns with zero
        >>> result = fill_nulls(df, strategy="zero", columns=['a', 'b'])
        >>> 
        >>> # Fill with custom value
        >>> result = fill_nulls(df, strategy="value", fill_value=999)
    """
    result = df.copy()
    target_cols = columns if columns else result.select_dtypes(include=['number']).columns
    
    if strategy == "mean":
        for col in target_cols:
            result[col] = result[col].fillna(result[col].mean())
    elif strategy == "median":
        for col in target_cols:
            result[col] = result[col].fillna(result[col].median())
    elif strategy == "mode":
        for col in target_cols:
            mode_val = result[col].mode()
            if len(mode_val) > 0:
                result[col] = result[col].fillna(mode_val[0])
    elif strategy == "zero":
        for col in target_cols:
            result[col] = result[col].fillna(0)
    elif strategy == "value":
        if fill_value is None:
            raise ValueError("fill_value required when strategy='value'")
        for col in target_cols:
            result[col] = result[col].fillna(fill_value)
    else:
        raise ValueError(f"Unknown strategy: {strategy}")
    
    return result
