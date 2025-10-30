# ODIBI Reusable Function Framework

A registry-based system for creating, organizing, and reusing data processing functions across Spark and Pandas engines.

## Overview

The ODIBI Function Framework provides:

- **Global Registry**: Centralized function storage with singleton pattern
- **Engine-Specific Functions**: Separate implementations for Spark and Pandas
- **Universal Functions**: Single implementation that works with both engines
- **Context Injection**: Automatic dependency injection for runtime resources
- **Backward Compatibility**: Decorated functions work normally without registry

## Quick Start

### 1. Register a Function

```python
from odibi_de_v2.odibi_functions import pandas_function

@pandas_function(module="cleaning")
def remove_nulls(df):
    """Remove rows with any null values."""
    return df.dropna()
```

### 2. Use Directly (Backward Compatible)

```python
import pandas as pd

df = pd.DataFrame({'a': [1, None, 3], 'b': [4, 5, 6]})
clean_df = remove_nulls(df)
```

### 3. Resolve from Registry

```python
from odibi_de_v2.odibi_functions import REGISTRY

fn = REGISTRY.resolve(None, "remove_nulls", "pandas")
result = fn(my_dataframe)
```

## Core Components

### FunctionRegistry

Singleton registry managing all registered functions.

```python
from odibi_de_v2.odibi_functions import REGISTRY

# Register manually
REGISTRY.register("my_func", "spark", my_function)

# Resolve with fallback
fn = REGISTRY.resolve(None, "my_func", "spark")  # spark version
fn = REGISTRY.resolve(None, "my_func", "pandas")  # falls back to "any" if exists

# List all functions
all_funcs = REGISTRY.get_all()
spark_funcs = REGISTRY.get_all(engine="spark")

# Get metadata
meta = REGISTRY.get_metadata("my_func", "spark")
print(meta['description'], meta['author'])
```

### Decorators

#### @odibi_function

General-purpose decorator for any engine:

```python
from odibi_de_v2.odibi_functions import odibi_function

@odibi_function(engine="pandas", module="validation", version="1.0")
def check_schema(df, required_cols):
    """Validate DataFrame schema."""
    return all(col in df.columns for col in required_cols)
```

#### @spark_function, @pandas_function, @universal_function

Convenience decorators:

```python
from odibi_de_v2.odibi_functions import spark_function, pandas_function, universal_function

@spark_function(module="ingestion")
def load_delta(path):
    return spark.read.format("delta").load(path)

@pandas_function(module="cleaning")
def fill_missing(df):
    return df.fillna(0)

@universal_function(module="utilities")
def get_shape(df):
    return (df.count(), len(df.columns))  # works for both
```

#### @with_context

Inject execution context automatically:

```python
from odibi_de_v2.odibi_functions import spark_function, with_context

@spark_function(module="ingestion")
@with_context()
def load_table(table_name, context=None):
    """Load table using context's spark session."""
    if context and hasattr(context, 'spark'):
        return context.spark.table(table_name)
    raise ValueError("Spark context required")

# Set context once
from odibi_de_v2.odibi_functions import ExecutionContext, set_current_context

ctx = ExecutionContext(spark=spark_session, config=my_config)
set_current_context(ctx)

# Functions automatically receive context
df = load_table("my_table")
```

### ExecutionContext

Container for runtime resources:

```python
from odibi_de_v2.odibi_functions import (
    ExecutionContext,
    set_current_context,
    get_current_context,
    clear_context
)

# Create context
context = ExecutionContext(
    spark=spark_session,
    config=config_dict,
    logger=my_logger
)

# Set for current thread
set_current_context(context)

# Access in functions
current = get_current_context()
if current:
    df = current.spark.table("data")

# Clear when done
clear_context()
```

## Usage Patterns

### Pattern 1: Engine-Specific Implementations

```python
from odibi_de_v2.odibi_functions import spark_function, pandas_function

@spark_function(module="aggregation")
def daily_summary(df):
    return df.groupBy("date").agg({"value": "sum"})

@pandas_function(module="aggregation")
def daily_summary(df):
    return df.groupby("date")["value"].sum().reset_index()

# Resolve based on engine
engine = "spark"  # or "pandas"
fn = REGISTRY.resolve(None, "daily_summary", engine)
result = fn(data)
```

### Pattern 2: Universal with Engine-Specific Overrides

```python
from odibi_de_v2.odibi_functions import universal_function, spark_function

# Default implementation
@universal_function(module="utilities")
def row_count(df):
    return len(df)

# Optimized for Spark
@spark_function(name="row_count", module="utilities")
def row_count_spark(df):
    return df.count()

# Resolution picks engine-specific when available
spark_fn = REGISTRY.resolve(None, "row_count", "spark")  # uses spark version
pandas_fn = REGISTRY.resolve(None, "row_count", "pandas")  # uses universal
```

### Pattern 3: Context-Aware Functions

```python
from odibi_de_v2.odibi_functions import spark_function, with_context

@spark_function(module="ingestion")
@with_context()
def load_with_config(table_name, context=None):
    """Load table with context configuration."""
    if not context:
        raise ValueError("Context required")
    
    env = context.config.get("environment", "dev")
    database = f"{env}_bronze"
    
    return context.spark.table(f"{database}.{table_name}")
```

### Pattern 4: Rich Metadata

```python
from odibi_de_v2.odibi_functions import pandas_function

@pandas_function(
    module="quality",
    description="Comprehensive data quality checks",
    author="data_team",
    version="2.1",
    tags=["validation", "quality", "audit"],
    dependencies=["pandas>=1.0", "numpy"],
    tested=True
)
def quality_check(df):
    """Run quality checks on DataFrame."""
    # implementation
    pass

# Retrieve metadata
meta = REGISTRY.get_metadata("quality_check", "pandas")
print(f"Version: {meta['version']}, Tags: {meta['tags']}")
```

## Examples

See [examples.py](examples.py) for working examples:

1. **deduplicate_spark** - Spark-specific deduplication
2. **deduplicate_pandas** - Pandas-specific deduplication
3. **get_columns** - Universal column extraction
4. **load_table** - Context-aware table loading
5. **validate_dataframe_schema** - Schema validation with metadata
6. **fill_nulls** - Multi-strategy null filling

## Architecture

### Registry Resolution Order

1. **Exact match**: `(function_name, engine)`
2. **Universal fallback**: `(function_name, "any")`
3. **None**: Function not found

```python
# Registered: ("clean", "spark"), ("clean", "any")

REGISTRY.resolve(None, "clean", "spark")   # Returns spark version
REGISTRY.resolve(None, "clean", "pandas")  # Returns "any" version
REGISTRY.resolve(None, "clean", "custom")  # Returns "any" version
REGISTRY.resolve(None, "other", "spark")   # Returns None
```

### Thread Safety

- Registry uses locks for thread-safe registration
- ExecutionContext is thread-local (one context per thread)

### Backward Compatibility

Decorated functions work exactly as before:

```python
@pandas_function(module="cleaning")
def clean_data(df):
    return df.dropna()

# Still works normally
result = clean_data(my_df)

# Also available in registry
fn = REGISTRY.resolve(None, "clean_data", "pandas")
```

## Integration with ODIBI

### In Transformers

```python
from odibi_de_v2.odibi_functions import REGISTRY

class CustomTransformer:
    def __init__(self, engine="pandas"):
        self.engine = engine
    
    def transform(self, df):
        # Resolve engine-specific function
        clean_fn = REGISTRY.resolve(None, "clean_data", self.engine)
        if clean_fn:
            df = clean_fn(df)
        
        validate_fn = REGISTRY.resolve(None, "validate_schema", self.engine)
        if validate_fn:
            validate_fn(df, required_cols=["id", "value"])
        
        return df
```

### In Pipeline Configs

```python
# transformation_config.json
{
    "transformations": [
        {
            "type": "registered_function",
            "function": "deduplicate_spark",
            "engine": "spark",
            "params": {"subset": ["id"]}
        },
        {
            "type": "registered_function",
            "function": "validate_schema",
            "engine": "pandas",
            "params": {"required_columns": ["id", "name", "value"]}
        }
    ]
}
```

## Testing

```python
from odibi_de_v2.odibi_functions import REGISTRY, pandas_function

def test_function_registration():
    REGISTRY.clear()  # Clean slate
    
    @pandas_function(module="test")
    def test_fn(df):
        return df
    
    # Verify registration
    fn = REGISTRY.resolve(None, "test_fn", "pandas")
    assert fn is not None
    
    # Verify metadata
    meta = REGISTRY.get_metadata("test_fn", "pandas")
    assert meta['module'] == "test"
    
    REGISTRY.clear()  # Cleanup
```

## Best Practices

1. **Use descriptive names**: `remove_duplicates` not `dedup`
2. **Add docstrings**: Include Args, Returns, Examples
3. **Specify engine correctly**: "spark", "pandas", or "any"
4. **Include metadata**: module, version, author for discoverability
5. **Write engine-agnostic when possible**: Use universal functions
6. **Test both paths**: Direct calls and registry resolution
7. **Clean up in tests**: Use `REGISTRY.clear()` in test fixtures

## Migration from Custom Functions

Before:
```python
def my_custom_transform(df):
    return df.dropna().drop_duplicates()
```

After:
```python
from odibi_de_v2.odibi_functions import pandas_function

@pandas_function(module="cleaning", version="1.0")
def my_custom_transform(df):
    """Clean DataFrame by removing nulls and duplicates."""
    return df.dropna().drop_duplicates()
```

Function still works the same but is now:
- Discoverable in registry
- Documented with metadata
- Reusable across projects
- Engine-aware
