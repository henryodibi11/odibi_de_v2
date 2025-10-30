# ODIBI Functions - Quick Start Guide

## Installation
```python
from odibi_de_v2.odibi_functions import (
    REGISTRY,
    pandas_function,
    spark_function,
    universal_function,
)
```

## 1-Minute Tutorial

### Create a Function
```python
@pandas_function(module="cleaning")
def remove_nulls(df):
    """Remove rows with null values."""
    return df.dropna()
```

### Use It Normally
```python
clean_df = remove_nulls(my_dataframe)
```

### Or Via Registry
```python
fn = REGISTRY.resolve(None, "remove_nulls", "pandas")
result = fn(my_dataframe)
```

## Common Patterns

### Pattern 1: Pandas Function
```python
@pandas_function(module="cleaning", version="1.0")
def clean_data(df):
    return df.dropna().drop_duplicates()
```

### Pattern 2: Spark Function
```python
@spark_function(module="ingestion")
def load_delta(path):
    return spark.read.format("delta").load(path)
```

### Pattern 3: Universal (Both Engines)
```python
@universal_function(module="utils")
def get_columns(df):
    return list(df.columns)
```

### Pattern 4: Engine-Specific Override
```python
# Default for all engines
@universal_function(name="row_count")
def count_any(df):
    return len(df)

# Optimized for Spark
@spark_function(name="row_count")
def count_spark(df):
    return df.count()
```

### Pattern 5: With Context
```python
from odibi_de_v2.odibi_functions import with_context, ExecutionContext, set_current_context

@spark_function(module="ingestion")
@with_context()
def load_table(table_name, context=None):
    return context.spark.table(table_name)

# Setup context once
ctx = ExecutionContext(spark=spark_session)
set_current_context(ctx)

# Use anywhere
df = load_table("my_table")
```

## Registry Operations

### List Functions
```python
# All functions
all_funcs = REGISTRY.get_all()

# By engine
pandas_funcs = REGISTRY.get_all(engine="pandas")
spark_funcs = REGISTRY.get_all(engine="spark")
universal = REGISTRY.get_all(engine="any")
```

### Get Metadata
```python
meta = REGISTRY.get_metadata("my_function", "pandas")
print(meta['module'], meta['version'], meta['author'])
```

### Manual Registration
```python
def my_func(df):
    return df

REGISTRY.register(
    name="my_func",
    engine="pandas",
    fn=my_func,
    module="custom",
    version="1.0"
)
```

## Metadata Examples

### Minimal
```python
@pandas_function(module="cleaning")
def clean(df):
    return df.dropna()
```

### Rich Metadata
```python
@pandas_function(
    module="quality",
    description="Comprehensive data quality checks",
    author="data_team",
    version="2.1",
    tags=["validation", "quality"],
    dependencies=["pandas>=1.0"],
)
def quality_check(df):
    # implementation
    pass
```

## Tips

1. **Module Organization**: Use module to group related functions
   - `module="cleaning"` - data cleaning functions
   - `module="validation"` - data quality checks
   - `module="ingestion"` - data loading functions

2. **Engine Selection**:
   - Use `"any"` for functions that work with both engines
   - Use `"spark"` or `"pandas"` for engine-specific optimizations

3. **Naming**:
   - Use descriptive names: `remove_duplicates` not `dedup`
   - Use `name=` parameter to register under different name

4. **Backward Compatibility**:
   - Decorated functions work normally
   - No code changes needed for existing functions

5. **Testing**:
   - Use `REGISTRY.clear()` in test setup
   - Test both direct calls and registry resolution

## Examples in Action

See `odibi_de_v2/odibi_functions/examples.py` for:
- `deduplicate_spark` / `deduplicate_pandas`
- `get_columns` (universal)
- `load_table` (with context)
- `validate_dataframe_schema`
- `fill_nulls` (multi-strategy)

## Full Documentation

- **README.md** - Comprehensive guide
- **examples.py** - Working examples
- **IMPLEMENTATION_SUMMARY.md** - Technical details
