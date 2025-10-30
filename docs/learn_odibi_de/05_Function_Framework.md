# Function Framework - Building Reusable Transformations

## Why Reusable Functions?

Picture this: You've written a great function to deduplicate records in your Energy Efficiency project. Now your Sales Analytics project needs the same logic. Do you copy-paste the code?

**No!** The ODIBI Function Framework lets you:
- ✅ Write once, use everywhere
- ✅ Switch between Spark and Pandas seamlessly
- ✅ Share functions across teams and projects
- ✅ Version and document your logic
- ✅ Test transformations independently

Think of it as a **library of data operations** that your pipelines can call by name.

---

## The Registry Concept

The `odibi_functions` registry is a **central catalog** of all your transformation functions. It's like a phone book where:
- **Keys** = Function names (e.g., "deduplicate_records")
- **Values** = The actual Python functions
- **Tags** = Metadata (engine, module, version, etc.)

```python
from odibi_de_v2.odibi_functions import REGISTRY

# The registry knows about all registered functions
print(REGISTRY.list_all())
# Output: [('deduplicate_spark', 'spark'), ('deduplicate_pandas', 'pandas'), ...]
```

---

## Creating Your First Function (Step-by-Step)

### Step 1: Write the Function

Start with plain Python. Here's a simple deduplication function:

```python
def deduplicate_records(df, subset=None):
    """Remove duplicate rows."""
    if subset:
        return df.drop_duplicates(subset=subset)
    return df.drop_duplicates()
```

### Step 2: Add the @odibi_function Decorator

The decorator **registers** your function automatically when the module loads:

```python
from odibi_de_v2.odibi_functions import pandas_function

@pandas_function(
    module="cleaning",
    description="Remove duplicate rows from Pandas DataFrame",
    version="1.0"
)
def deduplicate_records(df, subset=None):
    """Remove duplicate rows."""
    if subset:
        return df.drop_duplicates(subset=subset)
    return df.drop_duplicates()
```

**What just happened?**
- ✅ Function registered as `deduplicate_records` in the global registry
- ✅ Tagged as `engine="pandas"`
- ✅ Added to `module="cleaning"` category
- ✅ Versioned as `1.0`

### Step 3: Test It

```python
import pandas as pd
from odibi_de_v2.odibi_functions import REGISTRY

# Create test data
df = pd.DataFrame({
    'id': [1, 1, 2, 3],
    'value': ['a', 'a', 'b', 'c']
})

# Method 1: Call directly
clean_df = deduplicate_records(df)
assert len(clean_df) == 3  # ✓ Duplicates removed

# Method 2: Resolve from registry
fn = REGISTRY.resolve(None, "deduplicate_records", "pandas")
clean_df = fn(df, subset=['id'])
assert len(clean_df) == 3  # ✓ Works via registry too!
```

### Step 4: Use in a Transformation Pipeline

Now use it in your config-driven transformation:

```python
# In TransformationConfig table
{
    "function_name": "deduplicate_records",
    "function_params": {"subset": ["customer_id", "transaction_date"]},
    "output_table": "silver.unique_transactions"
}
```

The `TransformationRunner` will:
1. Look up `deduplicate_records` in the registry
2. Find the Pandas version (if using Pandas engine)
3. Call it with your parameters
4. Save the result to `silver.unique_transactions`

---

## Complete Example 1: Universal Deduplication

This function works with **both Spark and Pandas** because they share the same API:

```python
from odibi_de_v2.odibi_functions import universal_function

@universal_function(
    module="cleaning",
    description="Remove duplicate rows from any DataFrame",
    author="data_engineering_team",
    version="1.0",
    tags=["cleaning", "universal"]
)
def deduplicate_records(df, subset=None):
    """
    Remove duplicate rows from Spark or Pandas DataFrame.
    
    Args:
        df: Spark or Pandas DataFrame
        subset: Optional list of columns to consider for deduplication
    
    Returns:
        DataFrame with duplicates removed
    
    Example:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'id': [1, 1, 2], 'val': ['a', 'a', 'b']})
        >>> clean_df = deduplicate_records(df)
        >>> len(clean_df)
        2
    """
    if subset:
        return df.drop_duplicates(subset=subset)
    return df.drop_duplicates()

# ✅ Works with Pandas
import pandas as pd
pdf = pd.DataFrame({'id': [1, 1, 2], 'val': ['a', 'a', 'b']})
result = deduplicate_records(pdf)

# ✅ Works with Spark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sdf = spark.createDataFrame([(1, 'a'), (1, 'a'), (2, 'b')], ['id', 'val'])
result = deduplicate_records(sdf)
```

**When to use `@universal_function`:**
- Both engines support the same method (e.g., `.drop_duplicates()`, `.columns`)
- Logic doesn't depend on engine-specific features
- You want maximum code reuse

---

## Complete Example 2: Spark-Specific Energy Metrics

This function uses Spark SQL expressions for complex calculations:

```python
from odibi_de_v2.odibi_functions import spark_function
from pyspark.sql import functions as F

@spark_function(
    module="energy_efficiency",
    description="Calculate OEE and energy metrics",
    author="energy_team",
    version="2.1",
    tags=["energy", "aggregation", "OEE"]
)
def calculate_energy_metrics(df, group_by_cols=None):
    """
    Calculate energy efficiency metrics using Spark.
    
    Computes:
    - Total energy consumption
    - Average power demand
    - OEE (Overall Equipment Effectiveness)
    - Peak demand periods
    
    Args:
        df: Spark DataFrame with columns:
            - timestamp
            - energy_kwh
            - runtime_hours
            - production_count
        group_by_cols: Optional list of columns to group by (e.g., ['facility', 'equipment'])
    
    Returns:
        Spark DataFrame with aggregated metrics
    
    Example:
        >>> df = spark.read.table("bronze.energy_raw")
        >>> metrics = calculate_energy_metrics(df, group_by_cols=['facility'])
        >>> metrics.show()
    """
    # Default grouping
    if group_by_cols is None:
        group_by_cols = ['facility_id', 'equipment_id']
    
    # Calculate metrics using Spark SQL functions
    result = df.groupBy(group_by_cols).agg(
        F.sum('energy_kwh').alias('total_energy_kwh'),
        F.avg('power_kw').alias('avg_power_kw'),
        F.max('power_kw').alias('peak_power_kw'),
        F.sum('runtime_hours').alias('total_runtime_hours'),
        F.sum('production_count').alias('total_production'),
        
        # Calculate OEE = (Actual Production / Theoretical Max Production) * 100
        (F.sum('production_count') / (F.sum('runtime_hours') * F.lit(100))).alias('oee_pct'),
        
        # Energy per unit = Total Energy / Total Production
        (F.sum('energy_kwh') / F.sum('production_count')).alias('energy_per_unit'),
        
        F.count('*').alias('record_count')
    )
    
    # Add calculation timestamp
    result = result.withColumn('calculated_at', F.current_timestamp())
    
    return result

# Usage in config:
# {
#     "function_name": "calculate_energy_metrics",
#     "function_params": {"group_by_cols": ["facility_id", "shift", "date"]},
#     "output_table": "gold.energy_metrics_by_shift"
# }
```

**When to use `@spark_function`:**
- Using Spark-specific features (window functions, broadcast joins, etc.)
- Need distributed computing for large datasets
- Require Spark SQL optimizations

---

## Complete Example 3: Pandas Sales Aggregation

This function leverages Pandas' rich API for time-series analysis:

```python
from odibi_de_v2.odibi_functions import pandas_function
import pandas as pd
import numpy as np

@pandas_function(
    module="sales_analytics",
    description="Aggregate sales data with time-based features",
    author="sales_team",
    version="1.3",
    tags=["sales", "aggregation", "time-series"]
)
def aggregate_sales(df, freq='D', group_by_cols=None):
    """
    Aggregate sales data with rolling averages and growth metrics.
    
    Args:
        df: Pandas DataFrame with columns:
            - timestamp (datetime)
            - amount (float)
            - quantity (int)
            - customer_id (str)
        freq: Aggregation frequency ('D'=daily, 'W'=weekly, 'M'=monthly)
        group_by_cols: Optional list of columns to group by before aggregation
    
    Returns:
        Pandas DataFrame with aggregated sales metrics
    
    Example:
        >>> df = pd.read_csv('sales_raw.csv', parse_dates=['timestamp'])
        >>> daily_sales = aggregate_sales(df, freq='D')
        >>> monthly_sales = aggregate_sales(df, freq='M', group_by_cols=['region'])
    """
    # Ensure timestamp is datetime
    df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Set timestamp as index for resampling
    df = df.set_index('timestamp')
    
    # Define aggregation rules
    agg_dict = {
        'amount': ['sum', 'mean', 'count'],
        'quantity': ['sum', 'mean'],
        'customer_id': 'nunique'
    }
    
    # Perform aggregation
    if group_by_cols:
        result = df.groupby(group_by_cols).resample(freq).agg(agg_dict)
        result = result.reset_index()
    else:
        result = df.resample(freq).agg(agg_dict)
        result = result.reset_index()
    
    # Flatten multi-level columns
    result.columns = ['_'.join(col).strip('_') if isinstance(col, tuple) else col 
                      for col in result.columns]
    
    # Add rolling 7-period average (7 days/weeks/months depending on freq)
    result['amount_sum_rolling_7'] = result['amount_sum'].rolling(window=7, min_periods=1).mean()
    
    # Calculate period-over-period growth
    result['amount_growth_pct'] = result['amount_sum'].pct_change() * 100
    
    # Add flags
    result['is_high_volume'] = result['quantity_sum'] > result['quantity_sum'].quantile(0.75)
    
    return result

# Usage in config:
# {
#     "function_name": "aggregate_sales",
#     "function_params": {"freq": "W", "group_by_cols": ["region", "product_category"]},
#     "output_table": "gold.weekly_sales_by_region"
# }
```

**When to use `@pandas_function`:**
- Working with smaller datasets that fit in memory
- Need Pandas-specific features (time resampling, rolling windows, etc.)
- Rapid prototyping and data exploration

---

## Function Resolution and Fallback

The registry uses a **smart resolution strategy**:

```
1. Try exact engine match first (e.g., "pandas" → pandas version)
2. Fall back to universal ("any") version if available
3. Raise error if no match found
```

**Example:**

```python
# You have two versions registered:
@spark_function(module="cleaning")
def deduplicate_spark(df, subset=None):
    return df.dropDuplicates(subset) if subset else df.dropDuplicates()

@pandas_function(module="cleaning")
def deduplicate_pandas(df, subset=None):
    return df.drop_duplicates(subset=subset) if subset else df.drop_duplicates()

# Resolution behavior:
fn_spark = REGISTRY.resolve(None, "deduplicate_spark", engine="spark")
# ✓ Returns the Spark version

fn_pandas = REGISTRY.resolve(None, "deduplicate_pandas", engine="pandas")
# ✓ Returns the Pandas version

fn_universal = REGISTRY.resolve(None, "deduplicate_spark", engine="pandas")
# ✗ Raises error - no Pandas version of "deduplicate_spark"

# But if you have a universal version:
@universal_function(module="cleaning")
def get_columns(df):
    return list(df.columns)

fn = REGISTRY.resolve(None, "get_columns", engine="spark")
# ✓ Returns universal version (works for Spark)

fn = REGISTRY.resolve(None, "get_columns", engine="pandas")
# ✓ Returns universal version (works for Pandas too)
```

---

## Listing All Registered Functions

### List All Functions

```python
from odibi_de_v2.odibi_functions import REGISTRY

# Get all registered functions
all_functions = REGISTRY.list_all()
print(all_functions)
# [('deduplicate_spark', 'spark'), ('deduplicate_pandas', 'pandas'), ...]
```

### Filter by Engine

```python
# Get only Spark functions
spark_functions = REGISTRY.list_by_engine('spark')
print(f"Spark functions: {spark_functions}")

# Get only Pandas functions
pandas_functions = REGISTRY.list_by_engine('pandas')
print(f"Pandas functions: {pandas_functions}")

# Get universal functions
universal_functions = REGISTRY.list_by_engine('any')
print(f"Universal functions: {universal_functions}")
```

### Get Function Metadata

```python
# View metadata for a specific function
metadata = REGISTRY.get_metadata('calculate_energy_metrics', engine='spark')
print(f"Module: {metadata['module']}")
print(f"Description: {metadata['description']}")
print(f"Author: {metadata['author']}")
print(f"Version: {metadata['version']}")
print(f"Tags: {metadata['tags']}")
```

### Inspect Available Functions

```python
# Pretty-print all available functions
def print_registry_summary():
    """Display all registered functions in a readable format."""
    all_funcs = REGISTRY.list_all()
    
    print("\n" + "="*80)
    print("ODIBI FUNCTION REGISTRY")
    print("="*80)
    
    by_engine = {}
    for name, engine in all_funcs:
        if engine not in by_engine:
            by_engine[engine] = []
        by_engine[engine].append(name)
    
    for engine in sorted(by_engine.keys()):
        print(f"\n{engine.upper()} Functions ({len(by_engine[engine])}):")
        print("-" * 80)
        for name in sorted(by_engine[engine]):
            meta = REGISTRY.get_metadata(name, engine)
            desc = meta.get('description', 'No description')
            module = meta.get('module', 'unknown')
            print(f"  • {name:<30} [{module:<15}] {desc}")
    
    print("\n" + "="*80)

print_registry_summary()
```

---

## Best Practices

### ✅ DO

1. **Use descriptive names:**
   ```python
   # Good
   @pandas_function(module="validation")
   def validate_customer_schema(df, required_cols):
       pass
   
   # Bad
   @pandas_function(module="misc")
   def check(df, cols):
       pass
   ```

2. **Add docstrings with examples:**
   ```python
   @spark_function(module="cleaning")
   def remove_outliers(df, column, n_std=3):
       """
       Remove outliers beyond N standard deviations.
       
       Args:
           df: Spark DataFrame
           column: Column name to check for outliers
           n_std: Number of standard deviations (default: 3)
       
       Returns:
           Spark DataFrame with outliers removed
       
       Example:
           >>> df = spark.read.table("raw_data")
           >>> clean_df = remove_outliers(df, column='temperature', n_std=2.5)
       """
       pass
   ```

3. **Version your functions:**
   ```python
   @pandas_function(module="sales", version="2.0")  # ✓ Good
   def calculate_revenue(df):
       pass
   ```

4. **Use meaningful metadata:**
   ```python
   @spark_function(
       module="energy_efficiency",
       description="Calculate OEE metrics with downtime analysis",
       author="energy_analytics_team",
       version="3.1",
       tags=["OEE", "downtime", "production"]
   )
   def analyze_oee(df):
       pass
   ```

5. **Write engine-specific versions when needed:**
   ```python
   # Different implementations for different engines
   @spark_function(module="ml")
   def predict_churn(df, model_path):
       # Use MLlib or Spark ML
       pass
   
   @pandas_function(module="ml")
   def predict_churn(df, model_path):
       # Use scikit-learn
       pass
   ```

### ❌ DON'T

1. **Don't use generic names:**
   ```python
   @pandas_function(module="utils")
   def process(df):  # ❌ Too vague
       pass
   ```

2. **Don't mix concerns:**
   ```python
   @spark_function(module="everything")
   def do_it_all(df):  # ❌ Function should do one thing well
       # reads, transforms, validates, saves, sends email...
       pass
   ```

3. **Don't skip error handling:**
   ```python
   @pandas_function(module="math")
   def divide_columns(df, num_col, denom_col):
       # ❌ No check for division by zero
       df['result'] = df[num_col] / df[denom_col]
       return df
   ```

4. **Don't hardcode values:**
   ```python
   @spark_function(module="filtering")
   def filter_records(df):
       # ❌ Hardcoded date
       return df.filter(F.col('date') > '2023-01-01')
   
   # ✓ Better: accept parameters
   def filter_records(df, start_date):
       return df.filter(F.col('date') > start_date)
   ```

5. **Don't mutate input DataFrames (for Pandas):**
   ```python
   @pandas_function(module="cleaning")
   def clean_data(df):
       # ❌ Mutates original DataFrame
       df['cleaned'] = df['value'].str.upper()
       return df
   
   # ✓ Better: work on a copy
   def clean_data(df):
       result = df.copy()
       result['cleaned'] = result['value'].str.upper()
       return result
   ```

---

## Common Mistakes

### Mistake 1: Forgetting to Import the Decorator

```python
# ❌ Won't work - decorator not imported
def my_function(df):
    return df

# ✓ Correct
from odibi_de_v2.odibi_functions import pandas_function

@pandas_function(module="my_module")
def my_function(df):
    return df
```

### Mistake 2: Wrong Engine Decorator

```python
# ❌ Using Spark methods with @pandas_function
@pandas_function(module="processing")
def process_data(df):
    return df.select('col1', 'col2')  # .select() is Spark syntax!

# ✓ Correct
@spark_function(module="processing")
def process_data(df):
    return df.select('col1', 'col2')
```

### Mistake 3: Name Collisions

```python
# ❌ Two functions with same name and engine
@pandas_function(module="cleaning")
def clean_data(df):
    return df.dropna()

@pandas_function(module="validation")
def clean_data(df):  # ❌ Overwrites the first one!
    return df.drop_duplicates()

# ✓ Use different names or explicit name parameter
@pandas_function(module="cleaning", name="clean_data_dropna")
def clean_data_v1(df):
    return df.dropna()

@pandas_function(module="validation", name="clean_data_dedup")
def clean_data_v2(df):
    return df.drop_duplicates()
```

---

## Pro Tips

### Tip 1: Context Injection for Shared Resources

Use `@with_context` to access Spark sessions, configs, or connections:

```python
from odibi_de_v2.odibi_functions import spark_function, with_context

@spark_function(module="ingestion")
@with_context(context_param="context")
def load_from_catalog(table_name, context=None):
    """Load table using context's Spark session."""
    if context is None:
        raise ValueError("Context required")
    return context.spark.table(table_name)
```

### Tip 2: Combine Functions in Pipelines

```python
# In TransformationConfig:
[
    {"function_name": "deduplicate_records", "function_params": {"subset": ["id"]}},
    {"function_name": "validate_schema", "function_params": {"required_cols": ["id", "timestamp"]}},
    {"function_name": "calculate_energy_metrics", "function_params": {"group_by_cols": ["facility"]}}
]
```

### Tip 3: Dynamic Function Loading

```python
# Load functions from custom modules at runtime
import my_custom_functions  # Functions auto-register when module imports

# Now use them
fn = REGISTRY.resolve(None, "my_custom_transform", "pandas")
```

### Tip 4: Test Functions Independently

```python
# Write unit tests for each function
import pytest
import pandas as pd
from odibi_de_v2.odibi_functions import REGISTRY

def test_deduplicate_records():
    # Arrange
    df = pd.DataFrame({'id': [1, 1, 2], 'val': ['a', 'a', 'b']})
    fn = REGISTRY.resolve(None, "deduplicate_records", "pandas")
    
    # Act
    result = fn(df)
    
    # Assert
    assert len(result) == 2
    assert list(result['id']) == [1, 2]
```

---

## Mini-Challenge Exercises

### Exercise 1: Basic Function (Easy)

Create a function that converts all string columns to uppercase:

```python
# Your code here
@pandas_function(module="cleaning")
def uppercase_strings(df):
    # TODO: Implement
    pass

# Test it
df = pd.DataFrame({'name': ['alice', 'bob'], 'city': ['nyc', 'la']})
result = uppercase_strings(df)
assert result['name'][0] == 'ALICE'
```

<details>
<summary>Solution</summary>

```python
from odibi_de_v2.odibi_functions import pandas_function

@pandas_function(
    module="cleaning",
    description="Convert all string columns to uppercase"
)
def uppercase_strings(df):
    result = df.copy()
    for col in result.select_dtypes(include=['object']).columns:
        result[col] = result[col].str.upper()
    return result
```
</details>

### Exercise 2: Spark Aggregation (Medium)

Create a Spark function that calculates daily summary statistics:

```python
# Your code here
@spark_function(module="aggregation")
def daily_summary(df, date_col='date', value_col='amount'):
    # TODO: Calculate min, max, avg, count per day
    pass

# Should return columns: date, min_amount, max_amount, avg_amount, count
```

<details>
<summary>Solution</summary>

```python
from odibi_de_v2.odibi_functions import spark_function
from pyspark.sql import functions as F

@spark_function(
    module="aggregation",
    description="Calculate daily summary statistics"
)
def daily_summary(df, date_col='date', value_col='amount'):
    return df.groupBy(date_col).agg(
        F.min(value_col).alias('min_amount'),
        F.max(value_col).alias('max_amount'),
        F.avg(value_col).alias('avg_amount'),
        F.count(value_col).alias('count')
    )
```
</details>

### Exercise 3: Universal Validator (Hard)

Create a universal function that validates data quality rules:

```python
# Your code here
@universal_function(module="validation")
def validate_quality_rules(df, rules):
    """
    rules = {
        'id': {'not_null': True, 'unique': True},
        'amount': {'min': 0, 'max': 10000}
    }
    """
    # TODO: Check all rules, return dict of validation results
    pass
```

<details>
<summary>Solution</summary>

```python
from odibi_de_v2.odibi_functions import universal_function

@universal_function(
    module="validation",
    description="Validate data quality rules"
)
def validate_quality_rules(df, rules):
    results = {}
    
    for col, checks in rules.items():
        results[col] = {}
        
        if checks.get('not_null'):
            null_count = df[df[col].isna()].count() if hasattr(df, 'isna') else df.filter(df[col].isNull()).count()
            results[col]['has_nulls'] = null_count > 0
        
        if checks.get('unique'):
            total = df.count() if hasattr(df, 'count') and callable(df.count) else len(df)
            distinct = df.select(col).distinct().count() if hasattr(df, 'select') else df[col].nunique()
            results[col]['is_unique'] = total == distinct
        
        if 'min' in checks:
            min_val = df.agg({col: 'min'}).collect()[0][0] if hasattr(df, 'agg') else df[col].min()
            results[col]['meets_min'] = min_val >= checks['min']
        
        if 'max' in checks:
            max_val = df.agg({col: 'max'}).collect()[0][0] if hasattr(df, 'agg') else df[col].max()
            results[col]['meets_max'] = max_val <= checks['max']
    
    return results
```
</details>

---

## Next Steps

Now that you've mastered the Function Framework, explore:

1. **[06_Hooks_and_Events.md](06_Hooks_and_Events.md)** - Add observability to your functions
2. **[Tutorial 02 - Function Registry](../tutorials/02-function-registry-tutorial.ipynb)** - Interactive notebook examples
3. **[odibi_functions/examples.py](../../odibi_de_v2/odibi_functions/examples.py)** - More production examples

**Happy coding! 🚀**
