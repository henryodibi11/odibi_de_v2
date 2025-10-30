# 09 - Advanced Concepts

**Level:** Advanced  
**Duration:** 90-120 minutes  
**Prerequisites:** Completed tutorials 01-04, exercises 1-2

---

## Table of Contents
1. [Transformation Contracts and Composability](#transformation-contracts-and-composability)
2. [TransformationTracker Deep Dive](#transformationtracker-deep-dive)
3. [Event-Driven Patterns](#event-driven-patterns)
4. [Testing Strategies](#testing-strategies)
5. [Performance Optimization](#performance-optimization)
6. [Safe Refactoring Practices](#safe-refactoring-practices)
7. [Future Extensibility](#future-extensibility)

---

## Transformation Contracts and Composability

### What is a Transformation Contract?

A **transformation contract** defines the expected inputs, outputs, and behavior of a transformation function. It ensures functions can be safely composed and chained.

**Contract Components:**
1. **Input Schema**: Expected DataFrame columns and types
2. **Output Schema**: Guaranteed DataFrame columns and types
3. **Side Effects**: External state changes (files, tables, APIs)
4. **Preconditions**: Requirements that must be true before execution
5. **Postconditions**: Guarantees that will be true after execution

### Example Contract

```python
from typing import Protocol, Optional
import pandas as pd
from dataclasses import dataclass

@dataclass
class TransformationContract:
    """Define transformation contract"""
    input_schema: dict  # {'col_name': 'col_type'}
    output_schema: dict
    required_columns: list
    adds_columns: list
    removes_columns: list
    side_effects: list  # ['writes_table', 'calls_api']
    idempotent: bool = True  # Can run multiple times safely

def validate_contract(contract: TransformationContract):
    """Decorator to enforce transformation contract"""
    def decorator(func):
        def wrapper(df: pd.DataFrame, **kwargs):
            # Precondition: Validate input schema
            missing_cols = set(contract.required_columns) - set(df.columns)
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            # Execute transformation
            result = func(df, **kwargs)
            
            # Postcondition: Validate output schema
            for col in contract.adds_columns:
                if col not in result.columns:
                    raise ValueError(f"Contract violation: {col} not added")
            
            print(f"✓ Contract validated for {func.__name__}")
            return result
        return wrapper
    return decorator

# Usage
contract_celsius_to_fahrenheit = TransformationContract(
    input_schema={'temperature_celsius': 'float'},
    output_schema={'temperature_celsius': 'float', 'temperature_fahrenheit': 'float'},
    required_columns=['temperature_celsius'],
    adds_columns=['temperature_fahrenheit'],
    removes_columns=[],
    side_effects=[],
    idempotent=True
)

@validate_contract(contract_celsius_to_fahrenheit)
def celsius_to_fahrenheit(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    df['temperature_fahrenheit'] = (df['temperature_celsius'] * 9/5) + 32
    return df
```

### Composability Patterns

#### 1. Sequential Composition (Chaining)

```python
def compose(*functions):
    """Compose functions left-to-right: compose(f, g, h)(x) = h(g(f(x)))"""
    def composed(df):
        result = df
        for func in functions:
            result = func(result)
        return result
    return composed

# Example: Build transformation pipeline
pipeline = compose(
    ingest_bronze,
    clean_nulls,
    add_derived_columns,
    aggregate_daily
)

result = pipeline(raw_data)
```

#### 2. Parallel Composition (Fan-out)

```python
def parallel(*functions):
    """Run functions in parallel and union results"""
    from concurrent.futures import ThreadPoolExecutor
    
    def parallel_exec(df):
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(func, df.copy()) for func in functions]
            results = [f.result() for f in futures]
        return pd.concat(results, ignore_index=True)
    return parallel_exec

# Example: Process multiple entity types
process_all = parallel(
    process_customers,
    process_products,
    process_orders
)
```

#### 3. Conditional Composition

```python
def when(predicate, true_func, false_func=lambda x: x):
    """Conditional transformation"""
    def conditional(df):
        if predicate(df):
            return true_func(df)
        else:
            return false_func(df)
    return conditional

# Example: Handle large vs small datasets differently
smart_transform = when(
    predicate=lambda df: len(df) > 1_000_000,
    true_func=spark_heavy_aggregation,
    false_func=pandas_light_aggregation
)
```

### Contract-Driven Design Example

```python
from typing import Callable, List

class TransformationPipeline:
    """Build pipelines with contract validation"""
    
    def __init__(self):
        self.stages: List[tuple] = []  # (func, contract)
    
    def add_stage(self, func: Callable, contract: TransformationContract):
        """Add transformation stage with contract"""
        # Validate compatibility with previous stage
        if self.stages:
            prev_contract = self.stages[-1][1]
            # Check output → input compatibility
            missing = set(contract.required_columns) - set(prev_contract.adds_columns + list(prev_contract.input_schema.keys()))
            if missing:
                raise ValueError(f"Contract mismatch: {func.__name__} needs {missing}")
        
        self.stages.append((func, contract))
        print(f"✓ Added {func.__name__} to pipeline")
    
    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute pipeline with contract validation"""
        result = df
        for func, contract in self.stages:
            validated_func = validate_contract(contract)(func)
            result = validated_func(result)
        return result

# Build validated pipeline
pipeline = TransformationPipeline()
pipeline.add_stage(celsius_to_fahrenheit, contract_celsius_to_fahrenheit)
pipeline.add_stage(calculate_stats, contract_stats)

result = pipeline.execute(raw_data)
```

---

## TransformationTracker Deep Dive

### Architecture Overview

**TransformationTracker** logs every transformation step to Delta Lake for lineage, performance analysis, and quality monitoring.

**Source:** [transformation_tracker.py](file:///d:/projects/odibi_de_v2/odibi_de_v2/transformer/spark/transformation_tracker.py)

### How It Works

```python
from odibi_de_v2.transformer import TransformationTracker

# Initialize tracker
tracker = TransformationTracker(
    table_path="abfss://data@storage.dfs.core.windows.net/lineage/transformations",
    table_name="transformation_log",
    database="qat_metadata",
    project="energy_efficiency",
    auto_register=True
)

# Start new run
tracker.new_run()  # Generates unique run_id

# Log transformation
tracker.log(
    before_df=df_input,
    after_df=df_output,
    node_name="calculate_efficiency",
    step_type="aggregation",
    intent="Calculate asset-level efficiency metrics",
    parent_node="clean_sensor_data"
)
```

### Tracked Metadata

Each logged transformation captures:

```python
{
    "timestamp": "2024-01-15T10:30:45.123Z",
    "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "table_name": "qat_energy.asset_efficiency",
    "node_name": "calculate_efficiency",
    "parent_node": "clean_sensor_data",
    "step_type": "aggregation",  # join, filter, aggregation, ml
    "intent": "Calculate asset-level efficiency metrics",
    
    # Data volume
    "input_row_count": 1_500_000,
    "output_row_count": 50_000,
    
    # Schema tracking
    "input_schema_hash": "a3f7e2...",
    "output_schema_hash": "b8c4d1...",
    "schema_change_summary": {
        "added": ["efficiency_pct", "uptime_hours"],
        "removed": ["raw_sensor_data"]
    },
    "output_schema_json": "{...}",  # Full schema
    
    # Execution metadata
    "step_order": 3,
    "status": "success",
    "duration_seconds": 42.5
}
```

### Performance Optimization

#### Schema Hashing with Cache

```python
def _hash_schema(self, df: Optional[DataFrame]) -> Optional[str]:
    """Cache schema hashes to avoid recomputing identical ones"""
    if df is None:
        return None
    schema_json = json.dumps(df.schema.jsonValue(), sort_keys=True)
    
    # Check cache
    if schema_json not in self._schema_cache:
        self._schema_cache[schema_json] = hashlib.sha256(schema_json.encode()).hexdigest()
    
    return self._schema_cache[schema_json]
```

**Optimization:** Avoid re-hashing identical schemas across transformations.

#### Lazy Row Counting

```python
tracker.log(
    before_df=df_input,
    after_df=df_output,
    node_name="join_tables",
    count_rows=False  # Skip expensive .count() on large DataFrames
)
```

**When to Skip:** Very large DataFrames (>1B rows) where `.count()` is expensive.

### Lineage Queries

Once tracked, query lineage:

```sql
-- Find all transformations in a run
SELECT 
    node_name,
    step_type,
    input_row_count,
    output_row_count,
    duration_seconds
FROM qat_metadata.energy_efficiency_transformation_log
WHERE run_id = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
ORDER BY step_order;

-- Find transformations with high data loss
SELECT 
    node_name,
    intent,
    input_row_count,
    output_row_count,
    (1.0 - CAST(output_row_count AS DOUBLE) / input_row_count) * 100 AS pct_loss
FROM qat_metadata.energy_efficiency_transformation_log
WHERE output_row_count < input_row_count * 0.5  -- >50% row loss
ORDER BY pct_loss DESC;

-- Find schema drift over time
SELECT 
    date_trunc('day', timestamp) AS day,
    node_name,
    COUNT(DISTINCT output_schema_hash) AS schema_versions
FROM qat_metadata.energy_efficiency_transformation_log
GROUP BY day, node_name
HAVING schema_versions > 1  -- Schema changed
ORDER BY day DESC;
```

### Production Patterns

#### Pattern 1: Auto-track in Workflows

```python
from odibi_de_v2.transformer import SparkWorkflowNode

node = SparkWorkflowNode("calculate_metrics")

# Tracker auto-logs every step
node.add_step(
    lambda df: df.filter(F.col("quality") == "GOOD"),
    step_type="filter",
    intent="Remove low-quality sensors"
)

node.add_step(
    lambda df: df.groupBy("asset_id").agg(F.avg("temperature").alias("avg_temp")),
    step_type="aggregation",
    intent="Calculate average temperature per asset"
)

result = node.run(input_df)
# All steps automatically tracked!
```

#### Pattern 2: Performance Anomaly Detection

```python
# Query for slow transformations
slow_transforms = spark.sql("""
    SELECT 
        node_name,
        AVG(duration_seconds) AS avg_duration,
        STDDEV(duration_seconds) AS stddev_duration,
        COUNT(*) AS run_count
    FROM qat_metadata.energy_efficiency_transformation_log
    WHERE timestamp > CURRENT_DATE - INTERVAL 7 DAYS
    GROUP BY node_name
    HAVING avg_duration > 60  -- Slower than 1 minute
    ORDER BY avg_duration DESC
""")

# Alert on anomalies
for row in slow_transforms.collect():
    if row.avg_duration > row.stddev_duration * 3:  # 3σ threshold
        print(f"⚠️ ALERT: {row.node_name} is anomalously slow ({row.avg_duration:.1f}s)")
```

---

## Event-Driven Patterns

### Hook System Architecture

**Hooks** allow custom code to run at specific lifecycle events without modifying core framework code.

### Available Events

```python
LIFECYCLE_EVENTS = [
    "pre_read",          # Before reading input data
    "post_read",         # After reading input data
    "pre_transform",     # Before transformation logic
    "post_transform",    # After transformation logic
    "pre_save",          # Before saving output
    "post_save",         # After saving output
    "on_error",          # When error occurs
    "on_complete"        # After full pipeline completes
]
```

### Advanced Hook Patterns

#### Pattern 1: Circuit Breaker

Automatically stop pipeline on consecutive failures:

```python
class CircuitBreaker:
    def __init__(self, max_failures=3):
        self.failures = 0
        self.max_failures = max_failures
        self.is_open = False
    
    def on_error_handler(self, error, context):
        """Track failures and open circuit if threshold exceeded"""
        self.failures += 1
        
        if self.failures >= self.max_failures:
            self.is_open = True
            raise RuntimeError(
                f"Circuit breaker opened after {self.failures} failures. "
                f"Last error: {error}"
            )
        
        print(f"⚠️ Error {self.failures}/{self.max_failures}: {error}")
    
    def on_complete_handler(self, context):
        """Reset on successful completion"""
        self.failures = 0
        print(f"✓ Circuit breaker reset")

# Register
breaker = CircuitBreaker(max_failures=3)
register_hook("on_error", breaker.on_error_handler, hook_manager)
register_hook("on_complete", breaker.on_complete_handler, hook_manager)
```

#### Pattern 2: Adaptive Caching

Cache based on runtime performance:

```python
class AdaptiveCache:
    def __init__(self, cache_threshold_seconds=30):
        self.cache_threshold = cache_threshold_seconds
        self.performance_log = {}
    
    def post_transform_handler(self, df, context):
        """Cache slow transformations automatically"""
        layer = context.get('layer')
        duration = context.get('duration_seconds', 0)
        
        self.performance_log[layer] = duration
        
        if duration > self.cache_threshold and not df.is_cached:
            df.cache()
            df.count()  # Materialize cache
            print(f"🚀 Auto-cached {layer} (took {duration:.1f}s)")
        
        return df

cache = AdaptiveCache(cache_threshold_seconds=30)
register_hook("post_transform", cache.post_transform_handler, hook_manager)
```

#### Pattern 3: Data Quality SLA Enforcement

Fail pipeline if quality drops below threshold:

```python
class QualitySLA:
    def __init__(self, min_completeness=0.95, min_accuracy=0.99):
        self.min_completeness = min_completeness
        self.min_accuracy = min_accuracy
    
    def post_transform_handler(self, df, context):
        """Enforce data quality SLAs"""
        layer = context.get('layer')
        
        # Check completeness (non-null rate)
        completeness = 1 - (df.isnull().sum().sum() / (len(df) * len(df.columns)))
        
        if completeness < self.min_completeness:
            raise ValueError(
                f"SLA VIOLATION: {layer} completeness {completeness:.2%} "
                f"< {self.min_completeness:.2%}"
            )
        
        # Check accuracy (valid values)
        if 'is_valid' in df.columns:
            accuracy = df['is_valid'].mean()
            if accuracy < self.min_accuracy:
                raise ValueError(
                    f"SLA VIOLATION: {layer} accuracy {accuracy:.2%} "
                    f"< {self.min_accuracy:.2%}"
                )
        
        print(f"✓ SLA check passed: {layer} (completeness={completeness:.2%})")
        return df

sla = QualitySLA(min_completeness=0.95, min_accuracy=0.99)
register_hook("post_transform", sla.post_transform_handler, hook_manager)
```

---

## Testing Strategies

### Unit Testing Transformations

```python
import pytest
import pandas as pd

def test_celsius_to_fahrenheit():
    """Unit test for temperature conversion"""
    # Arrange
    input_df = pd.DataFrame({
        'temperature_celsius': [0, 100, -40]
    })
    expected = pd.DataFrame({
        'temperature_celsius': [0, 100, -40],
        'temperature_fahrenheit': [32.0, 212.0, -40.0]
    })
    
    # Act
    result = celsius_to_fahrenheit(input_df)
    
    # Assert
    pd.testing.assert_frame_equal(result, expected)

def test_celsius_to_fahrenheit_handles_nulls():
    """Test NULL handling"""
    input_df = pd.DataFrame({
        'temperature_celsius': [20, None, 25]
    })
    
    with pytest.raises(AssertionError, match="NULL temperatures"):
        celsius_to_fahrenheit(input_df)
```

### Integration Testing Pipelines

```python
def test_bronze_to_gold_pipeline():
    """Integration test for full pipeline"""
    # Arrange: Create test Bronze data
    bronze_data = pd.DataFrame({
        'timestamp': pd.date_range('2024-01-01', periods=100, freq='H'),
        'sensor_id': ['A'] * 100,
        'temperature_celsius': [20 + i*0.1 for i in range(100)]
    })
    
    # Act: Run full pipeline
    silver_data = bronze_to_silver(bronze_data)
    gold_data = silver_to_gold(silver_data)
    
    # Assert: Check Gold output
    assert len(gold_data) > 0, "Gold layer should have data"
    assert 'daily_avg_temp' in gold_data.columns
    assert gold_data['daily_avg_temp'].notna().all()
    assert gold_data['daily_avg_temp'].min() >= -100  # Reasonable range
    assert gold_data['daily_avg_temp'].max() <= 200
```

### Property-Based Testing

```python
from hypothesis import given, strategies as st
import hypothesis.extra.pandas as pdst

@given(
    df=pdst.data_frames(
        columns=[
            pdst.column('temperature_celsius', dtype=float, elements=st.floats(-50, 60))
        ],
        index=pdst.range_indexes(min_size=1, max_size=1000)
    )
)
def test_temperature_conversion_properties(df):
    """Property-based test: Any valid input should produce valid output"""
    result = celsius_to_fahrenheit(df)
    
    # Property 1: Output has same number of rows
    assert len(result) == len(df)
    
    # Property 2: Fahrenheit column exists
    assert 'temperature_fahrenheit' in result.columns
    
    # Property 3: Conversion is correct (F = C * 9/5 + 32)
    for _, row in result.iterrows():
        expected_f = row['temperature_celsius'] * 9/5 + 32
        assert abs(row['temperature_fahrenheit'] - expected_f) < 0.01
```

### Snapshot Testing

```python
import json

def test_gold_output_snapshot():
    """Snapshot test: Detect unexpected output changes"""
    # Run transformation
    result = run_gold_transformation()
    
    # Convert to JSON for snapshot
    snapshot = result.to_dict(orient='records')
    snapshot_file = Path("tests/snapshots/gold_output.json")
    
    if not snapshot_file.exists():
        # Create baseline snapshot
        snapshot_file.write_text(json.dumps(snapshot, indent=2))
        pytest.skip("Created baseline snapshot")
    
    # Compare with baseline
    baseline = json.loads(snapshot_file.read_text())
    assert snapshot == baseline, "Output changed from baseline!"
```

---

## Performance Optimization

### 1. Lazy Evaluation with Spark

```python
# ❌ BAD: Eager evaluation
df1 = spark.table("bronze_table").cache()
df1.count()  # Forces materialization
df2 = df1.filter(F.col("quality") == "GOOD")
df2.count()  # Another action
result = df2.groupBy("asset").count()

# ✅ GOOD: Lazy evaluation
result = (
    spark.table("bronze_table")
    .filter(F.col("quality") == "GOOD")
    .groupBy("asset")
    .count()
    .cache()  # Cache final result only
)
result.show()  # Single action
```

### 2. Partition Pruning

```python
# ❌ BAD: Full table scan
df = spark.table("bronze_sensors")
filtered = df.filter(F.col("date") == "2024-01-15")

# ✅ GOOD: Partition pruning (if partitioned by date)
df = spark.table("bronze_sensors")
filtered = spark.sql("""
    SELECT * FROM bronze_sensors
    WHERE date = '2024-01-15'  -- Uses partition pruning
""")
```

### 3. Broadcast Joins for Small Tables

```python
from pyspark.sql.functions import broadcast

# ❌ BAD: Shuffle join on large + small table
result = large_fact_table.join(small_dim_table, "dim_id")

# ✅ GOOD: Broadcast join
result = large_fact_table.join(
    broadcast(small_dim_table),  # Broadcast small table to all nodes
    "dim_id"
)
```

### 4. Column Pruning

```python
# ❌ BAD: Read all columns
df = spark.table("bronze_sensors")  # 50 columns
result = df.groupBy("asset_id").agg(F.avg("temperature"))

# ✅ GOOD: Read only needed columns
result = (
    spark.table("bronze_sensors")
    .select("asset_id", "temperature")  # Only 2 columns
    .groupBy("asset_id")
    .agg(F.avg("temperature"))
)
```

### 5. Adaptive Query Execution (AQE)

```python
# Enable AQE for automatic optimizations
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# AQE automatically:
# - Combines small partitions
# - Handles skewed joins
# - Optimizes join strategies at runtime
```

---

## Safe Refactoring Practices

### 1. Strangler Fig Pattern

Gradually replace old code with new:

```python
# Phase 1: Add new implementation alongside old
def calculate_efficiency_v1(df):  # Old
    return df.withColumn("efficiency", F.col("output") / F.col("input"))

def calculate_efficiency_v2(df):  # New
    return df.withColumn("efficiency", 
        F.when(F.col("input") > 0, F.col("output") / F.col("input"))
         .otherwise(0))

# Phase 2: Feature flag to switch between versions
USE_V2 = True

def calculate_efficiency(df):
    if USE_V2:
        return calculate_efficiency_v2(df)
    return calculate_efficiency_v1(df)

# Phase 3: Remove old implementation after validation
```

### 2. Schema Evolution with Backward Compatibility

```python
# Add column with default value for backward compatibility
def add_new_column_safe(df):
    if 'new_column' not in df.columns:
        df = df.withColumn('new_column', F.lit(None).cast("string"))
    return df

# Remove column gracefully
def remove_old_column_safe(df):
    if 'old_column' in df.columns:
        df = df.drop('old_column')
    return df
```

### 3. Canary Deployments

```python
import random

def canary_transform(df, new_func, old_func, canary_pct=0.1):
    """Route % of traffic to new implementation"""
    if random.random() < canary_pct:
        print(f"🐤 Canary: Using new implementation")
        return new_func(df)
    else:
        return old_func(df)

# Gradually increase canary_pct: 0.1 → 0.5 → 1.0
```

---

## Future Extensibility

### AI Integration Patterns

```python
# Future: AI-powered transformation suggestions
class AITransformationAssistant:
    def suggest_transformations(self, df, intent: str):
        """AI suggests transformations based on intent"""
        # Use LLM to analyze schema and intent
        prompt = f"""
        Given DataFrame schema: {df.schema}
        User intent: {intent}
        
        Suggest PySpark transformation code.
        """
        # Call OpenAI/Azure OpenAI
        suggestion = llm.generate(prompt)
        return suggestion

# Usage
assistant = AITransformationAssistant()
code = assistant.suggest_transformations(df, "Calculate daily average temperature")
```

### Lineage Visualization (Story Feature)

```python
# Future: Interactive lineage graph
class LineageVisualizer:
    def generate_story(self, run_id: str):
        """Generate interactive HTML lineage graph"""
        lineage = self.query_tracker(run_id)
        
        # Build graph
        G = nx.DiGraph()
        for step in lineage:
            G.add_edge(step.parent_node, step.node_name)
        
        # Generate interactive HTML with D3.js
        html = self.render_d3_graph(G)
        return html
```

### Auto-Optimization

```python
# Future: AI-powered performance optimization
class AutoOptimizer:
    def optimize_pipeline(self, config):
        """Automatically optimize based on profiling"""
        profile = self.profile_pipeline(config)
        
        # Detect bottlenecks
        bottlenecks = [s for s in profile if s.duration > 60]
        
        # Apply optimizations
        for step in bottlenecks:
            if step.type == "join" and step.small_table_size < 100_000:
                step.add_broadcast_hint()
            elif step.type == "aggregation" and step.partition_count > 1000:
                step.coalesce_partitions(200)
```

---

## Next Steps

- **Practice:** Implement contract validation in your pipelines
- **Explore:** Query TransformationTracker for your project
- **Build:** Create custom hooks for your use cases
- **Test:** Add property-based tests to critical transformations

---

## Related Documentation

- [07 - Project Structure](file:///d:/projects/odibi_de_v2/docs/learn_odibi_de/07_Project_Structure_and_Self_Bootstrap.md)
- [08 - Tutorials](file:///d:/projects/odibi_de_v2/docs/learn_odibi_de/08_Tutorials_and_Practice.md)
- [10 - Rebuild Challenge](file:///d:/projects/odibi_de_v2/docs/learn_odibi_de/10_Rebuild_Challenge.md)
- [Architecture Map](file:///d:/projects/odibi_de_v2/docs/ARCHITECTURE_MAP.md)
