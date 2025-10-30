# 10 - Rebuild Challenge: From First Principles

**Level:** Expert  
**Duration:** 4-8 hours  
**Prerequisites:** Deep understanding of all previous guides

---

## Table of Contents
1. [Challenge Overview](#challenge-overview)
2. [Phase 1: Minimal Framework (20 lines)](#phase-1-minimal-framework-20-lines)
3. [Phase 2: Add Ingestion (+50 lines)](#phase-2-add-ingestion-50-lines)
4. [Phase 3: Add Transformation (+80 lines)](#phase-3-add-transformation-80-lines)
5. [Phase 4: Add Orchestration (+100 lines)](#phase-4-add-orchestration-100-lines)
6. [Phase 5: Add Hooks (+40 lines)](#phase-5-add-hooks-40-lines)
7. [Phase 6: Add Registry (+60 lines)](#phase-6-add-registry-60-lines)
8. [Success Criteria](#success-criteria)
9. [Reflection Questions](#reflection-questions)
10. [Comparison with Full Framework](#comparison-with-full-framework)

---

## Challenge Overview

### The Challenge

**Build a functional data engineering framework from scratch** that can:
1. Read data from CSV
2. Apply transformations
3. Save results
4. Chain transformations (medallion pattern)
5. Emit lifecycle events (hooks)
6. Track registered functions

**Constraints:**
- Start with just 20 lines
- Add features incrementally
- No copying from odibi_de_v2 source
- Use first principles thinking
- Compare your design with actual framework

### Learning Objectives

By rebuilding from scratch, you will:
- ✅ Understand architectural decisions
- ✅ Appreciate abstraction layers
- ✅ Recognize design patterns in the real framework
- ✅ Identify trade-offs between simplicity and features
- ✅ Gain confidence to extend the framework

### Rules

1. **No peeking** at implementation until comparison step
2. **Write tests** after each phase
3. **Reflect** on design decisions
4. **Document** your reasoning
5. **Compare** with actual framework at end

---

## Phase 1: Minimal Framework (20 lines)

### Goal

Create the smallest possible working data pipeline:
- Read CSV
- Transform data
- Write CSV

### Implementation

```python
# mini_framework.py (20 lines)
import pandas as pd
from typing import Callable
from pathlib import Path

class Pipeline:
    """Minimal data pipeline"""
    
    def __init__(self, input_path: str, output_path: str):
        self.input_path = input_path
        self.output_path = output_path
    
    def run(self, transform: Callable[[pd.DataFrame], pd.DataFrame]):
        """Read → Transform → Write"""
        # Read
        df = pd.read_csv(self.input_path)
        
        # Transform
        result = transform(df)
        
        # Write
        result.to_csv(self.output_path, index=False)
        
        return result
```

### Test It

```python
# test_phase1.py
import pandas as pd
from pathlib import Path
from mini_framework import Pipeline

# Create test data
test_data = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})
test_data.to_csv('input.csv', index=False)

# Define transformation
def add_year_born(df):
    df['year_born'] = 2024 - df['age']
    return df

# Run pipeline
pipeline = Pipeline('input.csv', 'output.csv')
result = pipeline.run(add_year_born)

print(result)
# Expected:
#      name  age  year_born
# 0   Alice   25       1999
# 1     Bob   30       1994
# 2 Charlie   35       1989

# Verify output file exists
assert Path('output.csv').exists()
print("✅ Phase 1 complete!")
```

### Reflection Questions (Phase 1)

1. **What problems does this solve?**
   - Simple transformations without boilerplate
   - Clear separation: read → transform → write
   - Reusable for different transformations

2. **What problems remain?**
   - Hardcoded CSV format
   - No error handling
   - Single transformation only
   - No metadata tracking
   - Can't chain multiple transformations

3. **Design decision:** Why use a class instead of functions?
   - <details><summary>Your answer...</summary>
     Class allows state (input/output paths) to be configured once and reused. Functions would require passing paths repeatedly.
     </details>

4. **Alternative designs:**
   - Function composition: `write(transform(read()))`
   - Builder pattern: `Pipeline().read().transform().write()`
   - Context manager: `with Pipeline() as p: p.run()`

---

## Phase 2: Add Ingestion (+50 lines)

### Goal

Support multiple data sources:
- CSV
- JSON
- Parquet
- Database tables

### Implementation

```python
# mini_framework.py (add 50 lines)
from abc import ABC, abstractmethod
import pandas as pd

class DataReader(ABC):
    """Abstract data reader"""
    
    @abstractmethod
    def read(self, path: str) -> pd.DataFrame:
        """Read data from source"""
        pass

class CSVReader(DataReader):
    def read(self, path: str) -> pd.DataFrame:
        return pd.read_csv(path)

class JSONReader(DataReader):
    def read(self, path: str) -> pd.DataFrame:
        return pd.read_json(path)

class ParquetReader(DataReader):
    def read(self, path: str) -> pd.DataFrame:
        return pd.read_parquet(path)

class DataWriter(ABC):
    """Abstract data writer"""
    
    @abstractmethod
    def write(self, df: pd.DataFrame, path: str):
        """Write data to destination"""
        pass

class CSVWriter(DataWriter):
    def write(self, df: pd.DataFrame, path: str):
        df.to_csv(path, index=False)

class JSONWriter(DataWriter):
    def write(self, df: pd.DataFrame, path: str):
        df.to_json(path, orient='records', indent=2)

class ParquetWriter(DataWriter):
    def write(self, df: pd.DataFrame, path: str):
        df.to_parquet(path, index=False)

# Update Pipeline class
class Pipeline:
    """Enhanced pipeline with pluggable readers/writers"""
    
    def __init__(
        self, 
        input_path: str, 
        output_path: str,
        reader: DataReader = None,
        writer: DataWriter = None
    ):
        self.input_path = input_path
        self.output_path = output_path
        self.reader = reader or CSVReader()  # Default to CSV
        self.writer = writer or CSVWriter()
    
    def run(self, transform: Callable[[pd.DataFrame], pd.DataFrame]):
        """Read → Transform → Write"""
        # Read with pluggable reader
        df = self.reader.read(self.input_path)
        
        # Transform
        result = transform(df)
        
        # Write with pluggable writer
        self.writer.write(result, self.output_path)
        
        return result
```

### Test It

```python
# test_phase2.py
import pandas as pd
from mini_framework import Pipeline, JSONReader, ParquetWriter

# Create JSON input
test_data = pd.DataFrame({
    'product': ['Widget', 'Gadget'],
    'price': [10.99, 25.50]
})
test_data.to_json('input.json', orient='records')

# Read JSON, write Parquet
pipeline = Pipeline(
    'input.json', 
    'output.parquet',
    reader=JSONReader(),
    writer=ParquetWriter()
)

def add_tax(df):
    df['price_with_tax'] = df['price'] * 1.08
    return df

result = pipeline.run(add_tax)
print(result)

# Verify Parquet output
assert Path('output.parquet').exists()
print("✅ Phase 2 complete!")
```

### Reflection Questions (Phase 2)

1. **Why use abstract base classes (ABC)?**
   - <details><summary>Your answer...</summary>
     Enforces contract: all readers must implement `.read()`, all writers must implement `.write()`. Enables polymorphism.
     </details>

2. **Design pattern used:** Which design pattern is this?
   - <details><summary>Answer</summary>
     **Strategy Pattern**: Pluggable algorithms (readers/writers) with common interface.
     </details>

3. **How does this compare to odibi_de_v2?**
   - Real framework: [core/reader.py](file:///d:/projects/odibi_de_v2/odibi_de_v2/core/reader.py)
   - Uses factory pattern: `ReaderFactory.get_reader(format="csv")`
   - Supports Spark and Pandas engines
   - Has connection pooling for databases

---

## Phase 3: Add Transformation (+80 lines)

### Goal

Support transformation chaining (medallion pattern):
- Bronze → Silver → Gold
- Track intermediate results
- Handle errors gracefully

### Implementation

```python
# mini_framework.py (add 80 lines)
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime

@dataclass
class TransformationResult:
    """Track transformation execution"""
    name: str
    input_rows: int
    output_rows: int
    duration_seconds: float
    timestamp: datetime
    success: bool
    error: Optional[str] = None

class TransformationChain:
    """Chain multiple transformations"""
    
    def __init__(self):
        self.steps: List[tuple] = []  # (name, func)
        self.results: List[TransformationResult] = []
    
    def add_step(self, name: str, func: Callable):
        """Add transformation step"""
        self.steps.append((name, func))
        return self  # Enable chaining
    
    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute transformation chain"""
        result = df
        
        for step_name, transform_func in self.steps:
            start_time = datetime.now()
            input_rows = len(result)
            
            try:
                # Execute transformation
                result = transform_func(result)
                
                # Track success
                duration = (datetime.now() - start_time).total_seconds()
                self.results.append(TransformationResult(
                    name=step_name,
                    input_rows=input_rows,
                    output_rows=len(result),
                    duration_seconds=duration,
                    timestamp=start_time,
                    success=True
                ))
                
                print(f"✓ {step_name}: {input_rows} → {len(result)} rows ({duration:.2f}s)")
                
            except Exception as e:
                # Track failure
                duration = (datetime.now() - start_time).total_seconds()
                self.results.append(TransformationResult(
                    name=step_name,
                    input_rows=input_rows,
                    output_rows=0,
                    duration_seconds=duration,
                    timestamp=start_time,
                    success=False,
                    error=str(e)
                ))
                
                print(f"✗ {step_name} failed: {e}")
                raise
        
        return result
    
    def get_summary(self) -> pd.DataFrame:
        """Get execution summary"""
        return pd.DataFrame([
            {
                'step': r.name,
                'input_rows': r.input_rows,
                'output_rows': r.output_rows,
                'duration_s': r.duration_seconds,
                'success': r.success,
                'error': r.error
            }
            for r in self.results
        ])

# Update Pipeline to support chains
class Pipeline:
    """Pipeline with transformation chains"""
    
    def __init__(self, input_path: str, output_path: str, **kwargs):
        self.input_path = input_path
        self.output_path = output_path
        self.reader = kwargs.get('reader', CSVReader())
        self.writer = kwargs.get('writer', CSVWriter())
        self.chain = TransformationChain()
    
    def add_transformation(self, name: str, func: Callable):
        """Add transformation to chain"""
        self.chain.add_step(name, func)
        return self
    
    def run(self):
        """Execute full pipeline"""
        # Read
        df = self.reader.read(self.input_path)
        
        # Transform chain
        result = self.chain.run(df)
        
        # Write
        self.writer.write(result, self.output_path)
        
        return result, self.chain.get_summary()
```

### Test It

```python
# test_phase3.py - Medallion Pattern
import pandas as pd
from mini_framework import Pipeline

# Create Bronze data
bronze_data = pd.DataFrame({
    'customer_id': [1, 2, 3, 3],  # Has duplicate
    'email': ['a@x.com', None, 'c@x.com', 'c@x.com'],  # Has NULL
    'purchase_amount': [100, 200, 150, 150]
})
bronze_data.to_csv('bronze.csv', index=False)

# Define transformations
def bronze_to_silver(df):
    """Clean data"""
    # Remove NULLs
    df = df.dropna()
    # Remove duplicates
    df = df.drop_duplicates()
    return df

def silver_to_gold(df):
    """Aggregate metrics"""
    # Calculate total spend per customer
    df = df.groupby('customer_id').agg({
        'purchase_amount': 'sum',
        'email': 'first'
    }).reset_index()
    df.rename(columns={'purchase_amount': 'total_spend'}, inplace=True)
    return df

# Build pipeline
pipeline = (
    Pipeline('bronze.csv', 'gold.csv')
    .add_transformation('Bronze → Silver', bronze_to_silver)
    .add_transformation('Silver → Gold', silver_to_gold)
)

# Run
result, summary = pipeline.run()

print("\n📊 Execution Summary:")
print(summary)

print("\n🏆 Gold Result:")
print(result)

# Expected output:
# ✓ Bronze → Silver: 4 → 2 rows (0.01s)
# ✓ Silver → Gold: 2 → 2 rows (0.02s)
#
# Gold Result:
#    customer_id  total_spend      email
# 0            1          100  a@x.com
# 1            3          150  c@x.com

print("✅ Phase 3 complete!")
```

### Reflection Questions (Phase 3)

1. **Why track TransformationResult?**
   - <details><summary>Your answer...</summary>
     Observability: know what happened, how long it took, where failures occurred. Critical for debugging production pipelines.
     </details>

2. **Why return `self` in add_transformation()?**
   - <details><summary>Answer</summary>
     **Fluent Interface**: Enables method chaining for readable pipeline construction.
     </details>

3. **Error handling strategy:**
   - Current: Fail entire pipeline on error
   - Alternative: Continue with warning, skip failed step
   - Trade-off: Fail-fast vs. fault-tolerant

---

## Phase 4: Add Orchestration (+100 lines)

### Goal

Run multi-layer pipelines with:
- Layer dependencies (Bronze → Silver → Gold)
- Parallel execution where possible
- Progress tracking

### Implementation

```python
# mini_framework.py (add 100 lines)
from typing import Dict, Set
from collections import defaultdict
import time

@dataclass
class LayerConfig:
    """Configuration for a pipeline layer"""
    name: str
    depends_on: List[str]
    input_path: str
    output_path: str
    transformations: List[tuple]  # [(name, func), ...]

class Orchestrator:
    """Orchestrate multi-layer pipelines"""
    
    def __init__(self):
        self.layers: Dict[str, LayerConfig] = {}
        self.execution_order: List[str] = []
    
    def add_layer(self, config: LayerConfig):
        """Add layer to orchestrator"""
        self.layers[config.name] = config
    
    def _compute_execution_order(self):
        """Topological sort of layer dependencies"""
        # Build dependency graph
        graph = {name: set(cfg.depends_on) for name, cfg in self.layers.items()}
        
        # Kahn's algorithm for topological sort
        in_degree = defaultdict(int)
        for deps in graph.values():
            for dep in deps:
                in_degree[dep] += 0  # Ensure all nodes in dict
        for node in graph:
            in_degree[node] = len(graph[node])
        
        queue = [node for node, degree in in_degree.items() if degree == 0]
        order = []
        
        while queue:
            node = queue.pop(0)
            order.append(node)
            
            # Update neighbors
            for neighbor, deps in graph.items():
                if node in deps:
                    deps.remove(node)
                    if len(deps) == 0:
                        queue.append(neighbor)
        
        if len(order) != len(graph):
            raise ValueError("Circular dependency detected!")
        
        self.execution_order = order
        return order
    
    def run(self):
        """Execute all layers in dependency order"""
        self._compute_execution_order()
        
        print(f"🚀 Executing {len(self.execution_order)} layers")
        print(f"📋 Order: {' → '.join(self.execution_order)}\n")
        
        results = {}
        total_start = time.time()
        
        for layer_name in self.execution_order:
            config = self.layers[layer_name]
            
            print(f"▶️  {layer_name}")
            layer_start = time.time()
            
            # Build pipeline for this layer
            pipeline = Pipeline(config.input_path, config.output_path)
            
            for transform_name, transform_func in config.transformations:
                pipeline.add_transformation(transform_name, transform_func)
            
            # Execute
            result, summary = pipeline.run()
            
            layer_duration = time.time() - layer_start
            results[layer_name] = {
                'result': result,
                'summary': summary,
                'duration': layer_duration
            }
            
            print(f"   ✅ Completed in {layer_duration:.2f}s\n")
        
        total_duration = time.time() - total_start
        
        print(f"🏁 All layers complete in {total_duration:.2f}s")
        
        return results
    
    def visualize_dag(self):
        """Print ASCII DAG"""
        print("📊 Pipeline DAG:")
        for layer_name in self.execution_order:
            config = self.layers[layer_name]
            if config.depends_on:
                for dep in config.depends_on:
                    print(f"  {dep} → {layer_name}")
            else:
                print(f"  [START] → {layer_name}")
```

### Test It

```python
# test_phase4.py - Full Orchestration
import pandas as pd
from mini_framework import Orchestrator, LayerConfig

# Create Bronze data
bronze = pd.DataFrame({
    'timestamp': pd.date_range('2024-01-01', periods=100, freq='H'),
    'sensor_id': ['A', 'B'] * 50,
    'temperature': [20 + i*0.1 for i in range(100)]
})
bronze.to_csv('bronze_sensors.csv', index=False)

# Define layers
bronze_config = LayerConfig(
    name="Bronze",
    depends_on=[],
    input_path="bronze_sensors.csv",
    output_path="bronze_clean.csv",
    transformations=[
        ("Validate", lambda df: df.dropna())
    ]
)

silver_config = LayerConfig(
    name="Silver",
    depends_on=["Bronze"],
    input_path="bronze_clean.csv",
    output_path="silver_hourly.csv",
    transformations=[
        ("Add Features", lambda df: df.assign(
            hour=pd.to_datetime(df['timestamp']).dt.hour,
            temp_f=df['temperature'] * 9/5 + 32
        ))
    ]
)

gold_config = LayerConfig(
    name="Gold",
    depends_on=["Silver"],
    input_path="silver_hourly.csv",
    output_path="gold_stats.csv",
    transformations=[
        ("Aggregate Daily", lambda df: df.groupby('sensor_id').agg({
            'temp_f': ['mean', 'min', 'max']
        }).reset_index())
    ]
)

# Build orchestrator
orch = Orchestrator()
orch.add_layer(bronze_config)
orch.add_layer(silver_config)
orch.add_layer(gold_config)

# Visualize
orch.visualize_dag()

# Run
results = orch.run()

print("\n📈 Final Gold Output:")
print(results['Gold']['result'])

print("✅ Phase 4 complete!")
```

### Reflection Questions (Phase 4)

1. **Why topological sort?**
   - <details><summary>Your answer...</summary>
     Ensures dependencies execute before dependents. Detects circular dependencies. Enables parallelization of independent layers.
     </details>

2. **How would you add parallel execution?**
   - <details><summary>Hint</summary>
     Use `ThreadPoolExecutor` to run independent layers (same dependency level) concurrently.
     </details>

3. **Compare with odibi_de_v2:**
   - Real: [generic_orchestrator.py](file:///d:/projects/odibi_de_v2/odibi_de_v2/orchestration/generic_orchestrator.py)
   - Uses manifest.json for configuration
   - Has retry logic, caching, hooks
   - Supports both Spark and Pandas

---

## Phase 5: Add Hooks (+40 lines)

### Goal

Add lifecycle events for observability:
- pre_read, post_read
- pre_transform, post_transform
- pre_write, post_write

### Implementation

```python
# mini_framework.py (add 40 lines)
from typing import Callable, List, Dict

class HookManager:
    """Manage lifecycle event hooks"""
    
    def __init__(self):
        self.hooks: Dict[str, List[Callable]] = defaultdict(list)
    
    def register(self, event: str, handler: Callable):
        """Register hook handler for event"""
        self.hooks[event].append(handler)
        print(f"📌 Registered hook for '{event}'")
    
    def emit(self, event: str, **context):
        """Emit event to all registered handlers"""
        for handler in self.hooks.get(event, []):
            try:
                handler(**context)
            except Exception as e:
                print(f"⚠️ Hook error in {event}: {e}")

# Add hooks to Pipeline
class Pipeline:
    """Pipeline with hooks"""
    
    def __init__(self, input_path: str, output_path: str, hook_manager=None, **kwargs):
        self.input_path = input_path
        self.output_path = output_path
        self.reader = kwargs.get('reader', CSVReader())
        self.writer = kwargs.get('writer', CSVWriter())
        self.chain = TransformationChain()
        self.hooks = hook_manager or HookManager()
    
    def run(self):
        """Execute with lifecycle hooks"""
        # Pre-read
        self.hooks.emit('pre_read', path=self.input_path)
        
        # Read
        df = self.reader.read(self.input_path)
        
        # Post-read
        self.hooks.emit('post_read', df=df, row_count=len(df))
        
        # Transform (with hooks in TransformationChain)
        self.hooks.emit('pre_transform', df=df)
        result = self.chain.run(df)
        self.hooks.emit('post_transform', df=result)
        
        # Pre-write
        self.hooks.emit('pre_write', df=result, path=self.output_path)
        
        # Write
        self.writer.write(result, self.output_path)
        
        # Post-write
        self.hooks.emit('post_write', path=self.output_path)
        
        return result, self.chain.get_summary()
```

### Test It

```python
# test_phase5.py - Hooks
import pandas as pd
from mini_framework import Pipeline, HookManager

# Create hooks
hooks = HookManager()

# Register hook handlers
def log_read(path, **kwargs):
    print(f"📥 Reading from {path}")

def log_row_count(df, **kwargs):
    print(f"📊 Loaded {len(df)} rows")

def validate_output(df, **kwargs):
    if len(df) == 0:
        raise ValueError("Output is empty!")
    print(f"✓ Validation passed: {len(df)} rows")

hooks.register('pre_read', log_read)
hooks.register('post_read', log_row_count)
hooks.register('post_transform', validate_output)

# Create test data
test_data = pd.DataFrame({'value': [1, 2, 3]})
test_data.to_csv('input.csv', index=False)

# Run pipeline with hooks
pipeline = Pipeline('input.csv', 'output.csv', hook_manager=hooks)
pipeline.add_transformation('Double', lambda df: df.assign(value=df['value'] * 2))

result, summary = pipeline.run()

# Expected output:
# 📌 Registered hook for 'pre_read'
# 📌 Registered hook for 'post_read'
# 📌 Registered hook for 'post_transform'
# 📥 Reading from input.csv
# 📊 Loaded 3 rows
# ✓ Double: 3 → 3 rows (0.01s)
# ✓ Validation passed: 3 rows

print("✅ Phase 5 complete!")
```

### Reflection Questions (Phase 5)

1. **Observer pattern:** Which design pattern is this?
   - <details><summary>Answer</summary>
     **Observer Pattern**: Hooks observe lifecycle events without coupling.
     </details>

2. **Hook error handling:** Why catch exceptions in emit()?
   - <details><summary>Your answer...</summary>
     Prevent one failing hook from breaking entire pipeline. Log error but continue execution.
     </details>

3. **Real-world hooks:**
   - Data quality validation
   - Performance monitoring
   - Alerting (Slack, email)
   - Audit logging
   - Cache warming

---

## Phase 6: Add Registry (+60 lines)

### Goal

Track registered transformation functions:
- Discover functions by name/tag
- Support engine variants (Spark/Pandas)
- Version management

### Implementation

```python
# mini_framework.py (add 60 lines)
from dataclasses import dataclass
from typing import Optional, List

@dataclass
class FunctionMetadata:
    """Metadata for registered function"""
    name: str
    func: Callable
    engine: str  # "pandas", "spark", "universal"
    version: str
    author: Optional[str] = None
    tags: List[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []

class FunctionRegistry:
    """Global function registry"""
    
    def __init__(self):
        self.functions: Dict[str, List[FunctionMetadata]] = defaultdict(list)
    
    def register(
        self, 
        name: str, 
        func: Callable, 
        engine: str = "universal",
        version: str = "1.0",
        **kwargs
    ):
        """Register function"""
        metadata = FunctionMetadata(
            name=name,
            func=func,
            engine=engine,
            version=version,
            author=kwargs.get('author'),
            tags=kwargs.get('tags', [])
        )
        
        self.functions[name].append(metadata)
        print(f"📝 Registered {name} (engine={engine}, v{version})")
    
    def get(self, name: str, engine: str = "universal") -> Optional[Callable]:
        """Get function by name and engine"""
        candidates = self.functions.get(name, [])
        
        # Try exact engine match
        for meta in candidates:
            if meta.engine == engine:
                return meta.func
        
        # Fallback to universal
        for meta in candidates:
            if meta.engine == "universal":
                return meta.func
        
        return None
    
    def find_by_tag(self, tag: str) -> List[FunctionMetadata]:
        """Find functions by tag"""
        results = []
        for metas in self.functions.values():
            for meta in metas:
                if tag in meta.tags:
                    results.append(meta)
        return results
    
    def list_all(self):
        """List all registered functions"""
        for name, metas in self.functions.items():
            for meta in metas:
                print(f"  • {name} ({meta.engine}, v{meta.version}) - {meta.tags}")

# Global registry
REGISTRY = FunctionRegistry()

# Decorator
def register_function(name: str, **kwargs):
    """Decorator to register function"""
    def decorator(func):
        REGISTRY.register(name, func, **kwargs)
        return func
    return decorator
```

### Test It

```python
# test_phase6.py - Function Registry
from mini_framework import REGISTRY, register_function
import pandas as pd

# Register functions
@register_function("calculate_tax", engine="pandas", version="1.0", tags=["finance"])
def calc_tax_pandas(df):
    df['tax'] = df['price'] * 0.08
    return df

@register_function("calculate_tax", engine="spark", version="1.0", tags=["finance"])
def calc_tax_spark(df):
    # (Would use PySpark)
    pass

@register_function("format_currency", engine="universal", tags=["formatting"])
def format_currency(df):
    df['price_formatted'] = df['price'].apply(lambda x: f"${x:.2f}")
    return df

# Discover functions
print("\n📚 Registered Functions:")
REGISTRY.list_all()

print("\n🔍 Finance Functions:")
finance_funcs = REGISTRY.find_by_tag("finance")
for func in finance_funcs:
    print(f"  • {func.name} ({func.engine})")

# Use registered function
data = pd.DataFrame({'price': [10.99, 25.50]})

# Get pandas version
tax_func = REGISTRY.get("calculate_tax", engine="pandas")
result = tax_func(data)

print("\n💰 Result:")
print(result)

print("✅ Phase 6 complete!")
```

### Reflection Questions (Phase 6)

1. **Why support engine variants?**
   - <details><summary>Your answer...</summary>
     Same logical transformation, different implementations (Pandas for local, Spark for big data). Registry resolves correct version.
     </details>

2. **Decorator pattern:** What does `@register_function` do?
   - <details><summary>Answer</summary>
     Wraps function to auto-register it when defined. Cleaner than manual `REGISTRY.register()` calls.
     </details>

3. **Compare with odibi_de_v2:**
   - Real: [odibi_functions/registry.py](file:///d:/projects/odibi_de_v2/odibi_de_v2/odibi_functions/registry.py)
   - Has module-based organization
   - Supports function overloading
   - Can persist to database

---

## Success Criteria

### Checklist

Your framework should support:

- ✅ **Phase 1:** Basic read-transform-write
- ✅ **Phase 2:** Multiple data formats (CSV, JSON, Parquet)
- ✅ **Phase 3:** Transformation chaining with error tracking
- ✅ **Phase 4:** Multi-layer orchestration with DAG
- ✅ **Phase 5:** Lifecycle hooks for observability
- ✅ **Phase 6:** Function registry with metadata

### Integration Test

```python
# final_test.py - Complete Integration Test
import pandas as pd
from mini_framework import (
    Pipeline, Orchestrator, LayerConfig, 
    HookManager, REGISTRY, register_function
)

# 1. Register functions
@register_function("clean_data", engine="pandas", tags=["quality"])
def clean(df):
    return df.dropna().drop_duplicates()

@register_function("aggregate", engine="pandas", tags=["analytics"])
def agg(df):
    return df.groupby('category').agg({'value': 'sum'}).reset_index()

# 2. Setup hooks
hooks = HookManager()
hooks.register('post_transform', lambda **ctx: print(f"  ✓ Transformed {len(ctx['df'])} rows"))

# 3. Create data
bronze = pd.DataFrame({
    'category': ['A', 'B', 'A', 'B', 'A'],
    'value': [10, 20, 10, 30, 15]  # Has duplicate
})
bronze.to_csv('final_bronze.csv', index=False)

# 4. Build orchestrator
orch = Orchestrator()
orch.add_layer(LayerConfig(
    name="Bronze",
    depends_on=[],
    input_path="final_bronze.csv",
    output_path="final_silver.csv",
    transformations=[("Clean", REGISTRY.get("clean_data", "pandas"))]
))
orch.add_layer(LayerConfig(
    name="Gold",
    depends_on=["Bronze"],
    input_path="final_silver.csv",
    output_path="final_gold.csv",
    transformations=[("Aggregate", REGISTRY.get("aggregate", "pandas"))]
))

# 5. Run
results = orch.run()

print("\n🏆 Final Gold Output:")
print(results['Gold']['result'])

# Expected:
#   category  value
# 0        A     35
# 1        B     50

print("\n✅ ALL PHASES COMPLETE!")
print("🎓 You've built a data engineering framework from scratch!")
```

---

## Reflection Questions

### First Principles Thinking

1. **What is the essence of a data framework?**
   - <details><summary>Your answer...</summary>
     Systematic way to: (1) read data, (2) transform it, (3) write results. Everything else is optimization or observability.
     </details>

2. **What did you learn about abstraction?**
   - Too little: Hard to extend, lots of duplication
   - Too much: Complex, hard to understand
   - Balance: Start simple, add abstractions as patterns emerge

3. **Which phase was hardest? Why?**
   - Phase 4 (Orchestration)? Topological sort is non-trivial
   - Phase 5 (Hooks)? Event-driven programming requires different thinking
   - Phase 6 (Registry)? Metadata management adds complexity

4. **What would you build differently?**
   - Configuration over code?
   - Async execution?
   - Type safety with Pydantic?

### Design Trade-offs

1. **Simplicity vs. Features**
   - Your framework: ~350 lines, core features only
   - odibi_de_v2: ~10,000 lines, production-grade
   - Trade-off: When to add complexity?

2. **Coupling vs. Cohesion**
   - How tightly are components coupled?
   - Could you swap out Pipeline with different implementation?
   - Could you use HookManager independently?

3. **Performance vs. Readability**
   - Where did you optimize for performance?
   - Where did you prioritize readability?
   - What would you change for 1B+ row datasets?

---

## Comparison with Full Framework

### Architecture Comparison

| Component | Your Framework | odibi_de_v2 |
|-----------|----------------|-------------|
| **Reader** | `DataReader` ABC | [core/reader.py](file:///d:/projects/odibi_de_v2/odibi_de_v2/core/reader.py) + factories |
| **Writer** | `DataWriter` ABC | [core/saver.py](file:///d:/projects/odibi_de_v2/odibi_de_v2/core/saver.py) + factories |
| **Transformation** | `TransformationChain` | [transformer/](file:///d:/projects/odibi_de_v2/odibi_de_v2/transformer/) + SparkWorkflowNode |
| **Orchestration** | `Orchestrator` | [orchestration/generic_orchestrator.py](file:///d:/projects/odibi_de_v2/odibi_de_v2/orchestration/generic_orchestrator.py) |
| **Hooks** | `HookManager` | [orchestration/hooks.py](file:///d:/projects/odibi_de_v2/odibi_de_v2/orchestration/hooks.py) |
| **Registry** | `FunctionRegistry` | [odibi_functions/registry.py](file:///d:/projects/odibi_de_v2/odibi_de_v2/odibi_functions/registry.py) |
| **Tracking** | `TransformationResult` | [TransformationTracker](file:///d:/projects/odibi_de_v2/odibi_de_v2/transformer/spark/transformation_tracker.py) |
| **Config** | `LayerConfig` | [ProjectManifest](file:///d:/projects/odibi_de_v2/odibi_de_v2/project/manifest.py) + manifest.json |

### What's Missing from Your Framework?

**Production Features:**
1. ✅ **Dual Engine Support**: Spark + Pandas
2. ✅ **Connection Management**: Azure, SQL, S3 connectors
3. ✅ **Retry Logic**: Automatic retries on transient failures
4. ✅ **Caching Strategy**: Intelligent caching of expensive operations
5. ✅ **Schema Validation**: Enforce schemas with decorators
6. ✅ **Metadata Tracking**: Delta Lake lineage logging
7. ✅ **Project Scaffolding**: `initialize_project()` boilerplate generation
8. ✅ **Configuration Management**: Environment-based configs
9. ✅ **Testing Utilities**: Test fixtures and mocks
10. ✅ **Documentation**: Comprehensive docstrings and examples

### Key Insights from Comparison

1. **Factories Everywhere**: Real framework uses factory pattern extensively for readers/savers/transformers
2. **Dual Engine**: Abstraction over Spark/Pandas is non-trivial (see `engine="pandas"` vs `engine="spark"`)
3. **Configuration-Driven**: Manifest.json centralizes all configuration
4. **Defensive Programming**: Extensive validation, error handling, logging
5. **Composability**: Every component can be used standalone or composed

---

## What Next?

### Extend Your Framework

1. **Add Spark Support**: Implement `SparkReader`, `SparkWriter`
2. **Add Retry Logic**: Decorator for automatic retries
3. **Add Caching**: LRU cache for expensive transformations
4. **Add Config Files**: YAML/JSON config for pipelines
5. **Add CLI**: `python mini_framework.py run --config pipeline.yaml`

### Contribute to odibi_de_v2

Now that you understand the architecture:
1. Fix bugs you encounter
2. Add new connectors (S3, Snowflake)
3. Improve documentation
4. Create new tutorials

### Build Your Own Project

Apply what you learned:
1. Use odibi_de_v2 for production project
2. Extend with domain-specific transformations
3. Build custom hooks for your observability stack
4. Create reusable function library

---

## Congratulations! 🎉

You've rebuilt a data engineering framework from first principles. You now understand:

- ✅ **Architectural patterns** (Strategy, Observer, Factory, Fluent Interface)
- ✅ **Framework design** (abstraction layers, extensibility, composability)
- ✅ **Production concerns** (error handling, observability, configuration)
- ✅ **Trade-offs** (simplicity vs. features, coupling vs. cohesion)

**You're ready to:**
- Master odibi_de_v2 advanced features
- Build production data pipelines
- Design your own frameworks
- Contribute to open source

---

## Related Documentation

- [07 - Project Structure](file:///d:/projects/odibi_de_v2/docs/learn_odibi_de/07_Project_Structure_and_Self_Bootstrap.md)
- [08 - Tutorials](file:///d:/projects/odibi_de_v2/docs/learn_odibi_de/08_Tutorials_and_Practice.md)
- [09 - Advanced Concepts](file:///d:/projects/odibi_de_v2/docs/learn_odibi_de/09_Advanced_Concepts.md)
- [System Overview](file:///d:/projects/odibi_de_v2/docs/00-SYSTEM_OVERVIEW.md)
- [Architecture Map](file:///d:/projects/odibi_de_v2/docs/ARCHITECTURE_MAP.md)

**Happy Building! 🚀**
