# TransformationRunnerFromConfig - Dual-Engine Update

## Summary

Updated `odibi_de_v2/transformer/transformation_runner_from_config.py` to support dual-engine execution (Spark/Pandas) with hooks, pluggable log sinks, and function registry resolution.

## Key Changes

### 1. **New Constructor Parameters**

```python
def __init__(
    self,
    sql_provider,
    project: str,
    env: str = "qat",
    log_level: str = "ERROR",
    max_workers: int = min(32, (os.cpu_count() or 1) + 4),
    layer: str = 'Silver',
    engine: str = "spark",              # NEW: Default "spark"
    hooks: Optional[HookManager] = None, # NEW: Optional hook manager
    log_sink: Optional[BaseLogSink] = None, # NEW: Pluggable log sink
    **kwargs
)
```

### 2. **Optional Spark Session**

- `self.spark` is now optional when `engine="pandas"`
- Only required for `engine="spark"` (raises `RuntimeError` if not found)
- Enables pandas-only pipelines without Spark dependency

### 3. **Pluggable Log Sink**

Replaced hardcoded Spark Delta logging with abstraction:

```python
# Before
self.spark.createDataFrame(data, schema=LOG_SCHEMA).write.saveAsTable(...)

# After
self.log_sink.write(records)
```

- Defaults to `SparkDeltaLogSink` when Spark is available
- Accepts custom log sinks (file, SQL, etc.)
- Returns early if no log sink configured

### 4. **Function Resolution via REGISTRY**

Added function resolution with fallback:

```python
# 1. Try REGISTRY first (engine-specific/universal functions)
func = REGISTRY.resolve(full_module, func_name, effective_engine)

# 2. Fallback to importlib for legacy functions
if func is None:
    module = importlib.import_module(full_module)
    func = getattr(module, func_name)
```

### 5. **ExecutionContext Support**

Functions can now accept optional `context` parameter:

```python
context = ExecutionContext(
    engine=Engine.SPARK or Engine.PANDAS,
    project=cfg['project'],
    env=cfg.get('env', self.env),
    spark=self.spark,
    sql_provider=self.sql_provider,
    logger=self.logger,
    hooks=self.hooks,
    extras={"plant": ..., "asset": ..., "layer": ...}
)

# Conditionally pass to functions that accept it
sig = inspect.signature(func)
if 'context' in sig.parameters:
    func_kwargs['context'] = context
```

### 6. **Per-Transformation Engine Override**

Transformations can override the default engine via `constants`:

```python
effective_engine = cfg.get('constants', {}).get('engine', self.engine.value)
```

Example config:
```json
{
  "transformation_id": "T001",
  "module": "my_module",
  "function": "my_func",
  "constants": {"engine": "pandas"}  // Override to pandas
}
```

### 7. **Hook Events**

Emits hooks at key lifecycle points:

- **`configs_loaded`**: After fetching transformation configs
- **`transform_start`**: Before executing a transformation
- **`transform_success`**: After successful execution
- **`transform_failure`**: After all retries exhausted
- **`transform_retry`**: On each retry attempt

Example hook registration:

```python
hooks = HookManager()
hooks.register("transform_start", lambda p: print(f"Starting {p['function']}"))

runner = TransformationRunnerFromConfig(
    sql_provider=provider,
    project="my_project",
    hooks=hooks
)
```

## Backward Compatibility

### ✅ Maintained

- **Default engine**: Still `"spark"` (same behavior as before)
- **Legacy functions**: Importlib fallback ensures old functions still work
- **Same kwargs pattern**: Still passes `inputs`, `constants`, `outputs`, `spark`, `env`
- **Same logging schema**: Records still have same structure (transformation_id, project, plant, asset, etc.)
- **Same retry logic**: Exponential backoff with `max_retries` unchanged
- **Same parallel execution**: ThreadPoolExecutor behavior unchanged

### 🆕 Opt-In Features

- **Pandas mode**: Set `engine="pandas"`
- **Custom hooks**: Pass `hooks=HookManager()` with registered callbacks
- **Custom log sink**: Pass `log_sink=FileLogSink(...)`
- **REGISTRY functions**: Register functions via `REGISTRY.register("name", "engine", func)`
- **ExecutionContext**: Functions can accept `context` kwarg for rich runtime info

## Example Usage

### Traditional (Backward Compatible)

```python
from pyspark.sql import SparkSession
from odibi_de_v2.core import SQLProvider

spark = SparkSession.builder.getOrCreate()
sql_provider = SQLProvider()

runner = TransformationRunnerFromConfig(
    sql_provider=sql_provider,
    project="energy_efficiency",
    env="dev",
    layer="Silver"
)
runner.run_parallel()
```

### Dual-Engine with Hooks

```python
from odibi_de_v2.hooks import HookManager
from odibi_de_v2.logging import FileLogSink

hooks = HookManager()
hooks.register("transform_start", lambda p: print(f"▶ {p['function']}"))
hooks.register("transform_success", lambda p: print(f"✓ {p['function']} ({p['duration_seconds']:.2f}s)"))

runner = TransformationRunnerFromConfig(
    sql_provider=sql_provider,
    project="analytics",
    env="prod",
    engine="pandas",  # Pandas mode
    hooks=hooks,
    log_sink=FileLogSink("/logs/transformations.csv")
)
runner.run_all()
```

## Migration Path

1. **No changes needed**: Existing code continues to work
2. **Opt-in to pandas**: Add `engine="pandas"` for pandas-only transformations
3. **Add hooks**: Incrementally add hooks for monitoring/debugging
4. **Register functions**: Migrate functions to REGISTRY for dual-engine support
5. **Accept context**: Update functions to accept `context` kwarg for rich runtime info

## Testing

Run existing tests to verify backward compatibility:

```bash
pytest odibi_de_v2/tests/transformer/test_transformation_runner_from_config.py
```
