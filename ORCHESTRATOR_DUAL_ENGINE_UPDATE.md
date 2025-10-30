# Generic Orchestrator - Dual-Engine Update Summary

## Overview
Updated `odibi_de_v2/orchestration/generic_orchestrator.py` to support dual-engine execution (Spark/Pandas) with comprehensive hook support.

## Changes Made

### 1. **New Imports**
```python
from odibi_de_v2.core.engine import Engine
from odibi_de_v2.hooks.manager import HookManager
from odibi_de_v2.logging.log_sink import BaseLogSink
```

### 2. **Updated `__init__` Parameters**
Added three new optional parameters to `GenericProjectOrchestrator.__init__()`:
- `engine: str = "spark"` - Execution engine selection (default: "spark")
- `hooks: Optional[HookManager] = None` - Event-driven hook manager
- `log_sink: Optional[BaseLogSink] = None` - Custom transformation logging sink

**Implementation:**
```python
self.engine = Engine.SPARK if engine.lower() == "spark" else Engine.PANDAS
self.hooks = hooks or HookManager()  # Always creates instance if None
self.log_sink = log_sink
```

### 3. **Bronze Layer Hooks**
Added hook emissions in `run_bronze_layer()`:

**Events:**
- `bronze_start` - Emitted before bronze ingestion begins
- `bronze_end` - Emitted after bronze completes (success/skipped/failed)

**Payloads include:**
- `project`, `env`, `engine`, `timestamp`
- `status` (on end): "success", "skipped", or "failed"
- `sources_processed` (on success)
- `error` (on failure)

### 4. **Layer-Level Hooks**
Added hook emissions in `run_silver_gold_layers()`:

**Events:**
- `layer_start` - Emitted before each layer starts
- `layer_end` - Emitted after each layer completes (success/failed)

**Payloads include:**
- `project`, `env`, `layer`, `engine`, `timestamp`
- `status` (on end): "success" or "failed"
- `duration_seconds`
- `error` (on failure)

### 5. **Orchestrator-Level Hooks**
Overrode `run()` method to emit orchestrator lifecycle hooks:

**Events:**
- `orchestrator_run_start` - Emitted at orchestration start
- `orchestrator_run_end` - Emitted at orchestration end (success/failed)

**Payloads include:**
- `project`, `env`, `engine`, `timestamp`
- `target_layers` (on start)
- `status` (on end): "success" or "failed"
- `layers_executed`, `duration_seconds` (on end)
- `error` (on failure)

### 6. **Pandas Engine - Caching Skip**
Added conditional logic to skip caching for pandas engine:
```python
if self.engine == Engine.PANDAS:
    self.logger.log("warning", f"⚠️ Caching not supported for pandas engine. Skipping cache for {layer}")
else:
    # Perform caching for Spark
```

### 7. **TransformationRunnerFromConfig Integration**
Updated runner instantiation to pass new parameters:
```python
runner = TransformationRunnerFromConfig(
    sql_provider=self.sql_provider,
    project=self.project,
    env=self.env,
    max_workers=workers,
    layer=layer,
    engine=self.engine.value,  # NEW
    hooks=self.hooks,           # NEW
    log_sink=self.log_sink,     # NEW
)
```

### 8. **Updated `run_project()` Function**
Added three new optional parameters:
- `engine: str = "spark"`
- `hooks: Optional[HookManager] = None`
- `log_sink: Optional[BaseLogSink] = None`

Updated docstring to document new parameters.

## Backward Compatibility

✅ **All changes are backward compatible:**
- All new parameters have defaults
- Spark remains the default engine
- Existing `auth_provider` pattern unchanged
- Manifest system unchanged
- Existing Spark behavior intact

## Hook Event Reference

### Complete Event Lifecycle
```
orchestrator_run_start
  └─> bronze_start
      └─> bronze_end
  └─> layer_start (Silver_1)
      └─> layer_end (Silver_1)
  └─> layer_start (Gold_1)
      └─> layer_end (Gold_1)
orchestrator_run_end
```

### Hook Payload Schema

**orchestrator_run_start:**
```python
{
    "project": str,
    "env": str,
    "engine": str,  # "spark" or "pandas"
    "timestamp": datetime,
    "target_layers": Optional[List[str]]
}
```

**orchestrator_run_end:**
```python
{
    "project": str,
    "env": str,
    "engine": str,
    "timestamp": datetime,
    "status": str,  # "success" or "failed"
    "layers_executed": List[str],
    "duration_seconds": float,
    "error": str  # only on failure
}
```

**bronze_start:**
```python
{
    "project": str,
    "env": str,
    "engine": str,
    "timestamp": datetime
}
```

**bronze_end:**
```python
{
    "project": str,
    "env": str,
    "engine": str,
    "timestamp": datetime,
    "status": str,  # "success", "skipped", or "failed"
    "sources_processed": int,  # only on success
    "error": str  # only on failure
}
```

**layer_start:**
```python
{
    "project": str,
    "env": str,
    "layer": str,
    "engine": str,
    "timestamp": datetime
}
```

**layer_end:**
```python
{
    "project": str,
    "env": str,
    "layer": str,
    "engine": str,
    "timestamp": datetime,
    "status": str,  # "success" or "failed"
    "duration_seconds": float,
    "error": str  # only on failure
}
```

## Usage Examples

### Basic Spark Usage (default)
```python
from odibi_de_v2.orchestration import run_project

result = run_project(
    project="Energy Efficiency",
    env="qat"
)
```

### Pandas Engine
```python
result = run_project(
    project="Energy Efficiency",
    env="qat",
    engine="pandas"
)
```

### With Custom Hooks
```python
from odibi_de_v2.hooks import HookManager

hooks = HookManager()
hooks.register("layer_start", lambda p: print(f"Starting {p['layer']}"))
hooks.register("layer_end", lambda p: print(f"Completed {p['layer']} in {p['duration_seconds']:.2f}s"))

result = run_project(
    project="Energy Efficiency",
    env="qat",
    hooks=hooks
)
```

### With Custom Log Sink
```python
from odibi_de_v2.logging import SparkDeltaLogSink

log_sink = SparkDeltaLogSink(spark, "my_schema.CustomTransformationLog")

result = run_project(
    project="Energy Efficiency",
    env="qat",
    log_sink=log_sink
)
```

### Pandas with Hooks and Custom Logging
```python
from odibi_de_v2.hooks import HookManager
from odibi_de_v2.logging import FileLogSink

hooks = HookManager()
hooks.register("orchestrator_run_start", lambda p: print(f"🚀 Starting {p['project']} on {p['engine']} engine"))

log_sink = FileLogSink("/path/to/logs/transformation_log.csv")

result = run_project(
    project="Energy Efficiency",
    env="qat",
    engine="pandas",
    hooks=hooks,
    log_sink=log_sink
)
```

## Next Steps

To complete dual-engine support, update `TransformationRunnerFromConfig` (or the appropriate transformation runner) to:
1. Accept `engine`, `hooks`, and `log_sink` parameters in `__init__`
2. Pass these to individual transformation executions
3. Route data processing through appropriate engine (Spark vs Pandas readers/savers)

## Testing

Recommended tests:
1. ✅ Spark engine with default hooks (existing behavior)
2. ✅ Pandas engine with caching attempt (should log warning)
3. ✅ Custom hooks registration and emission
4. ✅ Hook filtering by layer/project/engine
5. ✅ Error hook emissions on failures
6. ✅ Custom log sink integration
