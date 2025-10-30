# Hooks and Events - Pipeline Observability Made Simple

## What Are Hooks?

Think of hooks as **event listeners** for your data pipelines. Just like you can listen for button clicks in web apps, you can listen for pipeline events like:

- 📖 "Data just got read from bronze layer"
- 🔄 "Transformation is about to start"
- ✅ "Data was successfully saved to gold layer"
- ❌ "An error occurred during processing"

**Real-world analogy:** Hooks are like notifications on your phone. You don't need to constantly check your email app—it notifies you when something happens. Similarly, hooks notify your custom code when pipeline events occur.

---

## Why Hooks Matter

Without hooks, pipelines are **black boxes**. With hooks, you get:

### 1. **Observability**
```python
# Track how long each transformation takes
hooks.register("transform_start", start_timer)
hooks.register("transform_end", log_duration)
```

### 2. **Data Quality Gates**
```python
# Validate data after reading
def check_schema(payload):
    df = payload['df']
    if 'customer_id' not in df.columns:
        raise ValueError("Missing customer_id column!")

hooks.register("post_read", check_schema)
```

### 3. **Custom Metrics & Alerts**
```python
# Send metrics to monitoring system
def send_metrics(payload):
    rows = payload.get('rows_processed', 0)
    cloudwatch.put_metric('PipelineRows', rows)

hooks.register("post_save", send_metrics)
```

### 4. **Integration with External Systems**
```python
# Notify Slack when pipeline completes
def notify_slack(payload):
    slack.post_message(f"✅ Pipeline {payload['pipeline_name']} completed!")

hooks.register("pipeline_end", notify_slack)
```

---

## The 9 Standard Events

| Event | When It Fires | Typical Payload |
|-------|---------------|-----------------|
| `pre_read` | Before reading data from source | `{'table': 'bronze.raw', 'layer': 'bronze'}` |
| `post_read` | After data successfully read | `{'df': DataFrame, 'rows': 1000, 'layer': 'bronze'}` |
| `pre_transform` | Before transformation function runs | `{'function': 'clean_data', 'layer': 'silver'}` |
| `post_transform` | After transformation completes | `{'df': DataFrame, 'rows_in': 1000, 'rows_out': 950}` |
| `pre_save` | Before writing to destination | `{'table': 'gold.metrics', 'layer': 'gold'}` |
| `post_save` | After data successfully saved | `{'table': 'gold.metrics', 'rows': 950}` |
| `on_error` | When exception occurs | `{'error': Exception, 'stage': 'transform'}` |
| `pipeline_start` | At beginning of orchestration | `{'pipeline_name': 'energy_etl', 'engine': 'spark'}` |
| `pipeline_end` | At end of orchestration | `{'pipeline_name': 'energy_etl', 'duration': 120.5}` |

---

## Registering Hooks (Step-by-Step)

### Step 1: Import HookManager

```python
from odibi_de_v2.hooks import HookManager

# Create a hook manager instance
hooks = HookManager()
```

### Step 2: Define Your Callback Function

```python
def log_event(payload):
    """Simple logging callback."""
    print(f"[EVENT] {payload.get('event', 'unknown')}")
```

**Callback rules:**
- Must accept one parameter (the payload dict)
- Can read any data from payload
- Can raise exceptions to halt pipeline
- Return value is ignored

### Step 3: Register the Callback

```python
# Register for a specific event
hooks.register("pre_read", log_event)

# Now whenever pre_read fires, log_event will be called
```

### Step 4: Emit Events (Framework Does This)

```python
# The framework emits events automatically during pipeline execution
# But you can also emit manually for testing:

hooks.emit("pre_read", {
    "event": "pre_read",
    "table": "bronze.customer_data",
    "layer": "bronze"
})
# Output: [EVENT] pre_read
```

### Step 5: Pass HookManager to Pipeline Components

```python
from odibi_de_v2.orchestration import GenericOrchestrator

orchestrator = GenericOrchestrator(
    project_name="energy_efficiency",
    engine="spark",
    hooks=hooks  # ← Pass your configured hooks
)

# Now the orchestrator will emit events to your hooks!
orchestrator.run()
```

---

## Practical Example 1: Progress Logger

Track pipeline progress with timestamps:

```python
from odibi_de_v2.hooks import HookManager
from datetime import datetime

hooks = HookManager()

def progress_logger(payload):
    """Log pipeline progress with timestamps."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    event = payload.get('event', 'unknown')
    
    # Extract relevant info based on event type
    if event == 'pre_read':
        table = payload.get('table', 'unknown')
        print(f"[{timestamp}] 📖 Reading from {table}")
    
    elif event == 'post_read':
        rows = payload.get('rows', 0)
        print(f"[{timestamp}] ✓ Loaded {rows:,} rows")
    
    elif event == 'pre_transform':
        function = payload.get('function', 'unknown')
        print(f"[{timestamp}] 🔄 Running {function}")
    
    elif event == 'post_transform':
        rows_in = payload.get('rows_in', 0)
        rows_out = payload.get('rows_out', 0)
        print(f"[{timestamp}] ✓ Transformed {rows_in:,} → {rows_out:,} rows")
    
    elif event == 'post_save':
        table = payload.get('table', 'unknown')
        rows = payload.get('rows', 0)
        print(f"[{timestamp}] 💾 Saved {rows:,} rows to {table}")
    
    elif event == 'pipeline_end':
        duration = payload.get('duration', 0)
        print(f"[{timestamp}] 🎉 Pipeline completed in {duration:.2f}s")

# Register for all relevant events
for event in ['pre_read', 'post_read', 'pre_transform', 'post_transform', 'post_save', 'pipeline_end']:
    hooks.register(event, progress_logger)

# Output example:
# [2024-01-15 10:23:45] 📖 Reading from bronze.energy_raw
# [2024-01-15 10:23:47] ✓ Loaded 15,432 rows
# [2024-01-15 10:23:47] 🔄 Running clean_energy_data
# [2024-01-15 10:23:49] ✓ Transformed 15,432 → 15,201 rows
# [2024-01-15 10:23:49] 💾 Saved 15,201 rows to silver.energy_clean
# [2024-01-15 10:23:50] 🎉 Pipeline completed in 5.23s
```

---

## Practical Example 2: Data Quality Validator

Automatically validate data quality at key checkpoints:

```python
from odibi_de_v2.hooks import HookManager

hooks = HookManager()

def validate_bronze_data(payload):
    """
    Validate data quality after reading from bronze layer.
    Raises exception if validation fails (halts pipeline).
    """
    layer = payload.get('layer')
    
    # Only validate bronze layer
    if layer != 'bronze':
        return
    
    df = payload.get('df')
    if df is None:
        return
    
    # Check 1: No empty DataFrame
    row_count = df.count() if hasattr(df, 'count') and callable(df.count) else len(df)
    if row_count == 0:
        raise ValueError(f"❌ Bronze data validation failed: Empty DataFrame")
    
    # Check 2: Required columns present
    required_cols = ['id', 'timestamp', 'value']
    actual_cols = list(df.columns)
    missing = [col for col in required_cols if col not in actual_cols]
    
    if missing:
        raise ValueError(f"❌ Bronze data validation failed: Missing columns {missing}")
    
    # Check 3: No all-null columns
    for col in df.columns:
        null_count = df.filter(df[col].isNull()).count() if hasattr(df, 'filter') else df[col].isna().sum()
        total_count = df.count() if hasattr(df, 'count') and callable(df.count) else len(df)
        
        if null_count == total_count:
            raise ValueError(f"❌ Bronze data validation failed: Column '{col}' is all nulls")
    
    print(f"✓ Bronze validation passed: {row_count:,} rows, {len(actual_cols)} columns")

# Register validator to run after reading bronze data
hooks.register("post_read", validate_bronze_data)

# If validation fails, the pipeline will stop with a clear error message
```

---

## Practical Example 3: Performance Monitor

Track performance metrics and identify bottlenecks:

```python
from odibi_de_v2.hooks import HookManager
import time

hooks = HookManager()

# Store timing data
timers = {}

def start_timer(payload):
    """Record start time for operations."""
    operation = payload.get('function') or payload.get('table') or 'unknown'
    timers[operation] = {
        'start': time.time(),
        'payload': payload
    }

def end_timer(payload):
    """Calculate and log duration."""
    operation = payload.get('function') or payload.get('table') or 'unknown'
    
    if operation not in timers:
        return
    
    duration = time.time() - timers[operation]['start']
    
    # Log performance
    if duration > 60:
        print(f"⚠️  SLOW: {operation} took {duration:.2f}s")
    elif duration > 10:
        print(f"⏱️  {operation} took {duration:.2f}s")
    else:
        print(f"✓ {operation} took {duration:.2f}s")
    
    # Store for analysis
    timers[operation]['duration'] = duration

def print_performance_summary(payload):
    """Print summary at end of pipeline."""
    print("\n" + "="*80)
    print("PERFORMANCE SUMMARY")
    print("="*80)
    
    total_duration = 0
    operations = []
    
    for op, data in timers.items():
        if 'duration' in data:
            operations.append((op, data['duration']))
            total_duration += data['duration']
    
    # Sort by duration (slowest first)
    operations.sort(key=lambda x: x[1], reverse=True)
    
    print(f"\nTotal Pipeline Duration: {total_duration:.2f}s\n")
    print(f"{'Operation':<40} {'Duration':<15} {'% of Total':<15}")
    print("-"*80)
    
    for op, dur in operations:
        pct = (dur / total_duration * 100) if total_duration > 0 else 0
        print(f"{op:<40} {dur:>10.2f}s {pct:>12.1f}%")
    
    print("="*80 + "\n")

# Register timing hooks
hooks.register("pre_transform", start_timer)
hooks.register("post_transform", end_timer)
hooks.register("pre_save", start_timer)
hooks.register("post_save", end_timer)
hooks.register("pipeline_end", print_performance_summary)

# Example output:
# ================================================================================
# PERFORMANCE SUMMARY
# ================================================================================
# 
# Total Pipeline Duration: 45.67s
# 
# Operation                                Duration        % of Total     
# --------------------------------------------------------------------------------
# calculate_energy_metrics                    25.34s         55.5%
# aggregate_sales_data                        12.45s         27.3%
# validate_schema                              4.23s          9.3%
# deduplicate_records                          3.65s          8.0%
# ================================================================================
```

---

## Practical Example 4: Slack Notifier

Send real-time notifications to your team:

```python
from odibi_de_v2.hooks import HookManager
import requests
import os

hooks = HookManager()

SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')

def notify_pipeline_start(payload):
    """Send notification when pipeline starts."""
    pipeline_name = payload.get('pipeline_name', 'unknown')
    engine = payload.get('engine', 'unknown')
    
    message = {
        "text": f"🚀 Pipeline `{pipeline_name}` started",
        "attachments": [{
            "color": "#36a64f",
            "fields": [
                {"title": "Engine", "value": engine, "short": True},
                {"title": "Status", "value": "Running", "short": True}
            ]
        }]
    }
    
    if SLACK_WEBHOOK_URL:
        requests.post(SLACK_WEBHOOK_URL, json=message)

def notify_pipeline_success(payload):
    """Send success notification with stats."""
    pipeline_name = payload.get('pipeline_name', 'unknown')
    duration = payload.get('duration', 0)
    
    message = {
        "text": f"✅ Pipeline `{pipeline_name}` completed successfully!",
        "attachments": [{
            "color": "#36a64f",
            "fields": [
                {"title": "Duration", "value": f"{duration:.2f}s", "short": True},
                {"title": "Status", "value": "Success", "short": True}
            ]
        }]
    }
    
    if SLACK_WEBHOOK_URL:
        requests.post(SLACK_WEBHOOK_URL, json=message)

def notify_error(payload):
    """Send error notification with details."""
    error = payload.get('error', 'Unknown error')
    stage = payload.get('stage', 'unknown')
    pipeline_name = payload.get('pipeline_name', 'unknown')
    
    message = {
        "text": f"❌ Pipeline `{pipeline_name}` FAILED",
        "attachments": [{
            "color": "#ff0000",
            "fields": [
                {"title": "Stage", "value": stage, "short": True},
                {"title": "Error", "value": str(error), "short": False}
            ]
        }]
    }
    
    if SLACK_WEBHOOK_URL:
        requests.post(SLACK_WEBHOOK_URL, json=message)

# Register Slack hooks
hooks.register("pipeline_start", notify_pipeline_start)
hooks.register("pipeline_end", notify_pipeline_success)
hooks.register("on_error", notify_error)
```

---

## Filtering Hooks by Project, Layer, and Engine

### Why Filter?

You might want different hooks for different scenarios:
- Bronze layer → Validation hooks
- Silver layer → Transformation metrics
- Gold layer → Business KPI alerts
- Spark engine → Performance tuning
- Specific project → Custom business logic

### How to Filter

Use the `filters` parameter when registering:

```python
from odibi_de_v2.hooks import HookManager

hooks = HookManager()

# Hook that ONLY runs for bronze layer
def bronze_logger(payload):
    print(f"Bronze layer: {payload.get('table')}")

hooks.register(
    "post_read",
    bronze_logger,
    filters={"layer": "bronze"}
)

# Hook that ONLY runs for silver layer
def silver_validator(payload):
    print("Validating silver layer data quality")

hooks.register(
    "post_transform",
    silver_validator,
    filters={"layer": "silver"}
)

# Hook that ONLY runs for Spark engine
def spark_optimizer(payload):
    print("Applying Spark broadcast joins")

hooks.register(
    "pre_transform",
    spark_optimizer,
    filters={"engine": "spark"}
)

# Hook with multiple filters (all must match)
def energy_project_gold_layer_hook(payload):
    print("Energy project gold layer processing")

hooks.register(
    "post_save",
    energy_project_gold_layer_hook,
    filters={
        "project": "energy_efficiency",
        "layer": "gold"
    }
)
```

### Filter Matching Behavior

**All filters must match for hook to fire:**

```python
# Hook requires BOTH project AND layer to match
hooks.register(
    "post_transform",
    my_callback,
    filters={"project": "energy", "layer": "silver"}
)

# ✓ Fires (both match)
hooks.emit("post_transform", {"project": "energy", "layer": "silver"})

# ✗ Does NOT fire (layer doesn't match)
hooks.emit("post_transform", {"project": "energy", "layer": "bronze"})

# ✗ Does NOT fire (project doesn't match)
hooks.emit("post_transform", {"project": "sales", "layer": "silver"})
```

### Complete Filtering Example

```python
from odibi_de_v2.hooks import HookManager

hooks = HookManager()

# 1. Universal hook (no filters) - fires for ALL events
def universal_logger(payload):
    print(f"[ALL] Event: {payload.get('event')}")

hooks.register("post_transform", universal_logger)

# 2. Layer-specific hooks
def bronze_hook(payload):
    print("[BRONZE] Raw data ingested")

def silver_hook(payload):
    print("[SILVER] Business logic applied")

def gold_hook(payload):
    print("[GOLD] Analytics-ready data")

hooks.register("post_save", bronze_hook, filters={"layer": "bronze"})
hooks.register("post_save", silver_hook, filters={"layer": "silver"})
hooks.register("post_save", gold_hook, filters={"layer": "gold"})

# 3. Engine-specific hooks
def spark_performance_hook(payload):
    print("[SPARK] Checking partition count")

def pandas_memory_hook(payload):
    print("[PANDAS] Checking memory usage")

hooks.register("pre_transform", spark_performance_hook, filters={"engine": "spark"})
hooks.register("pre_transform", pandas_memory_hook, filters={"engine": "pandas"})

# 4. Project-specific hooks
def energy_custom_validation(payload):
    print("[ENERGY] Validating OEE metrics")

hooks.register("post_transform", energy_custom_validation, filters={"project": "energy_efficiency"})

# Test different scenarios
print("\nScenario 1: Bronze layer + Spark engine")
hooks.emit("post_save", {"layer": "bronze", "engine": "spark", "event": "post_save"})
# Output:
# [BRONZE] Raw data ingested

print("\nScenario 2: Silver layer + Energy project")
hooks.emit("post_transform", {
    "layer": "silver",
    "project": "energy_efficiency",
    "event": "post_transform"
})
# Output:
# [ALL] Event: post_transform
# [ENERGY] Validating OEE metrics

print("\nScenario 3: Pandas engine pre-transform")
hooks.emit("pre_transform", {"engine": "pandas", "event": "pre_transform"})
# Output:
# [PANDAS] Checking memory usage
```

---

## Hook Payload Structures

Each event type has a standard payload structure. Here's what to expect:

### pre_read / post_read

```python
{
    "event": "pre_read",  # or "post_read"
    "table": "bronze.energy_raw",
    "layer": "bronze",
    "project": "energy_efficiency",
    "engine": "spark",
    "df": <DataFrame>,  # Only in post_read
    "rows": 15432,      # Only in post_read
    "schema": {...}     # Only in post_read (optional)
}
```

### pre_transform / post_transform

```python
{
    "event": "pre_transform",  # or "post_transform"
    "function": "calculate_energy_metrics",
    "layer": "silver",
    "project": "energy_efficiency",
    "engine": "spark",
    "params": {"group_by_cols": ["facility"]},
    "df": <DataFrame>,       # Input DataFrame (pre) or Output DataFrame (post)
    "rows_in": 15432,        # Only in post_transform
    "rows_out": 1543,        # Only in post_transform
    "duration": 12.34        # Only in post_transform (optional)
}
```

### pre_save / post_save

```python
{
    "event": "pre_save",  # or "post_save"
    "table": "gold.energy_metrics",
    "layer": "gold",
    "project": "energy_efficiency",
    "engine": "spark",
    "df": <DataFrame>,  # Only in pre_save
    "rows": 1543,
    "mode": "overwrite"  # or "append"
}
```

### on_error

```python
{
    "event": "on_error",
    "error": <Exception>,
    "error_message": "Division by zero",
    "stage": "transform",  # or "read", "save", etc.
    "pipeline_name": "energy_efficiency",
    "layer": "silver",
    "traceback": "..."  # Full traceback string
}
```

### pipeline_start / pipeline_end

```python
{
    "event": "pipeline_start",  # or "pipeline_end"
    "pipeline_name": "energy_efficiency",
    "engine": "spark",
    "config": {...},
    "timestamp": "2024-01-15T10:23:45",
    "duration": 45.67  # Only in pipeline_end (seconds)
}
```

---

## Integration with Story System (Visualization)

Hooks are the perfect integration point for the **Story** system (your visual pipeline monitor):

```python
from odibi_de_v2.hooks import HookManager
import json

hooks = HookManager()

# Story events collector
story_events = []

def collect_story_event(payload):
    """Collect events for Story visualization."""
    story_event = {
        "timestamp": payload.get('timestamp', datetime.now().isoformat()),
        "event_type": payload.get('event'),
        "layer": payload.get('layer'),
        "details": {
            "function": payload.get('function'),
            "rows": payload.get('rows') or payload.get('rows_out'),
            "table": payload.get('table'),
            "duration": payload.get('duration')
        }
    }
    story_events.append(story_event)

# Register for all events
for event in ['pre_read', 'post_read', 'pre_transform', 'post_transform', 'pre_save', 'post_save']:
    hooks.register(event, collect_story_event)

def export_story_data(payload):
    """Export collected events to Story system."""
    pipeline_name = payload.get('pipeline_name')
    
    # Save to JSON for Story UI
    story_data = {
        "pipeline": pipeline_name,
        "events": story_events,
        "summary": {
            "total_duration": payload.get('duration'),
            "total_events": len(story_events),
            "status": "success"
        }
    }
    
    with open(f"story_data/{pipeline_name}_events.json", "w") as f:
        json.dump(story_data, f, indent=2)
    
    print(f"✓ Story data exported: {len(story_events)} events")

hooks.register("pipeline_end", export_story_data)
```

---

## Common Mistakes

### Mistake 1: Modifying Payload (Don't!)

```python
# ❌ BAD: Modifying the payload
def bad_hook(payload):
    payload['df'] = payload['df'].filter(...)  # Don't modify!
    payload['new_key'] = 'value'  # Don't add keys!

# ✓ GOOD: Read-only access
def good_hook(payload):
    df = payload.get('df')
    row_count = df.count() if df else 0
    print(f"Processing {row_count} rows")
```

### Mistake 2: Forgetting to Handle Missing Keys

```python
# ❌ BAD: Assuming keys exist
def bad_hook(payload):
    df = payload['df']  # KeyError if 'df' not in payload!

# ✓ GOOD: Use .get() with defaults
def good_hook(payload):
    df = payload.get('df')
    if df is None:
        return
    # Now safe to use df
```

### Mistake 3: Slow Hooks Blocking Pipeline

```python
# ❌ BAD: Slow synchronous operation
def bad_hook(payload):
    time.sleep(10)  # Pipeline waits 10 seconds!
    send_email(payload)

# ✓ GOOD: Async or quick operations
def good_hook(payload):
    # Quick: just log to queue
    event_queue.put(payload)
    
# Or use threading for long operations
import threading

def async_hook(payload):
    def send_notification():
        send_email(payload)  # Runs in background
    
    thread = threading.Thread(target=send_notification)
    thread.start()
```

### Mistake 4: Not Filtering When You Should

```python
# ❌ BAD: Hook runs for ALL layers (inefficient)
def validate_gold_data(payload):
    if payload.get('layer') != 'gold':
        return  # Skipping manually
    # ... validation logic

# ✓ GOOD: Use filters
hooks.register(
    "post_save",
    validate_gold_data,
    filters={"layer": "gold"}
)
```

---

## Pro Tips

### Tip 1: Chain Hooks for Complex Logic

```python
# Break complex logic into focused hooks
def hook1_validate(payload):
    """Step 1: Validate schema"""
    pass

def hook2_enrich(payload):
    """Step 2: Add metadata"""
    pass

def hook3_export(payload):
    """Step 3: Export metrics"""
    pass

# Register in order (they run in registration order)
hooks.register("post_read", hook1_validate)
hooks.register("post_read", hook2_enrich)
hooks.register("post_read", hook3_export)
```

### Tip 2: Use Hooks for A/B Testing

```python
import random

def ab_test_hook(payload):
    """Route 10% of traffic to experimental function."""
    if random.random() < 0.10:
        payload['function_override'] = 'experimental_transform_v2'

hooks.register("pre_transform", ab_test_hook, filters={"layer": "silver"})
```

### Tip 3: Debug with list_hooks()

```python
# See all registered hooks
registered = hooks.list_hooks()

for event, hook_list in registered.items():
    print(f"\n{event}:")
    for hook in hook_list:
        print(f"  - {hook['callback'].__name__}")
        if hook['filters']:
            print(f"    Filters: {hook['filters']}")
```

### Tip 4: Create Hook Libraries

```python
# hooks_library.py
from odibi_de_v2.hooks import HookManager

def setup_monitoring_hooks(hooks: HookManager):
    """Standard monitoring hooks for all projects."""
    hooks.register("pipeline_start", log_start)
    hooks.register("pipeline_end", log_end)
    hooks.register("on_error", send_alert)

def setup_quality_hooks(hooks: HookManager, layer: str):
    """Quality checks for specific layer."""
    hooks.register("post_read", validate_schema, filters={"layer": layer})
    hooks.register("post_transform", check_nulls, filters={"layer": layer})

# Usage in your pipeline
from hooks_library import setup_monitoring_hooks, setup_quality_hooks

hooks = HookManager()
setup_monitoring_hooks(hooks)
setup_quality_hooks(hooks, layer="silver")
```

---

## Mini-Challenge Exercises

### Exercise 1: Row Counter (Easy)

Create a hook that counts total rows processed across all transformations:

```python
from odibi_de_v2.hooks import HookManager

hooks = HookManager()
total_rows = {"count": 0}

# Your code here
def count_rows(payload):
    # TODO: Add rows_out from payload to total_rows
    pass

hooks.register("post_transform", count_rows)

# Test it
hooks.emit("post_transform", {"rows_out": 100})
hooks.emit("post_transform", {"rows_out": 250})
assert total_rows["count"] == 350  # Should pass
```

<details>
<summary>Solution</summary>

```python
def count_rows(payload):
    rows = payload.get('rows_out', 0)
    total_rows["count"] += rows
    print(f"Processed {rows} rows (Total: {total_rows['count']})")
```
</details>

### Exercise 2: Error Rate Tracker (Medium)

Track error rates and send alert if > 5%:

```python
hooks = HookManager()
stats = {"success": 0, "errors": 0}

# Your code here - create two hooks:
# 1. Track successes on "post_transform"
# 2. Track errors on "on_error"
# 3. Alert if error_rate > 5%

def track_success(payload):
    # TODO
    pass

def track_error(payload):
    # TODO
    pass
```

<details>
<summary>Solution</summary>

```python
def track_success(payload):
    stats["success"] += 1

def track_error(payload):
    stats["errors"] += 1
    total = stats["success"] + stats["errors"]
    error_rate = (stats["errors"] / total) * 100 if total > 0 else 0
    
    if error_rate > 5:
        print(f"🚨 ALERT: Error rate is {error_rate:.1f}%")

hooks.register("post_transform", track_success)
hooks.register("on_error", track_error)
```
</details>

### Exercise 3: Custom Metric Exporter (Hard)

Export custom metrics to JSON file every 10 events:

```python
hooks = HookManager()
metrics = []

# Your code here
def collect_metric(payload):
    # TODO: Collect event data
    # TODO: Export to metrics.json every 10 events
    pass
```

<details>
<summary>Solution</summary>

```python
import json

def collect_metric(payload):
    metric = {
        "event": payload.get('event'),
        "layer": payload.get('layer'),
        "rows": payload.get('rows') or payload.get('rows_out'),
        "timestamp": datetime.now().isoformat()
    }
    metrics.append(metric)
    
    # Export every 10 events
    if len(metrics) % 10 == 0:
        with open('metrics.json', 'w') as f:
            json.dump(metrics, f, indent=2)
        print(f"✓ Exported {len(metrics)} metrics to metrics.json")

# Register for multiple events
for event in ['post_read', 'post_transform', 'post_save']:
    hooks.register(event, collect_metric)
```
</details>

---

## Next Steps

Congratulations! You now understand the full power of hooks. Next:

1. **Combine Functions + Hooks** - Build observable, reusable pipelines
2. **[Tutorial 03 - Hooks & Observability](../tutorials/03-hooks-observability-tutorial.ipynb)** - Interactive examples
3. **Explore [hooks/manager.py](../../odibi_de_v2/hooks/manager.py)** - Read the source code

**Build smarter pipelines! 🎯**
