# Bug Fix: TransformationRunnerFromConfig

## Issue
The `TransformationRunnerFromConfig` was failing with:
```
'NoneType' object has no attribute 'startswith'
```

## Root Cause
The `TransformationRunnerFromConfig` was still querying the old `TransformationConfig` table instead of the new `TransformationRegistry` table. Additionally, it wasn't parsing the new JSON fields (inputs, constants, outputs).

## Solution
Updated `transformation_runner_from_config.py` to:

### 1. Query TransformationRegistry Table
Changed the `_fetch_configs()` method to query the new schema:

```python
SELECT
    transformation_id as id,
    project,
    environment as env,
    layer,
    entity_1 as plant,
    entity_2 as asset,
    module,
    function,
    inputs,      # JSON field
    constants,   # JSON field  
    outputs,     # JSON field
    enabled
FROM TransformationRegistry
```

### 2. Parse JSON Fields
Added JSON parsing for the new fields:

```python
config['inputs'] = json.loads(config['inputs']) if config.get('inputs') else []
config['constants'] = json.loads(config['constants']) if config.get('constants') else {}
config['outputs'] = json.loads(config['outputs']) if config.get('outputs') else []
```

### 3. Pass Data to Transformation Functions
Updated `_run_one()` to pass the parsed data to transformation functions:

```python
func_kwargs = {
    **self.kwargs,
    'inputs': cfg.get('inputs', []),
    'constants': cfg.get('constants', {}),
    'outputs': cfg.get('outputs', []),
    'spark': self.spark,
    'env': cfg.get('env', self.env)
}

func(**func_kwargs)
```

### 4. Backward Compatibility
Added fallback to legacy `TransformationConfig` table if `TransformationRegistry` doesn't exist:

```python
except Exception as e:
    # Fall back to legacy TransformationConfig table
    query = f"SELECT * FROM TransformationConfig WHERE..."
    # Convert legacy format to new format
    config['inputs'] = [config.get('input_table')] if config.get('input_table') else []
    config['constants'] = {}
    config['outputs'] = [{'table': config.get('target_table'), 'mode': 'overwrite'}]
```

## Testing
Run the updated transformer:

```python
from odibi_de_v2 import run_project

result = run_project(
    project="Energy Efficiency",
    env="qat",
    target_layers=["Silver_1"]
)
```

## Files Modified
- `odibi_de_v2/transformer/transformation_runner_from_config.py` - Completely rewritten to support TransformationRegistry

## Next Steps
1. Test with a single Silver layer first
2. Verify transformation functions receive the correct inputs, constants, and outputs
3. Run full pipeline once validated
