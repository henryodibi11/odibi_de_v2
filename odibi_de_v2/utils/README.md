# utils Module
The `utils/` module provides a suite of lightweight, framework-agnostic utility functions for use throughout the `odibi_de_v2` data engineering framework. These utilities help enforce consistency, reduce duplication, and enable environment-aware automation.
---
## Modules
| Module                | Description                                                                 |
|-----------------------|-----------------------------------------------------------------------------|
| `type_checks.py`      | Type and structure validation (e.g., is_empty_dict, is_boolean)             |
| `string_utils.py`     | String and column name transformations, normalization, and cleaning         |
| `file_utils.py`       | File path parsing, extension validation, and folder extraction              |
| `validation_utils.py` | Reusable schema and key validation for configs, dataframes, and formats     |
| `env_utils.py`        | Execution context detection (e.g., local vs. Databricks) and env access     |
---

## Usage Example
```python
from utils import (
   is_empty_dict,
   clean_column_name,
   get_file_extension,
   validate_required_keys,
   is_running_in_databricks,
)
assert is_empty_dict({}) is True
assert clean_column_name("Total Sales ($)") == "total_sales"
assert get_file_extension("data/file.csv") == "csv"
config = {"source": "sql", "format": "csv"}
assert validate_required_keys(config, ["source", "format"]) is True
if is_running_in_databricks():
   print("Running in Databricks environment.")
```