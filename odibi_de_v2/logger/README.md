# logger Module
The `logger/` module provides a suite of lightweight, framework-agnostic utility functions for use throughout the `odibi_de_v2` data engineering framework. These utilities help enforce consistency, reduce duplication, and enable environment-aware automation.
---
## Modules
| Module                | Description                                                     |
|-----------------------|-----------------------------------------------------------------|
| `dynamic_logger.py`   | Core logger logic with metadata injection and handler setup     |
| `log_singleton.py`    | Singleton wrapper around the logger                             |
| `metadata_manager.py` | Stores and updates runtime metadata                             |
| `capturing_handler.py`| In-memory handler for capturing log messages                    |
| `log_utils.py`        | Convenience methods: `log_info`, `log_error`, etc.              |
| `error_utils.py`      | Error formatting helper using `ErrorType` enum                  |
---

## Example Usage
```python
from logger import (
   log_info,
   log_error,
   get_logger,
   format_error,
   MetadataManager
)
from core.enums import ErrorType
# Step 1: Set up metadata
manager = MetadataManager()
manager.update_metadata(project="OEE", operation="ingest")
# Step 2: Initialize logger with metadata (optional if already set)
logger = get_logger(manager)
# Step 3: Log a success message
log_info(format_error(
   module="INGESTION",
   component="CsvReader",
   method="load",
   error_type=ErrorType.NO_ERROR,
   message="CSV file loaded successfully"
))
# Step 4: Log an error message
log_error(format_error(
   module="INGESTION",
   component="CsvReader",
   method="load",
   error_type=ErrorType.FILE_NOT_FOUND,
   message="Path '/data/missing.csv' does not exist"
))
# Step 5: Access captured logs (e.g. for test assertions or UI display)
print(logger.get_logs())
```