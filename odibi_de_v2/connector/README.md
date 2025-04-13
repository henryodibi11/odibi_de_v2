# utils Module
This module provides a standardized interface and pluggable implementations for accessing cloud-based storage systems such as Azure Blob Storage, with planned support for other providers like AWS S3, Google Cloud Storage, and more.
---

## Features
- **Abstract Base Class for Connectors**  
 All connectors inherit from `CloudConnector`, ensuring a consistent interface.
- **Framework-Aware Path Resolution**  
 Supports both Spark and Pandas path styles using the `Framework` enum.
- **Secure Configuration Handling**  
 Connector initialization requires explicit credentials, with input validation and structured error handling.
- **Standardized Logging**  
 All connectors use the centralized logger with `format_error()` and `ErrorType` for consistent, traceable messages.

---
## Modules
| Module                         | Description                                  |
|--------------------------------|----------------------------------------------|
| `azure_blob_connector.py`      | Concrete class: `AzureBlobConnector`         |

---

## Usage Example
```python
from connector import AzureBlobConnector
from core.enums import Framework
# Create connector
connector = AzureBlobConnector(
   account_name="myaccount",
   account_key="mykey"
)
# Get client
client = connector.get_connection()
# Generate Spark-style path
spark_path = connector.get_file_path(
   container="raw",
   blob_name="2024/03/file.json",
   framework=Framework.SPARK
)
# Get config for Spark
config = connector.get_framework_config(Framework.SPARK)
```