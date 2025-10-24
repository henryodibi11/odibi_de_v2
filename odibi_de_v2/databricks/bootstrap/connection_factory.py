"""
odibi_de_v2.databricks.bootstrap.build_connection_from_config
-------------------------------------------------------------

Centralized factory for building connection objects from a standardized
configuration dictionary.

This class powers ingestion and transformation workflows by interpreting
metadata from configuration tables (e.g., `IngestionSourceConfig`,
`TransformationConfig`) and instantiating the correct connector:
SQL, ADLS, or Local.

Design Philosophy
-----------------
Every ingestion or transformation in odibi_de_v2 starts with a **configuration-first**
approach. Instead of manually creating connectors in code, metadata tables
describe *what* to connect to, and this factory decides *how* to connect.

Supported Platforms
-------------------
| Platform | Connection Method            | Returns                        |
|-----------|-----------------------------|---------------------------------|
| sql       | init_sql_config_connection  | SQLDatabaseConnection           |
| adls      | init_spark_with_azure_secrets | AzureBlobConnection (SparkSession) |
| local     | init_local_connection       | LocalConnection                 |

Each platform type is inferred from either:
    - `source_type` / `target_type` fields (e.g., "sql", "adls", "local")
    - Whether the row contains `source_name` or `target_name`

Example Configuration
---------------------
```python
config = {
    "source_name": "calendar_source",
    "source_type": "local",
    "secret_scope": "azure_scope",
    "server": "<database_name>.database.windows.net",
    "identifier_key": "sql_username",
    "credential_key": "sql_password",
    "connection_config": {
        "database": "<database_name>",
        "base_path": "/dbfs/FileStore"
    }
}
```

Example Usage
-------------
```python
from odibi_de_v2.databricks.bootstrap.build_connection_from_config import BuildConnectionFromConfig

builder = BuildConnectionFromConfig(config)

# Dynamically selects correct connection type
connection = builder.get_connection()

print(type(connection))
# → <class 'odibi_de_v2.connector.LocalConnection'>
```

Behavior Summary
----------------
| Step | Action |
|------|---------|
| 1️⃣ | Determine if config refers to a source or target. |
| 2️⃣ | Identify platform type (`sql`, `adls`, or `local`). |
| 3️⃣ | Invoke appropriate initializer function. |
| 4️⃣ | Return a ready-to-use connection object. |
"""

from odibi_de_v2.databricks.bootstrap.init_spark_with_azure_secrets import init_spark_with_azure_secrets
from odibi_de_v2.databricks.bootstrap.init_sql_config_connection import init_sql_config_connection
from odibi_de_v2.databricks.bootstrap.local_initializer import init_local_connection


class BuildConnectionFromConfig:
    """Factory for creating connection objects from configuration metadata."""

    def __init__(self, config: dict):
        """Initializes connection context based on whether configuration refers to a source or target system."""
        self.config = config
        self.is_source = "source_name" in config
        self.name_field = "source_name" if self.is_source else "target_name"
        self.platform = self.config.get(
            "source_type" if self.is_source else "target_type", ""
        ).lower()

    def get_connection(self):
        """Determines the correct connection type and builds it."""
        match self.platform:
            case "sql":
                return self._build_sql_connection()
            case "adls":
                return self._build_adls_connection()
            case "local":
                return self._build_local_connection()
            case _:
                raise Exception(f"Unsupported platform: {self.platform}")

    def _build_sql_connection(self):
        """Initializes and returns a SQLDatabaseConnection."""
        cfg = self.config
        conn_cfg = cfg.get("connection_config", {})
        return init_sql_config_connection(
            secret_scope=cfg["secret_scope"],
            host_key=cfg["server"],
            database=conn_cfg.get("database"),
            user_key=cfg["identifier_key"],
            password_key=cfg["credential_key"]
        )

    def _build_adls_connection(self):
        """Initializes and returns an AzureBlobConnection with SparkSession."""
        cfg = self.config
        _, connection = init_spark_with_azure_secrets(
            app_name="ADLS_Ingestion",
            secret_scope=cfg["secret_scope"],
            account_name_key=cfg["identifier_key"],
            account_key_key=cfg["credential_key"],
            logger_metadata={
                "project": cfg.get("project"),
                "table": cfg.get(self.name_field),
                "step": "INGESTION"
            }
        )
        return connection

    def _build_local_connection(self):
        """Initializes and returns a LocalConnection for file-based ingestion."""
        conn_cfg = self.config.get("connection_config", {})
        base_path = conn_cfg.get("base_path", "/dbfs/FileStore")
        _, connection = init_local_connection(
            app_name="Local_Ingestion",
            base_path=base_path
        )
        return connection
