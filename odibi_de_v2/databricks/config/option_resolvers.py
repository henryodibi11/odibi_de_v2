"""
odibi_de_v2.databricks.config.option_resolvers
----------------------------------------------

This module contains resolver classes that normalize and sanitize the `source_options`
and `target_options` blocks within ingestion and transformation configurations.

These resolvers interpret the configuration structure stored in SQL tables such as
`IngestionSourceConfig` and `TransformationConfig`, ensuring a consistent, engine-agnostic
metadata schema for Spark operations.

They guarantee that both source and target configurations follow the canonical structure:

Source JSON (default schema in SQL):
------------------------------------
{
    "engine": "databricks",
    "databricks": {
        "batch": {
            "options": {},
            "format": "",
            "schema": ""
        },
        "streaming": {
            "options": {},
            "format": "",
            "schema": ""
        }
    }
}

Target JSON (default schema in SQL):
------------------------------------
{
    "engine": "databricks",
    "databricks": {
        "method": "",
        "batch": {
            "bucketBy": [],
            "format": "",
            "insertInto": "",
            "mode": "overwrite",
            "options": {
                "delta.columnMapping.mode": "name",
                "overwriteSchema": "true"
            },
            "partitionBy": [],
            "sortBy": []
        },
        "streaming": {
            "foreach": "",
            "foreachBatch": "",
            "format": "",
            "options": {},
            "outputMode": "",
            "partitionBy": [],
            "queryName": "",
            "trigger": {}
        }
    }
}

Each resolver ensures this structure remains valid, removes any empty or null fields,
and resolves paths or schema references when required.

This uniform schema enables:
    - Consistent read/write operations across all connectors.
    - Metadata-driven orchestration with zero branching logic.
    - Extensibility to future engines (e.g., AWS Glue, Synapse) with minimal changes.

Design Philosophy
-----------------
These resolvers embody odibi_de_v2’s principle of *truth-preserving configuration*:
all ingestion and transformation logic should be metadata-driven, composable, and
reproducible without manual branching or per-source code changes.
"""


class SourceOptionsResolver:
    """
    Resolves and prepares **read options** for a data source based on the provided
    configuration, engine, and mode.

    **Typical Flow**
        SparkDataReaderFromConfig
            └── BuildConnectionFromConfig
                    └── (Connector)
            └── SourceOptionsResolver
                    └── resolve() → clean dict of Spark read options

    **Expected Config**
        ```python
        config = {
            "source_options": {
                "databricks": {
                    "batch": {
                        "options": {"header": "true", "inferSchema": "true"},
                        "format": "csv",
                        "schema": ""
                    },
                    "streaming": {
                        "options": {
                            "cloudFiles.format": "csv",
                            "cloudFiles.schemaLocation": "FileStore/schemas/"
                        },
                        "format": "cloudFiles",
                        "schema": ""
                    }
                }
            },
            "connection_config": {
                "storage_unit": "",
                "object_name": "FileStore/data.csv"
            }
        }
        ```

    **Example**
        ```python
        from odibi_de_v2.databricks.config.option_resolvers import SourceOptionsResolver
        from odibi_de_v2.connector import LocalConnection

        connector = LocalConnection()
        resolver = SourceOptionsResolver(connector, config, mode="batch", engine="databricks")
        resolved_options = resolver.resolve()

        print(resolved_options)
        # → {'options': {'header': 'true', 'inferSchema': 'true'}, 'format': 'csv'}
        ```
    """

    def __init__(self, connector, config: dict, mode: str, engine: str = "databricks"):
        """Initializes resolver for a specific engine and mode."""
        self.connector = connector
        self.config = config
        self.engine = engine
        self.mode = mode.lower()
        self.options = config["source_options"][self.engine][self.mode].copy()

    def resolve(self) -> dict:
        """Resolves schema paths and removes empty fields."""
        storage_unit = self.config["connection_config"]["storage_unit"]
        schema_rel_path = self.options.get("options", {}).get("cloudFiles.schemaLocation")
        if schema_rel_path:
            resolved_path = self.connector.get_file_path(storage_unit, schema_rel_path, "")
            self.options["options"]["cloudFiles.schemaLocation"] = resolved_path

        self.options = {k: v for k, v in self.options.items() if v not in (None, "", [], {})}
        return self.options


class TargetOptionsResolver:
    """
    Resolves and sanitizes **write options** for a target dataset based on the
    configuration, mode, and engine.

    **Example**
        ```python
        from odibi_de_v2.databricks.config.option_resolvers import TargetOptionsResolver

        resolver = TargetOptionsResolver(config, mode="batch", engine="databricks")
        resolved_options = resolver.resolve()

        print(resolved_options)
        # → {'format': 'delta', 'mode': 'overwrite', 'options': {...}, 'partitionBy': ['Plant', 'Asset']}
        ```
    """

    def __init__(self, config: dict, mode: str, engine: str = "databricks"):
        """Initializes resolver for given engine and mode."""
        self.config = config
        self.mode = mode.lower()
        self.engine = engine
        self.options = (
            config.get("target_options", {})
                  .get(self.engine, {})
                  .get(self.mode, {})
                  .copy()
        )

    def resolve(self) -> dict:
        """Removes null, empty, or placeholder values."""
        self.options = {k: v for k, v in self.options.items() if v not in (None, "", [], {})}
        return self.options
