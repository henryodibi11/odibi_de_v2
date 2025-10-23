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
"""

class SourceOptionsResolver:
    """
    Resolves and prepares **read options** for a data source based on the provided
    configuration, engine, and mode.

    This class is responsible for extracting the `source_options` block from the
    configuration dictionary and cleaning it up for use by the Spark reader. It ensures
    that every source (SQL, ADLS, Local, API, etc.) adheres to the same nested schema,
    allowing the ingestion framework to handle all inputs uniformly.

    **Typical Flow**
        SparkDataReaderFromConfig
            └── BuildConnectionFromConfig
                    └── (Connector)
            └── SourceOptionsResolver
                    └── resolve() → clean dict of Spark read options

    **Expected Config Shape**
        The resolver expects a structure similar to what’s stored in your SQL configuration tables:
        ```python
        config = {
            "source_options": {
                "databricks": {
                    "batch": {
                        "options": {
                            "header": "true",
                            "inferSchema": "true"
                        },
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

    **Behavior Summary**
        | Step | Action |
        |------|---------|
        | 1️⃣ | Loads options for the specified `engine` (`databricks`) and `mode` (`batch` or `streaming`). |
        | 2️⃣ | If present, resolves `cloudFiles.schemaLocation` using the connector’s `get_file_path()`. |
        | 3️⃣ | Removes any keys whose values are empty (`None`, `''`, `{}`, or `[]`). |
        | 4️⃣ | Returns the cleaned dictionary. |

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

    **Common Errors**
        - `KeyError: 'databricks'` → missing engine block in `source_options`.
        - `KeyError: 'batch'` → missing mode block.
        - `KeyError: 'storage_unit'` → missing `connection_config` details.
    """

    def __init__(self, connector, config: dict, mode: str, engine: str = "databricks"):
        """
        Initializes the resolver for a specific processing engine and mode.

        Args:
            connector: Connector instance with a `get_file_path()` method
                (e.g., LocalConnection, AzureBlobConnection).
            config (dict): Full source configuration dictionary containing nested
                `source_options` and `connection_config`.
            mode (str): Operational mode — either 'streaming' or 'batch'.
            engine (str, optional): Processing engine; defaults to 'databricks'.

        Raises:
            KeyError: If the specified engine or mode keys are missing.
        """
        self.connector = connector
        self.config = config
        self.engine = engine
        self.mode = mode.lower()
        self.options = config["source_options"][self.engine][self.mode].copy()

    def resolve(self) -> dict:
        """
        Resolves schema paths and removes empty fields from the options dictionary.

        - If an Auto Loader schema path (`cloudFiles.schemaLocation`) exists,
          it is resolved to an absolute path using the connector.
        - All empty or null top-level fields are stripped out.

        Returns:
            dict: Sanitized options ready for Spark `.read()` or `.readStream()`.

        Example:
            ```python
            resolver = SourceOptionsResolver(connector, config, "streaming")
            clean_opts = resolver.resolve()
            # {'options': {'cloudFiles.format': 'csv',
            #              'cloudFiles.schemaLocation': 'dbfs:/FileStore/schemas'}}
            ```
        """
        storage_unit = self.config["connection_config"]["storage_unit"]

        # Resolve schema location if applicable
        schema_rel_path = self.options.get("options", {}).get("cloudFiles.schemaLocation")
        if schema_rel_path:
            resolved_path = self.connector.get_file_path(storage_unit, schema_rel_path, "")
            self.options["options"]["cloudFiles.schemaLocation"] = resolved_path

        # Remove blank top-level fields
        self.options = {
            k: v for k, v in self.options.items()
            if v not in (None, "", [], {})
        }
        return self.options


class TargetOptionsResolver:
    """
    Resolves and sanitizes **write options** for a target dataset based on the
    configuration, mode, and engine.

    This class ensures that every target follows the same nested structure
    used in your SQL configuration tables. It removes empty or null fields and
    returns a ready-to-use dictionary for Spark `.write()` or `.writeStream()` calls.

    **Expected Config Shape**
        ```python
        config = {
            "target_options": {
                "databricks": {
                    "method": "save",
                    "batch": {
                        "format": "delta",
                        "mode": "overwrite",
                        "options": {
                            "delta.columnMapping.mode": "name",
                            "overwriteSchema": "true"
                        },
                        "partitionBy": ["Plant", "Asset"],
                        "sortBy": []
                    },
                    "streaming": {
                        "format": "delta",
                        "options": {
                            "checkpointLocation": "FileStore/checkpoints"
                        },
                        "outputMode": "append"
                    }
                }
            }
        }
        ```

    **Behavior Summary**
        | Step | Action |
        |------|---------|
        | 1️⃣ | Extracts `target_options[engine][mode]`. |
        | 2️⃣ | Removes any null, empty, or placeholder fields. |
        | 3️⃣ | Returns a clean dict ready for Spark `.write()` or `.writeStream()`. |

    **Example**
        ```python
        from odibi_de_v2.databricks.config.option_resolvers import TargetOptionsResolver

        resolver = TargetOptionsResolver(config, mode="batch", engine="databricks")
        resolved_options = resolver.resolve()

        print(resolved_options)
        # → {'format': 'delta', 'mode': 'overwrite', 'options': {...}, 'partitionBy': ['Plant', 'Asset']}
        ```

    **Common Errors**
        - `KeyError: 'databricks'` → missing engine block.
        - `KeyError: 'batch'` → mode not defined.
        - Misaligned key names (e.g., using `target_config` instead of `target_options`).
    """

    def __init__(self, config: dict, mode: str, engine: str = "databricks"):
        """
        Initializes the resolver for a given engine and operational mode.

        Args:
            config (dict): Target configuration containing nested option dictionaries.
            mode (str): Write mode, such as 'batch' or 'streaming'.
            engine (str, optional): Processing engine; defaults to 'databricks'.
        """
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
        """
        Removes null, empty, or placeholder values from the target options.

        Returns:
            dict: A sanitized dictionary suitable for use in Spark write operations.

        Example:
            ```python
            resolver = TargetOptionsResolver(config, "streaming")
            clean_opts = resolver.resolve()
            # {'format': 'delta', 'outputMode': 'append', 'options': {...}}
            ```
        """
        self.options = {
            k: v for k, v in self.options.items()
            if v not in (None, "", [], {})
        }
        return self.options
