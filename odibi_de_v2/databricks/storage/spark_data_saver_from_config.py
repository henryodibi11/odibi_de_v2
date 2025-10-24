
"""
spark_data_saver_from_config.py
--------------------------------
Config-driven Spark DataFrame writer that abstracts batch and streaming persistence logic.

This module powers the Silver and Gold layer writes in odibi_de_v2 by dynamically resolving
storage methods based on configuration and registry-defined save functions.

**Dependency Chain**
    TargetOptionsResolver
        ↓
    SparkDataSaverFromConfig
        ↓
    SaverProvider
        ↓
    DeltaTableManager / Custom save_xxx registry

**Supported Use Cases**
-----------------------
1. **Batch Save (ADLS, SQL, Delta)**
    >>> from pyspark.sql import SparkSession
    >>> from odibi_de_v2.databricks.storage.spark_data_saver_from_config import SparkDataSaverFromConfig
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
    >>> config = {
    ...     "connection_config": {"storage_unit": "bronze", "object_name": "test/path"},
    ...     "target_path_or_table": "qat_energy.bronze_test",
    ...     "target_options": {"databricks": {"batch": {"options": {}, "format": "delta"}}}
    ... }
    >>> connector = SomeConnector()
    >>> saver = SparkDataSaverFromConfig(spark, config, connector)
    >>> saver.save(df)

2. **Streaming Save (with registered foreachBatch handler)**
    >>> from odibi_de_v2.databricks.storage.spark_data_saver_from_config import SparkDataSaverFromConfig
    >>> df = spark.readStream.format("rate").load()
    >>> config = {
    ...     "connection_config": {"storage_unit": "silver", "object_name": "stream_test"},
    ...     "target_path_or_table": "qat_energy.silver_stream",
    ...     "target_options": {
    ...         "databricks": {"streaming": {"foreachBatch": "save_or_merge_delta"}}
    ...     }
    ... }
    >>> saver = SparkDataSaverFromConfig(spark, config, connector)
    >>> saver.save(df)

Note:
    - Batch saves delegate to SaverProvider (default Delta).
    - Streaming saves can wrap registered handlers dynamically using the function registry.
    - All saves automatically add ingestion metadata.

🔄 Custom Registry Packages
----------------------------
If you want to use your own project-specific savers instead of the default registry,
you can point the saver to a new package at runtime:

    >>> from odibi_de_v2.databricks.storage.function_registry import set_registry_package
    >>> # Use a custom saver registry
    >>> set_registry_package("my_project.storage")

Any function in that package beginning with `save_` or `merge_` will be automatically discovered
and callable from config using its name (e.g., `"method": "save_custom_gold_table"`).

⚙️ Method-based Delegation Example
----------------------------------
You can also call existing framework savers (like `save_static_data_from_config`) directly via config.
The saver will handle the connection and mode automatically.

    >>> from odibi_de_v2.databricks.storage.spark_data_saver_from_config import SparkDataSaverFromConfig

    >>> config = {
    ...     "connection_config": {
    ...         "storage_unit": "silver",
    ...         "object_name": "processed/energy_data"
    ...     },
    ...     "target_path_or_table": "qat_energy.silver_energy_data",
    ...     "target_options": {
    ...         "databricks": {
    ...             "method": "save_static_data_from_config"
    ...         }
    ...     }
    ... }

    >>> saver = SparkDataSaverFromConfig(spark, config, connector)
    >>> saver.save(df)

This will:
    • Dynamically resolve `save_static_data_from_config` from the registry
    • Use your current `connection_config` for container and path resolution
    • Apply the overwrite mode automatically (as defined in that saver’s logic)
"""

import copy
from pyspark.sql import DataFrame, SparkSession
from odibi_de_v2.core.enums import DataType
from odibi_de_v2.utils.decorators import log_call
from odibi_de_v2.databricks.config import TargetOptionsResolver
from odibi_de_v2.databricks.utils.metadata_helpers import add_ingestion_metadata
from odibi_de_v2.storage import SaverProvider
from odibi_de_v2.databricks.storage.helpers import wrap_for_foreach_batch_from_registry
from odibi_de_v2.databricks.storage.function_registry import get_function_registry
from odibi_de_v2.databricks.delta.delta_table_manager import DeltaTableManager


class SparkDataSaverFromConfig:
    """
    Executes config-driven persistence for both batch and streaming DataFrames.
    """

    def __init__(self, spark: SparkSession, config: dict, connector):
        self.spark = spark
        self.config = copy.deepcopy(config)
        self.connector = connector
        self.storage_unit = config["connection_config"]["storage_unit"]
        self.object_name = config["connection_config"]["object_name"]
        self.file_path = connector.get_file_path(self.storage_unit, self.object_name, "")
        self.target_path_or_table = config["target_path_or_table"]
        self.data_type = DataType(config.get("file_format", "delta"))
        self.function_registry = get_function_registry()

    @log_call(module="DATABRICKS", component="SparkDataSaverFromConfig")
    def save(self, df: DataFrame):
        """Route to batch or streaming save based on DataFrame type."""
        if df.isStreaming:
            self._save_stream(df)
        else:
            self._save_batch(df)

    @log_call(module="DATABRICKS", component="SparkDataSaverFromConfig")
    def _save_batch(self, df: DataFrame):
        """Saves batch DataFrames using SaverProvider or registry-defined methods."""
        method_name = (
            self.config.get("target_options", {})
            .get("databricks", {})
            .get("method", "")
            .strip()
        )

        if method_name:
            save_fn = self.function_registry.get(method_name)
            if not save_fn:
                raise ValueError(f"Batch save method '{method_name}' not found in registry.")
            return save_fn(df=df, spark=self.spark, connector=self.connector, config=self.config)

        df = add_ingestion_metadata(df)
        options = TargetOptionsResolver(self.config, mode="batch").resolve()

        SaverProvider(self.connector).save(
            df=df,
            data_type=self.data_type,
            container=self.storage_unit,
            path_prefix=self.object_name,
            object_name="",
            spark=self.spark,
            is_stream=False,
            **options,
        )

        database, table = self.target_path_or_table.split(".")
        DeltaTableManager(self.spark, self.file_path, is_path=True).register_table(
            table_name=table,
            database=database,
        )

    @log_call(module="DATABRICKS", component="SparkDataSaverFromConfig")
    def _save_stream(self, df: DataFrame):
        """Saves streaming DataFrames using checkpointing and optional foreachBatch handlers."""
        options = TargetOptionsResolver(self.config, mode="streaming").resolve()
        checkpoint_rel_path = options.get("options", {}).get("checkpointLocation", "")
        resolved_checkpoint = self.connector.get_file_path(self.storage_unit, checkpoint_rel_path, "")
        options["options"]["checkpointLocation"] = resolved_checkpoint

        foreach_fn_name = options.get("foreachBatch")
        if foreach_fn_name:
            handler = wrap_for_foreach_batch_from_registry(
                function_name=foreach_fn_name,
                spark=self.spark,
                connector=self.connector,
                config=self.config,
            )
            options["foreachBatch"] = handler

        SaverProvider(self.connector).save(
            df=df,
            data_type=self.data_type,
            container=self.storage_unit,
            path_prefix="",
            object_name=self.object_name,
            spark=self.spark,
            is_stream=True,
            **options,
        )
