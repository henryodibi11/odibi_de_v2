"""
odibi_de_v2.databricks.config.spark_data_reader_from_config
-----------------------------------------------------------

Reads data from SQL, ADLS, API, or local sources using a unified configuration-driven approach.

**Dependency Chain**
    IngestionConfigConstructor
        ↓
    BuildConnectionFromConfig
        ↓
    SourceOptionsResolver
        ↓
    SparkDataReaderFromConfig
        ↓
    ReaderProvider

This class interprets configuration rows (from `IngestionSourceConfig`) and automatically builds
the appropriate Spark DataFrame reader — whether from a JDBC source, an ADLS path, or an API call.

**Example – SQL (JDBC)**
```python
config = {
    "source_type": "sql",
    "file_format": "sql",
    "source_path_or_query": "SELECT * FROM SampleTable",
    "connection_config": {
        "database": "my_database",
        "port": 1433,
        "table": "SampleTable",
        "query_params": {},
        "extra_config": {}
    },
    "source_options": {
        "databricks": {
            "batch": {"options": {}, "format": "jdbc", "schema": ""},
            "streaming": {"options": {}, "format": "", "schema": ""}
        }
    }
}
reader = SparkDataReaderFromConfig(spark, config)
df = reader.read_data()
```

**Example – ADLS**
```python
config = {
    "source_type": "adls",
    "file_format": "avro",
    "connection_config": {
        "storage_unit": "raw-data",
        "object_name": "Energy/Boilers/*/*/*"
    },
    "source_options": {
        "databricks": {
            "batch": {"options": {}, "format": "avro", "schema": ""},
            "streaming": {"options": {}, "format": "", "schema": ""}
        }
    }
}
reader = SparkDataReaderFromConfig(spark, config)
df = reader.read_data()
```

**Example – API**
```python
config = {
    "source_type": "api",
    "file_format": "api",
    "connection_config": {},
    "source_options": {},
    "api_details": {
        "endpoint": "https://api.example.com/data",
        "headers": {"Authorization": "Bearer <token>"}
    }
}
reader = SparkDataReaderFromConfig(spark, config)
df = reader.read_data()
```
"""

import copy
from pyspark.sql import SparkSession, DataFrame
from odibi_de_v2.core import DataType, Framework
from odibi_de_v2.ingestion import ReaderProvider
from odibi_de_v2.databricks.config import SourceOptionsResolver
from odibi_de_v2.databricks.bootstrap import BuildConnectionFromConfig
from odibi_de_v2.databricks.utils.api_ingestion import prepare_api_reader_kwargs_from_config
from odibi_de_v2.connector import LocalConnection
from odibi_de_v2.utils.decorators import log_call


class SparkDataReaderFromConfig:
    """Reads data using Spark based on standardized configuration metadata."""

    @log_call(module="DATABRICKS", component="SparkDataReaderFromConfig")
    def __init__(self, spark: SparkSession, config: dict, dbutils=None):
        self.spark = spark
        self.config = copy.deepcopy(config)
        self.dbutils = dbutils
        self.file_format = self.config["file_format"].lower()
        self.data_type = DataType(self.file_format)
        self.is_streaming = self.config.get("is_autoloader", False)

        if self.data_type != DataType.API:
            self.connection = BuildConnectionFromConfig(self.config).get_connection()
            self.storage_unit = self.config["connection_config"]["storage_unit"]
            self.object_name = self.config["connection_config"]["object_name"]
            self.options = self._resolve_options("streaming" if self.is_streaming else "batch")
        else:
            self.connection = LocalConnection()
            self.storage_unit = ""
            self.object_name = ""
            self.options = {}

    @log_call(module="DATABRICKS", component="SparkDataReaderFromConfig")
    def _resolve_options(self, mode: str) -> dict:
        """Resolves options for a data source based on mode (streaming or batch)."""
        return SourceOptionsResolver(self.connection, self.config, mode).resolve()

    @log_call(module="DATABRICKS", component="SparkDataReaderFromConfig")
    def read_data(self) -> DataFrame:
        """Reads and returns a Spark DataFrame for SQL, ADLS, API, or local sources."""
        provider = ReaderProvider(connector=self.connection, local_engine=Framework.SPARK)

        if self.data_type == DataType.API:
            api_kwargs = prepare_api_reader_kwargs_from_config(self.config, self.dbutils)
            return provider.read(
                data_type=DataType.API,
                container="", path_prefix="", object_name="",
                spark=self.spark, **api_kwargs
            )

        object_name = (
            self.config["connection_config"]["object_name"]
            if self.config["source_type"] != "sql"
            else self.config["source_path_or_query"]
        )

        return provider.read(
            data_type=self.data_type,
            container=self.storage_unit,
            path_prefix="",
            object_name=object_name,
            spark=self.spark,
            is_stream=self.is_streaming,
            **self.options
        )
