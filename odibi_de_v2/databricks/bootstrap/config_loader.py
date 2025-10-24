"""
odibi_de_v2.databricks.bootstrap.load_ingestion_configs_from_sql
----------------------------------------------------------------

Loads **source and target ingestion configurations** (along with secrets)
from a centralized SQL metadata database into Pandas DataFrames.

This function is a key entry point for **metadata-driven ingestion pipelines**
in `odibi_de_v2`. It enables dynamic pipeline construction by reading the
configurations stored in three standard tables:

1. **SecretsConfig** — credential and connection secret metadata  
2. **IngestionSourceConfig** — source table/file/API ingestion details  
3. **IngestionTargetConfig** — target write and storage metadata  

The function uses Spark’s JDBC reader (via `ReaderProvider`) to pull metadata,
automatically joins to `SecretsConfig`, and returns the results as Pandas
DataFrames for use in the `IngestionConfigConstructor` class.

========================================================================================
🏗️ Required Tables (Minimal Summary)
========================================================================================
- `SecretsConfig`  
  Stores scope, keys, and identifiers for accessing secrets or connections.

- `IngestionSourceConfig`  
  Defines where and how to read data. Can reference a query, path, or endpoint.

- `IngestionTargetConfig`  
  Defines where and how to write the processed output (SQL, ADLS, etc.).

========================================================================================
🧱 Canonical JSON Structures
========================================================================================
**Source JSON**
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

**Target JSON**
{
    "engine": "databricks",
    "databricks": {
        "method": "",
        "batch": {
            "format": "delta",
            "mode": "overwrite",
            "options": {
                "overwriteSchema": "true"
            },
            "partitionBy": []
        },
        "streaming": {
            "format": "",
            "options": {},
            "outputMode": ""
        }
    }
}

========================================================================================
🧩 Example Use Cases
========================================================================================

1️⃣ **SQL → ADLS Bronze**
----------------------------------------------------
Reads a table from SQL and writes the results to a Bronze Delta folder.

>>> from pyspark.sql import SparkSession
>>> from odibi_de_v2.databricks.bootstrap.load_ingestion_configs_from_sql import load_ingestion_configs_from_sql
>>> spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()
>>> src_df, tgt_df = load_ingestion_configs_from_sql(
...     host="myserver.database.windows.net",
...     database="global_config_db",
...     user="config_reader",
...     password="***",
...     project="EnergyEfficiency",
...     source_id="src-sql-maintenance-tables",
...     target_id="tgt-adls-maintenance-bronze",
...     spark=spark,
...     environment="qat"
... )
>>> display(src_df)
>>> display(tgt_df)

2️⃣ **API → Delta Lake**
----------------------------------------------------
Fetches configuration for an external REST API endpoint
(e.g., weather, energy intensity, or equipment data).

>>> src_df, tgt_df = load_ingestion_configs_from_sql(
...     host="configserver.database.windows.net",
...     database="global_config_db",
...     user="config_reader",
...     password="***",
...     project="WeatherETL",
...     source_id="src-weather-api",
...     target_id="tgt-weather-bronze",
...     spark=spark,
...     environment="qat"
... )
>>> print(src_df[['source_name','source_path_or_query']])
>>> print(tgt_df[['target_name','target_path_or_table']])

3️⃣ **ADLS → SQL Silver**
----------------------------------------------------
Loads configuration for a pipeline that reads from ADLS (Bronze)
and writes results to SQL for analytical consumption.

>>> src_df, tgt_df = load_ingestion_configs_from_sql(
...     host="myserver.database.windows.net",
...     database="global_config_db",
...     user="config_reader",
...     password="***",
...     project="Reliability",
...     source_id="src-adls-workorders",
...     target_id="tgt-sql-reliability-silver",
...     spark=spark,
...     environment="qat"
... )
>>> display(src_df.head())
>>> display(tgt_df.head())

========================================================================================
🧭 Notes
========================================================================================
- This function expects the SQL Server to already contain the three configuration tables.  
- It is **non-destructive**: only performs SELECT queries via Spark JDBC.  
- The output is framework-ready — pass directly into `IngestionConfigConstructor`.  
- Uses the `@log_call` decorator from `odibi_de_v2.utils.decorators` for telemetry and audit logging.
"""


import pandas as pd
from pyspark.sql import SparkSession
from odibi_de_v2.connector import SQLDatabaseConnection
from odibi_de_v2.ingestion import ReaderProvider
from odibi_de_v2.core.enums import Framework, DataType
from odibi_de_v2.utils.decorators import log_call


@log_call(module="BOOTSTRAP", component="LoadIngestionConfigsFromSQL")
def load_ingestion_configs_from_sql(
    host: str,
    database: str,
    user: str,
    password: str,
    project: str,
    source_id: str,
    target_id: str,
    spark: SparkSession,
    environment: str = "qat"
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Loads source and target ingestion configuration metadata from SQL Server.

    The function queries both *IngestionSourceConfig* and *IngestionTargetConfig*, joining
    each to *SecretsConfig* for authentication and connection metadata. Results are returned
    as Pandas DataFrames for compatibility with odibi_de’s config constructors.
    """

    connection = SQLDatabaseConnection(
        host=host,
        database=database,
        user=user,
        password=password,
        framework=Framework.SPARK
    )

    provider = ReaderProvider(connector=connection, local_engine=Framework.SPARK)

    source_query = f"""
        SELECT
            isc.source_id, isc.project, isc.source_name, isc.source_type,
            isc.source_path_or_query, isc.file_format, isc.is_autoloader,
            isc.source_options, isc.connection_config,
            sc.secret_scope, sc.identifier_key, sc.credential_key,
            sc.server, sc.connection_string_key, sc.auth_type,
            sc.token_header_name, sc.[description]
        FROM IngestionSourceConfig isc
        LEFT JOIN SecretsConfig sc ON isc.secret_config_id = sc.secret_config_id
        WHERE isc.project = '{project}'
          AND isc.source_id = '{source_id}'
          AND (isc.environment = '{environment}' OR isc.environment IS NULL)
    """

    target_query = f"""
        SELECT
            itc.target_id, itc.project, itc.target_name, itc.target_type,
            itc.target_path_or_table, itc.write_mode, itc.target_options,
            itc.connection_config, itc.merge_config,
            sc.secret_scope, sc.identifier_key, sc.credential_key,
            sc.server, sc.connection_string_key, sc.auth_type,
            sc.token_header_name, sc.[description]
        FROM IngestionTargetConfig itc
        LEFT JOIN SecretsConfig sc ON itc.secret_config_id = sc.secret_config_id
        WHERE itc.project = '{project}'
          AND itc.target_id = '{target_id}'
          AND (itc.environment = '{environment}' OR itc.environment IS NULL)
    """

    print(f"[INFO] Loading source config for project={project}, source_id={source_id}")
    sources_df = provider.read(
        data_type=DataType.SQL,
        container="",
        path_prefix="",
        object_name=source_query,
        spark=spark
    ).toPandas()

    print(f"[INFO] Loading target config for project={project}, target_id={target_id}")
    targets_df = provider.read(
        data_type=DataType.SQL,
        container="",
        path_prefix="",
        object_name=target_query,
        spark=spark
    ).toPandas()

    print(f"[SUCCESS] Loaded {len(sources_df)} source rows and {len(targets_df)} target rows.")

    return sources_df, targets_df
