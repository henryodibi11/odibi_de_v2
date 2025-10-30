"""
odibi_de_v2: Production-Grade Data Engineering Framework
=========================================================

A comprehensive Python framework for building robust, scalable data pipelines
on Databricks with support for both Spark and Pandas workflows. Implements
medallion architecture (Bronze/Silver/Gold) with configuration-driven orchestration.

Key Features
------------
- **Medallion Architecture**: Bronze → Silver → Gold layered data transformations
- **Dual Engine Support**: Seamless Spark and Pandas integration
- **Configuration-Driven**: SQL-based config tables for ingestion and transformations
- **Cloud-Native**: Azure Blob Storage (ADLS), Databricks, Delta Lake support
- **Production-Ready**: Comprehensive logging, error handling, and validation
- **Project Scaffolding**: Automated project initialization and structure generation

Core Modules
------------
- **core**: Reader/Saver factories and base abstractions
- **connector**: Azure Blob, SQL Server, local file system connectors
- **ingestion**: Pandas and Spark data readers
- **storage**: Pandas and Spark data savers (CSV, Parquet, Delta)
- **transformer**: Transformation execution engine
- **orchestration**: Pipeline orchestration and workflow management
- **project**: Project scaffolding and manifest management
- **logger**: Structured logging and metadata tracking
- **databricks**: Databricks-specific utilities (Delta, secrets, notebooks)

Quick Start Examples
--------------------

**Example 1: Initialize a New Project**

    >>> from odibi_de_v2 import initialize_project
    >>> 
    >>> # Create a new manufacturing analytics project
    >>> result = initialize_project(
    ...     project_name="Plant Efficiency",
    ...     project_type="manufacturing"
    ... )
    >>> print(f"Project created at: {result['project_path']}")

**Example 2: Run a Complete Pipeline**

    >>> from odibi_de_v2 import run_project
    >>> 
    >>> # Run entire pipeline (Bronze → Silver → Gold)
    >>> run_project(
    ...     project="Energy Efficiency",
    ...     env="qat",
    ...     log_level="INFO"
    ... )

**Example 3: Run Specific Pipeline Layers**

    >>> from odibi_de_v2 import run_project
    >>> 
    >>> # Run only Silver and Gold layers
    >>> run_project(
    ...     project="Customer Churn",
    ...     env="prod",
    ...     target_layers=["Silver_1", "Gold_1"],
    ...     cache_plan={"Gold_1": ["aggregated_metrics"]}
    ... )

**Example 4: Read Data with Auto-Detection**

    >>> from odibi_de_v2.core import DataReader
    >>> from odibi_de_v2.connector import AzureBlobConnection
    >>> 
    >>> # Auto-detect file type and read
    >>> connection = AzureBlobConnection(
    ...     container_name="raw-data",
    ...     storage_account="mystorageaccount"
    ... )
    >>> reader = DataReader(
    ...     connection=connection,
    ...     file_path="sales/2024/sales_data.parquet"
    ... )
    >>> df = reader.read()

**Example 5: Save Data to Delta Lake**

    >>> from odibi_de_v2.core import DataSaver
    >>> from pyspark.sql import SparkSession
    >>> 
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df = spark.createDataFrame([
    ...     {"id": 1, "value": "test"}
    ... ])
    >>> 
    >>> # Save to Delta table
    >>> saver = DataSaver(
    ...     data=df,
    ...     table_name="my_catalog.my_schema.my_table",
    ...     save_mode="overwrite",
    ...     save_format="delta"
    ... )
    >>> saver.save()

**Example 6: Transform Data with Config-Driven Runner**

    >>> from odibi_de_v2.transformer import TransformationRunnerFromConfig
    >>> from my_sql_provider import get_sql_provider
    >>> 
    >>> # Run transformations for a specific layer
    >>> runner = TransformationRunnerFromConfig(
    ...     sql_provider=get_sql_provider(),
    ...     project="Energy Efficiency",
    ...     env="qat",
    ...     layer="Silver_1"
    ... )
    >>> runner.run_parallel()

**Example 7: Create Interactive Config UI**

    >>> from odibi_de_v2.config import TransformationRegistryUI
    >>> 
    >>> # Launch interactive configuration builder
    >>> ui = TransformationRegistryUI(
    ...     project="Energy Efficiency",
    ...     env="qat"
    ... )
    >>> ui.render()

Architecture
------------
The framework follows a clean, modular architecture:

    Projects/
    ├── project_name/
    │   ├── manifest.json              # Project configuration
    │   ├── transformations/           # Transformation modules
    │   │   ├── bronze/
    │   │   ├── silver/
    │   │   └── gold/
    │   ├── sql/                       # SQL scripts
    │   ├── notebooks/                 # Databricks notebooks
    │   ├── config/                    # Config files
    │   └── tests/                     # Unit tests

Configuration Tables
--------------------
The framework uses SQL config tables for declarative configuration:

- **IngestionSourceConfig**: Defines data sources and ingestion rules
- **TransformationRegistry**: Defines transformation logic and execution order
- **SecretsConfig**: Stores encrypted credentials and connection strings

For detailed documentation, visit: https://github.com/yourusername/odibi_de_v2
"""

from . import core
from . import logger
from . import connector
from . import utils
from . import pandas_utils
from . import spark_utils
from . import databricks
from . import config
from . import sql_builder
from . import storage
from . import transformer
from . import project
from . import orchestration

# Import top-level convenience functions
from .orchestration import run_project
from .project import initialize_project

__version__ = "2.0.0"

__all__ = [
    'core', 
    'logger', 
    'connector', 
    'utils', 
    'pandas_utils',
    'spark_utils',
    'databricks',
    'sql_builder', 
    'config', 
    'storage', 
    'transformer',
    'project',
    'orchestration',
    'run_project',
    'initialize_project',
]