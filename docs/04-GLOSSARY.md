# Glossary - Complete Reference

## Table of Contents
- [Core Classes](#core-classes)
- [Configuration Tables](#configuration-tables)
- [Enums and Data Classes](#enums-and-data-classes)
- [Decorators](#decorators)
- [Interfaces](#interfaces)
- [Key Concepts](#key-concepts)

---

## Core Classes

### GenericProjectOrchestrator

**Module**: `odibi_de_v2.orchestration.generic_orchestrator`

**Purpose**: Universal orchestration engine for any data pipeline project.

**Key Attributes**:
| Attribute | Type | Description |
|-----------|------|-------------|
| `project` | `str` | Project name (e.g., "Energy Efficiency") |
| `env` | `str` | Environment (qat, prod, dev) |
| `manifest` | `ProjectManifest` | Project configuration manifest |
| `engine` | `Engine` | Default execution engine (SPARK/PANDAS) |
| `hooks` | `HookManager` | Event-driven hook system |
| `log_level` | `str` | Logging verbosity (INFO, WARNING, ERROR) |

**Key Methods**:
- `run_project(layers=None, cache_plan=None)`: Execute entire pipeline or specific layers
- `run_bronze_layer()`: Execute Bronze ingestion jobs
- `run_transformation_layer(layer_name)`: Execute specific transformation layer
- `cache_delta_tables(table_names)`: Cache Delta tables to improve performance

**Example**:
```python
orchestrator = GenericProjectOrchestrator(
    project="Energy Efficiency",
    env="qat",
    log_level="INFO",
    save_logs=True,
    engine="spark"
)
orchestrator.run_project()
```

---

### TransformationRunnerFromConfig

**Module**: `odibi_de_v2.transformer.transformation_runner_from_config`

**Purpose**: Execute transformations defined in `TransformationRegistry` with dual-engine support.

**Key Attributes**:
| Attribute | Type | Description |
|-----------|------|-------------|
| `sql_provider` | `SQLProvider` | SQL query provider for dynamic queries |
| `project` | `str` | Project name |
| `env` | `str` | Environment identifier |
| `layer` | `str` | Target layer (Bronze, Silver, Gold) |
| `engine` | `Engine` | Default engine (SPARK/PANDAS) |
| `hooks` | `HookManager` | Hook manager for lifecycle events |
| `log_sink` | `BaseLogSink` | Pluggable logging backend |
| `max_workers` | `int` | Thread pool size for parallel execution |

**Key Methods**:
- `run()`: Execute all transformations for the configured layer
- `_fetch_configs()`: Fetch transformation configs from TransformationRegistry
- `_execute_transformation(cfg)`: Execute a single transformation with error handling

**Example**:
```python
runner = TransformationRunnerFromConfig(
    sql_provider=my_sql_provider,
    project="Energy Efficiency",
    env="qat",
    layer="Silver_1",
    engine="spark",
    hooks=hooks,
    log_level="INFO"
)
runner.run()
```

---

### FunctionRegistry

**Module**: `odibi_de_v2.odibi_functions.registry`

**Purpose**: Singleton registry for engine-specific and universal reusable functions.

**Key Attributes**:
| Attribute | Type | Description |
|-----------|------|-------------|
| `_instance` | `FunctionRegistry` | Singleton instance |
| `_functions` | `Dict[Tuple[str, str], Callable]` | (name, engine) -> function mapping |
| `_metadata` | `Dict[Tuple[str, str], Dict]` | Function metadata storage |

**Key Methods**:
- `register(name, engine, fn, module=None, description=None, **metadata)`: Register a function
- `resolve(module, name, engine)`: Resolve function with engine-specific priority
- `get_all()`: List all registered (name, engine) pairs
- `get_metadata(name, engine)`: Retrieve function metadata

**Example**:
```python
from odibi_de_v2.odibi_functions import REGISTRY

REGISTRY.register("clean_data", "spark", my_spark_function)
func = REGISTRY.resolve("my_module", "clean_data", "spark")
result = func(df)
```

---

### HookManager

**Module**: `odibi_de_v2.hooks.manager`

**Purpose**: Event-driven hook manager for lifecycle callbacks.

**Key Attributes**:
| Attribute | Type | Description |
|-----------|------|-------------|
| `_hooks` | `Dict[str, List[Dict]]` | Event -> callbacks mapping |

**Standard Events**:
- `pre_read`: Before data ingestion
- `post_read`: After data is read
- `pre_transform`: Before transformation executes
- `post_transform`: After transformation completes
- `pre_save`: Before data write
- `post_save`: After data save
- `on_error`: When exception occurs
- `pipeline_start`: Pipeline begins
- `pipeline_end`: Pipeline completes

**Key Methods**:
- `register(event, callback, filters=None)`: Register a callback for an event
- `emit(event, payload)`: Trigger all callbacks for an event
- `list_hooks()`: List all registered hooks

**Example**:
```python
from odibi_de_v2.hooks import HookManager

hooks = HookManager()

def log_transform(payload):
    print(f"Executing: {payload['function']}")

hooks.register("pre_transform", log_transform)
hooks.emit("pre_transform", {"function": "clean_data", "module": "cleaning"})
```

---

### ProjectManifest

**Module**: `odibi_de_v2.project.manifest`

**Purpose**: Define project structure, layers, and execution configuration.

**Key Attributes**:
| Attribute | Type | Description |
|-----------|------|-------------|
| `project_name` | `str` | Project identifier |
| `project_type` | `ProjectType` | MANUFACTURING, ANALYTICS, ML_PIPELINE, etc. |
| `layer_order` | `List[str]` | Ordered list of layers to execute |
| `layers` | `Dict[str, LayerConfig]` | Layer-specific configurations |
| `environments` | `List[str]` | Supported environments (qat, prod, dev) |
| `entity_labels` | `Dict[str, str]` | Domain-specific entity naming |
| `cache_plan` | `Dict[str, List[str]]` | Layer -> tables to cache mapping |

**Key Methods**:
- `to_dict()`: Convert to dictionary
- `to_json(path=None, indent=2)`: Serialize to JSON
- `from_json(path)`: Load from JSON file
- `from_dict(data)`: Load from dictionary

**Example**:
```python
from odibi_de_v2.project.manifest import ProjectManifest, ProjectType

manifest = ProjectManifest(
    project_name="Energy Efficiency",
    project_type=ProjectType.ANALYTICS,
    layer_order=["Bronze", "Silver_1", "Gold_1"],
    cache_plan={"Silver_1": ["silver.energy_clean"]}
)

manifest.to_json("manifest.json")
```

---

### BaseLogSink

**Module**: `odibi_de_v2.logging.log_sink`

**Purpose**: Abstract base class for pluggable logging backends.

**Key Methods**:
- `write(records: List[Dict[str, Any]])`: Write log records to storage

**Built-in Implementations**:
- `SparkDeltaLogSink`: Log to Delta table via Spark
- `PandasCSVLogSink`: Log to CSV file via Pandas
- `ConsoleLogSink`: Log to stdout

**Example**:
```python
from odibi_de_v2.logging import SparkDeltaLogSink

log_sink = SparkDeltaLogSink(spark, "config_driven.TransformationRunLog")

records = [{
    "run_id": "run_123",
    "transformation_id": "T001",
    "status": "success",
    "duration_seconds": 12.5
}]

log_sink.write(records)
```

---

### DeltaTableManager

**Module**: `odibi_de_v2.databricks.DeltaTableManager`

**Purpose**: Manage Delta Lake table operations (caching, optimization, vacuuming).

**Key Methods**:
- `cache_table(table_name)`: Cache Delta table in memory
- `optimize_table(table_name, zorder_cols=None)`: Optimize table with Z-ordering
- `vacuum_table(table_name, retention_hours=168)`: Vacuum old files
- `describe_history(table_name, limit=10)`: Get table history

**Example**:
```python
from odibi_de_v2.databricks import DeltaTableManager

manager = DeltaTableManager(spark)
manager.cache_table("silver.energy_clean")
manager.optimize_table("silver.energy_clean", zorder_cols=["plant", "date"])
```

---

## Configuration Tables

### TransformationRegistry

**Purpose**: Define transformation pipeline configurations.

**Schema**:
```sql
CREATE TABLE TransformationRegistry (
    transformation_id STRING PRIMARY KEY,
    project STRING,
    environment STRING,
    layer STRING,
    entity_1 STRING,
    entity_2 STRING,
    module STRING,
    function STRING,
    inputs TEXT,          -- JSON array: ["bronze.raw_data"]
    constants TEXT,       -- JSON object: {"subset": ["id"]}
    outputs TEXT,         -- JSON array: ["silver.data_clean"]
    enabled BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
```

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| `transformation_id` | `STRING` | Unique transformation identifier |
| `project` | `STRING` | Project name |
| `environment` | `STRING` | Environment (qat, prod, dev) |
| `layer` | `STRING` | Layer name (Bronze, Silver_1, Gold_1) |
| `entity_1` | `STRING` | Domain entity (e.g., plant, region) |
| `entity_2` | `STRING` | Sub-entity (e.g., asset, meter) |
| `module` | `STRING` | Python module path (e.g., "odibi_functions.examples") |
| `function` | `STRING` | Function name within module |
| `inputs` | `TEXT` | JSON array of input table names |
| `constants` | `TEXT` | JSON object of function parameters |
| `outputs` | `TEXT` | JSON array of output table names |
| `enabled` | `BOOLEAN` | Whether transformation is active |

**Example Row**:
```json
{
  "transformation_id": "T_DEDUP_ENERGY",
  "project": "Energy Efficiency",
  "environment": "qat",
  "layer": "Silver_1",
  "entity_1": "Plant_A",
  "entity_2": "Meter_1",
  "module": "odibi_functions.examples",
  "function": "deduplicate_spark",
  "inputs": "[\"bronze.meter_readings\"]",
  "constants": "{\"subset\": [\"meter_id\", \"timestamp\"]}",
  "outputs": "[\"silver.meter_readings_clean\"]",
  "enabled": true
}
```

---

### IngestionSourceConfig

**Purpose**: Define Bronze layer data ingestion sources.

**Schema**:
```sql
CREATE TABLE IngestionSourceConfig (
    ingestion_id STRING PRIMARY KEY,
    project STRING,
    environment STRING,
    source_type STRING,          -- "csv", "sql", "api", "blob", "delta"
    source_path STRING,
    target_table STRING,
    schema_definition TEXT,      -- JSON schema
    enabled BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
```

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| `ingestion_id` | `STRING` | Unique ingestion identifier |
| `project` | `STRING` | Project name |
| `environment` | `STRING` | Environment (qat, prod, dev) |
| `source_type` | `STRING` | Source type (csv, sql, api, blob, delta) |
| `source_path` | `STRING` | Path or connection string |
| `target_table` | `STRING` | Target Delta table name |
| `schema_definition` | `TEXT` | JSON schema definition |
| `enabled` | `BOOLEAN` | Whether ingestion is active |

**Example Row**:
```json
{
  "ingestion_id": "ING_METER_READINGS",
  "project": "Energy Efficiency",
  "environment": "qat",
  "source_type": "csv",
  "source_path": "abfss://landing@storage.dfs.core.windows.net/meter_readings/*.csv",
  "target_table": "bronze.meter_readings_raw",
  "schema_definition": "{\"meter_id\": \"string\", \"timestamp\": \"timestamp\", \"consumption_kwh\": \"double\"}",
  "enabled": true
}
```

---

### TransformationRunLog

**Purpose**: Log transformation execution history.

**Schema**:
```sql
CREATE TABLE config_driven.TransformationRunLog (
    log_id STRING PRIMARY KEY,
    run_id STRING,
    transformation_id STRING,
    project STRING,
    environment STRING,
    layer STRING,
    module STRING,
    function STRING,
    status STRING,              -- "success", "failed", "running"
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds DOUBLE,
    input_row_count BIGINT,
    output_row_count BIGINT,
    error_message STRING,
    created_at TIMESTAMP
)
```

**Example Row**:
```json
{
  "log_id": "log_12345",
  "run_id": "run_20240115_120000",
  "transformation_id": "T_DEDUP_ENERGY",
  "project": "Energy Efficiency",
  "environment": "qat",
  "layer": "Silver_1",
  "module": "odibi_functions.examples",
  "function": "deduplicate_spark",
  "status": "success",
  "start_time": "2024-01-15T12:00:00",
  "end_time": "2024-01-15T12:00:15",
  "duration_seconds": 15.2,
  "input_row_count": 1000000,
  "output_row_count": 985000,
  "error_message": null
}
```

---

## Enums and Data Classes

### Engine (Enum)

**Module**: `odibi_de_v2.core.engine`

**Purpose**: Define execution engine for data processing.

**Values**:
```python
class Engine(Enum):
    SPARK = "spark"
    PANDAS = "pandas"
```

**Example**:
```python
from odibi_de_v2.core import Engine

engine = Engine.SPARK
print(engine.value)  # "spark"
```

---

### ExecutionContext (DataClass)

**Module**: `odibi_de_v2.core.engine`

**Purpose**: Encapsulate runtime state for dual-engine functions.

**Attributes**:
```python
@dataclass
class ExecutionContext:
    engine: Engine                      # SPARK or PANDAS
    project: str                        # Project identifier
    env: str                            # Environment (qat, prod, dev)
    spark: Optional[Any] = None         # SparkSession (if SPARK)
    sql_provider: Optional[Any] = None  # SQL provider
    logger: Optional[Any] = None        # DynamicLogger
    hooks: Optional[HookManager] = None # Hook manager
    extras: Dict[str, Any] = field(default_factory=dict)
```

**Example**:
```python
from odibi_de_v2.core import Engine, ExecutionContext

context = ExecutionContext(
    engine=Engine.SPARK,
    project="Energy Efficiency",
    env="qat",
    spark=spark,
    extras={"plant": "Plant_A", "layer": "Silver_1"}
)
```

---

### ProjectType (Enum)

**Module**: `odibi_de_v2.project.manifest`

**Purpose**: Define common project archetypes.

**Values**:
```python
class ProjectType(str, Enum):
    MANUFACTURING = "manufacturing"
    ANALYTICS = "analytics"
    ML_PIPELINE = "ml_pipeline"
    DATA_INTEGRATION = "data_integration"
    CUSTOM = "custom"
```

---

### LayerConfig (DataClass)

**Module**: `odibi_de_v2.project.manifest`

**Purpose**: Configuration for a single layer in medallion architecture.

**Attributes**:
```python
@dataclass
class LayerConfig:
    name: str
    description: str
    depends_on: List[str] = field(default_factory=list)
    cache_tables: List[str] = field(default_factory=list)
    max_workers: Optional[int] = None
```

---

### DataType (Enum)

**Module**: `odibi_de_v2.core`

**Purpose**: Define data format types.

**Values**:
```python
class DataType(Enum):
    SPARK = "spark"
    PANDAS = "pandas"
    DELTA = "delta"
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
```

---

## Decorators

### @odibi_function

**Module**: `odibi_de_v2.odibi_functions.decorators`

**Purpose**: Register a function in the global ODIBI function registry.

**Signature**:
```python
def odibi_function(
    engine: str = "any",           # "spark", "pandas", "any"
    name: Optional[str] = None,    # Custom name (default: function.__name__)
    module: Optional[str] = None,  # Namespace for organization
    auto_register: bool = True,    # Auto-register on decoration
    **metadata                     # Additional metadata (author, version, tags)
) -> Callable:
```

**Example**:
```python
from odibi_de_v2.odibi_functions import odibi_function

@odibi_function(
    engine="spark",
    module="cleaning",
    author="data_team",
    version="1.0",
    tags=["quality"]
)
def remove_duplicates(df, subset=None):
    return df.dropDuplicates(subset)
```

---

### @spark_function

**Module**: `odibi_de_v2.odibi_functions.decorators`

**Purpose**: Shorthand for `@odibi_function(engine="spark")`.

**Example**:
```python
from odibi_de_v2.odibi_functions import spark_function

@spark_function(module="transformations")
def repartition_data(df, num_partitions=10):
    return df.repartition(num_partitions)
```

---

### @pandas_function

**Module**: `odibi_de_v2.odibi_functions.decorators`

**Purpose**: Shorthand for `@odibi_function(engine="pandas")`.

**Example**:
```python
from odibi_de_v2.odibi_functions import pandas_function

@pandas_function(module="cleaning")
def fill_missing(df, value=0):
    return df.fillna(value)
```

---

### @universal_function

**Module**: `odibi_de_v2.odibi_functions.decorators`

**Purpose**: Shorthand for `@odibi_function(engine="any")`.

**Example**:
```python
from odibi_de_v2.odibi_functions import universal_function

@universal_function(module="utilities")
def get_column_names(df):
    return list(df.columns)
```

---

### @with_context

**Module**: `odibi_de_v2.odibi_functions.decorators`

**Purpose**: Inject `ExecutionContext` into function calls.

**Signature**:
```python
def with_context(context_param: str = "context") -> Callable:
```

**Example**:
```python
from odibi_de_v2.odibi_functions import spark_function, with_context

@spark_function(module="ingestion")
@with_context(context_param="context")
def load_table(table_name, context=None):
    if context and context.spark:
        return context.spark.table(table_name)
    raise ValueError("Spark context required")
```

---

## Interfaces

### IDataTransformer

**Module**: `odibi_de_v2.core`

**Purpose**: Abstract interface for data transformations.

**Methods**:
```python
class IDataTransformer(ABC):
    @abstractmethod
    def transform(self, data: Any) -> Any:
        pass
```

---

### IDataReader

**Module**: `odibi_de_v2.ingestion`

**Purpose**: Abstract interface for data readers.

**Methods**:
```python
class IDataReader(ABC):
    @abstractmethod
    def read(self, **kwargs) -> Any:
        pass
```

---

### IDataSaver

**Module**: `odibi_de_v2.storage`

**Purpose**: Abstract interface for data savers.

**Methods**:
```python
class IDataSaver(ABC):
    @abstractmethod
    def save(self, data: Any, **kwargs) -> None:
        pass
```

---

## Key Concepts

### Medallion Architecture

A data lakehouse architecture pattern with three layers:
- **Bronze**: Raw ingestion from sources
- **Silver**: Cleansed and validated data
- **Gold**: Business-level aggregations and KPIs

---

### Config-Driven Pipeline

A pipeline design where logic is defined in metadata tables (`TransformationRegistry`, `IngestionSourceConfig`) rather than hardcoded in modules.

---

### Dual-Engine Execution

The ability to run transformations on either Spark (distributed) or Pandas (single-node) using the same function registry and orchestration logic.

---

### Hook System

An event-driven architecture that allows registration of callbacks at key lifecycle events (`pre_read`, `post_transform`, `on_error`, etc.).

---

### Function Registry

A singleton registry that maps function names and engines to callable functions, enabling dynamic resolution and metadata management.

---

### Execution Context

A data class that encapsulates runtime state (engine, Spark session, logger, hooks) for dual-engine functions.

---

### Project Manifest

A JSON configuration file (`manifest.json`) that defines project structure, layer order, dependencies, and caching strategy.

---

## Quick Reference Card

| Component | Module | Purpose |
|-----------|--------|---------|
| `GenericProjectOrchestrator` | `orchestration.generic_orchestrator` | Universal pipeline orchestration |
| `TransformationRunnerFromConfig` | `transformer.transformation_runner_from_config` | Execute transformations from registry |
| `FunctionRegistry` | `odibi_functions.registry` | Singleton function registry |
| `HookManager` | `hooks.manager` | Event-driven lifecycle callbacks |
| `ProjectManifest` | `project.manifest` | Project configuration schema |
| `Engine` | `core.engine` | Execution engine enum (SPARK/PANDAS) |
| `ExecutionContext` | `core.engine` | Runtime state for functions |
| `@odibi_function` | `odibi_functions.decorators` | Register reusable functions |
| `BaseLogSink` | `logging.log_sink` | Pluggable logging backend |
| `DeltaTableManager` | `databricks` | Delta Lake operations |

---

## Next Steps

- **[00-SYSTEM_OVERVIEW.md](00-SYSTEM_OVERVIEW.md)**: High-level architecture overview
- **[01-CORE_COMPONENTS.md](01-CORE_COMPONENTS.md)**: Detailed component breakdown
- **[02-DATAFLOW_EXAMPLES.md](02-DATAFLOW_EXAMPLES.md)**: Step-by-step examples
- **[03-EXTENDING_FRAMEWORK.md](03-EXTENDING_FRAMEWORK.md)**: Developer guide for extensions
- **[ARCHITECTURE_MAP.md](ARCHITECTURE_MAP.md)**: Visual module dependency diagrams
