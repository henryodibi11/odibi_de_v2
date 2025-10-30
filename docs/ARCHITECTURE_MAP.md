# Architecture Map - Visual Reference

## Table of Contents
- [Module Dependency Graph](#module-dependency-graph)
- [Package Structure](#package-structure)
- [Execution Flow Diagrams](#execution-flow-diagrams)
- [Data Flow Architecture](#data-flow-architecture)
- [Component Interaction Diagrams](#component-interaction-diagrams)

---

## Module Dependency Graph

### High-Level Module Dependencies

```mermaid
graph TB
    subgraph Core Layer
        A1[core/]
        A2[utils/]
        A3[logger/]
    end
    
    subgraph Configuration Layer
        B1[config/]
        B2[project/manifest]
        B3[sql_builder/]
    end
    
    subgraph Data Layer
        C1[connector/]
        C2[ingestion/]
        C3[storage/]
        C4[transformer/]
    end
    
    subgraph Orchestration Layer
        D1[orchestration/]
        D2[databricks/]
    end
    
    subgraph Extension Layer
        E1[odibi_functions/]
        E2[hooks/]
        E3[logging/]
    end
    
    A1 --> B1
    A1 --> C1
    A1 --> C2
    A1 --> C3
    A1 --> C4
    A2 --> C2
    A3 --> D1
    
    B1 --> D1
    B2 --> D1
    B3 --> C2
    
    C1 --> C2
    C1 --> C3
    C2 --> D1
    C3 --> D1
    C4 --> D1
    
    E1 --> C4
    E2 --> D1
    E3 --> D1
    
    D2 --> D1
    
    style A1 fill:#4CAF50,color:#fff
    style D1 fill:#FF9800,color:#fff
    style E1 fill:#9C27B0,color:#fff
    style E2 fill:#2196F3,color:#fff
```

---

## Package Structure

### Complete Directory Tree

```
odibi_de_v2/
├── cli/                          # Command-line interface
│   └── commands.py
│
├── config/                       # Configuration management
│   ├── ingestion_config.py
│   ├── transformation_config.py
│   └── transformation_registry_ui.py
│
├── connector/                    # Data source connectors
│   ├── azure_blob_connection.py
│   ├── sql_connection.py
│   └── local_file_connection.py
│
├── core/                         # Core abstractions
│   ├── __init__.py
│   ├── engine.py                 # Engine enum, ExecutionContext
│   ├── data_reader.py            # IDataReader interface
│   ├── data_saver.py             # IDataSaver interface
│   ├── data_transformer.py       # IDataTransformer interface
│   └── types.py                  # DataType, ErrorType enums
│
├── databricks/                   # Databricks-specific utilities
│   ├── orchestration/
│   │   ├── base_orchestrator.py
│   │   └── delta_table_manager.py
│   └── storage/
│       └── function_registry.py
│
├── hooks/                        # Event-driven hooks
│   ├── __init__.py
│   └── manager.py                # HookManager class
│
├── ingestion/                    # Data ingestion (Bronze)
│   ├── pandas_reader.py
│   ├── spark_reader.py
│   ├── reader_factory.py
│   └── reader_provider.py
│
├── logger/                       # Legacy logging (being replaced)
│   ├── dynamic_logger.py
│   ├── metadata_manager.py
│   └── decorator.py
│
├── logging/                      # New pluggable logging
│   ├── __init__.py
│   ├── log_sink.py               # BaseLogSink, SparkDeltaLogSink
│   └── console_sink.py
│
├── odibi_functions/              # Function registry system
│   ├── __init__.py
│   ├── registry.py               # FunctionRegistry singleton
│   ├── decorators.py             # @odibi_function, @spark_function
│   ├── context.py                # ExecutionContext helpers
│   └── examples.py               # Example functions
│
├── orchestration/                # Pipeline orchestration
│   ├── __init__.py
│   └── generic_orchestrator.py   # GenericProjectOrchestrator
│
├── pandas_utils/                 # Pandas-specific utilities
│   └── helpers.py
│
├── project/                      # Project manifest system
│   ├── __init__.py
│   └── manifest.py               # ProjectManifest, LayerConfig
│
├── spark_utils/                  # Spark-specific utilities
│   └── helpers.py
│
├── sql_builder/                  # Dynamic SQL generation
│   └── query_builder.py
│
├── storage/                      # Data saving (Delta, Parquet)
│   ├── pandas_saver.py
│   ├── spark_saver.py
│   ├── saver_factory.py
│   └── saver_provider.py
│
├── transformer/                  # Transformation logic
│   ├── transformation_runner_from_config.py  # TransformationRunnerFromConfig
│   ├── transformer_orchestrator.py
│   ├── transformer_function_registry.py
│   └── [transformation classes]
│
├── utils/                        # General utilities
│   ├── __init__.py
│   ├── thread_pool.py
│   └── validation.py
│
└── __init__.py
```

---

## Execution Flow Diagrams

### 1. Complete Pipeline Execution

```mermaid
flowchart TD
    Start([User calls orchestrator.run_project]) --> LoadManifest[Load ProjectManifest]
    LoadManifest --> ValidateLayers{Validate layer dependencies}
    ValidateLayers -->|Invalid| Error1[Raise ValidationError]
    ValidateLayers -->|Valid| LoopLayers{For each layer in layer_order}
    
    LoopLayers --> CheckLayerType{Layer type?}
    
    CheckLayerType -->|Bronze| EmitStart1[Emit pipeline_start hook]
    EmitStart1 --> RunBronze[run_bronze_layer]
    RunBronze --> FetchIngestion[Fetch IngestionSourceConfig]
    FetchIngestion --> InitIngestion[Initialize IngestionOrchestrator]
    InitIngestion --> RunIngestions[Run all enabled ingestions]
    RunIngestions --> EmitEnd1[Emit pipeline_end hook]
    EmitEnd1 --> NextLayer1{More layers?}
    
    CheckLayerType -->|Silver/Gold| EmitStart2[Emit pipeline_start hook]
    EmitStart2 --> RunTransform[run_transformation_layer]
    RunTransform --> FetchRegistry[Fetch TransformationRegistry]
    FetchRegistry --> InitRunner[Initialize TransformationRunner]
    InitRunner --> RunTransformations[Execute all transformations]
    RunTransformations --> CacheTables{cache_plan defined?}
    CacheTables -->|Yes| CacheDelta[Cache Delta tables]
    CacheTables -->|No| EmitEnd2[Emit pipeline_end hook]
    CacheDelta --> EmitEnd2
    EmitEnd2 --> NextLayer2{More layers?}
    
    NextLayer1 -->|Yes| LoopLayers
    NextLayer1 -->|No| SaveLogs{save_logs enabled?}
    NextLayer2 -->|Yes| LoopLayers
    NextLayer2 -->|No| SaveLogs
    
    SaveLogs -->|Yes| PersistLogs[Persist logs to storage]
    SaveLogs -->|No| Complete([Pipeline complete])
    PersistLogs --> Complete
    
    Error1 --> ErrorEnd([Error exit])
    
    style Start fill:#4CAF50,color:#fff
    style Complete fill:#4CAF50,color:#fff
    style RunBronze fill:#cd7f32,color:#fff
    style RunTransform fill:#c0c0c0
    style ErrorEnd fill:#f44336,color:#fff
```

### 2. Transformation Execution Flow

```mermaid
flowchart TD
    Start([TransformationRunner.run]) --> FetchConfigs[Fetch configs from TransformationRegistry]
    FetchConfigs --> FilterEnabled{Filter enabled=True}
    FilterEnabled --> LoopConfigs{For each config}
    
    LoopConfigs --> EmitPreTransform[Emit pre_transform hook]
    EmitPreTransform --> ParseInputs[Parse inputs JSON array]
    ParseInputs --> ParseConstants[Parse constants JSON object]
    ParseConstants --> DetermineEngine{Determine effective engine}
    
    DetermineEngine --> CheckOverride{constants.engine exists?}
    CheckOverride -->|Yes| UseOverride[Use constants.engine]
    CheckOverride -->|No| UseDefault[Use runner.engine]
    
    UseOverride --> ResolveFunction[REGISTRY.resolve module, function, engine]
    UseDefault --> ResolveFunction
    
    ResolveFunction --> CheckRegistry{Function found?}
    CheckRegistry -->|Yes| UseFunctionRef[Use registered function]
    CheckRegistry -->|No| Fallback[Fallback to importlib]
    
    Fallback --> ImportModule[Import module dynamically]
    ImportModule --> GetAttr[getattr function from module]
    
    UseFunctionRef --> InspectSig[Inspect function signature]
    GetAttr --> InspectSig
    
    InspectSig --> CheckContext{Has 'context' param?}
    CheckContext -->|Yes| BuildContext[Build ExecutionContext]
    CheckContext -->|No| SkipContext[Skip context]
    
    BuildContext --> ReadInputs[Read input DataFrames from Delta]
    SkipContext --> ReadInputs
    
    ReadInputs --> ExecuteFunc[Execute function with inputs + constants]
    ExecuteFunc --> CheckError{Exception?}
    
    CheckError -->|Yes| EmitError[Emit on_error hook]
    EmitError --> LogError[Log to TransformationRunLog status=failed]
    LogError --> NextConfig1{More configs?}
    
    CheckError -->|No| WriteOutputs[Write outputs to Delta tables]
    WriteOutputs --> EmitPostTransform[Emit post_transform hook]
    EmitPostTransform --> LogSuccess[Log to TransformationRunLog status=success]
    LogSuccess --> NextConfig2{More configs?}
    
    NextConfig1 -->|Yes| LoopConfigs
    NextConfig1 -->|No| End([Return summary])
    NextConfig2 -->|Yes| LoopConfigs
    NextConfig2 -->|No| End
    
    style Start fill:#FF9800,color:#fff
    style ExecuteFunc fill:#9C27B0,color:#fff
    style End fill:#4CAF50,color:#fff
    style EmitError fill:#f44336,color:#fff
```

### 3. Function Resolution Flow

```mermaid
flowchart LR
    A[TransformationConfig] -->|module, function, engine| B{REGISTRY.resolve}
    
    B -->|Check| C{Engine-specific match?}
    C -->|Yes| D[Return function name, engine]
    C -->|No| E{Universal match engine=any?}
    E -->|Yes| F[Return function name, any]
    E -->|No| G[Return None]
    
    D --> H[Callable function]
    F --> H
    G --> I[Fallback to importlib]
    
    I --> J[Import module]
    J --> K[getattr function]
    K --> H
    
    H --> L[Execute with inputs + constants]
    
    style B fill:#9C27B0,color:#fff
    style D fill:#4CAF50,color:#fff
    style F fill:#2196F3,color:#fff
    style I fill:#FF9800,color:#fff
```

---

## Data Flow Architecture

### Bronze → Silver → Gold Flow

```mermaid
graph LR
    subgraph Sources
        S1[CSV Files]
        S2[SQL Server]
        S3[REST API]
        S4[Azure Blob]
    end
    
    subgraph Bronze Layer
        B1[(bronze.raw_orders)]
        B2[(bronze.raw_customers)]
        B3[(bronze.raw_products)]
    end
    
    subgraph Silver Layer - Phase 1
        SL1[(silver.orders_clean)]
        SL2[(silver.customers_validated)]
        SL3[(silver.products_enriched)]
    end
    
    subgraph Silver Layer - Phase 2
        SL4[(silver.orders_customers)]
    end
    
    subgraph Gold Layer
        G1[(gold.daily_sales)]
        G2[(gold.customer_ltv)]
        G3[(gold.product_performance)]
    end
    
    S1 -->|Ingestion| B1
    S2 -->|Ingestion| B2
    S3 -->|Ingestion| B3
    S4 -->|Ingestion| B1
    
    B1 -->|Dedup, Validate| SL1
    B2 -->|Type cast, Clean nulls| SL2
    B3 -->|Schema enforcement| SL3
    
    SL1 -->|Join| SL4
    SL2 -->|Join| SL4
    
    SL4 -->|Aggregate| G1
    SL2 -->|Calculate LTV| G2
    SL3 -->|KPI calculation| G3
    
    style B1 fill:#cd7f32,color:#fff
    style B2 fill:#cd7f32,color:#fff
    style B3 fill:#cd7f32,color:#fff
    style SL1 fill:#c0c0c0
    style SL2 fill:#c0c0c0
    style SL3 fill:#c0c0c0
    style SL4 fill:#c0c0c0
    style G1 fill:#ffd700
    style G2 fill:#ffd700
    style G3 fill:#ffd700
```

### Dual-Engine Data Processing

```mermaid
graph TB
    A[TransformationConfig] -->|constants.engine| B{Engine Override?}
    
    B -->|"engine: spark"| C[Spark Execution Path]
    B -->|"engine: pandas"| D[Pandas Execution Path]
    B -->|No override| E[Use Runner Default]
    
    E --> F{runner.engine?}
    F -->|SPARK| C
    F -->|PANDAS| D
    
    C --> G[SparkSession.table]
    C --> H[Spark DataFrame]
    C --> I[PySpark operations]
    C --> J[Write to Delta via Spark]
    
    D --> K[Read from Delta as Parquet]
    D --> L[Pandas DataFrame]
    D --> M[Pandas operations]
    D --> N[Write to Delta as Parquet]
    
    J --> O[(Delta Lake)]
    N --> O
    
    style C fill:#e25822,color:#fff
    style D fill:#150458,color:#fff
    style O fill:#00ADD4,color:#fff
```

---

## Component Interaction Diagrams

### 1. Orchestrator → Runner → Registry Interaction

```
┌─────────────────────────────────────────────────────────────┐
│                  GenericProjectOrchestrator                 │
│  - Load manifest.json                                       │
│  - Determine layer execution order                          │
│  - Manage hooks and logging                                 │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          │ Initialize per layer
                          ↓
┌─────────────────────────────────────────────────────────────┐
│              TransformationRunnerFromConfig                 │
│  - Fetch TransformationRegistry configs                     │
│  - Execute transformations in parallel                      │
│  - Emit lifecycle hooks                                     │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          │ Resolve function
                          ↓
┌─────────────────────────────────────────────────────────────┐
│                    FunctionRegistry                         │
│  - Singleton registry                                       │
│  - Engine-specific function resolution                      │
│  - Metadata management                                      │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          │ Return callable
                          ↓
┌─────────────────────────────────────────────────────────────┐
│              Transformation Function                        │
│  @spark_function or @pandas_function                        │
│  - Receives inputs (DataFrames)                             │
│  - Receives constants (parameters)                          │
│  - Optionally receives ExecutionContext                     │
│  - Returns transformed DataFrame                            │
└─────────────────────────────────────────────────────────────┘
```

### 2. Hook Event Flow

```mermaid
sequenceDiagram
    participant Orchestrator
    participant Runner
    participant Hooks as HookManager
    participant User1 as Validation Hook
    participant User2 as Monitoring Hook
    participant User3 as Notification Hook
    
    Orchestrator->>Hooks: emit("pipeline_start", {project, env, layer})
    Hooks->>User2: callback({project, env, layer})
    User2-->>Hooks: (log metrics)
    
    Orchestrator->>Runner: run_transformations(layer)
    
    Runner->>Hooks: emit("pre_transform", {function, module})
    Hooks->>User2: callback({function, module})
    User2-->>Hooks: (start timer)
    
    Runner->>Runner: Execute transformation
    
    alt Success
        Runner->>Hooks: emit("post_transform", {df, duration})
        Hooks->>User1: callback({df, duration})
        User1->>User1: validate_schema(df)
        User1-->>Hooks: (validation passed)
        Hooks->>User2: callback({df, duration})
        User2-->>Hooks: (log metrics)
    else Error
        Runner->>Hooks: emit("on_error", {error, function})
        Hooks->>User3: callback({error, function})
        User3->>User3: send_slack_alert(error)
        User3-->>Hooks: (notification sent)
    end
    
    Orchestrator->>Hooks: emit("pipeline_end", {summary, duration})
    Hooks->>User2: callback({summary, duration})
    User2-->>Hooks: (final metrics)
```

### 3. Dual-Engine Context Injection

```
┌────────────────────────────────────────────────────────────┐
│          TransformationRunnerFromConfig                    │
│                                                            │
│  1. Determine effective_engine = cfg.get("constants",     │
│     {}).get("engine", self.engine.value)                  │
│                                                            │
│  2. Build ExecutionContext:                                │
│     ┌──────────────────────────────────────────┐          │
│     │  context = ExecutionContext(             │          │
│     │      engine=Engine[effective_engine],    │          │
│     │      project=cfg['project'],             │          │
│     │      env=cfg.get('env', self.env),       │          │
│     │      spark=self.spark,                   │          │
│     │      sql_provider=self.sql_provider,     │          │
│     │      logger=self.logger,                 │          │
│     │      hooks=self.hooks,                   │          │
│     │      extras={                            │          │
│     │          "plant": cfg.get('entity_1'),   │          │
│     │          "asset": cfg.get('entity_2'),   │          │
│     │          "layer": self.layer             │          │
│     │      }                                    │          │
│     │  )                                        │          │
│     └──────────────────────────────────────────┘          │
│                                                            │
│  3. Inspect function signature:                            │
│     if 'context' in inspect.signature(func).parameters:   │
│         func_kwargs['context'] = context                  │
│                                                            │
│  4. Execute:                                               │
│     result = func(**inputs, **constants, **func_kwargs)   │
└────────────────────────────────────────────────────────────┘
                           │
                           ↓
┌────────────────────────────────────────────────────────────┐
│       Transformation Function (with context)               │
│                                                            │
│  @spark_function(module="my_module")                       │
│  def my_transformation(df, param1, context=None):          │
│      if context:                                           │
│          # Access context.spark                            │
│          # Access context.logger                           │
│          # Access context.hooks                            │
│          # Access context.extras["plant"]                  │
│      return transformed_df                                 │
└────────────────────────────────────────────────────────────┘
```

---

## Component Responsibility Matrix

| Component | Responsibilities | Dependencies |
|-----------|-----------------|--------------|
| **GenericProjectOrchestrator** | - Load project manifest<br>- Execute layer sequence<br>- Manage caching<br>- Coordinate hooks | ProjectManifest, TransformationRunnerFromConfig, DeltaTableManager, HookManager |
| **TransformationRunnerFromConfig** | - Fetch transformation configs<br>- Resolve functions<br>- Execute transformations<br>- Log results | FunctionRegistry, ExecutionContext, BaseLogSink, HookManager |
| **FunctionRegistry** | - Store registered functions<br>- Resolve by (name, engine)<br>- Manage metadata | None (singleton) |
| **HookManager** | - Register event callbacks<br>- Emit events with payloads<br>- Filter by project/layer/engine | None |
| **ProjectManifest** | - Define project structure<br>- Validate layer dependencies<br>- Serialize/deserialize JSON | LayerConfig, ProjectType |
| **ExecutionContext** | - Encapsulate runtime state<br>- Provide access to resources | Engine, HookManager, Logger |
| **BaseLogSink** | - Abstract logging interface<br>- Write log records | None (interface) |
| **DeltaTableManager** | - Cache Delta tables<br>- Optimize tables<br>- Vacuum old files | SparkSession, Delta Lake |

---

## Layered Architecture View

```
┌─────────────────────────────────────────────────────────────┐
│                    Presentation Layer                       │
│                  (CLI, Notebooks, APIs)                     │
└───────────────────────┬─────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────┐
│                  Orchestration Layer                        │
│  GenericProjectOrchestrator, ProjectManifest                │
└───────────────────────┬─────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────┐
│                   Execution Layer                           │
│  TransformationRunnerFromConfig, IngestionOrchestrator      │
└───────────────────────┬─────────────────────────────────────┘
                        │
        ┌───────────────┼───────────────┐
        │               │               │
┌───────▼─────┐ ┌───────▼─────┐ ┌──────▼──────┐
│  Function   │ │    Hook     │ │   Logging   │
│  Registry   │ │   Manager   │ │    Sinks    │
└───────┬─────┘ └─────────────┘ └─────────────┘
        │
┌───────▼─────────────────────────────────────────────────────┐
│                    Data Access Layer                        │
│  Readers (Spark, Pandas) | Savers (Delta, Parquet)          │
└───────────────────────┬─────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────┐
│                   Storage Layer                             │
│  Delta Lake, Parquet, CSV, Azure Blob Storage               │
└─────────────────────────────────────────────────────────────┘
```

---

## Summary

This architecture map provides:

1. ✅ **Module dependency graph**: Understand how packages relate to each other
2. ✅ **Package structure**: Complete directory tree with file purposes
3. ✅ **Execution flow diagrams**: Visual step-by-step pipeline execution
4. ✅ **Data flow architecture**: Bronze → Silver → Gold medallion pattern
5. ✅ **Component interaction diagrams**: How classes collaborate
6. ✅ **Responsibility matrix**: What each component owns
7. ✅ **Layered architecture view**: Abstraction layers from presentation to storage

---

## Next Steps

- **[00-SYSTEM_OVERVIEW.md](00-SYSTEM_OVERVIEW.md)**: High-level architecture overview
- **[01-CORE_COMPONENTS.md](01-CORE_COMPONENTS.md)**: Detailed component breakdown
- **[02-DATAFLOW_EXAMPLES.md](02-DATAFLOW_EXAMPLES.md)**: Step-by-step examples
- **[03-EXTENDING_FRAMEWORK.md](03-EXTENDING_FRAMEWORK.md)**: Developer guide
- **[04-GLOSSARY.md](04-GLOSSARY.md)**: Complete reference
