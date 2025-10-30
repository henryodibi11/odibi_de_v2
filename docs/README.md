# ODIBI DE v2 - Developer Documentation

## 📚 Documentation Index

Welcome to the comprehensive developer documentation for the ODIBI DE v2 framework. This documentation is organized into six main sections, each covering a different aspect of the framework.

---

## 📖 Documentation Structure

### [00-SYSTEM_OVERVIEW.md](00-SYSTEM_OVERVIEW.md) - Start Here! 🚀

**Purpose**: High-level introduction to the framework's architecture and philosophy.

**Contents**:
- Purpose and design principles
- Medallion architecture (Bronze → Silver → Gold)
- Config-driven model explanation
- Core components interaction diagram
- Dual-engine architecture overview
- Quick start example

**Who should read this**: Everyone new to the framework, architects, project managers.

**Estimated reading time**: 15-20 minutes

---

### [01-CORE_COMPONENTS.md](01-CORE_COMPONENTS.md) - Deep Dive 🔍

**Purpose**: Detailed breakdown of major framework components.

**Contents**:
- `GenericProjectOrchestrator` - Universal pipeline orchestration
- `TransformationRunnerFromConfig` - Transformation execution engine
- `FunctionRegistry` & Decorators - Reusable function system
- `TransformationRegistry` & Manifest - Configuration schemas
- Dual-engine architecture (Engine enum, ExecutionContext)
- Hooks system - Event-driven callbacks

**Who should read this**: Developers, data engineers implementing pipelines.

**Estimated reading time**: 30-40 minutes

---

### [02-DATAFLOW_EXAMPLES.md](02-DATAFLOW_EXAMPLES.md) - Learn by Example 💡

**Purpose**: Step-by-step walkthroughs of real-world dataflow scenarios.

**Contents**:
- Complete pipeline: Ingestion → Bronze → Silver → Gold
- Spark workflow example (large-scale processing)
- Pandas workflow example (small dataset analytics)
- Hook event flow with payload examples
- Mixed-engine pipeline (Spark + Pandas)

**Who should read this**: Data engineers, analysts learning the framework.

**Estimated reading time**: 45-60 minutes (includes hands-on examples)

---

### [03-EXTENDING_FRAMEWORK.md](03-EXTENDING_FRAMEWORK.md) - Developer Guide 🛠️

**Purpose**: How to extend and customize the framework for your needs.

**Contents**:
- Creating custom transformation functions
- Building new transformation nodes (class-based)
- Adding custom hook types
- Implementing custom log sinks (PostgreSQL, Azure Blob)
- Creating custom readers/savers (REST API, Parquet)
- Databricks vs local development strategies

**Who should read this**: Framework contributors, advanced users building custom components.

**Estimated reading time**: 60-90 minutes (includes code examples)

---

### [04-GLOSSARY.md](04-GLOSSARY.md) - Reference 📋

**Purpose**: Complete reference of all classes, tables, and concepts.

**Contents**:
- Core classes (with all attributes and methods)
- Configuration tables (schemas and examples)
- Enums and data classes
- Decorators reference
- Interfaces (IDataTransformer, IDataReader, IDataSaver)
- Key concepts and definitions

**Who should read this**: Everyone (as a reference guide).

**Estimated reading time**: Reference material (searchable)

---

### [ARCHITECTURE_MAP.md](ARCHITECTURE_MAP.md) - Visual Reference 🗺️

**Purpose**: Visual diagrams showing module relationships and data flow.

**Contents**:
- Module dependency graph
- Complete package structure
- Execution flow diagrams (Mermaid)
- Data flow architecture (Bronze → Silver → Gold)
- Component interaction diagrams
- Responsibility matrix

**Who should read this**: Architects, visual learners, debugging complex issues.

**Estimated reading time**: 20-30 minutes (diagram-heavy)

---

## 🎯 Recommended Learning Paths

### For New Users
1. Start with **00-SYSTEM_OVERVIEW.md** to understand the big picture
2. Read **02-DATAFLOW_EXAMPLES.md** to see practical examples
3. Reference **04-GLOSSARY.md** as needed
4. Use **ARCHITECTURE_MAP.md** for visual understanding

### For Data Engineers Implementing Pipelines
1. **00-SYSTEM_OVERVIEW.md** - Understand the framework
2. **01-CORE_COMPONENTS.md** - Learn the orchestrator and runner
3. **02-DATAFLOW_EXAMPLES.md** - Follow step-by-step examples
4. **04-GLOSSARY.md** - Reference for TransformationRegistry schema
5. **ARCHITECTURE_MAP.md** - Visualize data flow

### For Framework Contributors/Extenders
1. **00-SYSTEM_OVERVIEW.md** - Understand design philosophy
2. **01-CORE_COMPONENTS.md** - Deep dive into all components
3. **03-EXTENDING_FRAMEWORK.md** - Learn extension patterns
4. **ARCHITECTURE_MAP.md** - Understand module dependencies
5. **04-GLOSSARY.md** - Reference for interfaces and base classes

### For Architects/Project Planners
1. **00-SYSTEM_OVERVIEW.md** - High-level architecture
2. **ARCHITECTURE_MAP.md** - Visual architecture diagrams
3. **01-CORE_COMPONENTS.md** - Component responsibilities
4. **04-GLOSSARY.md** - Project manifest schema

---

## 📊 Quick Reference Tables

### Core Components at a Glance

| Component | Module | Purpose | Key Method |
|-----------|--------|---------|------------|
| `GenericProjectOrchestrator` | `orchestration.generic_orchestrator` | Universal pipeline orchestration | `run_project()` |
| `TransformationRunnerFromConfig` | `transformer.transformation_runner_from_config` | Execute transformations | `run()` |
| `FunctionRegistry` | `odibi_functions.registry` | Function resolution & storage | `resolve()` |
| `HookManager` | `hooks.manager` | Event-driven callbacks | `emit()` |
| `ProjectManifest` | `project.manifest` | Project configuration | `from_json()` |

### Configuration Tables

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `TransformationRegistry` | Define transformations | `module`, `function`, `inputs`, `outputs` |
| `IngestionSourceConfig` | Define data sources | `source_type`, `source_path`, `target_table` |
| `TransformationRunLog` | Execution history | `status`, `duration_seconds`, `error_message` |

### Decorators

| Decorator | Purpose | Example |
|-----------|---------|---------|
| `@odibi_function` | Register any function | `@odibi_function(engine="spark")` |
| `@spark_function` | Register Spark function | `@spark_function(module="cleaning")` |
| `@pandas_function` | Register Pandas function | `@pandas_function(module="analysis")` |
| `@universal_function` | Register universal function | `@universal_function(module="utils")` |
| `@with_context` | Inject ExecutionContext | `@with_context(context_param="ctx")` |

---

## 🔗 Related Documentation

### Framework Root Documentation
- [README.md](../README.md) - Main project README
- [MIGRATION_GUIDE.md](../MIGRATION_GUIDE.md) - Migration from v1 to v2
- [QUICK_REFERENCE.md](../QUICK_REFERENCE.md) - Quick reference card
- [TRANSFORMATION_RUNNER_DUAL_ENGINE_UPDATE.md](../TRANSFORMATION_RUNNER_DUAL_ENGINE_UPDATE.md) - Dual-engine update details
- [ORCHESTRATOR_DUAL_ENGINE_UPDATE.md](../ORCHESTRATOR_DUAL_ENGINE_UPDATE.md) - Orchestrator update details

### In-Module Documentation
- [odibi_functions/README.md](../odibi_de_v2/odibi_functions/README.md) - Function registry system
- [odibi_functions/QUICK_START.md](../odibi_de_v2/odibi_functions/QUICK_START.md) - Function registry quick start

---

## 💡 Quick Start Guide

### 1. Understand the Basics (10 minutes)

Read the **Purpose**, **Architecture Philosophy**, and **Medallion Flow Diagram** sections in [00-SYSTEM_OVERVIEW.md](00-SYSTEM_OVERVIEW.md).

### 2. See It in Action (20 minutes)

Follow the **Complete Pipeline: Ingestion to Gold** example in [02-DATAFLOW_EXAMPLES.md](02-DATAFLOW_EXAMPLES.md).

### 3. Build Your First Pipeline (30 minutes)

1. Create a `manifest.json` for your project
2. Define transformation functions with `@spark_function` or `@pandas_function`
3. Add entries to `TransformationRegistry`
4. Run with `GenericProjectOrchestrator`

**Example**:
```python
from odibi_de_v2.orchestration import GenericProjectOrchestrator
from odibi_de_v2.odibi_functions import spark_function

@spark_function(module="my_project")
def clean_data(df, context=None):
    return df.dropna()

orchestrator = GenericProjectOrchestrator(
    project="My Project",
    env="dev",
    engine="spark"
)

orchestrator.run_project()
```

---

## ❓ Frequently Asked Questions

### Q: Where do I define my transformation logic?

**A**: You have two options:
1. **Function-based** (recommended): Use `@odibi_function`, `@spark_function`, or `@pandas_function` decorators
2. **Class-based**: Implement `IDataTransformer` interface

See [03-EXTENDING_FRAMEWORK.md](03-EXTENDING_FRAMEWORK.md#creating-custom-transformation-functions) for details.

---

### Q: How do I switch between Spark and Pandas for a transformation?

**A**: Set `"engine"` in the `constants` field of `TransformationRegistry`:

```json
{
  "transformation_id": "T001",
  "constants": {
    "engine": "pandas"  // Override to pandas
  }
}
```

See [01-CORE_COMPONENTS.md](01-CORE_COMPONENTS.md#dual-engine-example) for examples.

---

### Q: How do I add custom validation hooks?

**A**: Register a callback with `HookManager`:

```python
from odibi_de_v2.hooks import HookManager

hooks = HookManager()

def validate_schema(payload):
    df = payload["df"]
    required_cols = ["id", "timestamp"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

hooks.register("post_read", validate_schema)
```

See [03-EXTENDING_FRAMEWORK.md](03-EXTENDING_FRAMEWORK.md#adding-custom-hook-types) for details.

---

### Q: How do I log to a custom backend (not Delta)?

**A**: Implement `BaseLogSink` interface:

```python
from odibi_de_v2.logging import BaseLogSink

class PostgreSQLLogSink(BaseLogSink):
    def write(self, records):
        # Your PostgreSQL write logic
        pass

log_sink = PostgreSQLLogSink(connection_string)

runner = TransformationRunnerFromConfig(
    ...,
    log_sink=log_sink
)
```

See [03-EXTENDING_FRAMEWORK.md](03-EXTENDING_FRAMEWORK.md#implementing-custom-log-sinks) for full example.

---

### Q: Where can I find the schemas for TransformationRegistry and IngestionSourceConfig?

**A**: See [04-GLOSSARY.md](04-GLOSSARY.md#transformationregistry) for complete schemas with examples.

---

## 🧭 Navigation Tips

All documentation files are **cross-linked** for easy navigation:
- Click on section headings to jump within a document
- Click on file links (e.g., `[01-CORE_COMPONENTS.md]`) to navigate between docs
- Use your browser's search (`Ctrl+F` / `Cmd+F`) to find specific terms
- Mermaid diagrams are interactive in most Markdown viewers

---

## 📝 Contributing to Documentation

Found an error or want to improve the documentation?

1. Documentation lives in `/d:/projects/odibi_de_v2/docs/`
2. Follow the existing structure and formatting
3. Add examples where helpful
4. Update the cross-references if adding new sections
5. Test all code examples before committing

---

## 📧 Support

For questions or issues:
- Check the relevant documentation section first
- Search the [Glossary](04-GLOSSARY.md) for definitions
- Review [examples](02-DATAFLOW_EXAMPLES.md) for similar use cases
- Consult the [Architecture Map](ARCHITECTURE_MAP.md) for visual understanding

---

## 🎓 Certification Checklist

After reading this documentation, you should be able to:

- [ ] Explain the medallion architecture (Bronze → Silver → Gold)
- [ ] Create a project manifest (`manifest.json`)
- [ ] Register transformation functions with decorators
- [ ] Configure `TransformationRegistry` table entries
- [ ] Run pipelines with `GenericProjectOrchestrator`
- [ ] Switch between Spark and Pandas engines
- [ ] Add custom hooks for validation/monitoring
- [ ] Implement custom log sinks
- [ ] Understand the function resolution flow
- [ ] Use `ExecutionContext` in transformation functions

If you can check all boxes, you're ready to build production pipelines! 🎉

---

**Last Updated**: January 2024  
**Framework Version**: 2.0  
**Documentation Version**: 1.0
