# 07 - Project Structure and Self-Bootstrap

**Level:** Intermediate  
**Duration:** 45-60 minutes  
**Prerequisites:** Basic understanding of medallion architecture

---

## Table of Contents
1. [Introduction to Project Manifests](#introduction-to-project-manifests)
2. [Manifest Structure Deep Dive](#manifest-structure-deep-dive)
3. [How initialize_project() Works](#how-initialize_project-works)
4. [Energy Efficiency Case Study](#energy-efficiency-case-study)
5. [Customizing Manifests for Industries](#customizing-manifests-for-industries)
6. [Project Directory Structure](#project-directory-structure)
7. [Best Practices](#best-practices)

---

## Introduction to Project Manifests

### What is a Project Manifest?

A **project manifest** (`manifest.json`) is the single source of truth for your data engineering project. It defines:

- **Project identity**: Name, type, version, description
- **Layer architecture**: Bronze, Silver, Gold layers and dependencies
- **Environment config**: QAT, PROD, and other environment settings
- **Entity mapping**: Domain-specific naming (plant, asset, customer, etc.)
- **Execution paths**: Where to find transformation modules
- **Cache strategy**: Which tables to cache and when

**Key Benefit:** One manifest → Complete project scaffolding in seconds

### Why Manifests?

**Before Manifests (Manual Setup):**
```bash
# 2-3 hours of manual work
mkdir project/
mkdir project/transformations/{bronze,silver,gold}
touch project/transformations/__init__.py
# ... repeat 20+ times
# Write boilerplate code
# Configure paths
# Create README
```

**With Manifests (initialize_project()):**
```python
from odibi_de_v2 import initialize_project

# 5 seconds
result = initialize_project("Energy Efficiency", "manufacturing")
```

---

## Manifest Structure Deep Dive

### Complete Field Reference

Let's examine each field in `manifest.json`:

```json
{
  "project_name": "Energy Efficiency",
  "project_type": "manufacturing",
  "description": "Energy Efficiency - Manufacturing Data Pipeline",
  "version": "1.0.0",
  "layer_order": ["Bronze", "Silver_1", "Silver_2", "Gold_1", "Gold_2"],
  "layers": { /* ... */ },
  "environments": ["qat", "prod"],
  "default_env": "qat",
  "entity_labels": { /* ... */ },
  "transformation_modules": ["energy_efficiency.transformations"],
  "cache_plan": { /* ... */ },
  "owner": null,
  "tags": [],
  "metadata": {}
}
```

#### 1. Core Identification

**`project_name`** (string, required)
- Human-readable project name
- Used in logs, UIs, and documentation
- Example: `"Energy Efficiency"`, `"Customer Churn"`, `"Sales Forecasting"`

**`project_type`** (enum, required)
- Determines default layer structure and templates
- Options:
  - `"manufacturing"` → Bronze, Silver_1-2, Gold_1-2 (asset hierarchies)
  - `"analytics"` → Bronze, Silver_1, Gold_1 (standard medallion)
  - `"ml_pipeline"` → Bronze, Features, Training, Inference
  - `"data_integration"` → Ingestion, Transform, Export
  - `"custom"` → Define your own structure

**`description`** (string, required)
- Brief project description
- Shows in generated README and logs

**`version`** (string, default: "1.0.0")
- Semantic versioning for the project
- Track breaking changes in manifest structure

#### 2. Layer Architecture

**`layer_order`** (array, required)
- Execution order of layers
- Defines DAG topology
- **Critical:** Layers execute in this exact order

```json
"layer_order": ["Bronze", "Silver_1", "Silver_2", "Gold_1", "Gold_2"]
```

**`layers`** (object, required)
- Detailed configuration for each layer
- Must include all layers in `layer_order`

Each layer has:

```json
{
  "name": "Silver_1",
  "description": "Asset-level transformations",
  "depends_on": ["Bronze"],
  "cache_tables": [],
  "max_workers": null
}
```

**Fields:**
- `name`: Layer identifier (must match key in `layers` object)
- `description`: What this layer does
- `depends_on`: Parent layers (must complete before this runs)
- `cache_tables`: Tables to persist in memory/disk
- `max_workers`: Parallelism limit (null = unlimited)

#### 3. Environment Configuration

**`environments`** (array, required)
- List of valid environments
- Standard: `["qat", "prod"]`
- Custom: `["dev", "staging", "uat", "prod"]`

**`default_env`** (string, required)
- Default environment if not specified
- Usually `"qat"` or `"dev"`

**Usage:**
```python
from odibi_de_v2 import run_project

# Uses default_env = "qat"
run_project(project="Energy Efficiency")

# Explicit environment
run_project(project="Energy Efficiency", env="prod")
```

#### 4. Entity Labels (Domain Mapping)

**`entity_labels`** (object, optional)
- Maps generic entity names to domain-specific terms
- Enables domain-driven design

```json
"entity_labels": {
  "entity_1": "plant",
  "entity_2": "asset",
  "entity_3": "equipment"
}
```

**Manufacturing Example:**
- `entity_1` → "plant" (factory location)
- `entity_2` → "asset" (production line)
- `entity_3` → "equipment" (motor, sensor)

**Retail Example:**
```json
"entity_labels": {
  "entity_1": "store",
  "entity_2": "department",
  "entity_3": "product"
}
```

#### 5. Transformation Modules

**`transformation_modules`** (array, required)
- Python module paths containing transformation functions
- Used by orchestrator to discover functions

```json
"transformation_modules": [
  "energy_efficiency.transformations",
  "energy_efficiency.ml_models"
]
```

**Import Resolution:**
```python
# Orchestrator imports:
from energy_efficiency.transformations.silver_1.functions import calculate_efficiency
```

#### 6. Cache Plan

**`cache_plan`** (object, optional)
- Defines which tables to cache per layer
- Performance optimization for expensive joins/aggregations

```json
"cache_plan": {
  "Gold_1": [
    "qat_energy_efficiency.combined_dryers",
    "qat_energy_efficiency.plant_summary"
  ]
}
```

**When to Cache:**
- Large tables referenced multiple times
- Complex aggregations reused across layers
- Hot tables accessed by multiple transformations

#### 7. Metadata

**`owner`** (string, optional)
- Team or person responsible
- Example: `"data-engineering-team@company.com"`

**`tags`** (array, optional)
- Classification tags
- Example: `["pii-data", "gdpr", "critical"]`

**`metadata`** (object, optional)
- Custom key-value pairs
- Example: `{"slack_channel": "#data-eng", "jira_project": "DE"}`

---

## How initialize_project() Works

### Function Signature

```python
from odibi_de_v2 import initialize_project

result = initialize_project(
    project_name: str,
    project_type: str = "custom",
    base_path: Optional[Path] = None,
    overwrite: bool = False
)
```

**Parameters:**
- `project_name`: Human-readable name (e.g., "Energy Efficiency")
- `project_type`: One of: manufacturing, analytics, ml_pipeline, custom
- `base_path`: Where to create project (default: `./projects`)
- `overwrite`: Replace existing project if True

**Returns:**
```python
{
  "project_name": "Energy Efficiency",
  "project_slug": "energy_efficiency",
  "project_path": "/path/to/projects/energy_efficiency",
  "manifest_path": "/path/to/projects/energy_efficiency/manifest.json",
  "created_files": [...],  # List of all created files
  "layer_order": ["Bronze", "Silver_1", "Silver_2", "Gold_1", "Gold_2"],
  "next_steps": [...]  # What to do next
}
```

### Execution Flow

#### Step 1: Create Manifest Template

```python
# From odibi_de_v2/project/scaffolding.py
manifest = ProjectManifest.create_template(project_name, project_type)
```

**What Happens:**
- Generates layer structure based on `project_type`
- Sets default environments `["qat", "prod"]`
- Configures entity labels for domain
- Creates empty cache plan

#### Step 2: Create Directory Structure

**Generated Structure:**
```
energy_efficiency/
├── manifest.json              # Project configuration
├── transformations/           # Python transformation modules
│   ├── __init__.py
│   ├── bronze/               # Bronze layer (if needed)
│   ├── silver_1/
│   │   ├── __init__.py
│   │   └── functions.py      # Template functions
│   ├── silver_2/
│   │   ├── __init__.py
│   │   └── functions.py
│   ├── gold_1/
│   │   ├── __init__.py
│   │   └── functions.py
│   └── gold_2/
│       ├── __init__.py
│       └── functions.py
├── sql/                      # SQL scripts
│   ├── ddl/                  # Data definition language
│   └── queries/              # Analytical queries
├── notebooks/                # Databricks/Jupyter notebooks
│   ├── bronze/
│   ├── silver_1/
│   ├── silver_2/
│   ├── gold_1/
│   └── gold_2/
├── tests/                    # Unit and integration tests
├── config/                   # Configuration files
│   └── transformation_config_example.json
├── docs/                     # Documentation
└── metadata/                 # Metadata and schemas
```

#### Step 3: Generate Template Files

**Example: `transformations/silver_1/functions.py`**

```python
"""
Silver_1 Layer Transformations for Energy Efficiency

This module contains transformation functions for the Silver_1 layer.
Each function should be registered in the TransformationRegistry.
"""

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from typing import Optional, Dict, Any


def example_transformation(
    spark: Optional[SparkSession] = None,
    env: str = "qat",
    **kwargs
) -> DataFrame:
    """
    Example transformation function template.
    
    Args:
        spark: SparkSession (optional, will use active session if None)
        env: Environment (qat/prod)
        **kwargs: Additional parameters from transformation config
    
    Returns:
        Transformed DataFrame
    
    Example TransformationRegistry entry:
    {
        "transformation_id": "example-silver_1",
        "project": "energy_efficiency",
        "layer": "Silver_1",
        "module": "energy_efficiency.transformations.silver_1.functions",
        "function": "example_transformation",
        "inputs": ["input_table"],
        "outputs": [{"table": "output_table", "mode": "overwrite"}],
        "constants": {"param1": "value1"}
    }
    """
    if spark is None:
        spark = SparkSession.getActiveSession()
    
    # Get inputs from kwargs (passed from orchestrator)
    inputs = kwargs.get('inputs', [])
    constants = kwargs.get('constants', {})
    outputs = kwargs.get('outputs', [])
    
    # Example transformation logic
    if inputs:
        input_table = inputs[0]
        df = spark.table(input_table)
        
        # Your transformation logic here
        transformed_df = df.select("*")  # Replace with actual logic
        
        # Save to output(s)
        if outputs:
            output_config = outputs[0]
            output_table = output_config.get('table')
            output_mode = output_config.get('mode', 'overwrite')
            
            transformed_df.write.mode(output_mode).saveAsTable(output_table)
        
        return transformed_df
    
    return spark.createDataFrame([], schema="dummy STRING")


# Add more transformation functions below
```

#### Step 4: Create README

**Generated README includes:**
- Project overview
- Directory structure
- Quick start commands
- Layer execution examples
- Configuration instructions
- Next steps

#### Step 5: Return Result

```python
print(result["next_steps"])
# Output:
# 1. Navigate to energy_efficiency/
# 2. Edit transformations/silver_1/functions.py
# 3. Update manifest.json with your layers
# 4. Run: run_project(project="Energy Efficiency", env="qat")
```

---

## Energy Efficiency Case Study

### Real-World Manifest Analysis

Let's analyze the actual [Energy Efficiency manifest](file:///d:/projects/Energy%20Efficiency/manifest.json):

```json
{
  "project_name": "Energy Efficiency",
  "project_type": "manufacturing",
  "description": "Energy Efficiency - Manufacturing Data Pipeline",
  "version": "1.0.0",
  "layer_order": [
    "Bronze",
    "Silver_1",
    "Silver_2",
    "Gold_1",
    "Gold_2"
  ],
  "layers": {
    "Bronze": {
      "name": "Bronze",
      "description": "Raw data ingestion layer",
      "depends_on": [],
      "cache_tables": [],
      "max_workers": null
    },
    "Silver_1": {
      "name": "Silver_1",
      "description": "Asset-level transformations",
      "depends_on": ["Bronze"],
      "cache_tables": [],
      "max_workers": null
    },
    "Silver_2": {
      "name": "Silver_2",
      "description": "Plant-level transformations",
      "depends_on": ["Silver_1"],
      "cache_tables": [],
      "max_workers": null
    },
    "Gold_1": {
      "name": "Gold_1",
      "description": "Cross-plant aggregations",
      "depends_on": ["Silver_2"],
      "cache_tables": [],
      "max_workers": null
    },
    "Gold_2": {
      "name": "Gold_2",
      "description": "Business-level KPIs",
      "depends_on": ["Gold_1"],
      "cache_tables": [],
      "max_workers": null
    }
  },
  "environments": ["qat", "prod"],
  "default_env": "qat",
  "entity_labels": {
    "entity_1": "plant",
    "entity_2": "asset"
  },
  "transformation_modules": [
    "energy_efficiency.transformations"
  ],
  "cache_plan": {
    "Gold_1": [
      "qat_energy_efficiency.combined_dryers"
    ]
  }
}
```

### Design Decisions Explained

**Why 5 Layers?**
- **Bronze**: Raw sensor data (no dependencies)
- **Silver_1**: Asset-level metrics (depends on Bronze)
  - Individual motor efficiency, dryer performance
- **Silver_2**: Plant-level aggregations (depends on Silver_1)
  - Plant-wide energy consumption, uptime
- **Gold_1**: Cross-plant comparisons (depends on Silver_2)
  - Benchmarking, regional trends
- **Gold_2**: Executive KPIs (depends on Gold_1)
  - Cost savings, carbon footprint

**Dependency Chain:**
```
Bronze → Silver_1 → Silver_2 → Gold_1 → Gold_2
```

**Why Cache `combined_dryers` in Gold_1?**
- Expensive join across all plants and assets
- Reused by Gold_2 transformations
- 5x performance improvement in production

### How It Was Created

```python
from odibi_de_v2 import initialize_project

result = initialize_project(
    project_name="Energy Efficiency",
    project_type="manufacturing"
)

# Then manually customized:
# 1. Added 5-layer hierarchy
# 2. Defined entity_labels for plant/asset
# 3. Added cache_plan for combined_dryers
# 4. Saved to /d:/projects/Energy Efficiency/manifest.json
```

---

## Customizing Manifests for Industries

### Manufacturing Template

**Use Case:** Factory IoT sensors, asset hierarchies

```python
from odibi_de_v2 import initialize_project

result = initialize_project(
    "Smart Factory",
    project_type="manufacturing"
)
```

**Generated Structure:**
- Bronze: Raw sensor streams
- Silver_1: Asset-level (motor, conveyor)
- Silver_2: Line-level (production line)
- Gold_1: Plant-level aggregations
- Gold_2: Corporate KPIs

**Entity Labels:**
```json
"entity_labels": {
  "entity_1": "plant",
  "entity_2": "production_line",
  "entity_3": "machine"
}
```

### Analytics Template

**Use Case:** Customer analytics, marketing funnels

```python
result = initialize_project(
    "Customer 360",
    project_type="analytics"
)
```

**Generated Structure:**
- Bronze: Raw events, transactions
- Silver_1: Customer profiles, cleaned data
- Gold_1: Business metrics, dashboards

**Entity Labels:**
```json
"entity_labels": {
  "entity_1": "customer",
  "entity_2": "segment",
  "entity_3": "campaign"
}
```

### ML Pipeline Template

**Use Case:** Model training, inference

```python
result = initialize_project(
    "Churn Prediction",
    project_type="ml_pipeline"
)
```

**Generated Structure:**
- Bronze: Raw data
- Features: Feature engineering
- Training: Model training datasets
- Inference: Prediction outputs

**Entity Labels:**
```json
"entity_labels": {
  "entity_1": "model_version",
  "entity_2": "feature_set",
  "entity_3": "prediction_batch"
}
```

### Custom Template

**Use Case:** Unique requirements

```python
result = initialize_project(
    "My Pipeline",
    project_type="custom"
)
```

**Minimal Structure:**
- Bronze
- Silver_1
- Gold_1

**Then Customize:**
```python
import json
from pathlib import Path

manifest_path = Path("./projects/my_pipeline/manifest.json")
manifest = json.loads(manifest_path.read_text())

# Add custom layers
manifest["layer_order"] = ["Ingestion", "Validation", "Transform", "Export"]
manifest["layers"] = {
    "Ingestion": {"name": "Ingestion", "depends_on": [], ...},
    "Validation": {"name": "Validation", "depends_on": ["Ingestion"], ...},
    "Transform": {"name": "Transform", "depends_on": ["Validation"], ...},
    "Export": {"name": "Export", "depends_on": ["Transform"], ...}
}

manifest_path.write_text(json.dumps(manifest, indent=2))
```

---

## Project Directory Structure

### Standard Structure Explained

```
project_name/
├── manifest.json              # ⚙️ Configuration manifest
├── README.md                  # 📖 Auto-generated documentation
│
├── transformations/           # 🐍 Python transformation modules
│   ├── __init__.py           # Package init
│   ├── bronze/               # Layer-specific modules
│   │   ├── __init__.py
│   │   └── functions.py      # Transformation functions
│   ├── silver_1/
│   ├── silver_2/
│   └── gold_1/
│
├── sql/                      # 📊 SQL scripts
│   ├── ddl/                  # CREATE TABLE, ALTER, etc.
│   │   ├── bronze_schema.sql
│   │   └── silver_schema.sql
│   └── queries/              # SELECT queries for analysis
│       └── quality_checks.sql
│
├── notebooks/                # 📓 Interactive notebooks
│   ├── bronze/               # Per-layer notebooks
│   ├── silver_1/
│   └── analysis.ipynb        # Ad-hoc analysis
│
├── tests/                    # 🧪 Unit and integration tests
│   ├── __init__.py
│   ├── test_transformations.py
│   └── fixtures/             # Test data
│
├── config/                   # ⚙️ Configuration files
│   ├── transformation_config_example.json
│   ├── ingestion_config.csv  # IngestionSourceConfig
│   └── secrets_config.csv    # SecretsConfig
│
├── docs/                     # 📚 Documentation
│   ├── architecture.md
│   └── runbook.md
│
└── metadata/                 # 📋 Metadata and schemas
    ├── schemas/              # JSON/Avro schemas
    └── lineage.json          # Data lineage
```

### Best Practices for Organization

#### 1. Transformation Modules

**✅ DO:**
- One file per layer: `transformations/silver_1/functions.py`
- Group related functions together
- Use descriptive function names: `calculate_asset_efficiency()`

**❌ DON'T:**
- Mix layers in one file
- Use generic names: `transform1()`, `process()`

#### 2. SQL Scripts

**✅ DO:**
- Separate DDL (schema) from DML (queries)
- Version schemas: `bronze_schema_v1.sql`
- Include comments with business logic

**❌ DON'T:**
- Mix CREATE and SELECT in same file
- Hard-code table names (use templates)

#### 3. Notebooks

**✅ DO:**
- One notebook per layer or use case
- Clear markdown explanations
- Runnable top-to-bottom

**❌ DON'T:**
- Create monolithic 1000+ cell notebooks
- Include production secrets

#### 4. Tests

**✅ DO:**
- Mirror source structure: `tests/test_silver_1.py`
- Use fixtures for sample data
- Test both happy and error paths

**❌ DON'T:**
- Skip tests for "simple" transformations
- Use production data in tests

---

## Best Practices

### 1. Manifest Version Control

**Always commit manifest.json:**
```bash
git add manifest.json
git commit -m "feat: add Gold_2 layer for executive KPIs"
```

**Track breaking changes:**
```json
{
  "version": "2.0.0",  // ← Bumped from 1.5.0
  "metadata": {
    "changelog": "Breaking: renamed entity_1 from 'plant' to 'facility'"
  }
}
```

### 2. Environment Consistency

**Use same environments across projects:**
```json
"environments": ["dev", "qat", "uat", "prod"]
```

**Benefits:**
- Predictable deployment flow
- Reusable CI/CD pipelines
- Clear promotion path

### 3. Layer Design Principles

**Keep layers focused:**
- Bronze: Ingestion only
- Silver: Business logic
- Gold: Aggregations and KPIs

**Avoid deep hierarchies:**
- ✅ 3-5 layers (Bronze → Silver_1-2 → Gold_1-2)
- ❌ 10+ layers (maintenance nightmare)

**Dependencies should form a DAG:**
```
     Bronze
    /      \
Silver_1  Silver_2
    \      /
     Gold_1
```

### 4. Cache Plan Strategy

**Cache when:**
- Table accessed 3+ times
- Expensive computation (>1 min)
- Hot path (critical queries)

**Don't cache:**
- Small tables (<10MB)
- Frequently updated data
- One-time use tables

### 5. Documentation in Manifest

**Use descriptive names:**
```json
{
  "description": "Real-time energy monitoring for 50+ manufacturing plants",
  "layers": {
    "Silver_1": {
      "description": "Asset-level efficiency: OEE, uptime, energy per unit"
    }
  }
}
```

### 6. Module Organization

**Group by domain:**
```
transformations/
├── energy/         # Energy-specific logic
├── quality/        # Quality metrics
└── maintenance/    # Predictive maintenance
```

**Update manifest:**
```json
"transformation_modules": [
  "project.transformations.energy",
  "project.transformations.quality",
  "project.transformations.maintenance"
]
```

---

## Next Steps

### Practice Exercise

**Challenge:** Create a manifest for a retail analytics project

**Requirements:**
1. Use `project_type="analytics"`
2. Add 4 layers: Bronze, Silver, Gold_Sales, Gold_Inventory
3. Define entity_labels for store/product/customer
4. Add cache plan for top-selling products table

**Solution Template:**
```python
from odibi_de_v2 import initialize_project
import json
from pathlib import Path

# 1. Initialize
result = initialize_project("Retail Analytics", "analytics")

# 2. Customize manifest
manifest_path = Path(result["manifest_path"])
manifest = json.loads(manifest_path.read_text())

# 3. Add layers
manifest["layer_order"] = ["Bronze", "Silver", "Gold_Sales", "Gold_Inventory"]
manifest["layers"] = {
    "Bronze": {"name": "Bronze", "depends_on": [], ...},
    # Add others...
}

# 4. Entity labels
manifest["entity_labels"] = {
    "entity_1": "store",
    "entity_2": "product",
    "entity_3": "customer"
}

# 5. Cache plan
manifest["cache_plan"] = {
    "Gold_Sales": ["qat_retail.top_products"]
}

# 6. Save
manifest_path.write_text(json.dumps(manifest, indent=2))
```

---

## Related Documentation

- [08 - Tutorials and Practice](file:///d:/projects/odibi_de_v2/docs/learn_odibi_de/08_Tutorials_and_Practice.md)
- [09 - Advanced Concepts](file:///d:/projects/odibi_de_v2/docs/learn_odibi_de/09_Advanced_Concepts.md)
- [System Overview](file:///d:/projects/odibi_de_v2/docs/00-SYSTEM_OVERVIEW.md)
- [Quick Reference](file:///d:/projects/odibi_de_v2/docs/QUICK_REFERENCE.md)

---

**📁 Source Code:**
- [scaffolding.py](file:///d:/projects/odibi_de_v2/odibi_de_v2/project/scaffolding.py) - ProjectScaffolder implementation
- [manifest.py](file:///d:/projects/odibi_de_v2/odibi_de_v2/project/manifest.py) - ProjectManifest class
- [Energy Efficiency manifest](file:///d:/projects/Energy%20Efficiency/manifest.json) - Real example
