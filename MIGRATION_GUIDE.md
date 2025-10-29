# Migration Guide: odibi_de_v2 v1.x → v2.0

## 🎯 Overview

This guide walks you through migrating from the old Ingredion-specific framework to the new project-agnostic odibi_de_v2 v2.0.

**What's changing:**
- `TransformationConfig` → `TransformationRegistry` (more flexible schema)
- Industry-specific columns (plant, asset) → Generic entities (entity_1, entity_2, entity_3)
- Manual orchestration → Unified `run_project()` API
- Project-specific code → Manifest-driven configuration

**What's staying the same:**
- `IngestionSourceConfig` (unchanged)
- `IngestionTargetConfig` (unchanged)
- `SecretsConfig` (unchanged)
- Bronze ingestion logic (unchanged)
- Core transformation patterns (unchanged)

---

## 📋 Migration Checklist

- [ ] **Step 1:** Create SQL tables (TransformationRegistry)
- [ ] **Step 2:** Migrate existing TransformationConfig data
- [ ] **Step 3:** Create project manifest
- [ ] **Step 4:** Update transformation imports
- [ ] **Step 5:** Test with new run_project() API
- [ ] **Step 6:** (Optional) Retire legacy orchestrators

---

## 🗄️ Step 1: Create New SQL Tables

### 1.1 Create TransformationRegistry Table

Run the DDL script:

```sql
-- See: odibi_de_v2/sql/ddl/01_transformation_registry.sql
```

Or execute directly:

```sql
CREATE TABLE IF NOT EXISTS TransformationRegistry (
    transformation_id VARCHAR(100) PRIMARY KEY,
    transformation_group_id VARCHAR(100),
    project VARCHAR(100) NOT NULL,
    environment VARCHAR(20) NOT NULL DEFAULT 'qat',
    layer VARCHAR(50) NOT NULL,
    step INT NOT NULL DEFAULT 1,
    enabled BIT NOT NULL DEFAULT 1,
    entity_1 VARCHAR(100),
    entity_2 VARCHAR(100),
    entity_3 VARCHAR(100),
    module VARCHAR(255) NOT NULL,
    function VARCHAR(255) NOT NULL,
    inputs NVARCHAR(MAX),
    constants NVARCHAR(MAX),
    outputs NVARCHAR(MAX),
    description NVARCHAR(500),
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE(),
    created_by VARCHAR(100),
    updated_by VARCHAR(100),
    INDEX idx_project_env_layer (project, environment, layer),
    INDEX idx_enabled (enabled),
    INDEX idx_transformation_group (transformation_group_id)
);
```

---

## 🔄 Step 2: Migrate Existing Data

### 2.1 Automated Migration Script

Use the provided migration script:

```python
from odibi_de_v2.migration import migrate_transformation_config

# Migrate all records
migrate_transformation_config(
    source_table="TransformationConfig",
    target_table="TransformationRegistry",
    sql_provider=your_sql_provider,
    dry_run=True  # Set to False to execute
)
```

### 2.2 Manual Migration (SQL)

For Energy Efficiency project:

```sql
INSERT INTO TransformationRegistry (
    transformation_id,
    transformation_group_id,
    project,
    environment,
    layer,
    step,
    enabled,
    entity_1,
    entity_2,
    entity_3,
    module,
    function,
    inputs,
    outputs,
    description,
    created_at,
    updated_at
)
SELECT
    'energy-' + LOWER(REPLACE(plant + '-' + asset, ' ', '-')) + '-' + layer AS transformation_id,
    'energy-' + layer AS transformation_group_id,
    project,
    env AS environment,
    layer,
    ROW_NUMBER() OVER (PARTITION BY project, env, layer ORDER BY id) AS step,
    enabled,
    plant AS entity_1,
    asset AS entity_2,
    NULL AS entity_3,
    module,
    function,
    JSON_QUERY('[' + QUOTENAME(input_table, '"') + ']') AS inputs,
    JSON_QUERY('[{"table":"' + COALESCE(target_table, '') + '","mode":"overwrite"}]') AS outputs,
    'Migrated from TransformationConfig' AS description,
    created_at,
    updated_at
FROM TransformationConfig
WHERE project = 'energy efficiency';
```

### 2.3 Validation

Verify migration:

```sql
-- Check record counts
SELECT COUNT(*) FROM TransformationConfig WHERE project = 'energy efficiency';
SELECT COUNT(*) FROM TransformationRegistry WHERE project = 'energy efficiency';

-- Spot check
SELECT TOP 5
    transformation_id,
    entity_1,
    entity_2,
    module,
    function,
    inputs,
    outputs
FROM TransformationRegistry
WHERE project = 'energy efficiency'
ORDER BY layer, step;
```

---

## 📄 Step 3: Create Project Manifest

### 3.1 Generate Manifest

```python
from odibi_de_v2.project import ProjectManifest, ProjectType

# Create manifest for Energy Efficiency
manifest = ProjectManifest.create_template(
    project_name="Energy Efficiency",
    project_type=ProjectType.MANUFACTURING
)

# Customize
manifest.layer_order = ["Bronze", "Silver_1", "Silver_2", "Gold_1", "Gold_2"]
manifest.entity_labels = {
    "entity_1": "plant",
    "entity_2": "asset",
    "entity_3": "equipment"
}
manifest.cache_plan = {
    "Gold_1": ["qat_energy_efficiency.combined_dryers"]
}

# Save
manifest.to_json("Energy Efficiency/manifest.json")
```

### 3.2 Manual Manifest Creation

Create `Energy Efficiency/manifest.json`:

```json
{
  "project_name": "Energy Efficiency",
  "project_type": "manufacturing",
  "description": "Energy Efficiency - Manufacturing Data Pipeline",
  "version": "1.0.0",
  "layer_order": ["Bronze", "Silver_1", "Silver_2", "Gold_1", "Gold_2"],
  "layers": {
    "Bronze": {
      "name": "Bronze",
      "description": "Raw data ingestion layer",
      "depends_on": [],
      "cache_tables": []
    },
    "Silver_1": {
      "name": "Silver_1",
      "description": "Asset-level transformations",
      "depends_on": ["Bronze"],
      "cache_tables": []
    },
    "Gold_1": {
      "name": "Gold_1",
      "description": "Cross-plant aggregations",
      "depends_on": ["Silver_1"],
      "cache_tables": ["qat_energy_efficiency.combined_dryers"]
    }
  },
  "environments": ["qat", "prod"],
  "default_env": "qat",
  "entity_labels": {
    "entity_1": "plant",
    "entity_2": "asset",
    "entity_3": "equipment"
  },
  "cache_plan": {
    "Gold_1": ["qat_energy_efficiency.combined_dryers"]
  }
}
```

---

## 🔌 Step 4: Update Transformation Code

### 4.1 No Code Changes Required!

Your existing transformation functions remain unchanged:

```python
# silver/functions.py - NO CHANGES NEEDED
def process_argo_boilers(**kwargs):
    # Your existing logic
    pass
```

### 4.2 Update Module Imports (Optional)

If you want to organize better:

**Before:**
```
global-utils/
└── databricks_utils/
    └── silver/
        └── functions.py
```

**After:**
```
Energy Efficiency/
└── transformations/
    └── silver_1/
        └── functions.py
```

Update TransformationRegistry `module` field accordingly.

---

## 🚀 Step 5: Use New run_project() API

### 5.1 Replace Old Orchestrator

**Before (v1.x):**
```python
from global_utils.orchestrators import IngredionProjectOrchestrator

orchestrator = IngredionProjectOrchestrator(
    project="Energy Efficiency",
    env="qat",
    log_level="INFO",
    save_logs=True
)

result = orchestrator.run(
    repo_path="/Workspace/Repos/user/energy_efficiency",
    layer_order=["Silver_1", "Silver_2", "Gold_1"],
    cache_plan={"Gold_1": ["qat_energy_efficiency.combined_dryers"]},
    max_workers=os.cpu_count()
)
```

**After (v2.0):**
```python
from odibi_de_v2 import run_project

# Simplest form
result = run_project(project="Energy Efficiency", env="qat")

# With options
result = run_project(
    project="Energy Efficiency",
    env="qat",
    target_layers=["Silver_1", "Gold_1"],
    log_level="INFO",
    save_logs=True
)
```

### 5.2 Authentication Integration

If using custom authentication (like Ingredion's):

```python
from databricks_utils import bootstrap_environment

def custom_auth(env, repo_path, logger_metadata):
    env_ctx = bootstrap_environment(env=env, repo_path=repo_path)
    authenticate = env_ctx["authenticate"]
    spark, _, sql_provider = authenticate(logger_metadata=logger_metadata)
    return {"spark": spark, "sql_provider": sql_provider}

# Use with run_project
result = run_project(
    project="Energy Efficiency",
    env="qat",
    auth_provider=custom_auth
)
```

---

## 🧪 Step 6: Testing

### 6.1 Test Bronze Layer

```python
from odibi_de_v2 import run_project

result = run_project(
    project="Energy Efficiency",
    env="qat",
    target_layers=["Bronze"]
)

assert result['status'] == 'SUCCESS'
```

### 6.2 Test Single Silver Layer

```python
result = run_project(
    project="Energy Efficiency",
    env="qat",
    target_layers=["Silver_1"]
)
```

### 6.3 Test Full Pipeline

```python
result = run_project(project="Energy Efficiency", env="qat")
print(result)
# {
#     'status': 'SUCCESS',
#     'project': 'Energy Efficiency',
#     'env': 'qat',
#     'layers_run': ['Bronze', 'Silver_1', 'Silver_2', 'Gold_1', 'Gold_2'],
#     'duration_seconds': 142.85
# }
```

---

## 🆕 Step 7: Create New Projects

### 7.1 Initialize New Project

```python
from odibi_de_v2 import initialize_project

result = initialize_project(
    project_name="Customer Churn",
    project_type="analytics"
)

# Created:
# - projects/customer_churn/
# - projects/customer_churn/manifest.json
# - projects/customer_churn/transformations/
# - projects/customer_churn/README.md
# - ... (full scaffolding)
```

### 7.2 Configure and Run

```python
from odibi_de_v2 import run_project

# Run immediately
run_project(project="Customer Churn", env="qat")
```

---

## 🔄 Backward Compatibility

### Legacy Support

A compatibility view is provided:

```sql
-- Automatically created by migration
CREATE VIEW TransformationConfig_Legacy AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY transformation_id) AS id,
    project,
    entity_1 AS plant,
    entity_2 AS asset,
    module,
    function,
    NULL AS target_table,
    JSON_VALUE(inputs, '$[0]') AS input_table,
    enabled,
    environment AS env,
    created_at,
    updated_at,
    layer
FROM TransformationRegistry;
```

Old code can continue using the view temporarily.

---

## 📊 Feature Comparison

| Feature | v1.x (Old) | v2.0 (New) |
|---------|------------|------------|
| **Project Support** | Ingredion-specific | Any industry/domain |
| **Entity Columns** | plant, asset (hardcoded) | entity_1, entity_2, entity_3 (generic) |
| **Input Tables** | Single `input_table` | Multiple inputs (JSON array) |
| **Constants** | None | JSON constants object |
| **Outputs** | Single `target_table` | Multiple outputs (JSON array) |
| **Config Storage** | SQL only | SQL + JSON manifests |
| **Orchestration** | Manual instantiation | `run_project()` function |
| **Project Setup** | Manual | `initialize_project()` |
| **UI** | TransformationConfigUI | TransformationRegistryUI |

---

## 🛠️ Troubleshooting

### Issue: "No manifest found"

**Solution:** Create a manifest or specify path:

```python
run_project(
    project="Energy Efficiency",
    manifest_path="Energy Efficiency/manifest.json"
)
```

### Issue: "TransformationRegistry table not found"

**Solution:** Run the DDL script first (Step 1).

### Issue: JSON parsing error in inputs/outputs

**Solution:** Ensure valid JSON:

```json
# ❌ Wrong
inputs: "table1"

# ✅ Correct
inputs: ["table1"]

# ✅ Also correct
inputs: ["table1", "table2"]
```

### Issue: Module not found

**Solution:** Verify `module` path in TransformationRegistry matches your Python import path.

---

## 📞 Support

If you encounter issues:

1. Check the [troubleshooting section](#troubleshooting)
2. Review the [examples directory](examples/)
3. Consult the [full documentation](docs/)
4. Reach out to the data engineering team

---

## ✅ Migration Complete!

Once you've completed all steps:

- ✅ TransformationRegistry table created
- ✅ Data migrated from TransformationConfig
- ✅ Manifest file created
- ✅ Tested with `run_project()`
- ✅ Team trained on new API

You're ready to use odibi_de_v2 v2.0! 🎉

---

*Last Updated: 2025-10-29*
