# odibi_de_v2 v2.0 - Quick Reference

## 🚀 The Two Commands You Need

### Create a New Project
```python
from odibi_de_v2 import initialize_project

initialize_project("Customer Churn", project_type="analytics")
# Creates complete project structure in seconds
```

### Run Any Project
```python
from odibi_de_v2 import run_project

run_project(project="Energy Efficiency", env="qat")
# Runs full pipeline with one command
```

---

## 📋 Common Tasks

### 1. Initialize Projects

```python
from odibi_de_v2 import initialize_project

# Manufacturing project
initialize_project("Reliability Analytics", "manufacturing")

# Analytics project
initialize_project("Sales Forecasting", "analytics")

# ML pipeline
initialize_project("Churn Prediction", "ml_pipeline")

# Custom project
initialize_project("My Custom Project", "custom")
```

### 2. Run Pipelines

```python
from odibi_de_v2 import run_project

# Full pipeline
run_project("Energy Efficiency", "qat")

# Specific layers
run_project("Energy Efficiency", "qat", target_layers=["Silver_1", "Gold_1"])

# With caching
run_project(
    "Energy Efficiency",
    "qat",
    cache_plan={"Gold_1": ["combined_dryers"]}
)

# Bronze only
run_project("Energy Efficiency", "qat", target_layers=["Bronze"])
```

### 3. Configure Transformations

#### Option A: SQL Insert
```sql
INSERT INTO TransformationRegistry (
    transformation_id, project, environment, layer,
    entity_1, entity_2, module, function,
    inputs, outputs
) VALUES (
    'churn-features',
    'customer churn',
    'qat',
    'Silver',
    'North America',
    'Retail',
    'customer_churn.transformations.silver.functions',
    'calculate_features',
    '["transactions_bronze", "profiles_bronze"]',
    '[{"table": "features_silver", "mode": "overwrite"}]'
);
```

#### Option B: Interactive UI
```python
from odibi_de_v2.config import TransformationRegistryUI

ui = TransformationRegistryUI(project="Customer Churn", env="qat")
ui.render()
# Fill out form, click "Generate SQL", copy and execute
```

### 4. Write Transformation Functions

```python
# In your_project/transformations/silver/functions.py

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def calculate_features(spark=None, env="qat", **kwargs):
    """
    Your transformation logic.
    
    kwargs automatically contains:
    - inputs: List of input table names
    - constants: Dict of parameters
    - outputs: List of output configurations
    """
    if spark is None:
        spark = SparkSession.getActiveSession()
    
    # Get config
    inputs = kwargs.get('inputs', [])
    constants = kwargs.get('constants', {})
    outputs = kwargs.get('outputs', [])
    
    # Read inputs
    df1 = spark.table(inputs[0])
    df2 = spark.table(inputs[1]) if len(inputs) > 1 else None
    
    # Transform
    result = df1.filter(F.col("value") > constants.get('threshold', 0))
    
    # Write outputs
    if outputs:
        result.write.mode(outputs[0]['mode']).saveAsTable(outputs[0]['table'])
    
    return result
```

---

## 📁 Project Structure

After `initialize_project("My Project")`:

```
my_project/
├── manifest.json              # Project config
├── transformations/           # Your Python code
│   ├── bronze/
│   ├── silver/
│   │   └── functions.py      # Your transformations
│   └── gold/
├── sql/                       # SQL scripts
│   ├── ddl/
│   └── queries/
├── notebooks/                 # Databricks notebooks
├── tests/                     # Unit tests
├── config/                    # Config examples
└── README.md                 # Auto-generated docs
```

---

## 🎨 Entity Mapping Examples

Configure in `manifest.json`:

### Manufacturing
```json
{
  "entity_labels": {
    "entity_1": "plant",
    "entity_2": "asset",
    "entity_3": "equipment"
  }
}
```

### Retail
```json
{
  "entity_labels": {
    "entity_1": "region",
    "entity_2": "store",
    "entity_3": "department"
  }
}
```

### Finance
```json
{
  "entity_labels": {
    "entity_1": "business_unit",
    "entity_2": "product",
    "entity_3": "account_type"
  }
}
```

---

## 🔧 Advanced Options

### Custom Authentication
```python
def my_auth(env, repo_path, logger_metadata):
    # Your auth logic
    return {"spark": spark, "sql_provider": sql_provider}

run_project(
    "My Project",
    "qat",
    auth_provider=my_auth
)
```

### Multiple Inputs
```python
# In TransformationRegistry
inputs: '["table1", "table2", "table3"]'

# In your function
def transform(inputs, **kwargs):
    df1 = spark.table(inputs[0])
    df2 = spark.table(inputs[1])
    df3 = spark.table(inputs[2])
```

### Constants
```python
# In TransformationRegistry
constants: '{"threshold": 100, "window_days": 30}'

# In your function
def transform(constants, **kwargs):
    threshold = constants.get('threshold', 0)
    window = constants.get('window_days', 7)
```

### Multiple Outputs
```python
# In TransformationRegistry
outputs: '[
    {"table": "output1", "mode": "overwrite"},
    {"table": "output2", "mode": "append", "partitionBy": ["date"]}
]'

# In your function
def transform(outputs, **kwargs):
    for output_config in outputs:
        df.write.mode(output_config['mode']).saveAsTable(output_config['table'])
```

---

## 📊 SQL DDL Reference

### TransformationRegistry Table
```sql
CREATE TABLE TransformationRegistry (
    -- Core identification
    transformation_id VARCHAR(100) PRIMARY KEY,
    transformation_group_id VARCHAR(100),
    
    -- Scoping
    project VARCHAR(100) NOT NULL,
    environment VARCHAR(20) NOT NULL DEFAULT 'qat',
    layer VARCHAR(50) NOT NULL,
    step INT NOT NULL DEFAULT 1,
    enabled BIT NOT NULL DEFAULT 1,
    
    -- Generic entities (domain-agnostic)
    entity_1 VARCHAR(100),  -- e.g., plant, region, domain
    entity_2 VARCHAR(100),  -- e.g., asset, store, subdomain
    entity_3 VARCHAR(100),  -- e.g., equipment, dept, metric
    
    -- Transformation logic
    module VARCHAR(255) NOT NULL,
    function VARCHAR(255) NOT NULL,
    
    -- Data configuration (JSON)
    inputs NVARCHAR(MAX),      -- ["table1", "table2"]
    constants NVARCHAR(MAX),   -- {"param": "value"}
    outputs NVARCHAR(MAX),     -- [{"table": "...", "mode": "..."}]
    
    -- Metadata
    description NVARCHAR(500),
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE()
);
```

---

## 🧪 Testing

### Unit Test
```python
import pytest

def test_transformation(spark_session):
    from my_project.transformations.silver.functions import calculate_features
    
    result = calculate_features(
        spark=spark_session,
        inputs=["test_input"],
        constants={"threshold": 100},
        outputs=[{"table": "test_output", "mode": "overwrite"}]
    )
    
    assert result.count() > 0
```

### Integration Test
```python
from odibi_de_v2 import run_project

def test_pipeline():
    result = run_project("My Project", "test", target_layers=["Silver"])
    assert result['status'] == 'SUCCESS'
```

---

## 🔄 Migration Checklist

Migrating from v1.x to v2.0:

- [ ] Create TransformationRegistry table (run DDL)
- [ ] Migrate data from TransformationConfig
- [ ] Create manifest.json for your project
- [ ] Test with `run_project()`
- [ ] Update documentation
- [ ] Train team on new API

See [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) for details.

---

## 📚 Documentation

- **README_V2.md** - Complete framework documentation
- **MIGRATION_GUIDE.md** - Detailed migration instructions
- **REFACTORING_SUMMARY.md** - High-level overview
- **QUICK_REFERENCE.md** - This file

---

## 💡 Tips & Tricks

### Tip 1: Browse Existing Configs
```python
from odibi_de_v2.config import TransformationRegistryBrowser

browser = TransformationRegistryBrowser(
    sql_provider=your_provider,
    project="Energy Efficiency"
)
browser.render()
```

### Tip 2: Use Templates
When using the UI, generated SQL includes full INSERT statements you can copy/paste.

### Tip 3: Validate Manifests
```python
from odibi_de_v2.project import validate_manifest, ProjectManifest

manifest = ProjectManifest.from_json("my_project/manifest.json")
errors = validate_manifest(manifest)

if errors:
    print("Validation errors:", errors)
```

### Tip 4: Layer Targeting
Run specific layers for faster iteration:
```python
# Test just Silver
run_project("My Project", "qat", target_layers=["Silver_1"])

# Test Gold without re-running Bronze
run_project("My Project", "qat", target_layers=["Gold_1"])
```

---

## 🎯 Cheat Sheet

| Task | Command |
|------|---------|
| Create project | `initialize_project("Name", "type")` |
| Run full pipeline | `run_project("Project", "env")` |
| Run specific layers | `run_project(..., target_layers=["Silver"])` |
| Configure UI | `TransformationRegistryUI().render()` |
| Browse configs | `TransformationRegistryBrowser(...).render()` |
| Validate manifest | `validate_manifest(manifest)` |
| Load manifest | `ProjectManifest.from_json("path")` |

---

**Need more help?**
- See [README_V2.md](README_V2.md) for comprehensive docs
- See [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) for migration steps
- See [REFACTORING_SUMMARY.md](REFACTORING_SUMMARY.md) for architecture details

---

*odibi_de_v2 v2.0 - Freedom. Simplicity. Power.* 🚀
