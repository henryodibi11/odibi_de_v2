# Enhanced Docstrings - Quick Reference Guide

## How to Access Enhanced Documentation

### 1. Interactive Python Help
```python
# In Python/IPython/Jupyter
from odibi_de_v2 import run_project, initialize_project
from odibi_de_v2.config import TransformationRegistryUI
from odibi_de_v2.project import ProjectManifest

# View docstrings
help(run_project)
help(initialize_project)
help(TransformationRegistryUI)
help(ProjectManifest.create_template)
```

### 2. Databricks Notebook
```python
# Use ? or ?? for docstrings
from odibi_de_v2 import run_project

run_project?   # View docstring
run_project??  # View docstring + source code
```

### 3. IDE Tooltips
- **VS Code**: Hover over function names
- **PyCharm**: Ctrl+Q (Windows/Linux) or F1 (Mac)
- **Databricks**: Hover over function in notebook cell

---

## Top 10 Copy-Paste Examples

### 1. Initialize New Project
```python
from odibi_de_v2 import initialize_project

result = initialize_project(
    project_name="Energy Efficiency",
    project_type="manufacturing"
)
print(f"Created: {result['project_path']}")
```

### 2. Run Complete Pipeline
```python
from odibi_de_v2 import run_project

result = run_project(
    project="Energy Efficiency",
    env="qat",
    log_level="INFO"
)
```

### 3. Run Specific Layers
```python
from odibi_de_v2 import run_project

result = run_project(
    project="Customer Churn",
    env="prod",
    target_layers=["Silver_1", "Gold_1"]
)
```

### 4. Run with Caching
```python
from odibi_de_v2 import run_project

result = run_project(
    project="Energy Efficiency",
    env="qat",
    cache_plan={
        "Gold_1": [
            "qat_energy.combined_dryers",
            "qat_energy.aggregated_metrics"
        ]
    }
)
```

### 5. Create Manufacturing Manifest
```python
from odibi_de_v2.project import ProjectManifest, ProjectType

manifest = ProjectManifest.create_template(
    project_name="Plant Efficiency",
    project_type=ProjectType.MANUFACTURING
)

# Customize
manifest.owner = "data-team@company.com"
manifest.tags = ["manufacturing", "kpi"]

# Save
manifest.to_json("projects/plant_efficiency/manifest.json")
```

### 6. Create Analytics Manifest
```python
from odibi_de_v2.project import ProjectManifest, ProjectType

manifest = ProjectManifest.create_template(
    project_name="Sales Dashboard",
    project_type=ProjectType.ANALYTICS
)

print(manifest.layer_order)
# ['Bronze', 'Silver', 'Gold']
```

### 7. Interactive Config UI (Databricks)
```python
from odibi_de_v2.config import TransformationRegistryUI

ui = TransformationRegistryUI(
    project="Energy Efficiency",
    env="qat"
)
ui.render()
```

### 8. Programmatic Config Generation
```python
from odibi_de_v2.config import TransformationRegistryUI

ui = TransformationRegistryUI(project="Sales", env="prod")
ui.transformation_id.value = "aggregate-daily-sales"
ui.layer.value = "Gold_1"
ui.module.value = "sales.transformations.gold.aggregations"
ui.function.value = "aggregate_daily_sales"
ui.inputs.value = '["prod_sales.cleaned_transactions"]'
ui.outputs.value = '[{"table": "prod_sales.daily_summary", "mode": "overwrite"}]'

sql = ui.get_sql_insert()
print(sql)
```

### 9. Batch Project Creation
```python
from odibi_de_v2 import initialize_project

projects = [
    ("Sales Analytics", "analytics"),
    ("Inventory Management", "manufacturing"),
    ("Customer Segmentation", "ml_pipeline")
]

for name, ptype in projects:
    result = initialize_project(
        project_name=name,
        project_type=ptype,
        base_path="./workspace"
    )
    print(f"✅ Created: {name}")
```

### 10. Production Run with Full Logging
```python
from odibi_de_v2 import run_project

result = run_project(
    project="Financial Reporting",
    env="prod",
    log_level="INFO",
    save_logs=True,
    cache_plan={
        "Silver_2": ["prod_finance.validated_transactions"],
        "Gold_1": ["prod_finance.monthly_summary"]
    }
)

if result['status'] == 'success':
    print(f"✅ Pipeline succeeded")
    print(f"Layers: {', '.join(result['layers_executed'])}")
else:
    print(f"❌ Pipeline failed: {result['errors']}")
```

---

## Enhanced Functions/Classes Reference

| Module | Function/Class | Key Use Case |
|--------|----------------|--------------|
| `odibi_de_v2` | `run_project()` | Execute complete or partial pipelines |
| `odibi_de_v2` | `initialize_project()` | Create new project scaffolding |
| `odibi_de_v2.project` | `ProjectManifest.create_template()` | Generate manifest for project type |
| `odibi_de_v2.project` | `ProjectManifest.to_json()` | Save manifest to file |
| `odibi_de_v2.config` | `TransformationRegistryUI` | Interactive config builder |

---

## Common Patterns

### Pattern 1: Create → Configure → Run
```python
from odibi_de_v2 import initialize_project, run_project
from odibi_de_v2.project import ProjectManifest
from pathlib import Path

# Step 1: Initialize
result = initialize_project("My Project", "analytics")

# Step 2: Configure
manifest = ProjectManifest.from_json(Path(result['manifest_path']))
manifest.owner = "my-team@company.com"
manifest.to_json(Path(result['manifest_path']))

# Step 3: Run (after adding transformations to registry)
run_project(project="My Project", env="qat")
```

### Pattern 2: Iterative Layer Development
```python
from odibi_de_v2 import run_project

# Develop and test Bronze
run_project("MyProject", env="qat", target_layers=["Bronze"])

# Then develop Silver
run_project("MyProject", env="qat", target_layers=["Silver_1"])

# Finally Gold
run_project("MyProject", env="qat", target_layers=["Gold_1"])

# Run full pipeline
run_project("MyProject", env="qat")
```

### Pattern 3: Environment Promotion
```python
from odibi_de_v2 import run_project

# Test in QAT
result_qat = run_project("MyProject", env="qat", log_level="INFO")

if result_qat['status'] == 'success':
    # Promote to PROD
    result_prod = run_project(
        "MyProject",
        env="prod",
        log_level="WARNING",
        save_logs=True
    )
```

---

## Docstring Example Count by File

- **`__init__.py`**: 7 examples (framework overview)
- **`run_project()`**: 7 examples (execution patterns)
- **`create_template()`**: 5 examples (manifest creation)
- **`to_json()`**: 3 examples (serialization)
- **`initialize_project()`**: 7 examples (scaffolding)
- **`TransformationRegistryUI`**: 6 examples (UI usage)

**Total: 35 working examples** across all enhanced docstrings

---

## Tips for Using Enhanced Documentation

1. **Start with Examples**: Scroll to the Examples section first
2. **Copy-Paste and Modify**: All examples are ready to run with minor adjustments
3. **Check Parameters**: Review Args section for all available options
4. **Error Handling**: Look for "Raises" section to understand error cases
5. **Related Functions**: Check "See Also" for related functionality
6. **Notes Section**: Contains important usage tips and best practices

---

*Last Updated: 2025-10-29*  
*odibi_de_v2 v2.0.0*
