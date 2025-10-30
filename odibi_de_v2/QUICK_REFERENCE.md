# Quick Reference - Quality-of-Life Utilities

One-page reference for common data engineering tasks.

## 🏥 Health & Status

```python
from odibi_de_v2.utils import quick_health_check, print_project_summary

# Quick health check
quick_health_check(sql_provider, spark, "MyProject", "qat")

# Project summary
print_project_summary(sql_provider, spark, "MyProject", "qat")
```

## ✅ Validation

```python
from odibi_de_v2.config import validate_transformation_registry, ConfigValidator

# Quick validation
validate_transformation_registry(configs)

# Detailed validation
validator = ConfigValidator()
result = validator.validate_config(config)
```

## 📊 Project Information

```python
from odibi_de_v2.utils import list_projects, get_layer_summary, get_project_status

# List all projects
projects = list_projects(sql_provider, spark, "qat")

# Layer summary
layers = get_layer_summary(sql_provider, spark, "MyProject", "qat")

# Project status
status = get_project_status(sql_provider, spark, "MyProject", "qat", hours=24)
```

## 🔍 Troubleshooting

```python
from odibi_de_v2.utils import get_failed_transformations, get_transformation_config

# Get failures
failed = get_failed_transformations(sql_provider, spark, "MyProject", "qat")

# Get specific config
config = get_transformation_config(sql_provider, spark, "transform-id", "qat")
```

## 💾 Export & Backup

```python
from odibi_de_v2.utils import export_project_config

# Export to file
export_project_config(sql_provider, spark, "MyProject", "qat", "backup.json")
```

## 🖥️ CLI Commands

```bash
# Health check
python -m odibi_de_v2.cli.project_cli health --project "MyProject" --env qat

# Summary
python -m odibi_de_v2.cli.project_cli summary --project "MyProject"

# Validate
python -m odibi_de_v2.cli.project_cli validate --project "MyProject"

# List projects
python -m odibi_de_v2.cli.project_cli list --env qat

# Show failures
python -m odibi_de_v2.cli.project_cli failed --project "MyProject"

# Export config
python -m odibi_de_v2.cli.project_cli export --project "MyProject" --output config.json

# Generate template
python -m odibi_de_v2.cli.project_cli template --transformation-id "new-id"
```

## 🎯 Common Workflows

### Daily Check
```python
quick_health_check(sql_provider, spark, "MyProject", "qat")
print_project_summary(sql_provider, spark, "MyProject", "qat")
```

### Pre-Deployment
```python
validate_transformation_registry(configs)
hc = HealthCheck(sql_provider, spark, "MyProject", "qat")
report = hc.run_full_check()
```

### Debug Failures
```python
failed = get_failed_transformations(sql_provider, spark, "MyProject", "qat")
for f in failed:
    config = get_transformation_config(sql_provider, spark, f['transformation_id'], "qat")
    print(f"{f['transformation_id']}: {config['module']}.{config['function']}")
```

## 📦 Imports Cheat Sheet

```python
# Health & Validation
from odibi_de_v2.utils import HealthCheck, quick_health_check
from odibi_de_v2.config import ConfigValidator, validate_transformation_registry

# Helper Functions
from odibi_de_v2.utils import (
    list_projects,
    get_project_status,
    get_layer_summary,
    get_failed_transformations,
    retry_failed_transformations,
    get_transformation_config,
    print_project_summary,
    export_project_config
)

# CLI
from odibi_de_v2.cli import ProjectCLI
```
