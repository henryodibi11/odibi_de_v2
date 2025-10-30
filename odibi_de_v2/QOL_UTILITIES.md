# Quality-of-Life Utilities for Data Engineers

This document describes the quality-of-life utilities added to odibi_de_v2 to minimize daily work and streamline common tasks.

## 📋 Table of Contents

1. [CLI Helper](#cli-helper)
2. [Health Check Utility](#health-check-utility)
3. [Configuration Validator](#configuration-validator)
4. [Convenience Functions](#convenience-functions)
5. [Quick Start Examples](#quick-start-examples)

---

## 🔧 CLI Helper

**Location:** `odibi_de_v2/cli/project_cli.py`

Command-line interface for quick project management tasks without writing code.

### Available Commands

```bash
# List all projects
python -m odibi_de_v2.cli.project_cli list --env qat

# Check project health
python -m odibi_de_v2.cli.project_cli health --project "MyProject" --env qat

# Show project summary
python -m odibi_de_v2.cli.project_cli summary --project "MyProject" --env qat

# Validate configurations
python -m odibi_de_v2.cli.project_cli validate --project "MyProject" --env qat

# Generate config template
python -m odibi_de_v2.cli.project_cli template --transformation-id "new-transform"

# Export project config to JSON
python -m odibi_de_v2.cli.project_cli export --project "MyProject" --output config.json

# Show recent failures
python -m odibi_de_v2.cli.project_cli failed --project "MyProject" --hours 24
```

### Programmatic Usage

```python
from odibi_de_v2.cli import ProjectCLI

cli = ProjectCLI(sql_provider, spark)
cli.list_projects("qat")
cli.check_health("MyProject", "qat")
cli.show_summary("MyProject", "qat")
```

---

## 🏥 Health Check Utility

**Location:** `odibi_de_v2/utils/health_check.py`

Comprehensive diagnostics for TransformationRegistry and project configurations.

### Features

- Verifies TransformationRegistry data integrity
- Checks for valid JSON in inputs, constants, outputs
- Validates module/function availability
- Detects duplicate IDs
- Checks layer consistency
- Identifies common misconfigurations

### Usage

```python
from odibi_de_v2.utils import HealthCheck, quick_health_check

# Quick health check with report
quick_health_check(sql_provider, spark, "MyProject", "qat")

# Detailed health check
hc = HealthCheck(sql_provider, spark, "MyProject", "qat")
report = hc.run_full_check()
hc.print_report(report)

# Check specific layer
result = hc.check_layer("Silver_1")
if not result['healthy']:
    print(f"Issues: {result['issues']}")

# Check if all functions exist
missing = hc.check_all_functions_exist()
for item in missing:
    print(f"Missing: {item['module']}.{item['function']}")
```

### Health Check Report

```
🏥 ODIBI_DE_V2 HEALTH CHECK REPORT
============================================================
Project: Energy Efficiency
Environment: qat
Overall Health: HEALTHY

✅ Data Quality: OK
✅ JSON Validity: OK
✅ All Functions Found
✅ Layer Consistency: OK
✅ No Duplicate IDs
============================================================
```

---

## ✅ Configuration Validator

**Location:** `odibi_de_v2/config/validator.py`

Validates TransformationRegistry entries and project configurations.

### Features

- Validates JSON fields (inputs, constants, outputs)
- Checks required fields
- Verifies module paths exist
- Validates function names
- Checks input/output table references
- Generates validation reports

### Usage

```python
from odibi_de_v2.config import ConfigValidator, validate_transformation_registry

# Validate single config
validator = ConfigValidator()
result = validator.validate_config(config)

if not result.is_valid:
    print(f"Errors: {result.errors}")
    print(f"Warnings: {result.warnings}")

# Validate multiple configs
results = validator.validate_configs(all_configs)

# Generate comprehensive report
report = validator.generate_validation_report(all_configs)
print(f"Pass rate: {report['pass_rate']}")

# Quick validation with console output
validate_transformation_registry(all_configs)
```

### Validation Report

```
📋 TRANSFORMATION REGISTRY VALIDATION REPORT
============================================================
Total configurations: 45
✅ Valid: 42
❌ Invalid: 3
Pass rate: 93.3%

Issues found in 3 configuration(s):

🔧 transform-001
  Errors:
    ❌ outputs: Invalid JSON - Expecting ',' delimiter
  Warnings:
    ⚠️  No inputs defined
============================================================
```

---

## 🛠️ Convenience Functions

**Location:** `odibi_de_v2/utils/helpers.py`

Helper functions for common data engineering tasks.

### Available Functions

#### `list_projects()`

List all configured projects.

```python
from odibi_de_v2.utils import list_projects

projects = list_projects(sql_provider, spark, "qat")
for project in projects:
    print(f"- {project}")
```

#### `get_project_status()`

Get recent execution status.

```python
from odibi_de_v2.utils import get_project_status

status = get_project_status(sql_provider, spark, "MyProject", "qat", hours=24)
print(f"Success rate: {status['success_rate']}")
print(f"Failed: {status['failed_count']}")
print(f"Avg duration: {status['avg_duration_seconds']}s")
```

#### `get_layer_summary()`

Show layer execution statistics.

```python
from odibi_de_v2.utils import get_layer_summary

summary = get_layer_summary(sql_provider, spark, "MyProject", "qat")
for layer in summary:
    print(f"{layer['layer']}: {layer['enabled_count']} enabled, {layer['disabled_count']} disabled")
```

#### `print_project_summary()`

Print comprehensive project summary.

```python
from odibi_de_v2.utils import print_project_summary

print_project_summary(sql_provider, spark, "MyProject", "qat")
```

Output:
```
📊 PROJECT SUMMARY: Energy Efficiency (qat)
============================================================

📁 Layers:
  Bronze           5 enabled,   0 disabled
  Silver_1        12 enabled,   2 disabled
  Silver_2         8 enabled,   1 disabled
  Gold_1           6 enabled,   0 disabled

  Total configurations: 34

⏱️  Recent Execution Status (last 24h):
  Total runs: 156
  Success: 152 (97.4%)
  Failed: 4
  Avg duration: 23.5s
  Last run: 2025-10-29 14:30:00

✅ No recent failures
============================================================
```

#### `get_failed_transformations()`

Get list of recent failures.

```python
from odibi_de_v2.utils import get_failed_transformations

failed = get_failed_transformations(sql_provider, spark, "MyProject", "qat", hours=24)
for fail in failed:
    print(f"❌ {fail['transformation_id']}: {fail['error_message']}")
```

#### `retry_failed_transformations()`

Identify and retry failed transformations.

```python
from odibi_de_v2.utils import retry_failed_transformations

# Dry run - see what would be retried
result = retry_failed_transformations(
    sql_provider, spark, "MyProject", "qat", "Silver_1", dry_run=True
)
print(f"Would retry: {result['failed_ids']}")

# Actually retry
result = retry_failed_transformations(
    sql_provider, spark, "MyProject", "qat", "Silver_1", dry_run=False
)
```

#### `get_transformation_config()`

Get configuration for specific transformation.

```python
from odibi_de_v2.utils import get_transformation_config

config = get_transformation_config(sql_provider, spark, "my-transform", "qat")
if config:
    print(f"Module: {config['module']}")
    print(f"Function: {config['function']}")
    print(f"Inputs: {config['inputs']}")
```

#### `export_project_config()`

Export project configuration to JSON.

```python
from odibi_de_v2.utils import export_project_config

# Export to file
export_project_config(
    sql_provider, spark, "MyProject", "qat",
    output_file="myproject_config.json"
)

# Export to dict
config = export_project_config(sql_provider, spark, "MyProject", "qat")
```

---

## 🚀 Quick Start Examples

### Daily Workflow

```python
from odibi_de_v2.utils import (
    quick_health_check,
    print_project_summary,
    get_failed_transformations
)

# Morning routine: Check project health
quick_health_check(sql_provider, spark, "MyProject", "qat")

# Get project status
print_project_summary(sql_provider, spark, "MyProject", "qat")

# Check for failures
failed = get_failed_transformations(sql_provider, spark, "MyProject", "qat")
if failed:
    print(f"⚠️ {len(failed)} failed transformations need attention")
```

### Before Deployment

```python
from odibi_de_v2.config import validate_transformation_registry
from odibi_de_v2.utils import HealthCheck

# Load configs
configs = load_all_configs()  # Your loading logic

# Validate configurations
validate_transformation_registry(configs)

# Run health check
hc = HealthCheck(sql_provider, spark, "MyProject", "qat")
report = hc.run_full_check()

if report['overall_health'] != 'HEALTHY':
    print("❌ Fix issues before deploying")
else:
    print("✅ Ready for deployment")
```

### Troubleshooting

```python
from odibi_de_v2.utils import (
    get_failed_transformations,
    get_transformation_config,
    HealthCheck
)

# Find failures
failed = get_failed_transformations(sql_provider, spark, "MyProject", "qat")

for fail in failed:
    # Get config for failed transformation
    config = get_transformation_config(
        sql_provider, spark, fail['transformation_id'], "qat"
    )
    
    print(f"\n🔍 Debugging {fail['transformation_id']}")
    print(f"Error: {fail['error_message']}")
    print(f"Module: {config['module']}.{config['function']}")
    print(f"Inputs: {config['inputs']}")

# Check layer health
hc = HealthCheck(sql_provider, spark, "MyProject", "qat")
layer_report = hc.check_layer("Silver_1")
print(f"Layer healthy: {layer_report['healthy']}")
```

### Batch Operations

```python
from odibi_de_v2.utils import list_projects, print_project_summary

# Check all projects
projects = list_projects(sql_provider, spark, "qat")

for project in projects:
    print(f"\n{'='*60}")
    print(f"Checking: {project}")
    print(f"{'='*60}")
    print_project_summary(sql_provider, spark, project, "qat")
```

---

## 💡 Tips & Best Practices

### 1. Regular Health Checks

Run health checks daily or before critical operations:

```python
# Add to your daily script
quick_health_check(sql_provider, spark, "MyProject", "qat")
```

### 2. Validate Before Committing

Always validate configurations before pushing to source control:

```python
from odibi_de_v2.config import validate_transformation_registry

validate_transformation_registry(your_configs)
```

### 3. Use CLI for Quick Tasks

For quick checks, use the CLI instead of writing scripts:

```bash
# Quick health check
python -m odibi_de_v2.cli.project_cli health --project "MyProject" --env qat

# See failures
python -m odibi_de_v2.cli.project_cli failed --project "MyProject"
```

### 4. Export Configs for Backup

Regularly export configurations:

```python
from odibi_de_v2.utils import export_project_config
import datetime

today = datetime.date.today().strftime("%Y%m%d")
export_project_config(
    sql_provider, spark, "MyProject", "qat",
    output_file=f"backups/config_{today}.json"
)
```

### 5. Monitor Trends

Track success rates over time:

```python
from odibi_de_v2.utils import get_project_status

# Check different time periods
for hours in [1, 6, 24, 168]:  # 1h, 6h, 24h, 1 week
    status = get_project_status(sql_provider, spark, "MyProject", "qat", hours)
    print(f"Last {hours}h: {status['success_rate']}")
```

---

## 📚 Additional Resources

- **TransformationRegistry UI:** Interactive notebook interface for creating configs
- **Project Manifest:** Configure project-level settings and entity labels
- **MetadataManager:** Track pipeline execution metadata
- **DynamicLogger:** Structured logging for transformations

---

## 🆘 Getting Help

If you encounter issues:

1. Run health check: `quick_health_check(sql_provider, spark, project, env)`
2. Validate configs: `validate_transformation_registry(configs)`
3. Check recent failures: `get_failed_transformations(sql_provider, spark, project, env)`
4. Review configuration: `get_transformation_config(sql_provider, spark, transformation_id, env)`

---

**Last Updated:** October 2025  
**Version:** 2.0
