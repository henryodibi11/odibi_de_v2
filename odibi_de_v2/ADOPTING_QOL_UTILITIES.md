# Adopting Quality-of-Life Utilities

Quick guide for data engineers to start using the new QoL utilities.

## 🚀 5-Minute Quick Start

### 1. Try Your First Health Check (2 minutes)

Open a Databricks notebook:

```python
# Your existing setup
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

# Import the new utility
from odibi_de_v2.utils import quick_health_check

# Run it!
quick_health_check(sql_provider, spark, "Your Project Name", "qat")
```

### 2. Get Project Summary (1 minute)

```python
from odibi_de_v2.utils import print_project_summary

print_project_summary(sql_provider, spark, "Your Project Name", "qat")
```

### 3. Try the CLI (2 minutes)

In your terminal or Databricks notebook with `%sh`:

```bash
# List all projects
python -m odibi_de_v2.cli.project_cli list --env qat

# Check health
python -m odibi_de_v2.cli.project_cli health --project "Your Project" --env qat
```

---

## 📅 Daily Workflow Integration

### Morning Routine (5 minutes)

Add this cell to your daily notebook:

```python
from odibi_de_v2.utils import quick_health_check, print_project_summary

PROJECT = "Your Project Name"
ENV = "qat"

# Quick health check
print("🏥 HEALTH CHECK")
quick_health_check(sql_provider, spark, PROJECT, ENV)

# Project summary
print("\n📊 PROJECT STATUS")
print_project_summary(sql_provider, spark, PROJECT, ENV)
```

### Before Starting Work (3 minutes)

```python
from odibi_de_v2.utils import get_failed_transformations

# Check for failures from overnight runs
failed = get_failed_transformations(sql_provider, spark, PROJECT, ENV, hours=12)

if failed:
    print(f"⚠️ {len(failed)} failures need attention:")
    for f in failed:
        print(f"  - {f['transformation_id']}: {f['error_message'][:60]}")
else:
    print("✅ No failures - good to go!")
```

---

## 🔍 Troubleshooting Workflow

### When a Transformation Fails

```python
from odibi_de_v2.utils import get_transformation_config
from odibi_de_v2.config import ConfigValidator

# Get the config
config = get_transformation_config(sql_provider, spark, "failed-transform-id", "qat")

# Validate it
validator = ConfigValidator()
result = validator.validate_config(config)

if not result.is_valid:
    print("❌ Configuration issues:")
    for error in result.errors:
        print(f"  - {error}")
else:
    print("✅ Config is valid - issue is elsewhere")
    print(f"Module: {config['module']}.{config['function']}")
    print(f"Inputs: {config['inputs']}")
```

---

## 🎯 Common Use Cases

### 1. Before Code Review

```python
from odibi_de_v2.config import validate_transformation_registry

# Load your configs
configs = load_your_configs()  # Your loading logic

# Validate before committing
validate_transformation_registry(configs)
```

### 2. Weekly Health Report

```python
from odibi_de_v2.utils import HealthCheck, get_project_status

projects = ["Project A", "Project B", "Project C"]

for project in projects:
    print(f"\n{'='*60}")
    print(f"📊 {project}")
    print(f"{'='*60}")
    
    status = get_project_status(sql_provider, spark, project, "qat", hours=168)
    print(f"Weekly success rate: {status['success_rate']}")
```

### 3. Config Backup Script

```python
from odibi_de_v2.utils import export_project_config
import datetime

today = datetime.date.today().strftime("%Y%m%d")

for project in your_projects:
    export_project_config(
        sql_provider, spark, project, "qat",
        output_file=f"backups/{project}_{today}.json"
    )
```

---

## 📦 Adding to Existing Scripts

### Old Way
```python
# Manual health check
query = """
    SELECT COUNT(*) as total,
           SUM(CASE WHEN enabled = 1 THEN 1 ELSE 0 END) as enabled
    FROM TransformationRegistry
    WHERE project = 'MyProject'
"""
result = spark.sql(query).collect()[0]
print(f"Total: {result['total']}, Enabled: {result['enabled']}")
```

### New Way
```python
# One-line summary
from odibi_de_v2.utils import print_project_summary

print_project_summary(sql_provider, spark, "MyProject", "qat")
```

---

## 🎓 Learning Path

### Week 1: Basic Utilities
- [ ] Try `quick_health_check()`
- [ ] Use `print_project_summary()`
- [ ] Run `list_projects()`
- [ ] Check `get_failed_transformations()`

### Week 2: Validation
- [ ] Use `ConfigValidator` on one config
- [ ] Run `validate_transformation_registry()` on all configs
- [ ] Add validation to your workflow

### Week 3: CLI Tools
- [ ] Try all CLI commands
- [ ] Add CLI to your terminal shortcuts
- [ ] Use CLI for quick checks instead of notebooks

### Week 4: Advanced Usage
- [ ] Create custom health check workflow
- [ ] Set up automated daily reports
- [ ] Build project-specific utilities using these as foundation

---

## 💡 Tips for Adoption

### Do's
✅ Start with one utility at a time  
✅ Add to existing notebooks gradually  
✅ Share successful patterns with team  
✅ Create team-specific workflows on top of these  
✅ Use CLI for quick ad-hoc checks  
✅ Run health checks before deployments  

### Don'ts
❌ Don't try to adopt everything at once  
❌ Don't replace working scripts immediately  
❌ Don't skip reading the quick reference  
❌ Don't forget to validate before committing  

---

## 🔄 Migration Examples

### Migrate: Manual Config Validation

**Before:**
```python
# Custom validation logic
for config in configs:
    if not config.get('module'):
        print(f"Error in {config['transformation_id']}")
    try:
        json.loads(config['inputs'])
    except:
        print(f"Invalid JSON in {config['transformation_id']}")
```

**After:**
```python
from odibi_de_v2.config import validate_transformation_registry

validate_transformation_registry(configs)
```

### Migrate: Project Status Check

**Before:**
```python
# Multiple manual queries
total = spark.sql(f"SELECT COUNT(*) FROM TransformationRunLog WHERE project='{project}'").collect()[0][0]
success = spark.sql(f"SELECT COUNT(*) FROM TransformationRunLog WHERE project='{project}' AND status='SUCCESS'").collect()[0][0]
print(f"Success rate: {success/total*100:.1f}%")
```

**After:**
```python
from odibi_de_v2.utils import get_project_status

status = get_project_status(sql_provider, spark, project, "qat")
print(f"Success rate: {status['success_rate']}")
```

---

## 🆘 Getting Help

### Quick Reference
See [QUICK_REFERENCE.md](QUICK_REFERENCE.md) for common commands

### Full Documentation
See [QOL_UTILITIES.md](QOL_UTILITIES.md) for complete guide

### Examples
Open [examples_qol_utilities.ipynb](../examples_qol_utilities.ipynb) for interactive examples

### Issues
If something doesn't work:
1. Check the docstring: `help(function_name)`
2. Review examples in documentation
3. Verify your sql_provider and spark are properly initialized

---

## 📊 Measuring Success

Track time saved using these utilities:

| Activity | Old Time | New Time | Saved |
|----------|----------|----------|-------|
| Daily health check | 15 min | 2 min | 13 min |
| Config validation | 20 min | 2 min | 18 min |
| Find failures | 10 min | 1 min | 9 min |
| Export configs | 10 min | 1 min | 9 min |

**Total daily savings: ~40 minutes**  
**Weekly savings: ~3 hours**  
**Monthly savings: ~12 hours**

---

## ✅ Adoption Checklist

- [ ] Ran first health check
- [ ] Added to daily notebook
- [ ] Tried CLI commands
- [ ] Validated configurations
- [ ] Exported project config
- [ ] Read QUICK_REFERENCE.md
- [ ] Reviewed examples notebook
- [ ] Shared with team
- [ ] Identified time savings
- [ ] Built custom workflow on top

---

**Start today - save time tomorrow!** 🚀
