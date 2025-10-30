# ODIBI_DE_V2 Tutorials

Comprehensive tutorial series for learning the odibi_de_v2 data engineering framework.

## Tutorial Overview

### [01 - Pandas Workflow Tutorial](01-pandas-workflow-tutorial.ipynb)
**Level:** Beginner  
**Duration:** 20-30 minutes  
**Prerequisites:** Basic Python and Pandas knowledge

Learn the fundamentals of running transformations with the Pandas engine:
- Setting up sample data and project structure
- Creating transformation functions
- Using TransformationConfig for metadata-driven pipelines
- Running Bronze → Silver → Gold medallion architecture
- Working with constants for parameterized transformations

**Key Concepts:**
- `engine="pandas"` in TransformationRunnerFromConfig
- Local CSV file I/O
- Configuration-driven transformations
- Medallion layer progression

---

### [02 - Function Registry Tutorial](02-function-registry-tutorial.ipynb)
**Level:** Intermediate  
**Duration:** 30-40 minutes  
**Prerequisites:** Tutorial 01 completed

Master the function registry system for reusable transformations:
- Using `@odibi_function` decorator
- Creating engine-specific variants (Spark vs Pandas)
- Function resolution and fallback logic
- Shorthand decorators (`@spark_function`, `@pandas_function`, `@universal_function`)
- Metadata management (author, version, tags)
- Discovery and introspection

**Key Concepts:**
- Global function registry (REGISTRY)
- Resolution order: exact match → universal fallback
- Function metadata and documentation
- Module-based organization

---

### [03 - Hooks & Observability Tutorial](03-hooks-observability-tutorial.ipynb)
**Level:** Intermediate  
**Duration:** 30-40 minutes  
**Prerequisites:** Tutorial 01 completed

Build robust observability into your pipelines:
- Understanding lifecycle events (pre_read, post_transform, etc.)
- Registering hooks for monitoring
- Filtered hooks (by project, layer, engine)
- Data validation at pipeline stages
- Performance metrics collection
- Error handling and alerting
- Production-ready observability patterns

**Key Concepts:**
- HookManager for lifecycle callbacks
- Event filtering for targeted execution
- Schema and quality validation
- Audit logging
- Error notifications

---

### [04 - Complete Project Template](04-new-project-template.ipynb)
**Level:** Advanced  
**Duration:** 45-60 minutes  
**Prerequisites:** Tutorials 01-03 completed

End-to-end project setup with best practices:
- Professional project structure
- Customer analytics use case (Bronze → Silver → Gold)
- Dual-engine support (Pandas + Spark)
- Registered transformation functions
- Configuration management
- Integrated observability
- Orchestration script
- Production-ready patterns

**Key Concepts:**
- Complete project architecture
- Both Pandas and Spark implementations
- Medallion data flow
- Business metrics and insights
- Deployment-ready code

---

## Quick Start

### Installation

```bash
# Install odibi_de_v2
pip install -e /path/to/odibi_de_v2

# Required dependencies
pip install pandas

# Optional (for Spark examples)
pip install pyspark
```

### Running Tutorials

1. **Jupyter Notebook:**
   ```bash
   jupyter notebook docs/tutorials/
   ```

2. **JupyterLab:**
   ```bash
   jupyter lab docs/tutorials/
   ```

3. **VS Code:**
   - Open `.ipynb` files directly
   - Install Python and Jupyter extensions

### Recommended Learning Path

```
Tutorial 01 (Pandas Workflow)
    ↓
Tutorial 02 (Function Registry) ← Can run in parallel
    ↓                              ↓
Tutorial 03 (Hooks & Observability)
    ↓
Tutorial 04 (Complete Project Template)
```

## Database Schema

### [02_function_registry.sql](../../sql/ddl/02_function_registry.sql)

Optional SQL schema for tracking registered functions in a database:
- Function metadata table
- Versioning support
- Engine-specific variants
- Utility views for discovery
- Example records for reference

**Usage:**
```sql
-- Create table
CREATE TABLE FunctionRegistry (...);

-- Query active functions
SELECT * FROM vw_FunctionInventory;

-- Find by module
SELECT * FROM FunctionRegistry 
WHERE module = 'my_module' AND is_active = 1;
```

## Tutorial Features

### All Tutorials Include:
- ✓ Executable code cells (run top to bottom)
- ✓ Clear markdown explanations
- ✓ Sample data generation
- ✓ Incremental complexity
- ✓ Best practices
- ✓ Summary sections
- ✓ Real-world examples

### Common Patterns Demonstrated:
- Configuration-driven pipelines
- Engine abstraction (Pandas ↔ Spark)
- Medallion architecture
- Data quality validation
- Performance monitoring
- Error handling
- Audit logging

## Use Cases by Tutorial

| Tutorial | Use Case | Domain |
|----------|----------|---------|
| 01 | Sales analytics | Retail |
| 02 | Reusable functions | Cross-domain |
| 03 | Pipeline monitoring | Operations |
| 04 | Customer analytics | Marketing/CRM |

## Getting Help

### Documentation
- [Migration Guide](../MIGRATION_GUIDE.md)
- [Quick Reference](../QUICK_REFERENCE.md)
- [Transformation Runner Update](../../TRANSFORMATION_RUNNER_DUAL_ENGINE_UPDATE.md)

### Code Examples
- [ODIBI Functions Examples](../../odibi_de_v2/odibi_functions/examples.py)
- [Test Files](../../tests/)
- [Framework Evolution Tutorial](../../FRAMEWORK_EVOLUTION_TUTORIAL.ipynb)

### Common Issues

**Import Errors:**
```python
# Ensure project root is in path
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd().parent.parent))
```

**Module Not Found:**
```bash
# Install in development mode
pip install -e /path/to/odibi_de_v2
```

**Spark Not Available:**
- Tutorials 01-03 work with Pandas only
- Tutorial 04 has both Pandas and Spark variants
- Install PySpark if you want to run Spark examples

## Next Steps After Tutorials

1. **Adapt Tutorial 04 template** for your use case
2. **Create custom transformation functions** in your domain
3. **Set up CI/CD** for automated testing
4. **Deploy to production** (Databricks, Airflow, etc.)
5. **Build your function library** with registered, reusable functions

## Contributing

Found an issue or want to improve tutorials?
1. Create issue with tutorial name and section
2. Submit PR with improvements
3. Share your custom examples

## Version Compatibility

- **odibi_de_v2:** v2.0+
- **Python:** 3.8+
- **Pandas:** 1.3+
- **PySpark:** 3.0+ (optional)

---

**Happy Learning! 🚀**

Start with [01-pandas-workflow-tutorial.ipynb](01-pandas-workflow-tutorial.ipynb)
