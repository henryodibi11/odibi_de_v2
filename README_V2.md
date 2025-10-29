# odibi_de_v2 v2.0 - Universal Data Engineering Framework

**Transform any data pipeline with one command.**

```python
from odibi_de_v2 import run_project, initialize_project

# Run any project
run_project(project="Energy Efficiency", env="qat")

# Create new projects instantly
initialize_project("Customer Churn")
```

---

## 🎯 What's New in v2.0

### Project-Agnostic Design
No more hardcoded industry assumptions. Works for:
- ✅ Manufacturing (Energy, Reliability, Quality)
- ✅ Analytics (Customer Churn, Sales Forecasting)
- ✅ ML Pipelines (Model Training, Feature Engineering)
- ✅ Data Integration (ETL, CDC, Data Migration)
- ✅ **Your Custom Use Case**

### One-Command Execution
```python
run_project(project="Energy Efficiency", env="qat")
```
That's it. No more orchestrator boilerplate.

### Auto Project Scaffolding
```python
initialize_project("Customer Churn", project_type="analytics")
```
Creates complete project structure in seconds.

### Flexible Configuration
- **SQL Tables**: TransformationRegistry for transformation logic
- **JSON Manifests**: Project-level configuration
- **Generic Entities**: Adapt to any domain (plant→entity_1, region→entity_1, etc.)

---

## 📦 Installation

```bash
pip install odibi-de-v2
```

Or from source:
```bash
git clone https://github.com/yourorg/odibi_de_v2
cd odibi_de_v2
pip install -e .
```

---

## 🚀 Quick Start

### 1. Create a New Project

```python
from odibi_de_v2 import initialize_project

result = initialize_project(
    project_name="Customer Churn",
    project_type="analytics"  # or "manufacturing", "ml_pipeline"
)

print(result['project_path'])
# projects/customer_churn
```

This creates:
```
customer_churn/
├── manifest.json          # Project configuration
├── transformations/       # Your transformation code
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── sql/                   # SQL scripts
├── notebooks/             # Databricks notebooks
├── tests/                 # Unit tests
└── README.md             # Auto-generated docs
```

### 2. Configure Transformations

Add entries to the `TransformationRegistry` SQL table:

```sql
INSERT INTO TransformationRegistry (
    transformation_id,
    project,
    environment,
    layer,
    entity_1,
    entity_2,
    module,
    function,
    inputs,
    outputs
) VALUES (
    'churn-features-silver',
    'customer churn',
    'qat',
    'Silver',
    'North America',
    'Retail',
    'customer_churn.transformations.silver.functions',
    'calculate_features',
    '["customer_transactions_bronze", "customer_profile_bronze"]',
    '[{"table": "customer_features_silver", "mode": "overwrite"}]'
);
```

Or use the interactive UI:

```python
from odibi_de_v2.config import TransformationRegistryUI

ui = TransformationRegistryUI(project="Customer Churn", env="qat")
ui.render()
```

### 3. Write Transformation Logic

In `customer_churn/transformations/silver/functions.py`:

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def calculate_features(spark=None, env="qat", **kwargs):
    if spark is None:
        spark = SparkSession.getActiveSession()
    
    # Get inputs from kwargs (automatically passed by orchestrator)
    inputs = kwargs.get('inputs', [])
    constants = kwargs.get('constants', {})
    outputs = kwargs.get('outputs', [])
    
    # Read input tables
    transactions = spark.table(inputs[0])
    profiles = spark.table(inputs[1])
    
    # Your transformation logic
    features = transactions.join(profiles, "customer_id") \
        .groupBy("customer_id") \
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.sum("amount").alias("total_spend"),
            F.avg("amount").alias("avg_transaction")
        )
    
    # Save output
    output_table = outputs[0]['table']
    features.write.mode(outputs[0]['mode']).saveAsTable(output_table)
    
    return features
```

### 4. Run the Pipeline

```python
from odibi_de_v2 import run_project

# Run full pipeline
result = run_project(project="Customer Churn", env="qat")

print(result)
# {
#     'status': 'SUCCESS',
#     'project': 'Customer Churn',
#     'env': 'qat',
#     'layers_run': ['Bronze', 'Silver', 'Gold'],
#     'duration_seconds': 45.2
# }
```

---

## 🏭 Real-World Example: Energy Efficiency (Manufacturing)

### Project Structure

```python
initialize_project(
    project_name="Energy Efficiency",
    project_type="manufacturing"
)
```

### Entity Mapping

In `manifest.json`:
```json
{
  "entity_labels": {
    "entity_1": "plant",
    "entity_2": "asset",
    "entity_3": "equipment"
  }
}
```

### Sample Transformation

```sql
INSERT INTO TransformationRegistry VALUES (
    'energy-argo-boilers-silver',
    'energy-silver-assets',
    'energy efficiency',
    'qat',
    'Silver_1',
    1,
    1,
    'Argo',                          -- entity_1 (plant)
    'Boiler 6,7,8,10',               -- entity_2 (asset)
    NULL,                            -- entity_3 (equipment)
    'silver.functions',
    'process_argo_boilers',
    '["qat_energy_efficiency.argo_boilers_bronze"]',
    '{}',
    '[{"table": "qat_energy_efficiency.argo_boilers_silver", "mode": "overwrite"}]',
    'Process Argo boilers data'
);
```

### Run Pipeline

```python
from odibi_de_v2 import run_project

# Full pipeline
run_project(project="Energy Efficiency", env="qat")

# Specific layers
run_project(
    project="Energy Efficiency",
    env="qat",
    target_layers=["Silver_1", "Gold_1"]
)

# With caching
run_project(
    project="Energy Efficiency",
    env="qat",
    cache_plan={"Gold_1": ["combined_dryers"]}
)
```

---

## 📊 Key Concepts

### 1. TransformationRegistry

The heart of the configuration system. Stores all transformation metadata.

**Key Fields:**
- `transformation_id`: Unique identifier
- `project`: Project name
- `environment`: qat, prod, dev, etc.
- `layer`: Bronze, Silver_1, Gold_1, etc.
- `entity_1/2/3`: Generic hierarchy (adapt to your domain)
- `module`, `function`: Python transformation function
- `inputs`: JSON array of input tables
- `constants`: JSON object of parameters
- `outputs`: JSON array of output configurations

### 2. Project Manifest

Project-level configuration in JSON.

**Key Fields:**
- `layer_order`: Execution sequence
- `entity_labels`: Domain-specific naming
- `cache_plan`: Tables to cache per layer
- `transformation_modules`: Python module paths

### 3. Generic Entities

Flexible hierarchy that adapts to any domain:

| Domain | entity_1 | entity_2 | entity_3 |
|--------|----------|----------|----------|
| Manufacturing | plant | asset | equipment |
| Retail | region | store | department |
| Finance | business_unit | product | account_type |
| Healthcare | hospital | department | service |
| **Your Domain** | **custom_1** | **custom_2** | **custom_3** |

### 4. Layer Architecture

Standard medallion architecture:

- **Bronze**: Raw data ingestion (handled by IngestionSourceConfig)
- **Silver**: Cleansing, enrichment, business logic
- **Gold**: Aggregations, KPIs, reporting tables

Customize as needed:
```json
{
  "layer_order": ["Bronze", "Silver_1", "Silver_2", "Gold_1", "Gold_2"]
}
```

---

## 🔧 Advanced Usage

### Custom Authentication

```python
from odibi_de_v2 import run_project

def my_auth(env, repo_path, logger_metadata):
    # Your custom authentication logic
    spark = get_spark_session()
    sql_provider = get_sql_provider()
    return {"spark": spark, "sql_provider": sql_provider}

run_project(
    project="My Project",
    env="qat",
    auth_provider=my_auth
)
```

### Multiple Inputs

```sql
-- JSON array of multiple inputs
inputs: '["table1", "table2", "table3"]'
```

In your function:
```python
def transform(inputs=None, **kwargs):
    table1 = spark.table(inputs[0])
    table2 = spark.table(inputs[1])
    table3 = spark.table(inputs[2])
    # ...
```

### Constants/Parameters

```sql
-- JSON object of constants
constants: '{"threshold": 100, "window_days": 30, "min_records": 5}'
```

In your function:
```python
def transform(constants=None, **kwargs):
    threshold = constants.get('threshold', 100)
    window_days = constants.get('window_days', 30)
    # ...
```

### Multiple Outputs

```sql
-- JSON array of multiple outputs
outputs: '[
    {"table": "output_silver", "mode": "overwrite"},
    {"table": "output_archive", "mode": "append", "partition_by": ["date"]}
]'
```

In your function:
```python
def transform(outputs=None, **kwargs):
    for output_config in outputs:
        df.write.mode(output_config['mode']) \
            .saveAsTable(output_config['table'])
```

---

## 🎨 Configuration UI

### TransformationRegistry UI

```python
from odibi_de_v2.config import TransformationRegistryUI

ui = TransformationRegistryUI(project="Energy Efficiency", env="qat")
ui.render()
```

Features:
- ✅ Visual field editor
- ✅ JSON validation
- ✅ SQL generation
- ✅ Copy to clipboard
- ✅ Form templates

### Browse Existing Configs

```python
from odibi_de_v2.config import TransformationRegistryBrowser

browser = TransformationRegistryBrowser(
    sql_provider=your_sql_provider,
    project="Energy Efficiency",
    env="qat"
)
browser.render()
```

---

## 🧪 Testing

### Unit Tests

```python
import pytest
from your_project.transformations.silver.functions import process_data

def test_process_data(spark_session):
    # Mock inputs
    input_df = spark_session.createDataFrame([...])
    
    # Run transformation
    result = process_data(
        spark=spark_session,
        inputs=["test_input"],
        constants={"threshold": 100},
        outputs=[{"table": "test_output", "mode": "overwrite"}]
    )
    
    # Assertions
    assert result.count() > 0
    assert "expected_column" in result.columns
```

### Integration Tests

```python
from odibi_de_v2 import run_project

def test_full_pipeline():
    result = run_project(
        project="Customer Churn",
        env="test",
        target_layers=["Silver"]
    )
    
    assert result['status'] == 'SUCCESS'
    assert 'Silver' in result['layers_run']
```

---

## 📚 Project Types

### Manufacturing
```python
initialize_project("Reliability Analytics", "manufacturing")
```
- Entities: plant → asset → equipment
- Layers: Bronze → Silver_1 (asset-level) → Silver_2 (plant-level) → Gold (KPIs)

### Analytics
```python
initialize_project("Sales Forecasting", "analytics")
```
- Entities: domain → subdomain → metric
- Layers: Bronze → Silver (cleansing) → Gold (reporting)

### ML Pipeline
```python
initialize_project("Churn Prediction", "ml_pipeline")
```
- Entities: region → segment → cohort
- Layers: Bronze → Silver (features) → Gold (model training)

### Custom
```python
initialize_project("My Custom Project", "custom")
```
- Define your own entity labels
- Define your own layer architecture

---

## 🔄 Migration from v1.x

See [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) for detailed migration instructions.

**Quick summary:**
1. Create TransformationRegistry table
2. Migrate data from TransformationConfig
3. Create project manifest
4. Replace orchestrator with `run_project()`
5. Done! 🎉

---

## 🤝 Contributing

We welcome contributions!

1. Fork the repository
2. Create a feature branch
3. Add tests
4. Submit a pull request

---

## 📄 License

[Your License Here]

---

## 🙏 Acknowledgments

Built on the foundation of:
- Apache Spark
- Databricks
- Delta Lake
- odibi_de_v2 v1.x (Ingredion-specific framework)

---

## 📞 Support

- **Documentation**: [docs/](docs/)
- **Examples**: [examples/](examples/)
- **Migration Guide**: [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)
- **Issues**: [GitHub Issues](https://github.com/yourorg/odibi_de_v2/issues)

---

**Ready to transform your data pipelines?**

```python
from odibi_de_v2 import initialize_project, run_project

# Start your journey
initialize_project("My Amazing Project")
run_project(project="My Amazing Project", env="qat")
```

🚀 **Freedom. Simplicity. Power.**
