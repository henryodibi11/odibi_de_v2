# 08 - Tutorials and Practice

**Level:** Beginner to Intermediate  
**Duration:** 2-4 hours  
**Prerequisites:** Python basics, pandas knowledge

---

## Table of Contents
1. [Tutorial Overview](#tutorial-overview)
2. [Existing Tutorial Guide](#existing-tutorial-guide)
3. [Practice Exercise 1: Temperature Conversion Pipeline](#practice-exercise-1-temperature-conversion-pipeline)
4. [Practice Exercise 2: Custom Validation Hook](#practice-exercise-2-custom-validation-hook)
5. [Solutions](#solutions)
6. [Next Steps](#next-steps)

---

## Tutorial Overview

### Learning Path

```
📚 Foundation
│
├─ Tutorial 01: Pandas Workflow (30 min)
│   └─ Learn: Basic transformations, medallion architecture
│
├─ Tutorial 02: Function Registry (40 min)  
│   └─ Learn: Reusable functions, decorators
│
├─ Tutorial 03: Hooks & Observability (40 min)
│   └─ Learn: Pipeline monitoring, validation
│
└─ Tutorial 04: Complete Project Template (60 min)
    └─ Learn: End-to-end project structure

🎯 Practice Exercises (this guide)
│
├─ Exercise 1: Temperature Conversion Pipeline (45 min)
│   └─ Apply: Medallion pattern, transformations
│
└─ Exercise 2: Custom Validation Hook (30 min)
    └─ Apply: Hooks, data quality checks
```

### Quick Start

```bash
# Navigate to tutorials
cd d:\projects\odibi_de_v2\docs\tutorials

# Open in Jupyter
jupyter notebook

# Or VS Code
code 01-pandas-workflow-tutorial.ipynb
```

---

## Existing Tutorial Guide

### Tutorial 01: Pandas Workflow Tutorial

**📓 File:** [01-pandas-workflow-tutorial.ipynb](file:///d:/projects/odibi_de_v2/docs/tutorials/01-pandas-workflow-tutorial.ipynb)

**What You'll Learn:**
- Running transformations with the Pandas engine
- Creating transformation functions
- Using `TransformationConfig` for metadata-driven pipelines
- Implementing Bronze → Silver → Gold medallion architecture
- Working with constants for parameterized transformations

**Key Code Example:**
```python
from odibi_de_v2.orchestration import TransformationRunnerFromConfig
from odibi_de_v2.config import TransformationConfig

# Define transformation
config = TransformationConfig(
    transformation_id="bronze-to-silver",
    project="sales_analytics",
    environment="qat",
    layer="Silver_1",
    module="my_transformations",
    function="clean_sales_data",
    inputs=["bronze_sales.csv"],
    outputs=[{"table": "silver_sales.csv", "mode": "overwrite"}],
    constants={"min_sale_amount": 10.0}
)

# Run with pandas engine
runner = TransformationRunnerFromConfig(config, engine="pandas")
result = runner.run()
```

**When to Use:**
- Learning the framework basics
- Local development without Spark
- Small datasets (<100MB)

**Hands-on Activities:**
1. Create sample sales data
2. Write Bronze layer ingestion
3. Build Silver layer cleaning transformation
4. Implement Gold layer aggregations
5. Chain transformations together

---

### Tutorial 02: Function Registry Tutorial

**📓 File:** [02-function-registry-tutorial.ipynb](file:///d:/projects/odibi_de_v2/docs/tutorials/02-function-registry-tutorial.ipynb)

**What You'll Learn:**
- Using `@odibi_function` decorator
- Creating engine-specific variants (Spark vs Pandas)
- Function resolution and fallback logic
- Shorthand decorators (`@spark_function`, `@pandas_function`, `@universal_function`)
- Metadata management (author, version, tags)
- Discovery and introspection

**Key Code Example:**
```python
from odibi_de_v2.odibi_functions import odibi_function, REGISTRY

@odibi_function(
    name="calculate_revenue",
    engine="pandas",
    version="1.0",
    author="data-team",
    tags=["finance", "metrics"]
)
def calculate_revenue_pandas(df, tax_rate=0.08):
    """Calculate total revenue with tax"""
    df['revenue'] = df['price'] * df['quantity'] * (1 + tax_rate)
    return df

# Discover functions
finance_funcs = REGISTRY.find_by_tag("finance")
print(finance_funcs)
```

**When to Use:**
- Building reusable transformation libraries
- Supporting multiple execution engines
- Team collaboration with shared functions

**Hands-on Activities:**
1. Register a transformation function
2. Create Spark and Pandas variants
3. Use `@universal_function` for engine-agnostic code
4. Query registry by tags
5. Build a function catalog

---

### Tutorial 03: Hooks & Observability Tutorial

**📓 File:** [03-hooks-observability-tutorial.ipynb](file:///d:/projects/odibi_de_v2/docs/tutorials/03-hooks-observability-tutorial.ipynb)

**What You'll Learn:**
- Understanding lifecycle events (pre_read, post_transform, etc.)
- Registering hooks for monitoring
- Filtered hooks (by project, layer, engine)
- Data validation at pipeline stages
- Performance metrics collection
- Error handling and alerting
- Production-ready observability patterns

**Key Code Example:**
```python
from odibi_de_v2.orchestration.hooks import HookManager, register_hook

hook_manager = HookManager()

@register_hook("post_transform", hook_manager)
def validate_no_nulls(df, context):
    """Ensure no NULL values in critical columns"""
    critical_cols = ["customer_id", "transaction_date", "amount"]
    for col in critical_cols:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            raise ValueError(f"Found {null_count} NULLs in {col}")
    print(f"✓ Validation passed: {context['layer']}")

# Hook runs automatically after each transformation
```

**When to Use:**
- Production pipelines requiring quality checks
- Debugging transformation issues
- Performance monitoring
- Compliance and audit logging

**Hands-on Activities:**
1. Create a row count logging hook
2. Build a schema validation hook
3. Implement performance timing
4. Add error notification hook
5. Filter hooks by project/layer

---

### Tutorial 04: Complete Project Template

**📓 File:** [04-new-project-template.ipynb](file:///d:/projects/odibi_de_v2/docs/tutorials/04-new-project-template.ipynb)

**What You'll Learn:**
- Professional project structure
- Customer analytics use case (Bronze → Silver → Gold)
- Dual-engine support (Pandas + Spark)
- Registered transformation functions
- Configuration management
- Integrated observability
- Orchestration script
- Production-ready patterns

**Key Code Example:**
```python
from odibi_de_v2 import run_project

# Run entire pipeline
run_project(
    project="customer_analytics",
    env="qat",
    layers=["Bronze", "Silver_1", "Gold_1"]
)
```

**When to Use:**
- Starting a new production project
- Reference architecture
- Best practices template

**Hands-on Activities:**
1. Explore project structure
2. Run Bronze ingestion
3. Execute Silver transformations
4. Generate Gold KPIs
5. View observability logs

---

## Practice Exercise 1: Temperature Conversion Pipeline

### Objective

Build a medallion architecture pipeline that converts temperature sensor readings from Celsius to Fahrenheit and calculates daily statistics.

### Learning Goals
- Apply medallion pattern (Bronze → Silver → Gold)
- Write transformation functions
- Use TransformationConfig
- Chain multiple transformations

### Step-by-Step Instructions

#### Step 1: Setup (5 min)

Create project structure:

```python
import pandas as pd
from pathlib import Path

# Create directories
project_dir = Path("./temp_conversion_pipeline")
project_dir.mkdir(exist_ok=True)
(project_dir / "bronze").mkdir(exist_ok=True)
(project_dir / "silver").mkdir(exist_ok=True)
(project_dir / "gold").mkdir(exist_ok=True)
```

#### Step 2: Generate Sample Data (5 min)

Create temperature sensor readings:

```python
import numpy as np
from datetime import datetime, timedelta

# Generate sample data
np.random.seed(42)
dates = [datetime(2024, 1, 1) + timedelta(hours=i) for i in range(168)]  # 1 week
sensors = ['sensor_a', 'sensor_b', 'sensor_c']

data = []
for dt in dates:
    for sensor in sensors:
        temp_c = np.random.normal(20, 5)  # Mean 20°C, std 5°C
        data.append({
            'timestamp': dt,
            'sensor_id': sensor,
            'temperature_celsius': round(temp_c, 2),
            'location': np.random.choice(['warehouse', 'office', 'factory'])
        })

df_raw = pd.DataFrame(data)

# Save to Bronze
bronze_path = project_dir / "bronze" / "raw_temperatures.csv"
df_raw.to_csv(bronze_path, index=False)
print(f"✓ Created Bronze layer: {len(df_raw)} records")
print(df_raw.head())
```

**Expected Output:**
```
✓ Created Bronze layer: 504 records
            timestamp  sensor_id  temperature_celsius   location
0 2024-01-01 00:00:00   sensor_a                18.99  warehouse
1 2024-01-01 00:00:00   sensor_b                23.57     office
2 2024-01-01 00:00:00   sensor_c                17.03    factory
```

#### Step 3: Bronze → Silver Transformation (15 min)

Convert Celsius to Fahrenheit and add validation:

```python
from odibi_de_v2.config import TransformationConfig
from odibi_de_v2.orchestration import TransformationRunnerFromConfig

# Create transformation module
transformations_dir = project_dir / "transformations"
transformations_dir.mkdir(exist_ok=True)

# Write transformation function
silver_transform = """
import pandas as pd
from typing import Any

def celsius_to_fahrenheit(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    '''Convert Celsius to Fahrenheit and validate'''
    
    # Validate input
    assert 'temperature_celsius' in df.columns, "Missing temperature_celsius column"
    assert not df['temperature_celsius'].isnull().any(), "NULL temperatures found"
    
    # Convert to Fahrenheit
    df['temperature_fahrenheit'] = (df['temperature_celsius'] * 9/5) + 32
    
    # Add data quality flags
    df['is_valid'] = (
        (df['temperature_celsius'] >= -50) & 
        (df['temperature_celsius'] <= 60)
    )
    
    # Add processing metadata
    from datetime import datetime
    df['processed_at'] = datetime.now()
    
    print(f"✓ Converted {len(df)} records to Fahrenheit")
    print(f"✓ Valid records: {df['is_valid'].sum()} / {len(df)}")
    
    return df[['timestamp', 'sensor_id', 'location', 
               'temperature_celsius', 'temperature_fahrenheit', 
               'is_valid', 'processed_at']]
"""

with open(transformations_dir / "silver_transforms.py", "w") as f:
    f.write(silver_transform)

# Run transformation
import sys
sys.path.insert(0, str(project_dir))

from transformations.silver_transforms import celsius_to_fahrenheit

df_bronze = pd.read_csv(bronze_path)
df_silver = celsius_to_fahrenheit(df_bronze)

# Save to Silver
silver_path = project_dir / "silver" / "temperatures_fahrenheit.csv"
df_silver.to_csv(silver_path, index=False)

print(df_silver.head())
```

**Expected Output:**
```
✓ Converted 504 records to Fahrenheit
✓ Valid records: 504 / 504
            timestamp  sensor_id   location  temperature_celsius  temperature_fahrenheit  is_valid
0 2024-01-01 00:00:00   sensor_a  warehouse                18.99                   66.18      True
1 2024-01-01 00:00:00   sensor_b     office                23.57                   74.43      True
```

#### Step 4: Silver → Gold Aggregations (15 min)

Calculate daily statistics per sensor:

```python
# Write Gold transformation
gold_transform = """
import pandas as pd

def calculate_daily_stats(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    '''Calculate daily temperature statistics by sensor'''
    
    # Filter valid records only
    df_valid = df[df['is_valid'] == True].copy()
    
    # Convert timestamp to date
    df_valid['date'] = pd.to_datetime(df_valid['timestamp']).dt.date
    
    # Aggregate by date and sensor
    stats = df_valid.groupby(['date', 'sensor_id']).agg({
        'temperature_fahrenheit': ['min', 'max', 'mean', 'std'],
        'temperature_celsius': ['min', 'max', 'mean'],
        'location': lambda x: x.mode()[0] if len(x) > 0 else None  # Most common location
    }).reset_index()
    
    # Flatten column names
    stats.columns = ['date', 'sensor_id', 
                     'temp_f_min', 'temp_f_max', 'temp_f_avg', 'temp_f_std',
                     'temp_c_min', 'temp_c_max', 'temp_c_avg',
                     'primary_location']
    
    # Add metrics
    stats['temp_range_f'] = stats['temp_f_max'] - stats['temp_f_min']
    stats['is_stable'] = stats['temp_f_std'] < 5  # Low variance = stable
    
    print(f"✓ Aggregated {len(df_valid)} records → {len(stats)} daily stats")
    
    return stats
"""

with open(transformations_dir / "gold_transforms.py", "w") as f:
    f.write(gold_transform)

from transformations.gold_transforms import calculate_daily_stats

df_silver = pd.read_csv(silver_path)
df_gold = calculate_daily_stats(df_silver)

# Save to Gold
gold_path = project_dir / "gold" / "daily_temperature_stats.csv"
df_gold.to_csv(gold_path, index=False)

print(df_gold.head())
```

**Expected Output:**
```
✓ Aggregated 504 records → 21 daily stats
        date  sensor_id  temp_f_min  temp_f_max  temp_f_avg  temp_f_std  ...  is_stable
0 2024-01-01   sensor_a       55.12       78.45       68.34        6.23  ...      False
1 2024-01-01   sensor_b       60.89       81.22       72.11        5.67  ...      False
```

#### Step 5: Orchestrate Full Pipeline (5 min)

Chain all transformations:

```python
# Full pipeline orchestration
def run_temperature_pipeline():
    """Run Bronze → Silver → Gold pipeline"""
    
    print("🚀 Starting Temperature Conversion Pipeline")
    print("=" * 60)
    
    # Step 1: Bronze (already created)
    print("\n📥 BRONZE: Raw sensor data")
    df_bronze = pd.read_csv(project_dir / "bronze" / "raw_temperatures.csv")
    print(f"  Records: {len(df_bronze)}")
    
    # Step 2: Silver (convert temperatures)
    print("\n🔧 SILVER: Convert Celsius → Fahrenheit")
    from transformations.silver_transforms import celsius_to_fahrenheit
    df_silver = celsius_to_fahrenheit(df_bronze)
    silver_path = project_dir / "silver" / "temperatures_fahrenheit.csv"
    df_silver.to_csv(silver_path, index=False)
    
    # Step 3: Gold (daily statistics)
    print("\n📊 GOLD: Calculate daily statistics")
    from transformations.gold_transforms import calculate_daily_stats
    df_gold = calculate_daily_stats(df_silver)
    gold_path = project_dir / "gold" / "daily_temperature_stats.csv"
    df_gold.to_csv(gold_path, index=False)
    
    print("\n✅ Pipeline Complete!")
    print("=" * 60)
    print(f"\nGold Summary:")
    print(f"  Total days: {df_gold['date'].nunique()}")
    print(f"  Total sensors: {df_gold['sensor_id'].nunique()}")
    print(f"  Stable readings: {df_gold['is_stable'].sum()} / {len(df_gold)}")
    print(f"\nAverage temperature: {df_gold['temp_f_avg'].mean():.2f}°F")
    
    return df_gold

# Run it!
result = run_temperature_pipeline()
```

### Expected Output

```
🚀 Starting Temperature Conversion Pipeline
============================================================

📥 BRONZE: Raw sensor data
  Records: 504

🔧 SILVER: Convert Celsius → Fahrenheit
✓ Converted 504 records to Fahrenheit
✓ Valid records: 504 / 504

📊 GOLD: Calculate daily statistics
✓ Aggregated 504 records → 21 daily stats

✅ Pipeline Complete!
============================================================

Gold Summary:
  Total days: 7
  Total sensors: 3
  Stable readings: 8 / 21
  
Average temperature: 68.12°F
```

### Challenge Extensions

1. **Add alerting:** Send email when temperature exceeds 80°F
2. **Add caching:** Cache Silver layer for performance
3. **Add hooks:** Log row counts at each stage
4. **Add tests:** Unit test the conversion formula
5. **Add visualization:** Plot temperature trends

---

## Practice Exercise 2: Custom Validation Hook

### Objective

Build a custom hook that validates data quality after transformations and logs issues to a file.

### Learning Goals
- Understand hook lifecycle
- Implement custom validation logic
- Use context for filtering
- Write production-ready data quality checks

### Step-by-Step Instructions

#### Step 1: Create Hook Manager (5 min)

```python
from odibi_de_v2.orchestration.hooks import HookManager

# Initialize hook manager
hook_manager = HookManager()

print("📌 Hook Manager initialized")
print(f"Available events: {hook_manager.list_events()}")
```

#### Step 2: Build Validation Hook (15 min)

Create comprehensive data quality hook:

```python
from datetime import datetime
from pathlib import Path

def create_validation_hook():
    """Create data quality validation hook"""
    
    # Create validation log directory
    log_dir = Path("./validation_logs")
    log_dir.mkdir(exist_ok=True)
    
    validation_issues = []
    
    def validate_data_quality(df, context):
        """
        Validate data quality and log issues
        
        Checks:
        1. No NULL values in critical columns
        2. Row count > 0
        3. No duplicate records
        4. Numeric columns in valid ranges
        """
        issues = []
        layer = context.get('layer', 'Unknown')
        timestamp = datetime.now().isoformat()
        
        print(f"\n🔍 Validating {layer} layer...")
        
        # Check 1: Row count
        if len(df) == 0:
            issue = f"[{timestamp}] CRITICAL: {layer} has 0 rows"
            issues.append(issue)
            print(f"  ❌ {issue}")
        else:
            print(f"  ✓ Row count: {len(df)}")
        
        # Check 2: NULL values
        null_counts = df.isnull().sum()
        critical_nulls = null_counts[null_counts > 0]
        if len(critical_nulls) > 0:
            for col, count in critical_nulls.items():
                issue = f"[{timestamp}] WARNING: {layer}.{col} has {count} NULLs"
                issues.append(issue)
                print(f"  ⚠️  {issue}")
        else:
            print(f"  ✓ No NULL values")
        
        # Check 3: Duplicates
        dup_count = df.duplicated().sum()
        if dup_count > 0:
            issue = f"[{timestamp}] WARNING: {layer} has {dup_count} duplicate rows"
            issues.append(issue)
            print(f"  ⚠️  {issue}")
        else:
            print(f"  ✓ No duplicates")
        
        # Check 4: Numeric ranges (if applicable)
        for col in df.select_dtypes(include=['float64', 'int64']).columns:
            min_val = df[col].min()
            max_val = df[col].max()
            
            # Check for negative values in temperature columns
            if 'temperature' in col.lower() and min_val < -100:
                issue = f"[{timestamp}] ERROR: {layer}.{col} has invalid min: {min_val}"
                issues.append(issue)
                print(f"  ❌ {issue}")
            
            # Check for extreme outliers
            if abs(max_val) > 1e10:
                issue = f"[{timestamp}] WARNING: {layer}.{col} has extreme value: {max_val}"
                issues.append(issue)
                print(f"  ⚠️  {issue}")
        
        # Log issues to file
        if issues:
            log_file = log_dir / f"validation_{datetime.now().strftime('%Y%m%d')}.log"
            with open(log_file, "a") as f:
                for issue in issues:
                    f.write(issue + "\n")
            print(f"\n  📝 Logged {len(issues)} issues to {log_file}")
            validation_issues.extend(issues)
        else:
            print(f"\n  ✅ All validations passed for {layer}")
        
        return df  # Return unchanged DataFrame
    
    return validate_data_quality, validation_issues

# Create hook
validation_hook, issues_log = create_validation_hook()
```

#### Step 3: Register Hook (5 min)

```python
from odibi_de_v2.orchestration.hooks import register_hook

# Register for post_transform event
register_hook(
    event="post_transform",
    handler=validation_hook,
    hook_manager=hook_manager,
    filters={"project": "temperature_pipeline"}  # Only for temp pipeline
)

print("✓ Validation hook registered for 'post_transform' event")
```

#### Step 4: Test with Real Data (5 min)

```python
# Test with good data
print("=" * 60)
print("TEST 1: Valid Data")
print("=" * 60)

df_valid = pd.DataFrame({
    'sensor_id': ['A', 'B', 'C'],
    'temperature_celsius': [20.5, 22.3, 19.8],
    'location': ['office', 'warehouse', 'factory']
})

context = {'layer': 'Silver_1', 'project': 'temperature_pipeline'}
validation_hook(df_valid, context)

# Test with problematic data
print("\n" + "=" * 60)
print("TEST 2: Data with Issues")
print("=" * 60)

df_issues = pd.DataFrame({
    'sensor_id': ['A', 'B', 'C', 'A'],  # Duplicate row
    'temperature_celsius': [20.5, None, 19.8, 20.5],  # NULL value
    'location': ['office', 'warehouse', None, 'office']  # NULL location
})

validation_hook(df_issues, context)
```

**Expected Output:**
```
============================================================
TEST 1: Valid Data
============================================================

🔍 Validating Silver_1 layer...
  ✓ Row count: 3
  ✓ No NULL values
  ✓ No duplicates

  ✅ All validations passed for Silver_1

============================================================
TEST 2: Data with Issues
============================================================

🔍 Validating Silver_1 layer...
  ✓ Row count: 4
  ⚠️  [2024-01-15T10:30:45] WARNING: Silver_1.temperature_celsius has 1 NULLs
  ⚠️  [2024-01-15T10:30:45] WARNING: Silver_1.location has 1 NULLs
  ⚠️  [2024-01-15T10:30:45] WARNING: Silver_1 has 1 duplicate rows

  📝 Logged 3 issues to validation_logs/validation_20240115.log
```

#### Step 5: Advanced Filtering (5 min)

Create layer-specific hooks:

```python
# Critical validation for Gold layer only
def gold_critical_validation(df, context):
    """Strict validation for Gold layer"""
    layer = context.get('layer', '')
    
    if not layer.startswith('Gold'):
        return df  # Skip non-Gold layers
    
    print(f"\n🚨 CRITICAL VALIDATION for {layer}")
    
    # Gold layer must have aggregations
    if len(df) < 1:
        raise ValueError(f"Gold layer {layer} is empty!")
    
    # Check for required columns
    required_cols = ['date', 'sensor_id']
    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Gold layer missing columns: {missing}")
    
    print(f"  ✅ Gold layer validated successfully")
    return df

# Register with layer filter
register_hook(
    event="post_transform",
    handler=gold_critical_validation,
    hook_manager=hook_manager,
    filters={"layer": "Gold"}
)

print("✓ Gold-specific validation registered")
```

### Challenge Extensions

1. **Add metrics export:** Send metrics to monitoring system
2. **Add threshold configs:** Make validation rules configurable
3. **Add auto-remediation:** Fix simple issues automatically
4. **Add alerting:** Send Slack message for critical failures
5. **Add summary report:** Generate daily validation summary

---

## Solutions

### Exercise 1: Complete Solution

Full working code available at:
```python
# See complete implementation above in Exercise 1
# Key files created:
# - temp_conversion_pipeline/bronze/raw_temperatures.csv
# - temp_conversion_pipeline/silver/temperatures_fahrenheit.csv
# - temp_conversion_pipeline/gold/daily_temperature_stats.csv
# - temp_conversion_pipeline/transformations/silver_transforms.py
# - temp_conversion_pipeline/transformations/gold_transforms.py
```

**Success Criteria:**
- ✅ Bronze layer with 504 raw records
- ✅ Silver layer with Fahrenheit conversion
- ✅ Gold layer with 21 daily aggregations
- ✅ All transformations chainable
- ✅ Pipeline runs end-to-end

### Exercise 2: Complete Solution

```python
# See complete implementation above in Exercise 2
# Key components:
# 1. Hook manager initialized
# 2. Validation hook with 4 quality checks
# 3. Logging to validation_logs/
# 4. Filtering by project and layer
# 5. Both warning and critical validations
```

**Success Criteria:**
- ✅ Hook detects NULL values
- ✅ Hook detects duplicates
- ✅ Hook logs issues to file
- ✅ Hook works with post_transform event
- ✅ Layer-specific filtering works

---

## Next Steps

### After Completing Exercises

1. **Combine Both:** Add validation hooks to temperature pipeline
2. **Scale Up:** Run with 1M+ records using Spark
3. **Deploy:** Package as Databricks job
4. **Extend:** Add more layers (Gold_2 for forecasting)

### Advanced Challenges

1. **Build API Pipeline:**
   - Bronze: Ingest from REST API
   - Silver: Parse JSON, flatten nested data
   - Gold: Generate API usage metrics

2. **Build ML Pipeline:**
   - Bronze: Raw features
   - Silver: Feature engineering
   - Gold: Model training dataset
   - Use function registry for feature functions

3. **Build Multi-Source Pipeline:**
   - Bronze_Sales: CRM data
   - Bronze_Web: Web analytics
   - Silver: Join and deduplicate
   - Gold: Customer 360 view

### Resources

- [Tutorial 01-04](file:///d:/projects/odibi_de_v2/docs/tutorials/README.md)
- [Advanced Concepts](file:///d:/projects/odibi_de_v2/docs/learn_odibi_de/09_Advanced_Concepts.md)
- [Rebuild Challenge](file:///d:/projects/odibi_de_v2/docs/learn_odibi_de/10_Rebuild_Challenge.md)
- [Quick Reference](file:///d:/projects/odibi_de_v2/docs/QUICK_REFERENCE.md)

---

**🎓 Congratulations!** You've mastered the basics. Ready for advanced concepts?

**Next:** [09 - Advanced Concepts](file:///d:/projects/odibi_de_v2/docs/learn_odibi_de/09_Advanced_Concepts.md)
