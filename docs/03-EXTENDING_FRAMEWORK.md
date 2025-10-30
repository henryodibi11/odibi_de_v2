# Extending the Framework - Developer Guide

## Table of Contents
- [Creating Custom Transformation Functions](#creating-custom-transformation-functions)
- [Building New Transformation Nodes](#building-new-transformation-nodes)
- [Adding Custom Hook Types](#adding-custom-hook-types)
- [Implementing Custom Log Sinks](#implementing-custom-log-sinks)
- [Creating Custom Readers/Savers](#creating-custom-readerssavers)
- [Databricks vs Local Development](#databricks-vs-local-development)

---

## Creating Custom Transformation Functions

### Basic Function Registration

#### Step 1: Define Your Function

```python
# File: my_project/transformations/custom_functions.py
from odibi_de_v2.odibi_functions import spark_function, pandas_function, universal_function

@spark_function(
    module="custom_functions",
    description="Calculate customer lifetime value",
    author="analytics_team",
    version="1.0",
    tags=["analytics", "kpi"]
)
def calculate_customer_ltv(df, discount_rate=0.1, context=None):
    """
    Calculate customer lifetime value with discounted cash flows.
    
    Args:
        df: Spark DataFrame with columns [customer_id, purchase_date, revenue]
        discount_rate: Annual discount rate for NPV calculation
        context: ExecutionContext (optional, auto-injected)
    
    Returns:
        Spark DataFrame with columns [customer_id, lifetime_value]
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    # Calculate days since first purchase
    window = Window.partitionBy("customer_id").orderBy("purchase_date")
    
    df_with_age = df.withColumn(
        "days_since_first",
        F.datediff(F.col("purchase_date"), F.first("purchase_date").over(window))
    )
    
    # Calculate discounted value
    df_discounted = df_with_age.withColumn(
        "discounted_revenue",
        F.col("revenue") / F.pow(F.lit(1 + discount_rate), F.col("days_since_first") / 365)
    )
    
    # Aggregate to customer level
    ltv = df_discounted.groupBy("customer_id").agg(
        F.sum("discounted_revenue").alias("lifetime_value")
    )
    
    return ltv
```

#### Step 2: Register in TransformationRegistry

```sql
INSERT INTO TransformationRegistry VALUES (
    'T_CALC_LTV',
    'Customer Analytics',
    'prod',
    'Gold_1',
    '',
    '',
    'custom_functions',
    'calculate_customer_ltv',
    '["silver.customer_purchases"]',
    '{"discount_rate": 0.12}',
    '["gold.customer_ltv"]',
    true
);
```

#### Step 3: Execute

```python
from odibi_de_v2.orchestration import GenericProjectOrchestrator

orchestrator = GenericProjectOrchestrator(
    project="Customer Analytics",
    env="prod"
)

orchestrator.run_transformation_layer("Gold_1")
```

### Dual-Engine Function (Spark + Pandas)

When you need both Spark and Pandas implementations:

```python
from odibi_de_v2.odibi_functions import spark_function, pandas_function

# Spark implementation
@spark_function(module="dual_engine", name="normalize_scores")
def normalize_scores_spark(df, column="score", context=None):
    """Normalize scores to 0-1 range (Spark)."""
    from pyspark.sql import functions as F
    
    min_max = df.agg(
        F.min(column).alias("min_val"),
        F.max(column).alias("max_val")
    ).collect()[0]
    
    min_val = min_max["min_val"]
    max_val = min_max["max_val"]
    
    normalized = df.withColumn(
        f"{column}_normalized",
        (F.col(column) - F.lit(min_val)) / F.lit(max_val - min_val)
    )
    
    return normalized

# Pandas implementation
@pandas_function(module="dual_engine", name="normalize_scores")
def normalize_scores_pandas(df, column="score", context=None):
    """Normalize scores to 0-1 range (Pandas)."""
    result = df.copy()
    min_val = result[column].min()
    max_val = result[column].max()
    
    result[f"{column}_normalized"] = (result[column] - min_val) / (max_val - min_val)
    
    return result
```

**TransformationRegistry Config**:
```json
{
  "transformation_id": "T_NORMALIZE",
  "module": "dual_engine",
  "function": "normalize_scores",
  "constants": {
    "column": "revenue",
    "engine": "spark"  // Switch to "pandas" as needed
  }
}
```

### Context-Aware Functions

Use `ExecutionContext` to access runtime resources:

```python
from odibi_de_v2.odibi_functions import spark_function, with_context
from odibi_de_v2.core import ExecutionContext

@spark_function(module="advanced")
@with_context(context_param="context")
def dynamic_table_loader(
    table_pattern,
    database="silver",
    context: ExecutionContext = None
):
    """
    Load multiple tables matching a pattern using context.
    
    Args:
        table_pattern: Glob pattern for table names (e.g., "sales_*")
        database: Database name
        context: ExecutionContext (auto-injected)
    
    Returns:
        Unioned Spark DataFrame from all matching tables
    """
    if context is None or context.spark is None:
        raise ValueError("Spark context required")
    
    # Get all tables in database
    tables = context.spark.sql(f"SHOW TABLES IN {database}").collect()
    
    # Filter by pattern
    import fnmatch
    matching_tables = [
        row.tableName for row in tables
        if fnmatch.fnmatch(row.tableName, table_pattern)
    ]
    
    # Union all matching tables
    dfs = [context.spark.table(f"{database}.{t}") for t in matching_tables]
    
    if not dfs:
        raise ValueError(f"No tables matching pattern '{table_pattern}' in {database}")
    
    from functools import reduce
    result = reduce(lambda df1, df2: df1.union(df2), dfs)
    
    # Add metadata column
    result = result.withColumn("source_database", F.lit(database))
    
    return result
```

**Usage**:
```python
# Context is automatically injected by TransformationRunner
# Access: context.spark, context.logger, context.hooks, context.extras
```

### Advanced: Multi-Input Functions

```python
from odibi_de_v2.odibi_functions import spark_function

@spark_function(module="advanced")
def multi_table_join(customers_df, orders_df, products_df, context=None):
    """
    Join multiple tables with complex logic.
    
    Args:
        customers_df: Customer dimension table
        orders_df: Order fact table
        products_df: Product dimension table
        context: ExecutionContext
    
    Returns:
        Denormalized Spark DataFrame
    """
    # Join orders with customers
    orders_customers = orders_df.join(
        customers_df,
        orders_df.customer_id == customers_df.id,
        "left"
    ).select(
        orders_df["*"],
        customers_df["name"].alias("customer_name"),
        customers_df["segment"].alias("customer_segment")
    )
    
    # Join with products
    result = orders_customers.join(
        products_df,
        orders_customers.product_id == products_df.id,
        "left"
    ).select(
        orders_customers["*"],
        products_df["name"].alias("product_name"),
        products_df["category"].alias("product_category")
    )
    
    return result
```

**TransformationRegistry Config**:
```json
{
  "transformation_id": "T_MULTI_JOIN",
  "module": "advanced",
  "function": "multi_table_join",
  "inputs": [
    "silver.customers",
    "silver.orders",
    "silver.products"
  ],
  "constants": {},
  "outputs": ["gold.orders_denormalized"],
  "enabled": true
}
```

---

## Building New Transformation Nodes

### Custom Transformation Class

For complex transformations that don't fit the function model, create a class-based transformer:

```python
# File: my_project/transformers/custom_transformer.py
from odibi_de_v2.core import IDataTransformer
from pyspark.sql import DataFrame

class CustomerSegmentationTransformer(IDataTransformer):
    """
    Segment customers based on RFM (Recency, Frequency, Monetary) analysis.
    """
    
    def __init__(self, recency_threshold=90, frequency_threshold=5, monetary_threshold=1000):
        self.recency_threshold = recency_threshold
        self.frequency_threshold = frequency_threshold
        self.monetary_threshold = monetary_threshold
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply RFM segmentation.
        
        Args:
            df: Spark DataFrame with columns [customer_id, order_date, order_value]
        
        Returns:
            Spark DataFrame with added 'segment' column
        """
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        from datetime import datetime
        
        current_date = datetime.now()
        
        # Calculate RFM metrics
        rfm = df.groupBy("customer_id").agg(
            F.datediff(F.lit(current_date), F.max("order_date")).alias("recency"),
            F.count("*").alias("frequency"),
            F.sum("order_value").alias("monetary")
        )
        
        # Assign segment
        segmented = rfm.withColumn(
            "segment",
            F.when(
                (F.col("recency") <= self.recency_threshold) &
                (F.col("frequency") >= self.frequency_threshold) &
                (F.col("monetary") >= self.monetary_threshold),
                "Champion"
            ).when(
                (F.col("recency") <= self.recency_threshold) &
                (F.col("frequency") >= self.frequency_threshold),
                "Loyal"
            ).when(
                (F.col("recency") <= self.recency_threshold) &
                (F.col("monetary") >= self.monetary_threshold),
                "Big Spender"
            ).when(
                F.col("recency") > 180,
                "At Risk"
            ).otherwise("Potential")
        )
        
        return segmented
```

### Register Custom Transformer

```python
from odibi_de_v2.transformer import set_transformer_package

# Register custom transformer package
set_transformer_package("my_project.transformers")

# Now transformers are discoverable by the framework
```

---

## Adding Custom Hook Types

### Define Custom Event

```python
from odibi_de_v2.hooks import HookManager

hooks = HookManager()

# Custom event: data_quality_check
def data_quality_hook(payload):
    """
    Custom hook for data quality checks.
    
    Payload:
        - df: DataFrame to check
        - checks: List of quality checks to run
        - threshold: Acceptable failure rate (0.0 - 1.0)
    """
    df = payload["df"]
    checks = payload.get("checks", [])
    threshold = payload.get("threshold", 0.05)
    
    total_rows = df.count() if hasattr(df, 'count') else len(df)
    failed_rows = 0
    
    for check in checks:
        check_name = check["name"]
        check_expr = check["expression"]
        
        # Apply check (example for Spark)
        if hasattr(df, 'filter'):
            failed = df.filter(~F.expr(check_expr)).count()
        else:
            failed = len(df[~df.eval(check_expr)])
        
        failed_rows += failed
        print(f"Quality Check [{check_name}]: {failed} failures")
    
    failure_rate = failed_rows / total_rows
    
    if failure_rate > threshold:
        raise ValueError(f"Data quality check failed: {failure_rate:.2%} failure rate")

hooks.register("data_quality_check", data_quality_hook)
```

### Emit Custom Event in Function

```python
from odibi_de_v2.odibi_functions import spark_function

@spark_function(module="quality")
def validate_and_clean(df, context=None):
    """Apply data quality checks before cleaning."""
    
    # Emit custom hook
    if context and context.hooks:
        context.hooks.emit("data_quality_check", {
            "df": df,
            "checks": [
                {"name": "no_nulls", "expression": "value IS NOT NULL"},
                {"name": "positive_values", "expression": "value >= 0"}
            ],
            "threshold": 0.05
        })
    
    # Proceed with cleaning
    cleaned = df.dropna().filter(F.col("value") >= 0)
    
    return cleaned
```

### Event Catalog

**Standard Events** (built-in):
- `pre_read`, `post_read`
- `pre_transform`, `post_transform`
- `pre_save`, `post_save`
- `on_error`
- `pipeline_start`, `pipeline_end`

**Custom Events** (examples):
- `data_quality_check`: Validate data quality
- `schema_validation`: Validate schema compliance
- `performance_metrics`: Track performance metrics
- `cost_tracking`: Track compute costs
- `lineage_update`: Update data lineage graph

---

## Implementing Custom Log Sinks

### Custom Log Sink: PostgreSQL

```python
from odibi_de_v2.logging import BaseLogSink
from typing import List, Dict, Any
import psycopg2
from datetime import datetime

class PostgreSQLLogSink(BaseLogSink):
    """Log transformation runs to PostgreSQL database."""
    
    def __init__(self, connection_string: str, table_name: str = "transformation_logs"):
        self.connection_string = connection_string
        self.table_name = table_name
        self._create_table()
    
    def _create_table(self):
        """Create log table if not exists."""
        conn = psycopg2.connect(self.connection_string)
        cursor = conn.cursor()
        
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            log_id SERIAL PRIMARY KEY,
            run_id VARCHAR(100),
            transformation_id VARCHAR(100),
            project VARCHAR(100),
            environment VARCHAR(50),
            layer VARCHAR(50),
            module VARCHAR(200),
            function VARCHAR(100),
            status VARCHAR(20),
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            duration_seconds FLOAT,
            input_row_count BIGINT,
            output_row_count BIGINT,
            error_message TEXT
        )
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
    
    def write(self, records: List[Dict[str, Any]]) -> None:
        """Write log records to PostgreSQL."""
        if not records:
            return
        
        conn = psycopg2.connect(self.connection_string)
        cursor = conn.cursor()
        
        for record in records:
            cursor.execute(f"""
            INSERT INTO {self.table_name} (
                run_id, transformation_id, project, environment, layer,
                module, function, status, start_time, end_time,
                duration_seconds, input_row_count, output_row_count, error_message
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """, (
                record["run_id"],
                record["transformation_id"],
                record["project"],
                record["environment"],
                record["layer"],
                record["module"],
                record["function"],
                record["status"],
                record["start_time"],
                record["end_time"],
                record["duration_seconds"],
                record.get("input_row_count"),
                record.get("output_row_count"),
                record.get("error_message")
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
```

### Custom Log Sink: Azure Blob Storage (JSON)

```python
from odibi_de_v2.logging import BaseLogSink
from azure.storage.blob import BlobServiceClient
import json
from datetime import datetime

class AzureBlobLogSink(BaseLogSink):
    """Log transformation runs to Azure Blob Storage as JSON files."""
    
    def __init__(self, connection_string: str, container_name: str, prefix: str = "logs"):
        self.blob_service = BlobServiceClient.from_connection_string(connection_string)
        self.container_name = container_name
        self.prefix = prefix
        
        # Create container if not exists
        try:
            self.blob_service.create_container(container_name)
        except:
            pass
    
    def write(self, records: List[Dict[str, Any]]) -> None:
        """Write log records to Azure Blob Storage."""
        if not records:
            return
        
        # Create timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        blob_name = f"{self.prefix}/transformation_logs_{timestamp}.json"
        
        # Convert records to JSON
        log_data = {
            "timestamp": datetime.now().isoformat(),
            "records": records
        }
        
        json_content = json.dumps(log_data, indent=2, default=str)
        
        # Upload to blob
        blob_client = self.blob_service.get_blob_client(
            container=self.container_name,
            blob=blob_name
        )
        
        blob_client.upload_blob(json_content, overwrite=True)
```

### Use Custom Log Sink

```python
from odibi_de_v2.transformer import TransformationRunnerFromConfig

# Initialize custom log sink
log_sink = PostgreSQLLogSink(
    connection_string="postgresql://user:pass@localhost:5432/logs_db",
    table_name="transformation_logs"
)

# Use in TransformationRunner
runner = TransformationRunnerFromConfig(
    sql_provider=my_sql_provider,
    project="My Project",
    env="prod",
    layer="Silver",
    log_sink=log_sink  # Inject custom log sink
)

runner.run()
```

---

## Creating Custom Readers/Savers

### Custom Reader: REST API

```python
from odibi_de_v2.ingestion import IDataReader
import requests
import pandas as pd

class RestAPIReader(IDataReader):
    """Read data from REST API endpoints."""
    
    def __init__(self, api_url: str, headers: dict = None, auth_token: str = None):
        self.api_url = api_url
        self.headers = headers or {}
        
        if auth_token:
            self.headers["Authorization"] = f"Bearer {auth_token}"
    
    def read(self, **kwargs) -> pd.DataFrame:
        """
        Read data from REST API.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
        
        Returns:
            Pandas DataFrame
        """
        endpoint = kwargs.get("endpoint", "")
        params = kwargs.get("params", {})
        
        url = f"{self.api_url}/{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)
        
        if response.status_code != 200:
            raise ValueError(f"API request failed: {response.status_code} {response.text}")
        
        data = response.json()
        
        # Handle different response formats
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict) and "data" in data:
            df = pd.DataFrame(data["data"])
        else:
            df = pd.DataFrame([data])
        
        return df
```

### Custom Saver: Parquet with Compression

```python
from odibi_de_v2.storage import IDataSaver
import pandas as pd
from pathlib import Path

class CompressedParquetSaver(IDataSaver):
    """Save data to compressed Parquet files."""
    
    def __init__(self, base_path: str, compression: str = "snappy", partition_cols: list = None):
        self.base_path = Path(base_path)
        self.compression = compression
        self.partition_cols = partition_cols or []
    
    def save(self, data: pd.DataFrame, **kwargs):
        """
        Save DataFrame to Parquet with compression.
        
        Args:
            data: Pandas DataFrame
            table_name: Table name (used as subdirectory)
            mode: Write mode ("append" or "overwrite")
        """
        table_name = kwargs.get("table_name", "data")
        mode = kwargs.get("mode", "overwrite")
        
        output_path = self.base_path / table_name
        
        if mode == "overwrite" and output_path.exists():
            import shutil
            shutil.rmtree(output_path)
        
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Save with partitioning
        if self.partition_cols:
            data.to_parquet(
                output_path,
                engine="pyarrow",
                compression=self.compression,
                partition_cols=self.partition_cols
            )
        else:
            data.to_parquet(
                output_path / "data.parquet",
                engine="pyarrow",
                compression=self.compression
            )
```

### Register Custom Reader/Saver

```python
from odibi_de_v2.ingestion import ReaderProvider
from odibi_de_v2.storage import SaverProvider

# Register custom reader
reader_provider = ReaderProvider()
reader_provider.register_reader("rest_api", RestAPIReader)

# Register custom saver
saver_provider = SaverProvider()
saver_provider.register_saver("compressed_parquet", CompressedParquetSaver)
```

---

## Databricks vs Local Development

### Environment Detection

```python
import os

def is_databricks():
    """Detect if running in Databricks environment."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

def get_execution_mode():
    """Get execution mode (databricks, local_spark, local_pandas)."""
    if is_databricks():
        return "databricks"
    
    try:
        from pyspark.sql import SparkSession
        if SparkSession.getActiveSession() is not None:
            return "local_spark"
    except ImportError:
        pass
    
    return "local_pandas"
```

### Environment-Specific Configuration

```python
from odibi_de_v2.orchestration import GenericProjectOrchestrator

mode = get_execution_mode()

if mode == "databricks":
    orchestrator = GenericProjectOrchestrator(
        project="Energy Efficiency",
        env="prod",
        engine="spark",
        auth_provider=databricks_auth_provider,
        log_container="production-logs"
    )
elif mode == "local_spark":
    orchestrator = GenericProjectOrchestrator(
        project="Energy Efficiency",
        env="dev",
        engine="spark",
        save_logs=False  # Don't save logs in local dev
    )
else:  # local_pandas
    orchestrator = GenericProjectOrchestrator(
        project="Energy Efficiency",
        env="dev",
        engine="pandas",
        save_logs=False
    )

orchestrator.run_project()
```

### Databricks Secrets Integration

```python
from odibi_de_v2.odibi_functions import spark_function

@spark_function(module="databricks_utils")
def load_with_secrets(table_name, context=None):
    """Load data using Databricks secrets for authentication."""
    
    if context and context.spark:
        # Access Databricks secrets
        dbutils = context.extras.get("dbutils")
        
        if dbutils:
            storage_account = dbutils.secrets.get("key-vault", "storage-account")
            access_key = dbutils.secrets.get("key-vault", "storage-key")
            
            # Configure Spark to use secrets
            context.spark.conf.set(
                f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
                access_key
            )
        
        return context.spark.table(table_name)
    
    raise ValueError("Spark context required")
```

### Local Testing with Mock Data

```python
import pandas as pd
from odibi_de_v2.odibi_functions import pandas_function

@pandas_function(module="testing")
def create_mock_data(num_rows=1000, context=None):
    """Generate mock data for local testing."""
    import numpy as np
    from datetime import datetime, timedelta
    
    # Generate synthetic data
    dates = [datetime.now() - timedelta(days=i) for i in range(num_rows)]
    
    df = pd.DataFrame({
        "id": range(num_rows),
        "timestamp": dates,
        "value": np.random.randn(num_rows) * 100 + 500,
        "category": np.random.choice(["A", "B", "C"], num_rows),
        "is_valid": np.random.choice([True, False], num_rows, p=[0.9, 0.1])
    })
    
    return df
```

---

## Summary

This guide demonstrated:

1. ✅ **Creating custom transformation functions** with `@odibi_function` decorator
2. ✅ **Building class-based transformers** for complex logic
3. ✅ **Adding custom hook types** for extensible pipelines
4. ✅ **Implementing custom log sinks** (PostgreSQL, Azure Blob)
5. ✅ **Creating custom readers/savers** (REST API, compressed Parquet)
6. ✅ **Environment-specific configuration** for Databricks vs local development

---

## Next Steps

- **[04-GLOSSARY.md](04-GLOSSARY.md)**: Complete reference of classes and configs
- **[ARCHITECTURE_MAP.md](ARCHITECTURE_MAP.md)**: Visual module dependency diagrams
