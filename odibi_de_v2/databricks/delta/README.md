# Module Overview

### `delta_merge_manager.py`

#### Class: `DeltaMergeManager`

### Class: DeltaMergeManager

#### Purpose:
The `DeltaMergeManager` class is designed to manage and perform idempotent merge operations on Delta tables in Apache Spark. It ensures that only new or changed data is inserted or updated in the target Delta table, thereby preventing duplicates and maintaining data integrity.

#### Key Features:
- **Idempotent Merges**: The class uses a combination of merge keys and change-detection columns to perform updates only when necessary, avoiding redundant data insertion.
- **Hash-based Row Identification**: Utilizes hash values (`Merge_Id` for unique row identification and `Change_Id` for detecting changes in data) to manage and execute merges efficiently.
- **In-place Table Modification**: Directly modifies the target Delta table without the need for intermediate storage or additional data handling.

#### Inputs:
- **source_df (DataFrame)**: The DataFrame containing new data to be merged.
- **merge_keys (List[str])**: Column names from `source_df` that uniquely identify each row, used for generating `Merge_Id`.
- **change_columns (List[str])**: Column names that trigger an update in the target table when their values change, used for generating `Change_Id`.

#### Outputs:
- The class modifies the target Delta table directly and does not return any value from its operations.

#### Methods:
- **`__init__(self, spark, target_table, is_path=False)`**: Initializes the manager with a Spark session and target Delta table, which can be specified by path or name.
- **`merge(self, source_df, merge_keys, change_columns)`**: Executes the merge operation by adding necessary hash columns to `source_df`, and then performing conditional updates or inserts into the target Delta table based on these hashes.
- **`_add_hash_columns(self, df, merge_keys, change_columns)`**: A helper method that adds `Merge_Id` and `Change_Id` columns to the DataFrame.
- **`_quote(self, col_name)`**: A utility method to ensure column names are appropriately quoted if they contain special characters.

#### Usage Example:
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DeltaMergeExample").getOrCreate()
data = [(1, "NameA", 100), (2, "NameB", 200)]
columns = ["id", "name", "value"]
source_df = spark.createDataFrame(data, columns)

merge_manager = DeltaMergeManager(spark, "my_database.my_table")
merge_manager.merge(
    source_df=source_df,
    merge_keys=["id"],
    change_columns=["name", "value"]
)
```

#### Design Choices:
- **Idempotency**: Ensures that the merge operation does not create duplicate entries and updates are only made when actual changes occur.
- **Flexibility in Table Specification**: Allows the target Delta table to be specified either by a path or a database table name, accommodating different data management scenarios.
- **Efficiency**: By using hash values for row identification and change detection, the class minimizes the amount of data processed during the merge, enhancing performance.

#### Integration:
This class is typically used in data pipeline scripts or ETL jobs where data integrity and efficiency are crucial, especially in environments using Delta Lake with Apache Spark. It fits into larger applications that require robust data handling and merging capabilities for Delta tables.

---

### `delta_table_manager.py`

#### Class: `DeltaTableManager`

### Class Overview

The `DeltaTableManager` class is designed to facilitate the management of Delta Lake tables within Apache Spark environments. It provides a comprehensive suite of functionalities to interact with Delta tables, including transaction history viewing, table optimization, version restoration, and more.

### Attributes

- **spark (SparkSession):** An active SparkSession instance required to execute operations.
- **table_or_path (str):** Specifies the Delta table either by its name in the Spark metastore or by its filesystem path.
- **is_path (bool):** A flag indicating whether `table_or_path` is a filesystem path (`True`) or a metastore table name (`False`).
- **delta_table (DeltaTable):** A lazily loaded DeltaTable object, initialized when required by various methods.

### Key Methods

- **describe_detail():** Returns detailed metadata about the Delta table as a DataFrame.
- **describe_history():** Fetches the complete transaction log history of the Delta table.
- **show_history(limit=10):** Displays a limited number of recent transaction history entries.
- **time_travel(version=None, timestamp=None):** Allows querying the table as of a specific version or timestamp.
- **optimize(zorder_by=None):** Optimizes the table layout, optionally using ZORDER by specified columns.
- **vacuum(retention_hours=168, dry_run=False):** Cleans up old snapshots and files beyond a specified retention period.
- **get_latest_version():** Retrieves the latest version number of the Delta table.
- **restore_version(version):** Restores the table to a specified historical version.
- **register_table(table_name, database=None):** Registers a path-based Delta table in the metastore for SQL querying.

### Design Choices

- **Lazy Loading:** The `delta_table` attribute is initialized only when required by a method, optimizing resource usage.
- **Flexibility:** Supports both path-based and metastore-based Delta table management, enhancing usability across different deployment scenarios.
- **Error Handling:** Includes specific error raises (e.g., `ValueError`, `DeltaTableError`) to guide correct usage and troubleshooting.

### Usage in Applications

`DeltaTableManager` can be integrated into data engineering pipelines in Spark environments for managing Delta Lake tables. It is particularly useful for tasks involving data versioning, auditing, and performance optimization. The class's methods support both ad-hoc operations and scheduled data management tasks, making it a versatile tool for data management in large-scale data applications.

### Example

```python
from pyspark.sql import SparkSession
from your_package import DeltaTableManager

# Initialize Spark session and DeltaTableManager
spark = SparkSession.builder.getOrCreate()
manager = DeltaTableManager(spark, "db.table_name")

# View transaction history
history_df = manager.show_history()
history_df.show()

# Optimize the table and vacuum old files
manager.optimize(zorder_by=["column1"])
manager.vacuum(retention_hours=72)
```

This class is a crucial component for managing Delta Lake tables efficiently, providing high-level abstractions over common Delta Lake operations, which are otherwise manually intensive and error-prone.

---
