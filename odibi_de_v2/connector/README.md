# Module Overview

### `azure_blob_connector.py`

#### Class: `AzureBlobConnection`

### Class Overview

The `AzureBlobConnection` class is designed to facilitate interaction with Azure Blob Storage when using data processing frameworks such as Spark or Pandas. It provides methods to construct cloud file paths and generate appropriate configuration settings for these frameworks, ensuring seamless integration and data manipulation.

### Key Features

- **Framework-Specific Path Resolution**: Generates Azure Blob Storage File System (ABFS) paths tailored for either Spark or Pandas. For Spark, paths are prefixed with `abfss://`, and for Pandas, they are prefixed with `abfs://`.
- **Dynamic Configuration**: Automatically creates configuration dictionaries necessary for setting up Spark or Pandas to interact with Azure Blob Storage, handling authentication and other storage options.
- **Logging and Error Handling**: Implements logging at various stages of path resolution and configuration setup. It uses decorators to log method calls, enforce type checks on method arguments, and handle exceptions gracefully.

### Methods

1. **`__init__(account_name, account_key, framework)`**:
   - Initializes the connection with the Azure storage account credentials and specifies the target framework (Spark or Pandas).
   - Parameters:
     - `account_name (str)`: The Azure storage account name.
     - `account_key (str)`: The corresponding account key.
     - `framework (Framework)`: Enum indicating the target framework, either `Framework.SPARK` or `Framework.PANDAS`.

2. **`get_file_path(container, path_prefix, object_name)`**:
   - Constructs and returns a fully qualified ABFS path based on the specified container, path prefix, and object name.
   - Parameters:
     - `container (str)`: Name of the Azure Blob container.
     - `path_prefix (str)`: Folder or directory path inside the container.
     - `object_name (str)`: Name of the file or blob.
   - Returns:
     - `str`: A string representing the complete file path formatted for the specified framework.

3. **`get_storage_options()`**:
   - Generates and returns a dictionary containing storage options required for the specified framework. This includes authentication details and any other necessary configuration.
   - Returns:
     - `dict or None`: A dictionary with configuration settings suitable for Spark or Pandas, or `None` if the framework is unsupported.

### Design Choices

- **Framework Abstraction**: The class abstracts the differences between Spark and Pandas in handling Azure Blob Storage paths and configurations, providing a unified interface that adjusts based on the specified framework.
- **Decorators for Enhanced Functionality**:
  - `@log_call`: Logs entry and exit of methods, aiding in debugging and monitoring.
  - `@enforce_types`: Ensures that method arguments are of expected types, preventing runtime type errors.
  - `@log_exceptions`: Captures and logs exceptions, with an option to raise them, facilitating error handling and resilience.

### Integration into Larger Applications

`AzureBlobConnection` can be integrated into data processing pipelines that require interaction with Azure Blob Storage. It is particularly useful in scenarios involving data ingestion, transformation, or analysis where data resides in Azure Blob Storage. By handling path resolution and configuration internally, it allows developers to focus on core business logic without worrying about underlying storage specifics.

This class is part of a larger module, likely aimed at providing various connectors for different storage solutions, enhancing modularity and reusability across data engineering projects.

---

### `sql_database_connection.py`

#### Class: `SQLDatabaseConnection`

### Class: SQLDatabaseConnection

#### Purpose:
The `SQLDatabaseConnection` class is designed to facilitate connections to SQL databases using either Pandas or Spark frameworks. It dynamically generates the necessary connection options and constructs the appropriate SQL query or table string for data ingestion.

#### Key Features:
- **Framework Support**: The class supports two frameworks:
  - `Framework.PANDAS`: Utilizes pyodbc-compatible connection strings for operations with Pandas.
  - `Framework.SPARK`: Uses JDBC options suitable for Spark's `.read.format("jdbc")` method.
- **Integration**: Designed to integrate smoothly with `ReaderProvider` and any `DataReader` that handles SQL-based data workflows.

#### Usage:
- **Initialization**: The class is initialized with database connection parameters such as host, database name, user credentials, and the desired framework. Optional parameters include port and driver (specific to Pandas).
- **Data Reading**: Through methods like `get_file_path` and `get_storage_options`, it prepares the SQL query and connection settings required by data reading utilities.

#### Methods:
1. **`__init__`**:
   - Initializes the connection settings.
   - Inputs: `host`, `database`, `user`, `password`, `framework`, optional `port`, and `driver`.
   - Outputs: An instance of `SQLDatabaseConnection`.

2. **`get_file_path`**:
   - Constructs and returns the SQL query or table name.
   - Inputs: `container` (schema name, typically ignored), `path_prefix` (also ignored), and `object_name` (SQL table name or query).
   - Outputs: A string representing the SQL query or table name.

3. **`get_storage_options`**:
   - Generates and returns connection options based on the specified framework.
   - Outputs: A dictionary containing framework-specific connection details.
   - Raises `NotImplementedError` for unsupported frameworks.

#### Examples:
- **Pandas Example**:
  ```python
  connector = SQLDatabaseConnection(host="server", database="db", user="user", password="pass", framework=Framework.PANDAS)
  provider = ReaderProvider(connector)
  df = provider.read(data_type=DataType.SQL, object_name="SELECT * FROM customers")
  ```
- **Spark Example**:
  ```python
  spark = SparkSession.builder.getOrCreate()
  connector = SQLDatabaseConnection(host="server", database="db", user="user", password="pass", framework=Framework.SPARK)
  provider = ReaderProvider(connector)
  df = provider.read(data_type=DataType.SQL, object_name="SELECT * FROM sales_metrics", spark=spark)
  ```

#### Design Choices:
- **Framework Flexibility**: The class can toggle between Pandas and Spark, making it versatile for different environments and use cases.
- **Error Handling**: Implements robust logging and exception handling to ensure clear diagnostics and maintainability.
- **Compatibility**: Accepts parameters (`container`, `path_prefix`) for potential future extensions or compatibility with other components without affecting current functionality.

#### Integration:
This class is a component of a larger data ingestion and processing system, likely interacting with data reading and transformation modules. It is essential for establishing and managing database connections, crucial for data extraction and subsequent analysis or processing tasks.

---

### `local_connection.py`

#### Class: `LocalConnection`

### Class Overview

The `LocalConnection` class, inheriting from `BaseConnection`, is designed to handle file path resolution for data stored on local disk systems, mounted volumes, or specific local filesystems such as Databricks DBFS. It is primarily utilized when no cloud-based storage connector is specified, serving as a default connector in data reading or saving operations.

### Key Features and Decorators

- **No Authentication Required**: This class assumes that no authentication or special storage configurations are necessary, simplifying interactions with local file systems.
- **Decorators**:
  - `@log_call`: This decorator is used to log the entry and exit of methods within the class, aiding in debugging and monitoring by logging actions to specified modules and components.
  - `@enforce_types`: Ensures that the inputs to methods meet expected data types, enhancing code robustness by enforcing type safety.

### Methods

#### `__init__`
Initializes the instance by setting the `framework` attribute to `Framework.LOCAL`, indicating the local nature of the file handling.

#### `get_file_path`
- **Purpose**: Constructs and returns a full local file path by concatenating `path_prefix` and `object_name`.
- **Inputs**:
  - `container`: A string, typically ignored in this context but included for interface consistency with other connection types.
  - `path_prefix`: The directory path where the file is located or should be located (e.g., "/dbfs/tmp").
  - `object_name`: The name of the file (e.g., "data.csv").
- **Output**: Returns the fully resolved local file path as a string.
- **Behavior**: Logs the process of resolving the file path and the result, aiding in traceability and debugging.

#### `get_storage_options`
- **Purpose**: Provides storage options, which in the context of local file systems, are non-existent (i.e., returns `None`).
- **Output**: Always returns `None`, reflecting the lack of need for authentication or other storage options in local environments.
- **Behavior**: Logs that local file systems do not require authentication, providing clarity and documentation within operational logs.

### Usage in Larger Applications

In a broader application or module, `LocalConnection` can be used as a default or fallback connection method when no specific cloud storage options are provided. It ensures that applications dealing with file operations remain versatile and functional across different environments, including development, testing, and production scenarios where local file access might be necessary or preferred.

### Example Usage

```python
from odibi_de_v2.connector import LocalConnection

connector = LocalConnection()
path = connector.get_file_path(
    path_prefix="dbfs:/tmp/data",
    object_name="sales.csv"
)
options = connector.get_storage_options()  # returns None
```

This example demonstrates creating an instance of `LocalConnection`, resolving a file path, and confirming that no storage options are required for local file systems.

---
