# Module Overview

### `helpers.py`

#### Function: `resolve_storage_function`

### Function Overview

The function `resolve_storage_function` is designed to dynamically locate and return a reference to a storage-related function by its name from a specified list of modules. This capability is particularly useful in applications that require flexibility in selecting data storage strategies at runtime, based on configuration or user input.

### Inputs and Outputs

- **Input**: The function takes a single input parameter, `method_name`, which is a string representing the exact name of the storage function to be resolved.
- **Output**: It returns a callable (a function reference) that can be executed with the necessary arguments in the calling context.

### Key Logic and Design Choices

- **Module List**: The function searches for the `method_name` within a predefined list of modules (`modules_to_search`). Currently, this list contains only one module: `"odibi_de_v2.databricks.storage.delta_savers"`.
- **Dynamic Import and Resolution**: It uses Python's `importlib` to dynamically import the specified module and checks if the module contains an attribute (function) with the name provided in `method_name`.
- **Error Handling**: If the function name does not exist in any of the modules listed, the function raises an `ImportError` with a message indicating that the method was not found, thereby informing the user or calling process about the issue.

### Integration in Larger Applications

This function is typically used in data handling or storage systems where the method of storage can vary based on different parameters or configurations. By abstracting the function resolution process, `resolve_storage_function` allows developers to write more modular and configurable code. For instance, in a data processing application, different types of data might need to be saved in different formats or locations, and the decision on which method to use can be deferred until runtime, making the system adaptable and easier to maintain.

### Example Usage

```python
save_function = resolve_storage_function("save_static_data_from_config")
save_function(data_frame, config_parameters, spark_session, db_connector)
```

In this example, the `save_function` is dynamically resolved and then called with the necessary parameters, demonstrating how the function facilitates flexible data storage operations.

#### Function: `wrap_for_foreach_batch_from_registry`

### Function Overview

The function `wrap_for_foreach_batch_from_registry` is designed to facilitate the integration of custom save functions with Spark's structured streaming API, specifically through the `foreachBatch` method. It dynamically retrieves and wraps a save function from a registry, making it compatible for stream processing in Spark.

### Purpose

The primary purpose of this function is to allow users to easily apply custom save operations on streaming data in Spark by utilizing pre-registered functions. This approach abstracts the complexity of handling dependencies and configurations necessary for the save operations during runtime.

### Inputs and Outputs

- **Inputs:**
  - `function_name` (str): The name of the save function to retrieve from the registry.
  - `spark` (SparkSession): The Spark session instance, which is crucial for executing operations within a Spark environment.
  - `connector` (BaseConnection): An instance of a connector class, which manages connections to external data sources or sinks.
  - `config` (dict): A dictionary containing configuration parameters that the save function might require.

- **Output:**
  - The function returns a callable (`handler`) that conforms to the signature required by Spark's `foreachBatch` method. This handler takes a DataFrame and a batch ID as arguments, facilitating batch-wise processing of streaming data.

### Key Logic and Design Choices

- **Function Discovery and Validation:** The function first retrieves a dictionary of available save functions from a registry using `discover_save_functions()`. It then checks if the specified `function_name` exists within this registry, raising a `ValueError` if not found.
  
- **Handler Creation:** A nested function `handler` is defined within `wrap_for_foreach_batch_from_registry`. This handler is tailored to log its invocation and forward the necessary parameters (dataframe, Spark session, connector, and configuration) to the retrieved save function.

- **Dependency Injection:** The handler injects dependencies (Spark session, connector, and configuration) into the save function. This design pattern simplifies the save function's interface and usage, as it abstracts the complexity of managing these dependencies outside of the function itself.

### Integration with Larger Applications

This function is particularly useful in applications involving real-time data processing where data needs to be saved or processed in batches as it streams in. By using `wrap_for_foreach_batch_from_registry`, developers can leverage a variety of pre-defined save functions without modifying the core logic of their streaming application. This promotes code reusability and modular architecture.

### Usage Example

An example usage is provided in the docstring, demonstrating how to create a Spark session, instantiate a connector, define configuration settings, and apply the handler to a streaming DataFrame. This example serves as a practical guide for developers to integrate the function into their Spark streaming workflows.

#### Function: `validate_save_function_signature`

### Function Overview

The `validate_save_function_signature` function is designed to ensure that any decorated function adheres to a specified set of parameter requirements. It acts as a decorator factory, generating a decorator that validates the presence of required parameters in the function it decorates.

### Inputs and Outputs

- **Input:**
  - `required_params` (set, optional): This is a set of strings that specifies the names of parameters which must be present in the decorated function. It defaults to a constant `REQUIRED_PARAMS` if not provided.

- **Output:**
  - The function returns a decorator (`Callable`). This decorator can be applied to any function to check if it includes all the parameters specified in `required_params`.

### Key Logic and Design Choices

- **Decorator Factory:** The function uses a nested function, `decorator`, which captures the `required_params` from the outer scope. This design allows the creation of customized decorators on the fly depending on the `required_params` provided.

- **Parameter Validation:** Inside the decorator, it retrieves the parameters of the function it decorates using `inspect.signature(func).parameters`. It then checks if all items in `required_params` are present in the function's parameters. If any are missing, it raises a `ValueError` with a detailed message about the missing parameters.

- **Error Handling:** The function proactively raises a `ValueError` if the decorated function does not meet the parameter requirements, making it a useful tool for enforcing function signatures in a codebase.

### Integration into Larger Applications

This function is particularly useful in applications where certain functions are expected to conform to a predefined interface or signature, especially in large codebases with multiple developers. It can be used to enforce consistency and correctness in API implementations, data processing functions, or any scenario where functions must adhere to a specific contractual interface. This helps in maintaining robustness and reducing runtime errors due to incorrect function signatures.

### Example Usage

```python
@validate_save_function_signature({'user_id', 'data'})
def process_data(user_id, data, timestamp=None):
    pass
```

In the example, the `process_data` function is decorated to ensure it includes the parameters 'user_id' and 'data'. If either parameter is missing, the program will raise a `ValueError`, thus enforcing a strict compliance to the required parameters.

---

### `delta_savers.py`

#### Function: `save_static_data_from_config`

### Function Overview

The `save_static_data_from_config` function is designed to save a Spark DataFrame to a persistent storage system based on a set of configuration parameters and using a specified connector. This function is part of a larger data processing application where Spark is used for handling big data workflows.

### Inputs

- **df (DataFrame)**: The Spark DataFrame that needs to be saved. This is the actual data that will be persisted.
- **config (dict)**: A configuration dictionary that includes all necessary settings for the connection and target options. Key elements in this dictionary include:
  - `connection_config`: Contains details like storage unit and object name.
  - `target_path_or_table`: Specifies the target database and table name for the data.
- **spark (SparkSession)**: The current Spark session instance, which is essential for interacting with Spark capabilities.
- **connector (BaseConnection)**: An instance of a connector class (like `AzureBlobConnection`), which abstracts the details of how data is actually saved to a storage system.

### Key Logic and Design Choices

1. **Metadata Addition**: The function enhances the DataFrame by appending ingestion metadata, which could include timestamps, source information, etc.
2. **Batch Options Resolution**: It dynamically resolves batch save options based on the provided configuration, which helps in optimizing the data saving process.
3. **Path Construction**: Constructs a file path using the connector based on the storage unit and object name provided in the configuration.
4. **Data Saving**: Utilizes a `SaverProvider` to delegate the actual data saving task. This abstraction allows for flexibility in changing or upgrading the underlying storage system without modifying the core data handling logic.
5. **Error Handling**: The function is designed to encapsulate and raise exceptions during the saving process, ensuring that errors are caught and handled appropriately.
6. **Data Registration**: After saving, it registers the DataFrame as a table in a Spark SQL database, facilitating easy access and query capabilities on the saved data.

### Outputs and Side Effects

- The function does not return any value but performs the side effect of saving data to a specified location and registering it as a table in a database.
- It logs significant steps and outcomes throughout the process, aiding in debugging and monitoring.

### Integration in Larger Application

This function is likely part of a data pipeline in a larger Spark-based application where data collected from various sources is processed, transformed, and stored in a structured format for analysis and reporting. The use of configuration dictionaries and connectors makes the function versatile and adaptable to different storage systems and environments. This design supports scalability and maintainability in complex data applications.

#### Function: `_table_exists`

### Function Overview

The function `_table_exists` is designed to check the existence of a specified table within a Spark session's catalog. It is a utility function that can be used in applications involving data processing and analysis with Apache Spark, ensuring that operations dependent on specific tables are only attempted if those tables are present.

### Inputs and Outputs

- **Inputs:**
  - `spark`: An instance of `SparkSession`, which is the entry point to programming Spark with the Dataset and DataFrame API. This session is used to execute SQL queries.
  - `table_name`: A string representing the name of the table whose existence needs to be verified.

- **Output:**
  - The function returns a boolean value:
    - `True` if the table exists within the Spark session's catalog.
    - `False` otherwise.

### Key Logic and Design Choices

- The function uses Spark SQL's `DESCRIBE TABLE` command to check for the table's existence. This command provides details about the table's schema and is a straightforward method to verify the presence of a table.
- Exception Handling:
  - The function handles an `AnalysisException`, which is raised by Spark SQL if the `DESCRIBE TABLE` command fails (commonly due to the table not existing). In such cases, the function catches the exception and returns `False`.
  - If the table exists and the `DESCRIBE TABLE` command executes without errors, the function returns `True`.

### Integration into Larger Applications

- This function is particularly useful in data pipelines or ETL (Extract, Transform, Load) processes where subsequent steps may depend on the presence of certain tables. By checking table existence, the application can avoid runtime errors and handle such scenarios gracefully, potentially logging missing tables or creating them if necessary.
- It can also be used in data validation scripts, setup checks before application startup, or during automated testing to ensure the environment is correctly configured.

### Usage Example

```python
spark = SparkSession.builder.appName("Example").getOrCreate()
exists = _table_exists(spark, "users")
if exists:
    print("Table 'users' exists.")
else:
    print("Table 'users' does not exist.")
```

This function is a fundamental utility in Spark-based applications, aiding in robust and error-free data handling by verifying necessary resources (tables) before performing operations.

#### Function: `save_or_merge_delta`

### Function Overview

The `save_or_merge_delta` function is designed to manage the storage of Spark DataFrames into Delta tables. It either saves a new Delta table or merges data into an existing Delta table based on the provided configuration. This function is integral for applications that require efficient data management and versioning in a Delta Lake environment.

### Inputs

- **df (DataFrame)**: The Spark DataFrame that needs to be saved or merged.
- **config (dict)**: A configuration dictionary that includes:
  - `connection_config`: Contains settings like storage unit and object name.
  - `merge_config`: Specifies keys for merging and columns that may change.
  - `target_path_or_table`: The Delta table's path or identifier.
- **spark (SparkSession)**: The active Spark session used to handle DataFrame operations.
- **connector (Connector)**: An object providing methods to interact with storage systems, crucial for accessing and manipulating the underlying data storage.

### Key Logic and Design Choices

1. **Configuration Handling**: The function uses the `config` dictionary to extract necessary parameters for connecting to storage, handling data, and determining the target Delta table.

2. **Spark Configuration**: It dynamically sets Spark configurations based on the storage options returned by the `connector`.

3. **Data Preparation**: Before saving or merging, the function deduplicates the DataFrame based on merge keys and adds necessary metadata columns.

4. **Delta Table Management**:
   - **Table Existence Check**: It checks if the target Delta table exists.
   - **New Table Creation**: If the table does not exist, it bootstraps a new Delta table using the `SaverProvider` and registers it in the catalog.
   - **Merging**: If the table exists, it performs a merge operation using `DeltaMergeManager`, which involves updating records based on specified keys and columns.

5. **Error Handling**: The function encapsulates its operations within a try-except block, raising a `RuntimeError` with detailed information if any step fails.

### Integration in Larger Applications

This function is typically used in data pipeline applications where data integrity and consistency are crucial, especially in environments utilizing Delta Lakes for large-scale data storage and processing. It can be part of ETL jobs, data ingestion pipelines, or data synchronization tasks, providing robust data handling capabilities with version control and full historical auditability.

### Usage Example

The provided example in the documentation demonstrates how to configure and use the `save_or_merge_delta` function within a Spark application, highlighting its role in managing data workflows efficiently in a cloud or distributed data environment.

---

### `function_registry.py`

#### Function: `set_registry_package`

### Function Overview

The function `set_registry_package` is designed to configure a global registry in a Python application, specifically targeting how and where save functions are located and utilized. This is particularly useful in applications that require dynamic handling of data serialization or persistence, allowing developers to specify custom logic for saving data.

### Purpose

The primary purpose of this function is to allow developers to override the default package used for searching save functions with a custom package. This is useful in scenarios where the default save mechanisms provided by a framework or library do not meet the specific needs of the application or when there is a need to enhance functionality with custom behavior.

### Behavior and Inputs

- **Input**: The function accepts a single argument, `package_name`, which is a string representing the dotted import path to the package containing the user-defined save functions.
- **Output**: There is no return value as the function's purpose is to update the global state.

### Key Logic and Design Choices

- **Global State Modification**: The function modifies two global variables:
  - `_USER_DEFINED_PACKAGE`: This is set to the `package_name` provided by the user, indicating where the application should now look for save functions.
  - `_FUNCTION_REGISTRY`: This is reset to `None`, effectively clearing any cached functions. This ensures that any subsequent operations that rely on save functions will use the updated settings, preventing the use of outdated or incorrect functions from a previously set package.

### Integration in Larger Applications

In a larger application or module, `set_registry_package` plays a critical role in customizing how data is saved, making it adaptable to different storage strategies or formats as required by the application's needs. It is especially relevant in modular applications where components might need different serialization strategies that are not covered by the main application logic.

### Usage Notes

- **Caution with Global State**: Since the function alters global state, it should be used with caution, particularly in environments where multiple threads might interact with the global state concurrently. Improper handling could lead to race conditions or inconsistent states across threads.
- **Example Usage**: An example provided in the documentation demonstrates how to use the function to set a custom package for save functions:
  ```python
  set_registry_package("my_project.custom_savers")
  ```

This function is a critical part of a flexible and dynamic system where customization of data handling is required, providing the necessary hooks for developers to tailor the application to specific requirements.

#### Function: `get_function_registry`

### Function Overview

The `get_function_registry` function is designed to manage and provide access to a centralized registry of callable functions. This registry serves as a lookup for function names mapped to their actual callable references, facilitating dynamic function invocation based on string identifiers.

### Purpose

The primary purpose of this function is to ensure that there is a single, consistent point of access for retrieving callable references by their function names, which is particularly useful in applications that require dynamic execution of various functions based on runtime decisions or configurations.

### Behavior and Logic

1. **Global Registry Check**: The function first checks if a global variable `_FUNCTION_REGISTRY` is already defined. This global variable acts as a cache to store the function registry.
   
2. **Initialization and Discovery**:
   - If `_FUNCTION_REGISTRY` is not initialized, the function determines which package to use for discovering functions. It chooses between a user-defined package (`_USER_DEFINED_PACKAGE`) and a default package (`_DEFAULT_REGISTRY_PACKAGE`), prioritizing the user-defined option if available.
   - It then populates `_FUNCTION_REGISTRY` by calling `discover_save_functions(package)`, a function presumably responsible for scanning the specified package and registering available functions.

3. **Return Value**: Once the registry is established (either retrieved from cache or newly created), it returns this dictionary. The dictionary's keys are the names of the functions, and the values are the corresponding callable objects.

### Inputs/Outputs

- **Inputs**: There are no direct inputs (parameters) to the function.
- **Outputs**: Returns a dictionary where each key is a string representing a function name, and each value is the callable object associated with that name.

### Error Handling

- **ImportError**: The function raises an `ImportError` if there is a failure in the function discovery process, which could be due to missing packages or an incorrect setup.

### Integration in Larger Applications

In a larger application, `get_function_registry` can be used to dynamically access and execute functions based on configurable or runtime parameters. This is particularly useful in modular applications where functionalities might be spread across different packages or modules, and there is a need to invoke them without hard-coding specific function calls. This approach enhances the flexibility and extensibility of the application.

### Example Usage

```python
registry = get_function_registry()
registry["save_or_merge_delta"](df, config, spark, connector)
```

In this example, the function `save_or_merge_delta` is retrieved from the registry and executed with the provided arguments, demonstrating how functions can be dynamically called through their names.

#### Function: `discover_save_functions`

### Function Overview

The `discover_save_functions` function is designed to dynamically discover and register Python functions that are specifically prefixed with 'save_' or 'merge_' within a given package. This utility is particularly useful for applications that need to dynamically handle data persistence or merging operations, allowing for a modular and scalable approach by automatically detecting and incorporating such functions from specified packages.

### Inputs and Outputs

- **Input:**
  - `package_name` (str): The name of the package from which the function attempts to discover and register functions. It defaults to a value stored in `_DEFAULT_REGISTRY_PACKAGE` if not explicitly provided.

- **Output:**
  - Returns a dictionary (`Dict[str, Callable]`) where the keys are the names of the discovered functions and the values are the callable functions themselves.

### Key Logic and Design Choices

1. **Dynamic Importing:** The function uses Python's `importlib` to dynamically import modules from the specified package. This approach allows for flexibility and extensibility in applications, as new functions can be added to the package without modifying the core discovery mechanism.

2. **Function Filtering:** It filters functions by their names, specifically looking for those starting with 'save_' or 'merge_'. This naming convention is crucial as it defines the criteria for function discovery and registration, ensuring that only relevant functions are included in the registry.

3. **Error Handling:** The function is designed to raise specific exceptions if issues occur during the import process:
   - `ImportError` if the package cannot be imported, which could be due to issues like incorrect package name or path.
   - `AttributeError` if the package lacks a `__path__` attribute, indicating it is not a standard Python package.

### Integration into Larger Applications

The `discover_save_functions` function is ideal for applications that require modular handling of data saving or merging tasks. By centralizing the discovery and registration of these functions, the application can maintain cleaner code and improve maintainability. Developers can add new 'save_' or 'merge_' functions to the specified package as the application grows, and these will be automatically available for use throughout the application without additional integration efforts.

### Usage Example

```python
discovered_functions = discover_save_functions('my_app.data_handlers')
discovered_functions['save_user_data'](user_data)
```

In this example, `discover_save_functions` is used to find all relevant functions in the 'my_app.data_handlers' package. The function 'save_user_data' is then accessed from the returned dictionary and called with `user_data` as an argument. This demonstrates how the function facilitates easy access to dynamically discovered functionality within a specified package.

---

### `spark_data_saver_from_config.py`

#### Class: `SparkDataSaverFromConfig`

## Class Documentation: SparkDataSaverFromConfig

### Overview
The `SparkDataSaverFromConfig` class is designed to facilitate the saving of Spark DataFrames to various storage systems, handling both batch and streaming data types. It leverages a configuration-driven approach to determine the appropriate saving mechanisms and paths, integrating with external systems through a connector object.

### Responsibilities
- **Initialization**: Set up the necessary configurations and paths for data handling using inputs such as a Spark session, a configuration dictionary, and a connector object.
- **Data Saving**: Determine whether the DataFrame is streaming or batch and invoke the corresponding private method to handle the save operation.

### Inputs and Outputs
- **Inputs**:
  - `spark`: An active `SparkSession` instance.
  - `config`: A dictionary containing parameters such as connection configurations and target paths or tables.
  - `connector`: An object responsible for connecting to data sources and handling file paths.
  - `df`: A Spark DataFrame, either streaming or batch, that needs to be saved.
- **Outputs**:
  - The class does not return data but performs operations that result in data being saved to specified locations.

### Key Logic and Design Choices
- **Configuration-Driven Logic**: The class uses a configuration dictionary to dynamically determine save methods and paths, allowing for flexible integration with different storage systems and formats.
- **Dual Handling for Streaming and Batch**: Separate private methods (`_save_batch` and `_save_stream`) are used to handle batch and streaming DataFrames, encapsulating the specific logic required for each type.
- **Error Handling**: The class includes error handling to manage common issues such as missing configuration keys or methods not found in the function registry.
- **Integration with External Systems**: Through the use of a connector object, the class can interact with external systems for path resolution and data saving, abstracting these details from the main saving logic.

### Integration in Larger Applications
This class is designed to be a component in larger data processing applications where Spark is used for data manipulation and analysis. It can be integrated into data pipelines for ETL processes, real-time data processing systems, or any application requiring robust data storage solutions with Spark. The use of a configuration dictionary and a connector object allows it to be adapted easily to different environments and storage systems.

### Usage Example
```python
spark = SparkSession.builder.appName("ExampleApp").getOrCreate()
config = {
    "connection_config": {
        "storage_unit": "my_storage",
        "object_name": "my_data_object"
    },
    "target_path_or_table": "database.table_name"
}
connector = SomeConnector(config)
saver = SparkDataSaverFromConfig(spark, config, connector)
df = spark.read.format("csv").option("header", "true").load("path/to/csv")
saver.save(df)
```

This class is a critical component for applications that require a flexible, configurable approach to saving data within Spark-based data workflows.

---
