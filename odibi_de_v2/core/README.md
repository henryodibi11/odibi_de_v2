# Module Overview

### `saver_factory.py`

#### Class: `SaverFactory`

### Class Overview: SaverFactory

The `SaverFactory` class is an abstract base class designed to facilitate the creation of data saver instances for different file formats. It serves as a template for concrete factory classes that implement specific methods to handle various data formats such as CSV, JSON, Avro, and Parquet. This class is part of a larger application or module that deals with data storage and serialization.

### Purpose

The primary purpose of the `SaverFactory` is to define a standard interface (`csv_saver`, `json_saver`, `avro_saver`, `parquet_saver`) for creating data saver instances. Each method in the factory is responsible for instantiating a saver object that can handle the serialization and storage of data in a specific format. This design promotes a clean separation of concerns and enhances modularity in applications that require support for multiple data storage formats.

### Methods and Their Responsibilities

1. **csv_saver(file_path: str, **kwargs) -> DataSaver**:
   - Creates an instance capable of saving data in CSV format.
   - `file_path`: Specifies the location where the CSV file will be saved.
   - `**kwargs`: Accepts additional parameters such as storage options or session configurations, which are specific to the concrete implementation.

2. **json_saver(file_path: str, **kwargs) -> DataSaver**:
   - Generates a saver instance for handling JSON data.
   - `file_path`: Designates the path for the JSON file.
   - `**kwargs`: Allows for extra arguments that might be necessary for specific environments or settings.

3. **avro_saver(file_path: str, **kwargs) -> DataSaver**:
   - Produces a saver instance for Avro format.
   - `file_path`: Indicates where the Avro file should be stored.
   - `**kwargs`: Supports additional options pertinent to the implementation, enhancing flexibility.

4. **parquet_saver(file_path: str, **kwargs) -> DataSaver**:
   - Creates a saver instance for Parquet files.
   - `file_path`: Specifies the destination for the Parquet file.
   - `**kwargs`: Captures optional parameters that cater to specific requirements or configurations.

### Usage Example

An example usage scenario provided in the documentation illustrates how a concrete implementation of this factory (e.g., `PandasSaverFactory`) can be utilized:

```python
from my_module.saver_factory import PandasSaverFactory
factory = PandasSaverFactory()
saver = factory.csv_saver("data.csv", storage_options={"account_name": "xyz", "account_key": "xyx"})
df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
saver.save_data(df, index=False)
```

This example demonstrates creating a CSV saver using the factory, configuring it with specific storage options, and using it to save a pandas DataFrame.

### Key Design Choices

- **Abstraction and Flexibility**: By using an abstract base class, `SaverFactory` allows different implementations to be swapped seamlessly, providing flexibility across various storage configurations and environments.
- **Extensibility**: New methods for additional file formats can be easily added to the factory, making it extensible.
- **Separation of Concerns**: Each saver method is responsible for one specific data format, adhering to the single responsibility principle.

### Integration into Larger Applications

`SaverFactory` fits into a larger data processing or ETL (Extract, Transform, Load) application where handling multiple formats of data efficiently and flexibly is crucial. It abstracts the complexities of data saving behind a simple interface, allowing the rest of the application to operate independently of the specific data storage details.

---

### `reader_provider.py`

#### Class: `ReaderProvider`

### Class Overview: `ReaderProvider`

The `ReaderProvider` class serves as an abstract base class designed to standardize the creation of data readers for various data types and storage systems in cloud environments. This class is part of a larger data handling framework that facilitates reading data from different storage solutions like Azure Blob Storage or AWS S3 using a consistent interface.

### Key Attributes:
- **factory** (`ReaderFactory`): An instance of `ReaderFactory` that is responsible for creating specific data reader objects.
- **data_type** (`DataType`): Specifies the type of data to be read (e.g., CSV, JSON).
- **connector** (`BaseConnection`): An optional attribute that represents a connection instance to a cloud service, providing necessary credentials and endpoints.

### Key Method: `create_reader`
- **Purpose**: To instantiate and return a data reader object configured for a specific data type and cloud storage location.
- **Inputs**:
  - **storage_unit** (`str`): Identifies the container or folder in the cloud storage where the data is located.
  - **object_name** (`str`): Specifies the exact path to the data file or object within the storage unit.
- **Output**:
  - **DataReader**: Returns an instance that implements the `DataReader` interface, capable of reading data from the specified location.

### Design Choices:
- **Abstract Base Class**: By using an abstract base class, `ReaderProvider` ensures that all subclasses implement the `create_reader` method, providing a consistent interface for data reading across different implementations.
- **Decoupling**: The separation of data reading logic into factory, data type, and connector allows for flexible and interchangeable components, making the system adaptable to various data formats and cloud services without modifying the core logic.
- **Dynamic Path Construction**: The method constructs the full path to the data dynamically, utilizing the connector's logic to prepend appropriate prefixes like `abfss://` for Azure or `s3://` for AWS, ensuring compatibility with different cloud storage APIs.

### Integration in Applications:
The `ReaderProvider` class is typically used in data processing or ETL (Extract, Transform, Load) applications where data needs to be read from various cloud sources. Developers can extend this class to implement specific readers for different data formats or cloud services by providing a custom `ReaderFactory` and `BaseConnection`. This setup promotes code reuse and simplifies the integration of new data sources or formats into existing systems.

### Example Usage:
```python
factory = PandasReaderFactory()
connector = BaseConnection("account_name", "account_key")
provider = PandasReaderProvider(factory, DataType.CSV, connector)
reader = provider.create_reader("my_storage_container", "data/object.csv")
```
In this example, a `PandasReaderProvider` is instantiated using a CSV data type and a generic cloud connection. It then creates a reader that is ready to load CSV data from the specified Azure Blob Storage container and object path.

---

### `reader_factory.py`

#### Class: `ReaderFactory`

### Overview

The `ReaderFactory` class serves as an abstract base class (ABC) designed to standardize the creation of data reader instances for various file formats. It is part of a larger application or module that handles data ingestion from different sources and formats, such as CSV, JSON, Avro, and Parquet files.

### Purpose

The primary purpose of the `ReaderFactory` is to define a common interface for creating data readers. This ensures that different parts of an application can rely on a consistent method of data reading, regardless of the file format. The factory pattern used here allows for easy extension and integration of new file formats without modifying existing codebases significantly.

### Methods and Behavior

`ReaderFactory` includes the following abstract methods, each corresponding to a specific file format:

1. **csv_reader(file_path: str, **kwargs) -> DataReader**:
   - Creates a reader for CSV files.
   - `file_path`: Path to the CSV file.
   - `**kwargs`: Optional arguments specific to the implementation, such as storage options or session configurations.

2. **json_reader(file_path: str, **kwargs) -> DataReader**:
   - Creates a reader for JSON files.
   - Similar parameters and optional arguments as the CSV reader.

3. **avro_reader(file_path: str, **kwargs) -> DataReader**:
   - Creates a reader for Avro files.
   - Parameters and optional arguments are consistent with other methods.

4. **parquet_reader(file_path: str, **kwargs) -> DataReader**:
   - Creates a reader for Parquet files.
   - Follows the same parameter structure, allowing for specific configurations through `**kwargs`.

### Key Design Choices

- **Abstract Base Class**: By using an ABC, `ReaderFactory` ensures that any subclass must implement the specified methods, providing a consistent interface for reader creation.
- **Method Signatures**: Each method accepts a file path and an arbitrary set of keyword arguments (`**kwargs`), offering flexibility in passing additional parameters needed by specific reader implementations.
- **Flexibility and Extensibility**: The design allows easy addition of new methods for different file formats without impacting existing implementations.

### Integration in Applications

In a larger application, instances of concrete subclasses of `ReaderFactory` would be used to instantiate appropriate data readers based on the file format. This could be part of a data processing pipeline where data from various sources needs to be ingested, processed, and possibly stored or analyzed. The use of a factory pattern here simplifies the management of these diverse data sources by centralizing reader creation in one component, promoting code reusability and maintainability.

### Example Usage

```python
from my_module.reader_factory import PandasReaderFactory

factory = PandasReaderFactory()
reader = factory.csv_reader("data.csv", storage_options={"account_name": "xyz", "account_key": "xyx"})
df = reader.read_data()
```

This example demonstrates how to use a concrete implementation of `ReaderFactory` to read CSV data into a pandas DataFrame, highlighting the practical application of the class in a data processing context.

---

### `saver.py`

#### Class: `DataSaver`

### Class Documentation: DataSaver

#### Overview
The `DataSaver` class serves as an abstract base class (ABC) designed to outline a standardized interface for saving data to various file formats. This class is intended to be subclassed by concrete implementations that specify how data should be saved to specific file types, such as CSV, JSON, or XML.

#### Attributes
- **file_path (str)**: Specifies the path to the file where the data will be saved. This attribute must be set by subclasses.

#### Methods
- **save_data(data, file_path, **kwargs)**: This is an abstract method that must be implemented by subclasses. The method is responsible for saving a dataset to a file located at `file_path`. The `data` parameter is the dataset to be saved, which could be in any format such as a pandas DataFrame, depending on the implementation. The method can also accept additional keyword arguments (`**kwargs`) which are specific to the implementation, such as options to include headers or to control indexing in a CSV file.

#### Usage
To use the `DataSaver` class, one must create a subclass that implements the `save_data` method. This method should handle the logic for writing the `data` to a file in the desired format. The subclass can then be instantiated with a specific `file_path`, and the `save_data` method can be called with the dataset and any additional parameters.

#### Example
Here's a brief example demonstrating how to subclass `DataSaver` to create a simple CSV data saver:

```python
import pandas as pd

class CSVSaver(DataSaver):
    def save_data(self, data, **kwargs):
        data.to_csv(self.file_path, **kwargs)

# Usage
saver = CSVSaver("example.csv")
df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
saver.save_data(df, index=False)
```

#### Design Choices
- **Abstract Base Class**: Using an ABC ensures that all subclasses implement the necessary `save_data` method, providing a consistent interface for data saving across different formats.
- **Flexibility through kwargs**: The use of `**kwargs` allows flexibility in how data is saved, enabling users to pass format-specific options directly through to the underlying saving mechanism without modifying the interface.

#### Integration
The `DataSaver` class is designed to be a part of larger applications or data processing pipelines where data needs to be exported to various formats. It abstracts the file saving mechanism, allowing other parts of the application to save data without concerning themselves with the details of how the data is written to file. This promotes code reusability and separation of concerns.

---

### `saver_provider.py`

#### Class: `SaverProvider`

### Class Overview: SaverProvider

`SaverProvider` is an abstract base class designed to standardize the creation of data savers which are specific to both the type of data and the cloud storage service being used. This class is part of a larger data handling framework where data savers are utilized to write data to various storage solutions in a consistent and reliable manner.

### Attributes:
- **factory** (`SaverFactory`): An instance of `SaverFactory` that is responsible for creating data saver objects.
- **data_type** (`DataType`): Specifies the type of data (e.g., CSV, JSON) that the saver will handle.
- **connector** (`BaseConnection`): An instance representing the connection to a cloud storage service, which handles specifics of the cloud storage API and credentials.

### Method: create_saver
- **Purpose**: To abstract the process of creating a data saver that is tailored to both the data type and the specific storage backend.
- **Inputs**:
  - **storage_unit** (`str`): Represents the container or folder in the cloud storage where the data will be saved. Examples include 'ADLS Gen 2' for Azure or 'S3' for AWS.
  - **object_name** (`str`): Specifies the exact path or name of the object where data will be stored within the storage unit.
- **Output**:
  - **DataSaver**: Returns an instance that conforms to the `DataSaver` interface. This instance is ready to save data to the specified location using the methods defined in the data saver class.

### Key Logic and Design Choices:
- **Abstraction and Flexibility**: By using an abstract base class, `SaverProvider` allows for flexibility in implementing various types of data savers for different data types and storage backends without altering the interface. This makes it easier to extend functionality to new data types or cloud services.
- **Factory Pattern**: The use of a `SaverFactory` within the class emphasizes the factory design pattern, centralizing the creation logic of data saver instances and promoting code reusability and separation of concerns.
- **Cloud-Agnostic Design**: The inclusion of a generic `BaseConnection` allows the class to be agnostic of the underlying cloud service provider, thereby supporting a wide range of cloud storage solutions.

### Integration in a Larger Application:
`SaverProvider` fits into a data processing and storage module where applications need to save various types of data to different cloud storage providers. It acts as a bridge between the application's data saving logic and the specific requirements and capabilities of the cloud storage backend. This setup is particularly useful in environments where data is sourced or consumed in diverse formats and storage solutions, requiring a robust and adaptable saving mechanism.

### Usage Example:
The provided example demonstrates how to instantiate a specific `SaverProvider` (e.g., `PandasSaverProvider` for handling pandas DataFrames), configure it with a factory, data type, and connector, and then use it to save a DataFrame to Azure Blob Storage. This showcases the practical application of the class in a real-world scenario, emphasizing its role in simplifying cloud data storage operations.

---

### `enums.py`

#### Class: `DataType`

### Class Documentation: `DataType`

#### Overview
The `DataType` class is an enumeration defined within the `odibi_de` framework, which specifies the various data formats supported by the system. This class is essential for managing and validating the types of data formats that can be processed or handled within the framework.

#### Attributes
- `CSV`: Represents Comma-Separated Values (CSV) file format.
- `JSON`: Represents JavaScript Object Notation (JSON) file format.
- `AVRO`: Represents AVRO file format, typically used for serializing data in a compact binary format.
- `PARQUET`: Represents Parquet file format, an optimized columnar storage format.
- `DELTA`: Represents Delta Lake format, an open-source storage layer that brings ACID transactions to Apache Spark™ and big data workloads.
- `CLOUDFILES`: Represents a generic cloud file storage format.
- `SQL`: Represents SQL data format, typically used for interacting with relational databases.
- `API`: Represents data obtained or sent via APIs, usually in JSON or XML format.

#### Usage
This class is primarily used to:
- Define and enforce consistency in data format specifications across different components of the `odibi_de` framework.
- Facilitate the handling and processing of different data formats by providing a clear and standardized set of format identifiers.
- Serve as a reference point for functions and methods that require data format specification, ensuring that they operate with supported and expected data types.

#### Integration
`DataType` fits into a larger application or module by:
- Being a central component in data ingestion, processing, and output modules which need to handle various data formats.
- Providing a robust and extendable way to manage data formats, which can be easily updated and maintained as new data formats are adopted or deprecated in the framework.

#### Example
```python
# Example of using DataType
def load_data(file_path, data_type):
    if data_type == DataType.CSV.value:
        return load_csv(file_path)
    elif data_type == DataType.JSON.value:
        return load_json(file_path)
    # Additional conditions for other data types
```

This class does not directly interact with data but acts as a blueprint for other parts of the system to ensure compatibility and correctness in data format handling.

#### Class: `CloudService`

### Class: CloudService

#### Overview
The `CloudService` class is an enumeration defined within the `odibi_de` framework that specifies a set of supported cloud service providers. This class facilitates the management and identification of different cloud platforms in a standardized manner throughout the application.

#### Attributes
- `AZURE`: Represents Microsoft Azure cloud services. It is stored as the string `"azure"`.

#### Usage
Instances of `CloudService` are typically used to:
- Clearly define and enforce the use of supported cloud service providers within the framework.
- Serve as a standardized reference point for cloud service identifiers, which helps in reducing the likelihood of errors due to typos or inconsistent naming conventions.

#### Example
To retrieve the value associated with Microsoft Azure, you can access the enumeration as follows:
```python
>>> CloudService.AZURE.value
'azure'
```

#### Integration
In a larger application or module, `CloudService` can be used to configure services, manage deployments, or handle cloud-specific operations by providing a clear and consistent set of identifiers for supported cloud platforms. This can be particularly useful in scenarios where the application needs to interact with multiple cloud environments, ensuring that each service is correctly identified and handled according to its specific requirements.

#### Design Choices
- The use of an `Enum` class provides a robust and error-resistant way to handle predefined constants. This choice ensures that the cloud service identifiers are immutable and can be safely used throughout the application without the risk of runtime modifications.
- The class currently supports only Microsoft Azure but is designed to be easily extendable to include other providers such as AWS or GCP by simply adding additional enumeration members.

This design promotes maintainability and scalability in applications that are intended to operate across multiple cloud environments.

#### Class: `Framework`

### Class Overview

The `Framework` class is an enumeration (Enum) defined in Python to represent various computational frameworks supported by the `odibi_de` framework. This class is used to standardize the specification of frameworks across the application, ensuring consistent reference and comparison.

### Attributes

- `PANDAS`: Represents the Pandas library, a popular tool for data analysis in Python.
- `SPARK`: Represents Apache Spark, a powerful distributed data processing engine.
- `LOCAL`: Represents local execution, where computations are performed without the use of a distributed framework.

### Usage and Integration

This enum class is particularly useful in a larger application where different parts of the codebase might need to behave differently depending on the underlying computational framework being used. By using the `Framework` enum, developers can write cleaner, more maintainable code that conditionally executes different logic based on the framework specified.

For example, a data processing function in the `odibi_de` framework might check the `Framework` enum to decide whether to use Pandas functions or Spark methods, or simply execute Python code locally.

### Key Design Choices

- **Enum Usage**: The use of an enumeration for framework representation is a deliberate choice to leverage Python's `Enum` capabilities for clear, concise, and error-resistant code. Enums provide a way to group constant and related values under a single type, enhancing code readability and maintainability.
- **String Values**: Each enum member holds a string value (e.g., `"pandas"`, `"spark"`, `"local"`), which can be easily used in the application, for instance in configuration files or environment settings, where these string literals might be directly used.

### Example

```python
# Example of using Framework enum in a function
def process_data(framework):
    if framework == Framework.PANDAS:
        # Use Pandas to process data
        pass
    elif framework == Framework.SPARK:
        # Use Spark to process data
        pass
    elif framework == Framework.LOCAL:
        # Process data locally without any external libraries
        pass

# Example of retrieving the value of an enum member
print(Framework.PANDAS.value)  # Output: 'pandas'
```

### Conclusion

The `Framework` enum is a critical component of the `odibi_de` framework, providing a standardized and reliable method for specifying and checking computational frameworks across different parts of the application. This approach not only aids in maintaining consistency but also enhances the flexibility and scalability of the application by abstracting the framework specifics into a well-defined enum class.

#### Class: `ValidationType`

### Overview

The `ValidationType` class, defined as an enumeration (Enum), specifies a set of constants that represent different types of validations that can be applied to datasets within a data processing or data validation application. Each member of this enum represents a specific type of validation check that can be performed on data, ensuring data integrity, correctness, and adherence to expected formats or constraints.

### Purpose

The primary purpose of the `ValidationType` class is to provide a standardized set of validation identifiers that can be used throughout an application to refer to specific data validation rules. This helps in maintaining consistency, improving code readability, and facilitating easier implementation of validation logic.

### Attributes

Each attribute in the `ValidationType` enum corresponds to a specific type of data validation:

- **MISSING_COLUMNS**: Checks for missing columns in a dataset.
- **EXTRA_COLUMNS**: Identifies any unexpected columns present in the dataset.
- **DATA_TYPE**: Validates the data types of columns.
- **NULL_VALUES**: Ensures certain columns do not contain null values.
- **VALUE_RANGE**: Verifies that values in a column fall within a specified range.
- **UNIQUENESS**: Ensures all values in a column are unique.
- **ROW_COUNT**: Validates the total number of rows in the dataset.
- **NON_EMPTY**: Checks that the dataset is not empty.
- **VALUE**: Validates specific values within columns.
- **REFERENTIAL_INTEGRITY**: Ensures values in one column match values in a reference dataset.
- **REGEX**: Validates column values against a specified regular expression pattern.
- **DUPLICATE_ROWS**: Identifies and flags duplicate rows in the dataset.
- **COLUMN_DEPENDENCY**: Checks dependencies between columns to ensure relational integrity.
- **SCHEMA**: Validates the overall schema of the dataset.
- **COLUMN_TYPE**: Validates the types of columns in the dataset.

### Usage in Applications

In a larger application, the `ValidationType` enum can be used in various components where data needs to be validated according to specific rules. For example, a data ingestion pipeline might use these validation types to ensure that incoming data conforms to expected formats before processing. Similarly, a data quality monitoring system might use these types to regularly check the quality of data in a database.

### Design Choices

Using an enumeration for validation types offers several benefits:
- **Consistency**: Ensures that validation rules are referred to by the same name throughout the application, reducing the risk of errors due to typos or mismatched strings.
- **Maintainability**: Centralizes the definition of what types of validations can be performed, making the system easier to update and maintain.
- **Extensibility**: New validation types can be easily added to the `ValidationType` enum as new requirements arise.

### Conclusion

The `ValidationType` class is a foundational component in applications requiring robust data validation mechanisms. By defining a clear and extensible set of validation types, it aids in building more reliable and maintainable data processing systems.

#### Class: `TransformerType`

### Overview

The `TransformerType` class is an enumeration defined in Python using the `Enum` base class. It specifies a set of transformation operations that can be applied to data structures, typically pandas DataFrames, within the context of ETL (Extract, Transform, Load) processes, data cleaning, or data pipeline execution tasks.

### Purpose

The primary purpose of the `TransformerType` class is to provide a standardized set of identifiers for various data transformation operations. These identifiers can be used to reference specific transformation actions in a clear and consistent manner across different parts of an application, enhancing code readability and maintainability.

### Transformations Supported

Each member of the `TransformerType` enum represents a different type of data transformation operation:

- **Column Operations:**
  - `COLUMN_ADDER`: Adds new columns.
  - `COLUMN_ADDER_STATIC`: Adds new columns with predefined static values.
  - `COLUMN_DROPPER`: Removes specified columns.
  - `COLUMN_MERGER`: Combines multiple columns into a single column.
  - `COLUMN_NAME_PREFIX_SUFFIX`: Modifies column names by adding prefixes or suffixes.
  - `COLUMN_NAME_REGEX_RENAMER`: Renames columns based on regular expressions.
  - `COLUMN_NAME_STANDARDIZER`: Standardizes the naming of columns.
  - `COLUMN_PATTERN_DROPPER`: Removes columns that match a specific pattern.
  - `COLUMN_RENAMER`: Renames columns based on a provided mapping.
  - `COLUMN_REORDERER`: Changes the order of columns.
  - `COLUMN_SPLITTER`: Splits a single column into multiple columns.
  - `COLUMN_TYPE_DROPPER`: Removes columns based on their data type.

- **Row Operations:**
  - `ROW_AGGREGATOR`: Combines multiple rows based on certain criteria.
  - `ROW_APPENDER`: Adds new rows to the DataFrame.
  - `ROW_DEDUPLICATOR`: Removes duplicate rows.
  - `ROW_DUPLICATOR`: Creates duplicates of certain rows.
  - `ROW_EXPANDER`: Generates multiple rows from a single row based on specific conditions.
  - `ROW_FILTER`: Filters rows based on specified conditions.
  - `ROW_SAMPLER`: Randomly samples a subset of rows.
  - `ROW_SORTER`: Sorts rows based on one or more columns.
  - `ROW_SPLITTER`: Divides rows into separate groups.

- **Value Operations:**
  - `VALUE_REPLACER`: Replaces specific values within the DataFrame.

- **Specialized Operations:**
  - `EMPTY_COLUMN_DROPPER`: Removes columns that are entirely empty.
  - `NULL_RATIO_COLUMN_DROPPER`: Drops columns exceeding a certain threshold of null values.

### Usage in Applications

In a larger application or module, `TransformerType` can be used to dynamically select and apply transformations based on configuration files or user inputs. This approach allows developers to build flexible and configurable data processing pipelines where the specific transformations applied can be easily adjusted without changing the underlying codebase.

### Example Usage

```python
def apply_transformation(df, transformation_type):
    if transformation_type == TransformerType.COLUMN_ADDER:
        # Logic to add columns
        pass
    elif transformation_type == TransformerType.ROW_FILTER:
        # Logic to filter rows
        pass
    # Additional conditions for other transformations
```

This class is crucial for applications that require dynamic and configurable data transformations, ensuring that the code remains clean and maintainable while supporting a wide range of data processing operations.

#### Class: `ErrorType`

### Overview of the `ErrorType` Enum Class

The `ErrorType` class is an enumeration defined within the Python `Enum` module, specifically tailored for use in the `odibi_de` framework. This class provides a standardized set of error type labels that are intended to be used across the entire framework to ensure consistent logging and exception handling.

### Purpose

The primary purpose of the `ErrorType` enum is to categorize different types of errors that can occur within the framework. By standardizing error labels, the framework enhances the maintainability and readability of error handling and logging code. This standardization helps in debugging and error tracking by providing clear, predefined categories for various error conditions.

### Attributes

Each attribute in the `ErrorType` enum represents a specific type of error:
- `VALUE_ERROR`: Used for errors related to invalid data values.
- `FILE_NOT_FOUND`: Indicates that a required file or resource is missing.
- `NOT_IMPLEMENTED`: Used when a feature is recognized but not yet supported.
- `AUTH_ERROR`: Represents errors related to authentication or permission issues.
- `CONNECTION_ERROR`: Indicates failures in connecting to external systems.
- `CONFIG_ERROR`: Used for issues related to configuration problems.
- `VALIDATION_ERROR`: Indicates failures in data or schema validation.
- `TYPE_ERROR`: Represents errors due to mismatched or unexpected data types.
- `UNKNOWN`: A fallback for errors that do not fit into other predefined categories.
- `NO_ERROR`: Indicates the absence of an error.
- `SCHEMA_ERROR`: Specifically used for schema validation failures.
- `Runtime_Error`: Indicates a generic runtime error.
- `READ_ERROR`: Used for errors encountered during data reading processes.
- `SAVE_ERROR`: Represents errors that occur during data saving processes.

### Usage

In the context of the `odibi_de` framework, these error types are used with a function named `format_error`. This function likely expects an `ErrorType` value to format error messages consistently, ensuring that all components of the framework handle and log errors in a uniform way.

### Integration

The `ErrorType` class is a critical component for error management in the `odibi_de` framework. It is used throughout the framework wherever error handling is required. The use of an enum class for error types as opposed to plain strings ensures that the code remains clean, less error-prone, and easier to maintain.

### Example

Here is how you might use the `ErrorType` enum in practice:

```python
if not user.is_authenticated:
    error_message = format_error(ErrorType.AUTH_ERROR, "User authentication failed.")
    log_error(error_message)
```

In this example, `format_error` would format an appropriate error message using the `AUTH_ERROR` type, and `log_error` would log this message to a logging system.

### Conclusion

The `ErrorType` enum is a foundational component designed to streamline error handling in the `odibi_de` framework, promoting a consistent approach to logging and managing errors across various parts of the application.

---

### `reader.py`

#### Class: `DataReader`

### Class Overview

The `DataReader` class serves as an abstract base class (ABC) designed for reading data from various file formats. It is intended to be subclassed by concrete implementations that specify how data is read from specific file types, such as CSV, JSON, or Excel files.

### Attributes

- `file_path (str)`: The path to the file from which data is to be read. This attribute should be initialized in the subclass.

### Methods

#### `read_data(**kwargs) -> Any`
This is an abstract method that must be implemented by subclasses. It is responsible for reading the entire dataset from the file. The method accepts variable keyword arguments (`**kwargs`) which can be used to pass additional parameters specific to different file formats, such as delimiters or encoding types.

**Returns**: The method returns a dataset object, the type of which depends on the implementation in the subclass.

#### `read_sample_data(n: int = 100, **kwargs) -> Any`
This method provides functionality to read a sample of the dataset, typically used for schema inference or initial data exploration. It defaults to reading the first 100 rows but can be adjusted by passing a different value for `n`.

- `n (int)`: Number of rows to sample from the dataset. Defaults to 100.
- `**kwargs`: Additional keyword arguments that are passed directly to the `read_data` method.

**Returns**: A sampled dataset object as defined by the concrete implementation.

### Logging
The method includes logging at the start and end, indicating the initiation and successful completion of the data reading process. This is useful for debugging and monitoring the data ingestion process.

### Example Usage
```python
class CSVReader(DataReader):
    def read_data(self, **kwargs):
        return pd.read_csv(self.file_path, **kwargs)

reader = CSVReader("example.csv")
df = reader.read_sample_data(n=10)
```

In this example, `CSVReader` is a concrete implementation of `DataReader` for reading CSV files. It utilizes the `pandas` library to read data and demonstrates how to use the `read_sample_data` method to read a sample of the data.

### Integration in Larger Applications
The `DataReader` class is designed to be a flexible component in data ingestion or ETL (Extract, Transform, Load) pipelines. By standardizing the interface for data reading, it allows developers to easily swap out or add new data source formats with minimal changes to the overall codebase. This design promotes modularity and scalability in applications that require data processing from diverse sources.

---

### `base_connector.py`

#### Class: `BaseConnection`

### Overview of `BaseConnection` Class

`BaseConnection` is an abstract base class designed to standardize interactions with various storage systems such as Azure Blob Storage, Amazon S3, Google Cloud Storage, and local file systems. It serves as a foundational component in applications that require a uniform way to access data across different storage backends.

### Purpose

The primary purpose of the `BaseConnection` class is to provide a consistent interface for:
1. Resolving file paths to data sources.
2. Retrieving storage-specific configuration and authentication options.

This abstraction allows other components of the application, particularly data readers and savers, to interact with any supported storage system without needing to know the specifics of the underlying storage mechanism.

### Key Methods

The class defines two abstract methods that must be implemented by any subclass:

1. **`get_file_path(...)`**:
   - **Purpose**: Constructs a fully qualified file path using the provided components such as container name, path prefix, and object name.
   - **Inputs**:
     - `container`: The top-level storage container or bucket name.
     - `path_prefix`: The folder or directory path within the container.
     - `object_name`: The actual file name or object identifier.
   - **Output**: Returns a string representing the fully resolved path appropriate for the configured storage engine (e.g., `abfss://...`, `s3://...`).

2. **`get_storage_options()`**:
   - **Purpose**: Provides necessary credentials or configuration options required to access the storage.
   - **Output**: Returns a dictionary containing authentication and configuration details, or `None` if no such details are required.

### Design Choices

- The class uses the `ABC` module to enforce the implementation of the abstract methods in any subclass, ensuring that all concrete connection classes provide consistent functionality.
- Optional parameters in `get_file_path` allow flexibility in specifying how paths are constructed, catering to different needs and storage systems.

### Integration into Larger Applications

In a broader application context, `BaseConnection` acts as a contract for data handling classes. By adhering to this interface, data readers and savers can remain agnostic of the specifics of the storage systems, promoting modularity and reusability. For instance, a data processing framework can use this interface to interact seamlessly with data stored in various locations, simply by switching out the connection object.

### Example Usage

```python
connector = AzureBlobConnection(..., framework=Framework.SPARK)
path = connector.get_file_path(
    container="bronze",
    path_prefix="raw/events",
    object_name="sales.csv"
)
options = connector.get_storage_options()
```

This example demonstrates how an instance of a concrete implementation of `BaseConnection` (e.g., `AzureBlobConnection`) can be used to obtain a data path and storage options, which can then be used by data processing tools or frameworks.

---
