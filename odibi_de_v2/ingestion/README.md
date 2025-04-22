# Module Overview

### `reader_provider.py`

#### Class: `ReaderProvider`

### Class Overview

The `ReaderProvider` class is designed to facilitate data reading from various sources, either local or cloud-based, by dynamically selecting the appropriate reading mechanism based on the execution context and data type. It supports multiple data formats and integrates with different data processing frameworks such as Pandas and Spark.

### Constructor: `__init__`

#### Purpose:
Initializes the `ReaderProvider` instance with specified connection and data processing settings.

#### Parameters:
- `connector` (BaseConnection, optional): Handles the connection to data sources. Defaults to `LocalConnection` if not specified.
- `local_engine` (Framework, optional): Specifies the data processing framework to use locally. Defaults to `Framework.PANDAS`.

#### Key Logic:
- The constructor allows for customization of the data connection and local data processing framework, enabling flexibility depending on the deployment environment or specific project requirements.

### Method: `read`

#### Purpose:
Reads data from specified storage using a configured reader based on the data type and execution context.

#### Inputs:
- `data_type` (DataType): Format of the data file (e.g., CSV, JSON, PARQUET).
- `container` (str): Name of the top-level storage container.
- `path_prefix` (str): Path within the container.
- `object_name` (str): Specific name of the data file.
- `spark` (SparkSession, optional): Spark session for Spark-based operations, default is None.
- `is_stream` (bool, optional): Indicates if Spark streaming should be used, default is False.
- `**kwargs`: Arbitrary keyword arguments for reader configurations (e.g., separators in CSV files).

#### Outputs:
- Returns a DataFrame object containing the loaded data, which could be either a pandas DataFrame or a Spark DataFrame depending on the execution context.

#### Key Logic:
- The method dynamically selects the data reading mechanism (Pandas, Spark, or Spark Streaming) based on provided parameters and environment configuration.
- It handles different storage frameworks and data formats, providing a flexible data ingestion solution for various use cases.

#### Error Handling:
- Raises `ValueError` if necessary parameters like a Spark session are missing.
- Raises `NotImplementedError` if the framework or data type is unsupported.
- Raises `RuntimeError` for general execution errors.

### Usage Examples:
```python
# Default usage with local files and Pandas
provider = ReaderProvider()
df = provider.read(DataType.CSV, "bronze", "raw", "sales.csv")

# Using a custom connector and Spark for data processing
custom_connector = CustomConnection()
provider = ReaderProvider(connector=custom_connector, local_engine=Framework.SPARK)
df = provider.read(DataType.JSON, "silver", "processed", "results.json", spark=my_spark_session)
```

### Integration:
`ReaderProvider` fits into a larger data processing or ETL pipeline by providing a robust and flexible solution for reading data from various sources and formats. It abstracts the complexities of data ingestion, allowing other components of the application to focus on data transformation, analysis, or storage without worrying about the specifics of the initial data reading process.

---

### `pandas_api_reader.py`

#### Class: `PandasAPIReader`

### Class Overview

The `PandasAPIReader` class, inheriting from `DataReader`, is designed to fetch data from a REST API and convert it into a Pandas DataFrame. It is capable of handling both simple and paginated API requests and can extract data from nested JSON structures.

### Key Features and Arguments

- **URL Configuration**: The `url` parameter specifies the API endpoint.
- **Parameters and Headers**: Optional `params` and `headers` dictionaries allow for customization of query parameters and HTTP headers, respectively. This is useful for including authorization tokens or specifying content types.
- **Pagination Support**: The `pagination_type` can be either 'offset' or 'page', facilitating data fetching from APIs that support pagination. If not specified, pagination is not handled.
- **Page Size**: The `page_size` argument determines the number of records fetched per page in paginated requests, defaulting to 1000.
- **Record Path**: The `record_path` list allows specification of the path to the data within a nested JSON response. If not provided, it defaults to an empty list, assuming data is at the top level.

### Methods

- **`read_data()`**: Public method that orchestrates the data fetching process. It decides whether to perform a single request or multiple paginated requests based on the `pagination_type`. It returns a Pandas DataFrame containing the fetched data.
- **`_read_single_page()`**: Private method that handles data fetching for non-paginated APIs. It makes a single HTTP GET request, processes the JSON response, and returns a DataFrame.
- **`_read_paginated()`**: Private method for handling paginated data fetching. It loops through pages of data, aggregates records, and returns a single DataFrame containing all records.
- **`_extract_records(json_data)`**: Private method that extracts records from the JSON response based on the specified `record_path`. It navigates through nested JSON to locate the data and raises a `ValueError` if the path does not lead to a list.

### Error Handling

- Raises `ValueError` if the `record_path` does not correctly lead to a list in the JSON structure.
- Uses `response.raise_for_status()` to handle HTTP errors during API requests.

### Integration and Usage

This class is designed to be integrated into larger data ingestion or processing workflows where data from REST APIs needs to be converted into a structured format for analysis or storage. It simplifies the process of API data extraction and transformation into a format that is easily manipulable using Pandas, a popular data manipulation library in Python.

### Dependencies

- Requires the `pandas` library for DataFrame operations and the `requests` library for making HTTP requests.

### Example Usage

```python
reader = PandasAPIReader(
    url="https://api.example.com/data",
    params={"start_date": "2024-01-01"},
    headers={"Authorization": "Bearer token"},
    pagination_type="offset",
    page_size=5000,
    record_path=["response", "data"]
)
df = reader.read_data()
print(df.head())
```

This example demonstrates initializing the reader for a paginated API with specific parameters, headers, and a nested JSON structure, and then reading the data into a DataFrame.

---

### `pandas_data_reader.py`

#### Class: `PandasDataReader`

## Class Overview: PandasDataReader

### Purpose
The `PandasDataReader` class is designed to facilitate the reading of structured data from various file formats into a pandas DataFrame. It supports a range of data types including CSV, JSON, Parquet, Avro, and SQL, making it a versatile tool for data ingestion in Python applications.

### Behavior and Key Features
- **Data Type Support**: The class can handle multiple file formats by leveraging pandas' built-in functions and some custom methods for specific formats like Avro and SQL.
- **Flexible File Path Input**: It accepts both local and cloud-based file paths, enhancing its utility in diverse environments.
- **Error Handling**: Comprehensive error handling is implemented to manage common issues like file not found, permission errors, and unexpected input types.
- **Decorator Enhancements**: The class methods are enhanced with decorators for type enforcement, input validation, output assurance, benchmarking, and logging. This ensures robustness and ease of maintenance.
- **Custom Read Methods**: For formats not directly supported by pandas (e.g., Avro and SQL), custom methods `_read_avro` and `_read_sql` are provided. These methods handle specific complexities associated with these formats.

### Inputs and Outputs
- **Inputs**:
  - `data_type`: An enum indicating the file format.
  - `file_path`: A string specifying the path to the data file.
  - `**kwargs`: Additional keyword arguments that are passed to the pandas reading functions, allowing for customization such as separators in CSV files or connection details for SQL databases.
- **Outputs**:
  - The main method `read_data` returns a pandas DataFrame populated with data from the specified file.

### Usage in a Larger Application
`PandasDataReader` is likely part of a larger data processing or data ingestion framework where data from various sources and formats needs to be standardized into pandas DataFrames for further analysis or processing. Its design allows for easy integration into data pipelines, especially where data sources vary widely in format and origin.

### Example Usage
```python
reader = PandasDataReader()
df = reader.read_data(
    data_type=DataType.CSV,
    file_path="data/sample.csv",
    sep=";", encoding="utf-8"
)
```

This example demonstrates reading a CSV file with specific encoding and separator parameters, showcasing the class's ability to adapt to different file specifics seamlessly.

### Conclusion
The `PandasDataReader` class is a robust and flexible solution for reading and converting various data formats into pandas DataFrames, equipped with detailed error handling and logging capabilities to ensure reliable data ingestion in Python applications.

---

### `spark_api_reader.py`

#### Class: `SparkAPIReader`

### Class Overview

The `SparkAPIReader` class is designed to facilitate the integration of data fetched from a REST API into a Spark environment by converting it into a Spark DataFrame. This class inherits from `DataReader` and acts as a bridge between Pandas and Spark, leveraging the `PandasAPIReader` for data fetching and then converting the data for use in Spark.

### Key Features and Inputs

- **Spark Session (`spark`)**: An active Spark session is required to create Spark DataFrames. This session is passed to the class during initialization.
- **API Details (`url`, `params`, `headers`)**: These inputs specify the REST API endpoint, query parameters, and HTTP headers necessary for the API request.
- **Pagination (`pagination_type`, `page_size`)**: Supports API pagination by specifying the pagination style (e.g., 'offset', 'page') and the number of records per API call.
- **Data Path (`record_path`)**: Specifies the path within the JSON response where the data records are located, allowing for targeted data extraction.

### Methods

- **`__init__`**: Initializes the `SparkAPIReader` instance, setting up the `PandasAPIReader` with all necessary configurations for API interaction.
- **`read_data`**: Fetches data using the configured `PandasAPIReader` and converts the resulting Pandas DataFrame into a Spark DataFrame. This method can handle additional keyword arguments (`**kwargs`) that are passed directly to the `PandasAPIReader`'s `read_data` method, allowing for flexible data querying.

### Outputs

- **Spark DataFrame**: The primary output of the `read_data` method is a Spark DataFrame, which contains the data fetched from the API and is ready for processing in Spark applications.

### Error Handling

- **HTTPError**: Raised if the API call fails, ensuring that API-related issues are flagged.
- **ValueError**: Triggered if there are issues in converting the Pandas DataFrame to a Spark DataFrame, such as data compatibility problems.

### Usage Example

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
reader = SparkAPIReader(
    spark=spark,
    url="https://api.example.com/data",
    params={"start_date": "2024-01-01"},
    headers={"Authorization": "Bearer token"},
    pagination_type="offset",
    page_size=5000,
    record_path=["response", "data"]
)
spark_df = reader.read_data()
spark_df.show()
```

### Design Choices and Integration

The `SparkAPIReader` is designed to be a versatile tool in data engineering pipelines, particularly where data from REST APIs needs to be integrated into Spark for large-scale data processing tasks. By abstracting the data fetching and conversion process, it allows developers to focus on data analysis rather than data acquisition details. The use of `PandasAPIReader` within `SparkAPIReader` encapsulates the complexity of API interactions and data parsing, streamlining the workflow from data retrieval to processing in Spark.

---

### `spark_data_reader.py`

#### Class: `SparkDataReader`

### Class Overview

The `SparkDataReader` class is a specialized reader designed to facilitate the ingestion of structured data files into Apache Spark DataFrames. It extends a base `DataReader` class, leveraging Spark's capabilities to handle various data formats and configurations dynamically. This class is particularly useful in data processing pipelines where data from different sources and formats needs to be efficiently loaded and manipulated using Spark.

### Key Features and Methods

#### `read_data`
- **Purpose**: Reads data from a specified file into a Spark DataFrame. It supports various file formats by dynamically chaining read operations based on the input `data_type` and other method options specified in `kwargs`.
- **Inputs**:
  - `data_type` (DataType): An enum indicating the file format (e.g., CSV, JSON).
  - `file_path` (str): The path to the file to be read.
  - `spark` (SparkSession, optional): An existing Spark session; if not provided, a new session is created.
  - `storage_options` (dict, optional): Options that modify how the file is accessed, such as credentials for remote systems.
  - `**kwargs`: Additional keyword arguments for customizing the read operation, like 'schema', 'header', etc.
- **Outputs**: Returns a Spark DataFrame containing the data read from the file.
- **Exceptions**: Handles various errors such as `PermissionError`, `FileNotFoundError`, and `RuntimeError`, providing clear feedback on the nature of the issue encountered.

#### `_read_sql`
- **Purpose**: An internal method to read data from a SQL database into a Spark DataFrame using JDBC, supporting both SQL queries and table reads.
- **Inputs**:
  - `query_or_table` (str): SQL query or table name.
  - `spark` (SparkSession): Spark session instance.
  - `storage_options` (dict, optional): Fallback storage options if JDBC details are not provided.
  - `**kwargs`: Must include JDBC connection options; additional options can customize the read operation.
- **Outputs**: Returns a DataFrame with data from the SQL database.
- **Exceptions**: Raises `ValueError` if JDBC connection details are incomplete.

### Design Choices and Logic

- **Dynamic Method Chaining**: Utilizes a flexible approach to apply read configurations dynamically based on the file format and additional parameters. This is achieved through method chaining, where methods are called in sequence based on the input parameters.
- **Error Handling**: Comprehensive error handling is built into the methods, ensuring that common issues like file not found, permission errors, and unexpected errors in Spark are appropriately managed and reported.
- **Logging and Benchmarking**: Decorators are used for logging and benchmarking, which help in maintaining detailed logs for operations and measuring performance metrics. This is crucial for debugging and optimizing data ingestion workflows.

### Integration into Larger Applications

`SparkDataReader` is designed to be a robust component in larger data ingestion and processing systems, particularly those utilizing Apache Spark for big data processing tasks. It can be integrated into ETL pipelines, data transformation jobs, and any system where data needs to be read from various sources into Spark for further analysis and processing.

### Usage Example

```python
spark_session = SparkSession.builder.appName("ExampleApp").getOrCreate()
reader = SparkDataReader()
df = reader.read_data(
    data_type=DataType.JSON,
    file_path="/mnt/data/file.json",
    spark=spark_session,
    option={"multiline": "true"}
)
```

This example demonstrates how to read a JSON file into a Spark DataFrame, highlighting the class's ability to handle complex data ingestion scenarios efficiently.

---

### `spark_streaming_data_reader.py`

#### Class: `SparkStreamingDataReader`

### Class Overview

The `SparkStreamingDataReader` class, inheriting from `DataReader`, is designed to facilitate the reading of streaming data using Apache Spark's structured streaming capabilities. It leverages the `cloudFiles` format for efficient and scalable ingestion from cloud storage sources. The class supports multiple data formats including CSV, JSON, and PARQUET, and allows for extensive configuration to cater to various data ingestion requirements.

### Key Features and Methodology

- **Data Format Flexibility**: The class can handle different data formats specified by the `data_type` parameter.
- **Spark Integration**: Utilizes an existing Spark session or creates a new one if none is provided, ensuring flexibility in its usage within different Spark-based applications.
- **Configurable**: Accepts additional keyword arguments (`**kwargs`) that are passed directly to Spark's `readStream` method, allowing for detailed customization such as schema definition, partitioning, and more.
- **Error Handling**: Robust error handling is built into the method, which raises specific exceptions for common issues like permission errors, file not found, and unsupported data types, ensuring clear and actionable error messages are provided.

### Inputs and Outputs

- **Inputs**:
  - `data_type` (DataType): Specifies the format of the data (CSV, JSON, PARQUET).
  - `file_path` (str): Path to the streaming data source.
  - `spark` (SparkSession, optional): An existing Spark session; if not provided, a new session is created.
  - `**kwargs`: Additional options for configuring the Spark `readStream`.

- **Output**:
  - Returns a Spark `DataFrame` that represents the streaming data, ready for further processing or analysis.

### Usage in Larger Applications

`SparkStreamingDataReader` is typically used in data ingestion pipelines where streaming data needs to be continuously read and processed. It fits into larger ETL (Extract, Transform, Load) or real-time analytics applications where data is ingested from cloud sources, transformed, and then loaded into data stores or used for real-time decision making.

### Example

```python
spark_session = SparkSession.builder.appName("ExampleApp").getOrCreate()
data_reader = SparkStreamingDataReader()
data_frame = data_reader.read_data(
    data_type=DataType.JSON,
    file_path="/path/to/data.json",
    spark=spark_session,
    options={"schemaLocation": "/mnt/schema"}
)
print(data_frame.isStreaming)  # Should output: True
```

This example demonstrates creating a streaming DataFrame from JSON files located in a cloud storage, using an existing Spark session. The `data_frame` object can then be used in the application for streaming transformations or queries.

---
