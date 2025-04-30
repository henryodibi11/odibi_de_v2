# Module Overview

### `saver_provider.py`

#### Class: `SaverProvider`

### Class Overview

The `SaverProvider` class is designed to facilitate data saving operations across different storage frameworks and environments (local or cloud). It abstracts the complexities of using different data saving mechanisms like Pandas, Spark, and Spark Streaming, providing a unified interface to save data frames in various formats.

### Responsibilities

- **Framework Selection**: Automatically selects the appropriate data saving class based on the specified or detected framework (Pandas, Spark, Spark Streaming).
- **Path and Storage Resolution**: Computes the full file path and resolves storage options using the provided connector.
- **Authentication and Configuration**: Applies necessary authentication and configuration settings for Spark operations when required.
- **Data Saving**: Delegates the data saving process to the chosen saver class based on the framework and data type.

### Inputs and Outputs

- **Inputs**:
  - `df`: A Pandas DataFrame or a Spark DataFrame to be saved.
  - `data_type`: The format of the output data (e.g., CSV, JSON, PARQUET).
  - `container`: The storage location (e.g., a cloud container or a local directory).
  - `path_prefix`: A path segment within the container to organize data.
  - `object_name`: The name of the file or object to be created.
  - `spark`: An optional Spark session, required for Spark-based operations.
  - `is_stream`: A boolean indicating if Spark Streaming should be used (default is False).
  - `**kwargs`: Additional parameters that might be required by the underlying saver classes.

- **Outputs**:
  - The function does not return any value but performs a side effect of saving data to the specified location.

### Key Logic and Design Choices

- **Flexible Connector Handling**: If no connector is provided, it defaults to a local connection. This flexibility allows easy integration with various storage systems.
- **Framework Fallback**: If the operation is intended for local storage and no specific framework is provided, it defaults to using Pandas.
- **Error Handling**: Implements structured error handling and logging, ensuring that any issues during the save operation are clearly reported and logged.
- **Streaming Support**: Seamlessly handles streaming data by integrating with Spark Streaming when the `is_stream` flag is set.

### Integration in Larger Applications

`SaverProvider` can be integrated into data processing pipelines, ETL jobs, or data analytics platforms where data needs to be saved in different formats and locations transparently. It simplifies codebases by reducing the need for repeated checks and conditions based on the storage framework and environment, making the data saving process more robust and easier to manage.

### Usage Examples

The class documentation provides clear examples on how to instantiate the `SaverProvider` and use it to save data frames in different scenarios, including local and cloud storage with various data formats. This helps developers understand how to integrate the class into their applications effectively.

---

### `pandas_data_saver.py`

#### Class: `PandasDataSaver`

### Class Overview

The `PandasDataSaver` class is a specialized component designed for saving Pandas DataFrames to various file formats, including CSV, JSON, Parquet, Excel, and Avro. It extends from a base class `DataSaver`, indicating its role in a broader data handling or storage framework.

### Key Features and Design Choices

- **Flexible Output Formats**: The class supports multiple output formats, which are determined by the `DataType` enum. This design allows for easy extension or modification to support additional formats as needed.
- **Error Handling and Logging**: Comprehensive error handling is implemented to catch and raise specific exceptions related to file operations, permissions, and data integrity. Logging is extensively used to provide detailed feedback about the operation's status, which is crucial for debugging and monitoring in production environments.
- **Decorator Enhancements**: The method `save_data` uses several decorators to enforce type checks, validate inputs, benchmark performance, and log method calls and exceptions. This structured approach enhances the robustness and maintainability of the code.
- **Cloud Integration**: The class handles cloud-based storage paths through `storage_options`, making it adaptable to modern cloud environments like AWS S3, Azure Blob Storage, etc.
- **Avro Special Handling**: Saving in Avro format requires a specific schema, and the class includes a dedicated method `_save_avro` to handle this complexity. This method ensures that the necessary schema is provided and correctly utilized, demonstrating the class's capability to manage format-specific requirements.

### Method Details

- **`save_data` Method**: This is the primary method of the class, responsible for saving a DataFrame to the specified file path in the format dictated by `data_type`. It accepts a DataFrame, a `DataType` enum, a file path, and additional keyword arguments that are passed to the underlying Pandas writer functions. The method is well-documented with details on arguments, supported formats, and possible exceptions, providing clear guidelines for users and maintainers.
- **`_save_avro` Private Method**: Handles the specifics of writing DataFrames in Avro format, including schema validation and cloud storage options. This method abstracts the complexity involved in Avro file operations, making the main `save_data` method cleaner and more focused.

### Usage in a Larger Application

`PandasDataSaver` fits into a larger data engineering or data processing application where structured data needs to be reliably and efficiently saved to various formats for downstream use, such as data analysis, reporting, or machine learning workflows. Its design aligns with modern software development practices, emphasizing modularity, error handling, and integration with external systems (like cloud storage), making it a versatile component in a data-centric application.

### Example Usage

The class is used by creating an instance and calling the `save_data` method with appropriate parameters. Example usage scenarios provided in the documentation illustrate how to save data in different formats, highlighting the class's flexibility and ease of use.

Overall, `PandasDataSaver` is a critical component designed to meet the needs of robust and scalable data storage solutions, ensuring data integrity and compatibility with various storage backends and formats.

---

### `spark_streaming_data_saver.py`

#### Class: `SparkStreamingDataSaver`

### Class Overview

The `SparkStreamingDataSaver` class is a subclass of `DataSaver` designed to handle the saving of streaming data using Apache Spark's structured streaming capabilities. It provides a flexible and robust way to write streaming data to various supported sinks such as Delta, Parquet, CSV, Kafka, and others.

### Key Features and Usage

- **Flexible Output Formats**: The class supports multiple output formats, allowing users to specify the format through the `data_type` parameter.
- **Dynamic Method Chaining**: Utilizes dynamic chaining of writeStream methods based on user inputs, enabling customization of the streaming write operation with parameters like `outputMode`, `trigger`, and additional options.
- **Error Handling**: Implements comprehensive error handling to manage and raise exceptions specific to file system issues, permissions, and Spark-related errors.
- **Logging and Benchmarking**: Integrated logging and benchmarking annotations help in tracking the performance and issues during the data saving process.

### Method Details

#### `save_data`
- **Purpose**: Initiates a streaming write operation to save data from a Spark DataFrame to a specified sink.
- **Inputs**:
  - `df (DataFrame)`: The streaming DataFrame to be written.
  - `data_type (DataType)`: The format of the output data (e.g., delta, parquet, kafka).
  - `file_path (str)`: The destination path or URI for the output data.
  - `**kwargs`: Additional keyword arguments to configure the streaming query, such as `outputMode`, `trigger`, and other Spark-specific options.
- **Output**:
  - Returns a `StreamingQuery` object that represents the active streaming query, which can be used to manage and monitor the ongoing operation.
- **Error Handling**:
  - Specific exceptions are raised for various error conditions like permission issues, file not found, and invalid configurations, ensuring that the user is informed of the exact failure reason.

### Example Usage

```python
saver = SparkStreamingDataSaver()
query = saver.save_data(
    df=streaming_df,
    data_type=DataType.DELTA,
    file_path="/mnt/bronze/table",
    outputMode="append",
    options={"checkpointLocation": "/mnt/bronze/checkpoints"}
)
query.awaitTermination()  # To wait for the streaming to finish
```

### Integration in Applications

The `SparkStreamingDataSaver` class is typically used in data engineering pipelines where real-time data needs to be ingested, processed, and stored continuously. It fits into larger applications that require reliable and scalable streaming data storage solutions, making it a critical component for real-time analytics and data processing systems.

---

### `spark_data_saver.py`

#### Class: `SparkDataSaver`

### Class Overview

The `SparkDataSaver` class is a specialized component designed for saving Spark DataFrames to various file formats. It extends a base `DataSaver` class, indicating that it is part of a larger data handling framework. This class is tailored to work with Apache Spark's DataFrame API and supports a variety of data formats such as CSV, JSON, Parquet, Avro, etc.

### Key Features and Usage

- **Method Chaining**: The class utilizes a method chaining approach that allows users to dynamically specify write configurations such as save mode, partitioning, and other options directly through method parameters.
- **Flexible Data Writing**: Users can specify the format of the data file (e.g., Parquet, CSV) through the `data_type` parameter, which is an enumeration. This design ensures that the file format is explicitly defined and managed.
- **Error Handling**: The class is robust in terms of error handling, providing detailed exceptions for various error conditions like permission issues, file not found, and I/O errors. This makes it easier to debug issues related to file saving operations.

### Function `save_data`

- **Purpose**: The primary method `save_data` is responsible for saving a Spark DataFrame to a specified file path in a specified format. It handles various configurations passed as keyword arguments.
- **Inputs**:
  - `df` (DataFrame): The Spark DataFrame to be saved.
  - `data_type` (DataType): An enumeration that specifies the format of the file to be saved.
  - `file_path` (str): The destination path where the DataFrame will be saved.
  - `**kwargs`: Additional keyword arguments for Spark write configurations such as save mode, partitioning, and custom options like compression.
- **Output**: There is no return value as the function is designed to perform a save operation. It raises exceptions if the operation fails.
- **Decorators**:
  - `@enforce_types`: Ensures that the inputs to the function adhere to specified type annotations.
  - `@validate_non_empty` and `@validate_input_types`: Validate that necessary parameters are not empty and are of correct types.
  - `@benchmark` and `@log_call`: Used for performance monitoring and logging function calls.
  - `@log_exceptions`: Handles logging and categorization of exceptions that may occur during the save operation.

### Integration and Logging

- **Logging**: Extensive logging is implemented to provide insights into the function's execution state, which is crucial for debugging and monitoring in production environments.
- **Error Management**: The function is designed to log specific error messages before raising exceptions, which aids in diagnosing issues in a complex data pipeline.

### Conclusion

The `SparkDataSaver` class is a critical component for applications that require efficient and flexible data storage solutions using Spark. Its design focuses on ease of use through method chaining, robust error handling, and detailed logging, making it a valuable tool for developers working with large-scale data processing and storage in a Spark environment.

---
