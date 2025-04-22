# Module Overview

### `spark_data_reader_from_config.py`

#### Class: `SparkDataReaderFromConfig`

### Class Overview

The `SparkDataReaderFromConfig` class is designed to facilitate data ingestion into a Spark environment from various sources such as Azure Data Lake Storage (ADLS), SQL databases, and APIs. The class is highly configurable, supporting both batch and streaming data operations based on the provided settings.

### Constructor and Attributes

- **Constructor (`__init__`)**: Initializes the class with a Spark session, a configuration dictionary, and optionally, database utilities (dbutils). The constructor sets up the necessary attributes for data reading based on the configuration.
- **Attributes**:
  - `spark`: An active Spark session.
  - `config`: A deep copy of the configuration dictionary provided.
  - `dbutils`: Optional. Used for API authentication and other database interactions.
  - `file_format`: The format of the data (e.g., 'csv', 'json', 'delta') as specified in the configuration.
  - `data_type`: An enumeration value representing the data format.
  - `is_streaming`: A boolean indicating if the data source is for streaming.
  - `connection`: An object to manage connections to the data source.
  - `storage_unit`: Name of the storage unit or container, relevant for file and database sources.
  - `object_name`: Name of the object or file to read from, relevant for file sources.
  - `options`: A dictionary of options specific to the mode of operation (streaming or batch).

### Methods

- **`_resolve_options`**: Internal method that resolves and returns options for data reading based on the operation mode (e.g., 'read', 'write'). It utilizes a `SourceOptionsResolver` to determine the appropriate settings.
- **`read_data`**: The primary method of the class, responsible for reading data from the configured source. It uses a `ReaderProvider` to abstract the details of reading from different source types (API, file, SQL). The method adjusts its behavior based on the data type and configuration.

### Key Design Choices

- **Flexibility and Extensibility**: The class is designed to handle various data sources and formats by abstracting the source-specific details into configuration settings and utilizing provider patterns.
- **Error Handling**: Raises exceptions (e.g., `ValueError`, `KeyError`) when required configuration items are missing or invalid, ensuring robustness.
- **Use of Enums and Providers**: Utilizes enums for data types and a provider pattern for reading operations, enhancing maintainability and scalability of the code.

### Integration into Larger Applications

This class can be integrated into data processing pipelines in Spark-based applications, where there is a need to ingest data from diverse sources with varying requirements. It is particularly useful in scenarios requiring dynamic switching between batch and streaming data, or different data formats and sources, driven by external configurations.

### Example Usage

```python
spark_session = SparkSession.builder.appName("Example").getOrCreate()
config = {
    "file_format": "json",
    "connection_config": {
        "storage_unit": "s3_bucket",
        "object_name": "data_file"
    },
    "is_autoloader": True
}
data_reader = SparkDataReaderFromConfig(spark_session, config)
dataframe = data_reader.read_data()
dataframe.show()
```

This example demonstrates initializing the class with a Spark session and a configuration dictionary, and then reading data into a DataFrame, showcasing the class's capability to handle complex data ingestion scenarios efficiently.

---
