# Module Overview

### `ingestion_config_constructor.py`

#### Class: `IngestionConfigConstructor`

### Class Overview

The `IngestionConfigConstructor` class is designed to process and prepare configuration data for data ingestion systems. It takes configuration details from two pandas DataFrames, one for source systems and another for target systems, and prepares them for use in data ingestion processes. The class ensures that the configurations are correctly parsed, validated, and formatted as dictionaries.

### Key Attributes

- `sources_df (pd.DataFrame)`: Contains configuration data for the source systems.
- `targets_df (pd.DataFrame)`: Contains configuration data for the target systems.

### Methods

#### `__init__(self, sources_df, targets_df)`
Initializes the class with two DataFrames:
- `sources_df`: DataFrame with source system configurations.
- `targets_df`: DataFrame with target system configurations.

#### `prepare(self) -> tuple[dict, dict]`
Processes the source and target DataFrames to produce two dictionaries:
- The first dictionary contains the prepared configuration for the source system.
- The second dictionary contains the prepared configuration for the target system.

This method internally calls `_prepare_source()` and `_prepare_target()` to handle the individual configurations.

#### `_prepare_source(self) -> dict`
Prepares the source configuration by converting the first row of `sources_df` into a dictionary, handling JSON fields, and ensuring required fields are present.

#### `_prepare_target(self) -> dict`
Similar to `_prepare_source()`, this method prepares the target configuration by processing the first row of `targets_df`.

#### `_prepare_row(self, row, json_fields, required_fields) -> dict`
A helper method used by `_prepare_source()` and `_prepare_target()` to parse JSON fields and validate required fields in a row dictionary.

#### `_parse_json_fields(self, row, fields) -> dict`
Decodes JSON-encoded strings in specified fields of a dictionary. Raises a `ValueError` if decoding fails.

#### `_validate_required_fields(self, row, required_fields)`
Checks if all required fields are present and non-empty in a dictionary. Raises a `ValueError` if any required fields are missing or empty.

### Usage and Integration

This class is typically used in data ingestion pipelines where configurations for source and target systems need to be dynamically loaded and validated from DataFrames before starting the ingestion process. It ensures that the configurations are correct and ready to be used, preventing runtime errors due to misconfiguration.

### Error Handling

The class includes comprehensive error handling:
- Raises `ValueError` if JSON parsing fails or if required fields are missing.
- Ensures that the configurations are ready for use without further checks downstream.

### Example

```python
sources_data = pd.DataFrame({'id': [1], 'value': ['{"source_type": "database", "source_path_or_query": "SELECT * FROM users"}']})
targets_data = pd.DataFrame({'id': [1], 'value': ['{"target_type": "database", "write_mode": "append"}']})
constructor = IngestionConfigConstructor(sources_df=sources_data, targets_df=targets_data)
source_config, target_config = constructor.prepare()
```

This example demonstrates how to initialize the class with DataFrames containing JSON strings, prepare the configurations, and retrieve them as dictionaries for further processing in a data ingestion pipeline.

---

### `option_resolvers.py`

#### Class: `SourceOptionsResolver`

### Class Overview

The `SourceOptionsResolver` class is designed to manage and sanitize source options for data processing tasks based on a given configuration, operational mode, and data processing engine. It primarily handles the resolution of file paths for schema locations and the removal of empty or unset configuration options.

### Constructor: `__init__`

**Purpose**: Initializes the `SourceOptionsResolver` instance with necessary configurations and operational parameters.

**Inputs**:
- `connector`: An object that must implement a `get_file_path()` method, used to resolve file paths.
- `config`: A dictionary containing configuration settings, including nested dictionaries for source options specific to different engines and modes.
- `mode`: A string indicating the operational mode ('streaming' or 'batch'), which affects how data is processed.
- `engine`: An optional string specifying the processing engine (default is "databricks").

**Behavior**:
- Extracts and stores source options from the `config` dictionary based on the specified `engine` and `mode`.
- Raises a `KeyError` if the necessary keys are not found in the configuration for the specified engine or mode.

**Example Usage**:
```python
connector_instance = MyConnector()
config = {
    "source_options": {
        "databricks": {
            "streaming": {"option1": "value1"},
            "batch": {"option1": "value2"}
        }
    }
}
instance = SourceOptionsResolver(connector_instance, config, "streaming")
print(instance.options)  # Output: {'option1': 'value1'}
```

### Method: `resolve`

**Purpose**: Resolves schema locations within the source options and sanitizes the options dictionary by removing empty or unset values.

**Outputs**:
- Returns a dictionary of the updated source options with resolved schema locations and without empty fields.

**Behavior**:
- Resolves the relative path of `cloudFiles.schemaLocation` to an absolute path using the `connector`.
- Cleans up the `options` dictionary by removing any top-level fields that are empty or set to `None`.

**Example Usage**:
```python
resolver = SourceOptionsResolver(connector, config, 'batch')
updated_options = resolver.resolve()
print(updated_options)  # Output: {'options': {'cloudFiles.schemaLocation': '/absolute/path/to/schema'}}
```

### Design Choices and Logic

- The class is designed to be flexible with respect to different processing engines and operational modes, making it suitable for applications that need to handle various data processing configurations dynamically.
- The use of a connector object abstracts the file path resolution, allowing for different implementations of data source interfacing, which can be tailored to specific storage or file system requirements.
- The method to clean up the options dictionary ensures that the configuration passed to downstream processes is clean and efficient, avoiding potential errors or inefficiencies in data processing tasks.

### Application Context

`SourceOptionsResolver` fits into larger data processing or ETL (Extract, Transform, Load) applications where configurations for data sources need to be dynamically resolved and sanitized based on the operational context. This class helps in ensuring that the data handling layers of such applications are robust and adaptable to different environments and configurations.

#### Class: `TargetOptionsResolver`

## TargetOptionsResolver Class Documentation

### Overview

The `TargetOptionsResolver` class is designed to process and sanitize configuration options for a target system based on specified operational modes and processing engines. It is particularly useful in environments where configurations need to be dynamically adjusted according to the mode of operation (e.g., 'streaming' or 'batch') and the type of processing engine (defaulting to 'databricks').

### Purpose

The primary purpose of this class is to:
- Extract relevant configuration options based on the provided mode and engine.
- Sanitize these options by removing any that are null, empty strings, empty lists, or empty dictionaries.

### Usage

This class is intended to be used in data processing applications where configurations for target systems vary by operational mode and engine type. It simplifies the management of these configurations by providing a clean, filtered set of options that can be directly utilized by the application.

### Inputs and Outputs

- **Inputs:**
  - `config`: A dictionary containing nested configurations potentially specific to different engines and modes.
  - `mode`: A string indicating the operational mode (e.g., 'streaming', 'batch').
  - `engine`: A string specifying the processing engine (defaults to 'databricks').

- **Outputs:**
  - The `resolve()` method outputs a dictionary containing sanitized configuration options that are relevant to the specified mode and engine.

### Key Logic and Design Choices

- **Initialization and Configuration Extraction:**
  - During initialization, the class extracts options specific to the provided engine and mode from the `config` dictionary. This extraction is case-insensitive for the mode to ensure flexibility.
  
- **Sanitization Process:**
  - The `resolve()` method filters out any configuration options that are `None`, empty strings, empty lists, or empty dictionaries. This ensures that the output dictionary contains only valid, non-empty settings.

### Integration in Larger Applications

In a larger application, an instance of `TargetOptionsResolver` could be used to manage and sanitize configurations for different target systems or data sinks. By providing a clean set of options, it helps in maintaining clarity and reducing errors in the configuration process, especially in complex data processing pipelines where different settings might be needed for different operational scenarios.

### Example

```python
config = {
    "target_options": {
        "databricks": {
            "streaming": {
                "option1": "value1",
                "option2": "",
                "option3": None
            }
        }
    }
}
resolver = TargetOptionsResolver(config, "streaming")
clean_options = resolver.resolve()
print(clean_options)  # Output: {'option1': 'value1'}
```

This example demonstrates how the class is used to filter and sanitize target options for a streaming mode on the Databricks engine.

---
