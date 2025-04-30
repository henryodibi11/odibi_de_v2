# Module Overview

### `metadata_manager.py`

#### Class: `MetadataManager`

### Class Overview

The `MetadataManager` class is designed to manage and store metadata in a dynamic manner, primarily for use in logging or contextual tracking within applications. It offers a straightforward interface to handle metadata as key-value pairs, allowing easy updates and retrieval of the metadata state.

### Key Attributes

- `metadata`: A dictionary that stores the current metadata values.

### Methods

#### `__init__(self)`
- **Purpose**: Initializes a new instance of `MetadataManager` with an empty metadata dictionary.
- **Inputs**: None.
- **Outputs**: An instance of `MetadataManager` with `metadata` attribute set to an empty dictionary.

#### `update_metadata(self, clear_existing=False, **kwargs)`
- **Purpose**: Updates the metadata dictionary with new key-value pairs. It can optionally clear all existing metadata before applying the updates.
- **Inputs**:
  - `clear_existing` (bool, optional): A flag to determine whether to clear the existing metadata before updating. Defaults to `False`.
  - `**kwargs`: Arbitrary keyword arguments representing metadata key-value pairs to be added or updated.
- **Outputs**: None, but modifies the `metadata` dictionary in-place.
- **Exceptions**:
  - `ValueError`: Raised if `clear_existing` is not a boolean.
- **Design Choices**: The method uses a boolean flag `clear_existing` to optionally reset the metadata, providing flexibility depending on whether the user wants to retain previous metadata or start fresh. The use of `**kwargs` allows for flexible and dynamic metadata updates without needing to predefine the structure.

#### `get_metadata(self)`
- **Purpose**: Retrieves the current state of the metadata dictionary.
- **Inputs**: None.
- **Outputs**: Returns the current metadata dictionary.

### Usage Example

```python
manager = MetadataManager()
manager.update_metadata(project="OEE", step="validate")
current_metadata = manager.get_metadata()  # Returns {'project': 'OEE', 'step': 'validate'}
manager.update_metadata(clear_existing=True, phase="ingest")
current_metadata = manager.get_metadata()  # Returns {'phase': 'ingest'}
```

### Integration into Larger Applications

`MetadataManager` can be integrated into larger applications or modules where maintaining and tracking metadata is crucial, such as in data processing pipelines, logging systems, or any context where dynamic information tracking is needed. It helps in maintaining a clean and manageable way of handling metadata without cluttering the main application logic.

---

### `decorator.py`

#### Function: `log_exceptions`

### Summary of `log_exceptions` Decorator

#### Purpose
The `log_exceptions` function is a decorator designed to standardize the process of exception handling across different parts of a Python application. It automates the logging of exceptions and optionally re-raises them, allowing for consistent error reporting and handling practices within specified modules and components of the application.

#### Behavior and Inputs
- **Module and Component Identification**: The decorator requires `module` and `component` parameters to specify the context in which the exception occurred, aiding in more granular logging (e.g., "INGESTION.reader").
- **Logging Level**: It accepts a `level` parameter that determines the severity of the log entry (default is "error"). Other levels can be "warning" or "info".
- **Error Type**: An `error_type` parameter categorizes the error, which can be useful for filtering or handling specific types of errors differently.
- **Exception Handling Options**: 
  - `raise_exception` (default True) decides whether to re-raise the caught exception after logging.
  - `raise_type` specifies the type of exception to be raised if `raise_exception` is True, with a default of `ValueError`.

#### Key Logic
- The decorator wraps any function, attempting to execute it normally.
- If an exception occurs, the error is logged with detailed information including the module, component, and function name, along with the error message and type.
- Depending on the `raise_exception` flag, it may also re-raise the exception as the specified `raise_type`, allowing for standard or custom exception flows within the application.

#### Integration in Applications
This decorator is particularly useful in large applications where different modules or components might handle exceptions differently. By standardizing how exceptions are logged and optionally re-raised, it helps maintain clean and consistent error handling and logging practices. This can be crucial for debugging, maintenance, and compliance with logging standards in complex systems.

#### Example Usage
```python
@log_exceptions(module="UTILS", component="testing", raise_exception=True)
def risky_fn():
    raise ValueError("bad input")
```
In this example, if `risky_fn()` is called and an exception occurs, it logs the error under the UTILS.testing context with an error level of "error", and then re-raises a `ValueError` with the message "bad input". This behavior is configurable per function or module/component basis.

---

### `log_helpers.py`

#### Function: `initialize_logger`

### Function Overview

The `initialize_logger` function is designed to configure a global logger instance by injecting it with dynamic runtime metadata specific to a project. This function is particularly useful in applications where logging context needs to reflect current operational parameters such as project details, database tables, or domain specifics.

### Inputs and Outputs

**Input:**
- `metadata_dict (dict)`: A dictionary containing key-value pairs of metadata. The keys represent metadata fields such as "project", "table", and "domain", and the values are the corresponding details to be logged.

**Output:**
- Returns an instance of `DynamicLogger` that has been updated with the provided metadata.

### Key Logic and Design Choices

1. **Singleton Logger Instance:** The function operates on a singleton logger instance obtained via `get_logger()`. This ensures that the same logger instance is used throughout the application, maintaining consistency in logging behavior and output.

2. **Metadata Management:** The logger's `metadata_manager` is used to update the logger's context. The `update_metadata` method is called with the `clear_existing=True` flag, which ensures that any previous metadata is cleared before new metadata is applied. This prevents stale or irrelevant metadata from persisting in the logger's context.

3. **Flexible Metadata Injection:** By accepting a dictionary and using `**metadata_dict` to pass it as keyword arguments, the function allows for flexible and dynamic metadata injection. This design supports varying metadata fields without needing changes to the function signature.

### Integration into Larger Applications

`initialize_logger` is typically used in applications that require detailed and context-specific logging, such as data processing pipelines, web services, or any multi-component system where the logging context changes based on runtime parameters. It helps in maintaining clear and relevant log entries across different parts of the application or during different phases of execution.

### Usage Example

```python
from odibi_de.logger.log_helpers import initialize_logger

logger = initialize_logger({
    "project": "MyPipeline",
    "table": "sales_orders",
    "domain": "Commercial"
})
logger.info("Logger initialized with custom context")
```

This example demonstrates initializing the logger with specific project metadata and then using it to log an informational message. This setup is crucial for troubleshooting and monitoring applications by providing a clear trace of operational contexts.

#### Function: `log_info`

### Function: `log_info`

#### Purpose
The `log_info` function is designed to log informational messages using a centralized logging system. This function is part of a larger application or module where logging is essential for monitoring and debugging purposes.

#### Behavior and Inputs
- **Input**: The function accepts a single input parameter, `message`, which is a string. This string represents the informational message that needs to be logged.
- **Output**: There is no direct output from the function (i.e., it returns `None`). However, the function outputs the message to the logging system at the "info" level.

#### Key Logic
- The function utilizes a global logger obtained through the `get_logger()` function. This design implies that the logger configuration (like handlers, formatters, and log level) is set up elsewhere in the application, ensuring that all logged messages are handled consistently.
- The function specifically logs messages at the "info" level, which is typically used for routine information that is useful for tracking the flow of the application and its state.

#### Integration in Larger Application
- `log_info` is likely part of a utility module or a common library used across different parts of the application. This allows developers to easily include logging in any part of the application without needing to repeatedly configure individual loggers.
- By abstracting the logging logic into a function, the application maintains cleaner code and promotes reuse and easier maintenance.

#### Usage Example
```python
log_info("User login successful.")
```
This line would log the message "User login successful." at the info level, assuming that the global logger has been appropriately configured elsewhere in the application.

#### Function: `log_debug`

### Function: `log_debug`

#### Purpose
The `log_debug` function is designed to log debug-level messages to a centralized logging system. This function simplifies the process of debug logging by providing a straightforward interface for developers to log debug information.

#### Behavior and Inputs
- **Input**: The function accepts a single parameter, `message`, which is a string. This string represents the debug message that the developer wishes to log.
- **Output**: There is no direct output returned by the function. Instead, the function sends the message to the logging system at the debug level.

#### Key Logic
- The function utilizes a global logger obtained through the `get_logger()` function. This design assumes the existence of a globally accessible logger configured elsewhere in the application.
- It logs the provided message at the "debug" level, which is typically used for detailed diagnostic information useful for debugging. The actual logging level and handling are managed by the logger configuration, not within this function.

#### Integration in Larger Application
- `log_debug` is part of a larger logging framework within an application. It is specifically tailored for debug messages, allowing developers to segregate debug information from other types of logs such as info, warning, or error messages.
- This function abstracts the complexity of the underlying logging system, offering a simple and consistent method for logging debug information. This can help maintain clean and maintainable code, especially in large applications where many components might emit debug information.
- The function's reliance on a global logger makes it easy to integrate into any part of the application without needing to pass logger instances around.

#### Usage
This function is typically used during development and debugging phases to track the flow of execution and internal state changes. It can be particularly useful in scenarios where understanding the sequence of operations and the data being processed is crucial for diagnosing issues.

#### Function: `log_warning`

### Function: `log_warning`

#### Purpose
The `log_warning` function is designed to log warning-level messages to a centralized logging system. This function is part of a larger application or module that utilizes logging for monitoring and debugging purposes.

#### Behavior and Inputs
- **Input**: The function accepts a single input parameter, `message`, which is a string. This string represents the warning message that needs to be logged.
- **Output**: There is no direct output returned by the function (`None` is implicitly returned). Instead, the function's primary effect is to send the warning message to the application's logging system.

#### Key Logic
- The function utilizes a global logger obtained through the `get_logger()` function. This design choice implies that the logger is configured elsewhere in the application, centralizing the logging configuration and allowing for consistent logging behavior across different parts of the application.
- It logs the message with a severity level of "warning", which is typically used to indicate potential issues or important cautionary messages that do not require immediate action but should be noted and possibly investigated.

#### Integration
- `log_warning` fits into applications where maintaining a log of events, especially warnings, is crucial for operational integrity, troubleshooting, and auditing.
- It is particularly useful in environments where different severity levels of logging are needed to distinguish between informational messages, warnings, and errors.
- This function abstracts the logging complexity and provides a simple interface for other parts of the application to log warnings, ensuring that all warning messages are handled consistently.

This function is a utility that enhances maintainability and readability of the code by encapsulating the logging logic and promoting reuse across the application.

#### Function: `log_error`

### Function: `log_error`

#### Purpose
The `log_error` function is designed to log error-level messages to a centralized logging system. It serves as a utility function within an application to standardize how error messages are recorded, facilitating easier debugging and monitoring of runtime issues.

#### Inputs
- **message (str)**: This is the error message that needs to be logged. The message should be a string that describes the error sufficiently to aid in diagnostics.

#### Behavior
- The function utilizes a globally accessible logger obtained through the `get_logger()` function. This design choice implies that the logger configuration (such as log level, output format, and destination) is set up elsewhere in the application, ensuring that all logged messages adhere to a consistent format and are directed to appropriate log handlers (e.g., files, consoles, external monitoring systems).
- It logs the provided message at the "error" level, which is typically used to record serious issues that could affect the application's functionality or performance. This level of logging is crucial for alerting developers or system administrators about problems that require immediate attention.

#### Integration
- `log_error` is likely part of a larger logging framework within the application, which might include other functions for different levels of logging (e.g., info, warning, debug). This function abstracts the complexity of the underlying logging mechanism, allowing developers to log error messages with a simple function call.
- By using this function, the application's error handling becomes more maintainable and its logs more uniform, which is beneficial for troubleshooting and maintaining the software.

#### Key Design Choices
- The use of a global logger and the abstraction of the logging process into a single function call simplifies the logging process for other parts of the application. This approach reduces the risk of inconsistent logging practices and centralizes the control of how errors are logged, which can be crucial for large-scale or complex applications.

#### Function: `log_and_optionally_raise`

### Function Overview

The function `log_and_optionally_raise` is designed to handle error logging and conditional exception raising within a Python application. It provides a structured way to log messages across different severity levels (error, warning, info) and can also raise exceptions based on the conditions provided by the caller. This function is particularly useful in applications that require detailed logging and error handling, such as web services, data processing pipelines, or any complex system where tracking errors and operational issues is crucial.

### Inputs

The function accepts several parameters to customize its behavior:

- `module` (str): Specifies the high-level module name where the log is being recorded.
- `component` (str): Denotes the specific component or submodule within the larger module.
- `method` (str, optional): Indicates the method name where the logging is taking place.
- `message` (str): The actual message content to be logged or included in the raised exception.
- `error_type` (ErrorType, optional): A predefined enumeration (`ErrorType`) that categorizes the type of error.
- `level` (str, optional): Sets the severity level of the log; defaults to "error" but can also be "warning" or "info".
- `raise_exception` (bool, optional): A flag to determine whether to raise an exception after logging; defaults to False.
- `raise_type` (Exception, optional): Specifies the type of exception to raise if `raise_exception` is True; defaults to `ValueError`.

### Behavior and Key Logic

1. **Message Formatting**: The function first formats the error message by incorporating the module, component, method, error type, and the message itself. This ensures that all logs are consistent and provide enough context to be useful.

2. **Logging**: Based on the `level` parameter, the function logs the formatted message at the appropriate severity level. This is handled by calling one of three functions: `log_error`, `log_warning`, or `log_info`, which would typically interface with a logging framework like Python's built-in `logging` module.

3. **Conditional Exception Raising**: If the `raise_exception` flag is set to True, the function raises an exception of the type specified by `raise_type` with the formatted message as the error message. This allows for flexible error handling where the caller can decide whether an error condition should also halt execution or just be logged.

### Integration into Larger Applications

`log_and_optionally_raise` fits into larger applications as a utility function for error management. It abstracts away the complexities of deciding how to log different types of errors and whether to interrupt the flow of execution by raising exceptions. This can help maintain clean and maintainable code in larger projects, where different modules or components might need standardized ways to handle errors and log information. This function could be part of a larger utility module or library that standardizes logging and error handling across an application.

---

### `capturing_handler.py`

#### Class: `CapturingHandler`

### CapturingHandler Class

#### Overview
`CapturingHandler` is a custom logging handler derived from Python's `logging.Handler`. It is designed to capture log messages into an internal list, allowing for programmatic access to the logs. This functionality is particularly useful for scenarios such as testing, debugging, or any situation where log data needs to be analyzed or manipulated within the application itself, rather than being output to external systems like files or consoles.

#### Attributes
- **records (list):** Holds the formatted log messages captured during the application's runtime.

#### Methods
- **__init__(self):** Initializes a new instance of `CapturingHandler`, setting up an empty list to store log records.
- **emit(self, record):** Processes logging events by capturing and storing formatted log messages. The `record` parameter is a `LogRecord` instance containing metadata about the logging event.
- **get_logs(self) -> list:** Returns a list of all captured log messages, allowing external access to the log data.
- **clear_logs(self):** Clears all stored log messages from the handler, resetting the `records` list to an empty state.

#### Usage
To use `CapturingHandler`, instantiate it and add it to a logger:

```python
import logging

handler = CapturingHandler()
logger = logging.getLogger("test_logger")
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logger.info("Hello, logs!")
print(handler.get_logs())  # Output: ['Hello, logs!']
```

This setup is particularly useful in testing environments where assertions might be made on the output of log messages, or in applications where logs need to be displayed in a user interface or analyzed for specific patterns or errors.

#### Design Choices
- Inherits from `logging.Handler` to leverage the existing logging infrastructure provided by Python.
- Uses a simple list to store log messages, prioritizing ease of access and manipulation over performance concerns that might arise with very large volumes of log data.
- Provides a method to clear logs, which is useful for long-running applications or during testing when logs need to be reset between tests.

#### Integration
`CapturingHandler` can be seamlessly integrated into any Python application using the standard `logging` module. It is especially valuable in modular applications where different components might need to inspect or react to log output programmatically.

---

### `dynamic_logger.py`

#### Class: `DynamicLogger`

### Overview

The `DynamicLogger` class is designed to facilitate advanced logging capabilities in Python applications. It integrates dynamic metadata management, console output, and in-memory log capturing, making it particularly useful for both development and testing environments.

### Key Features and Design

- **Dynamic Metadata Management**: Through integration with a `MetadataManager`, this logger can inject custom metadata into log messages, which enhances the traceability and contextual information of logs.
- **Console and In-Memory Logging**: It supports output to the console and captures logs in memory, aiding in real-time monitoring and post-execution analysis respectively.
- **Customizable Log Formatting**: Utilizes a custom formatter (`DynamicFormatter`) to format log messages by including dynamically injected metadata.
- **Flexible Log Levels**: Allows setting and changing the log level at runtime, supporting standard logging levels (DEBUG, INFO, WARNING, ERROR, CRITICAL).

### Usage

1. **Initialization**: Requires a `MetadataManager` instance to handle metadata. Optionally, a custom logger name can be specified.
2. **Logging**: Messages can be logged with dynamic metadata at various levels using the `log` method.
3. **Log Retrieval and Management**: Logs captured in memory can be retrieved and cleared, which is particularly useful in testing scenarios to assert on logged messages.

### Methods

- `__init__(metadata_manager, logger_name="DynamicLogger")`: Initializes the logger with a metadata manager and a logger name.
- `log(level, message)`: Logs a message at the specified level, enriched with current metadata.
- `get_logs()`: Returns a list of all messages captured in memory.
- `clear_logs()`: Clears all captured log messages.
- `set_log_level(level)`: Dynamically sets the logging level.

### Integration in Applications

`DynamicLogger` can be seamlessly integrated into any Python application where enhanced logging is necessary. It is particularly useful in applications requiring detailed execution traces and in scenarios where logs need to be analyzed or verified post-execution, such as in automated testing.

### Conclusion

The `DynamicLogger` class is a robust solution for applications needing advanced logging with capabilities of metadata injection and log capturing. It supports debugging and testing by providing detailed logs and the ability to inspect logged messages programmatically.

---

### `log_singleton.py`

#### Function: `get_logger`

### Function Overview

The `get_logger` function is designed to manage and provide a singleton instance of a `DynamicLogger`. This function ensures that only one instance of the logger is created and used throughout the application, adhering to the singleton design pattern. This approach is beneficial for consistent logging behavior and resource efficiency across different parts of an application.

### Inputs

- **metadata_manager** (`MetadataManager`, optional): This parameter allows the caller to provide a custom `MetadataManager` instance that the logger will use to handle metadata. If no `metadata_manager` is provided, the function initializes a default `MetadataManager` with predefined metadata values (`project="DefaultProject"` and `table="DefaultTable"`).

### Outputs

- **DynamicLogger**: Returns the singleton instance of `DynamicLogger`. Once initialized, this instance is reused in all subsequent calls to `get_logger`.

### Key Logic and Design Choices

1. **Singleton Implementation**: The function uses a global variable `_logger_instance` to store the logger instance. A lock (`_logger_lock`) ensures that the initialization of this instance is thread-safe, preventing multiple threads from creating multiple instances simultaneously.

2. **Conditional Initialization**: The logger instance is only created if `_logger_instance` is `None`, ensuring that the initialization code runs only once. If a `metadata_manager` is not provided during the first call, the function sets up a default `MetadataManager` with basic metadata, which is then used to initialize the `DynamicLogger`.

3. **Metadata Management**: The ability to pass a custom `MetadataManager` allows for flexible configuration of the logger based on different application needs, making the logging context-aware and potentially more useful for debugging and monitoring.

### Integration into Larger Applications

The `get_logger` function is typically used in applications that require consistent logging mechanisms across various components or modules. By using a singleton pattern, the application ensures that all components use the same logger instance, which simplifies configuration management, reduces memory overhead, and maintains consistent logging behavior (e.g., file outputs, formats, levels). This function is particularly useful in complex applications with multiple execution threads or processes where consistent logging is crucial for monitoring and debugging purposes.

#### Function: `refresh_logger`

### Function Overview

The function `refresh_logger` is designed to reset and reinitialize a global logger instance, typically used in applications where logging behavior needs to be dynamically adjusted during runtime. This function is particularly useful in scenarios such as testing or when different parts of an application require different logging configurations.

### Inputs

- **metadata_manager** (`MetadataManager`, optional): This parameter allows the user to specify a new metadata manager that the logger should use. The metadata manager is responsible for handling metadata associated with logging, such as project names, versions, or other contextual information. If no metadata manager is provided (`None`), the function defaults to creating or using a standard predefined metadata manager.

### Outputs

- **DynamicLogger**: The function returns an instance of `DynamicLogger`. This returned object is a reinitialized logger configured with the specified or default metadata manager.

### Key Logic and Design Choices

1. **Global Logger Instance**: The function manipulates a global logger instance (`_logger_instance`). This design choice suggests that the logger is intended to be accessed and used across different parts of the application, maintaining a consistent logging interface.

2. **Thread Safety**: The function uses a lock (`_logger_lock`) to ensure that the reset and reinitialization process is thread-safe. This prevents race conditions and ensures that changes to the logger instance are atomic, which is crucial in multi-threaded environments.

3. **Dynamic Reconfiguration**: By allowing the metadata manager to be replaced at runtime, the function supports dynamic reconfiguration of the logger. This is essential for applications that need to adapt their logging behavior based on runtime conditions or configurations.

### Integration in Larger Applications

In a larger application or module, `refresh_logger` would be part of a logging framework designed to offer flexible and dynamic logging capabilities. It would be used in situations where the logging configuration needs to be adjusted on-the-fly without stopping or restarting the application. Examples include changing the logging level or format based on user input, switching the logging context in multi-tenant applications, or updating metadata in response to changes in the application state or environment.

This function facilitates better control over logging, making it easier to manage and maintain large applications, especially those that require detailed and context-sensitive logging for debugging, monitoring, and auditing purposes.

---

### `error_utils.py`

#### Function: `format_error`

### Function Overview

The `format_error` function is designed to create a standardized error message format that is useful for logging and exception handling within a software application. This function ensures that error messages are consistent and structured, making them easier to read and debug.

### Inputs

The function accepts five parameters:
- `module` (str): The name of the high-level module where the error occurred, such as 'CONNECTOR'.
- `component` (str): The specific class or component within the module, like 'AzureBlobConnector'.
- `method` (str): The method or function name where the error was encountered, for example, 'load_file'.
- `error_type` (ErrorType): An enumeration (`ErrorType`) that categorizes the type of error.
- `message` (str): A detailed, human-readable description of the error.

### Output

The function returns a string that formats these inputs into a structured message. The format of the output string is:
```
'[MODULE].[COMPONENT].[METHOD] - ERROR TYPE: [ERROR_TYPE]: [MESSAGE]'
```
This structured format includes the module, component, and method context, followed by the error type and the descriptive message. This format helps in identifying the exact location and nature of the issue quickly.

### Key Logic and Design Choices

- **Use of Enums for Error Types**: The function uses an enumeration for the `error_type` parameter, which standardizes the possible error types and prevents typos or inconsistent error labeling.
- **Structured Message Format**: By structuring the error message in a consistent format, the function aids in maintaining uniformity in logs, which is beneficial for error tracking and analysis.

### Integration in Larger Applications

In a larger application, `format_error` can be integrated into error handling and logging mechanisms. It can be used across various modules and components to ensure that all errors are logged in a uniform format, facilitating easier monitoring, troubleshooting, and analysis of errors across the system. This function is particularly useful in applications where clarity and consistency in error reporting are crucial, such as in large-scale enterprise applications or systems with multiple interconnected components.

---
