# Module Overview

### `dbutils_helpers.py`

#### Function: `get_secret`

### Function Overview

The `get_secret` function is designed to securely retrieve secret values, such as API keys or database credentials, from a Databricks secret scope. This function simplifies the process of accessing sensitive information stored in Databricks, ensuring that such data can be fetched securely and efficiently within your applications.

### Inputs and Outputs

- **Inputs**:
  - `scope` (str): The name of the Databricks secret scope from which the secret is to be retrieved.
  - `key` (str): The specific key within the scope that identifies the secret value.

- **Output**:
  - Returns the secret value as a string associated with the specified `key` within the given `scope`.

### Key Logic and Design Choices

- The function utilizes the `DBUtils` and `SparkSession` from the PySpark library to interact with the Databricks environment. This choice leverages Databricks' built-in utilities to handle the complexities of secret management.
- It creates an instance of `DBUtils` by initializing a `SparkSession`, which is necessary to access the Databricks backend securely and efficiently.
- The function directly returns the result of `dbutils.secrets.get(scope, key)`, which fetches the secret value from the specified scope and key. This approach minimizes the function's footprint and keeps the operation straightforward.

### Integration into Larger Applications

- `get_secret` can be integrated into any Python application that requires secure access to sensitive configurations or credentials within a Databricks environment.
- It is particularly useful in scenarios where API keys, database passwords, or other sensitive data need to be retrieved without hard-coding them into the source code, thus enhancing security and maintainability.
- This function is essential in environments where security and compliance are priorities, as it ensures that sensitive data is accessed securely and only when necessary.

### Usage Example

```python
from databricks.utils.dbutils_helpers import get_secret
account_name = get_secret("GOATKeyVault", "GoatBlobStorageName")
account_key = get_secret("GOATKeyVault", "GoatBlobStorageKey")
```

### Additional Notes

- Proper configuration of the Databricks environment, including the availability of `DBUtils` and `SparkSession`, is crucial for the successful operation of this function.
- Error handling is built into the function to raise exceptions if the secret scope or key does not exist, or if there are issues with the Databricks environment, thus aiding in troubleshooting and robustness of application development.

This function is a critical component for applications that rely on Databricks for managing and accessing secrets securely, helping maintain the integrity and security of the application's sensitive data.

---

### `api_handlers.py`

#### Function: `call_api_core`

### Function Overview

The `call_api_core` function is designed to facilitate REST API interactions by sending HTTP GET requests and handling the responses. It is particularly useful in data processing contexts, such as with Pandas or Spark frameworks, where data from APIs often needs to be integrated and manipulated.

### Inputs

- **url (str)**: The URL of the API endpoint. This is the only mandatory parameter and specifies where the request should be sent.
- **params (dict, optional)**: A dictionary containing any query parameters that need to be included in the API request. Defaults to `None` if not provided.
- **headers (dict, optional)**: A dictionary of HTTP headers to accompany the request. This can include headers for authentication, content type, etc. Defaults to `None`.
- **timeout (int, optional)**: The maximum duration in seconds that the function will wait for a server response before aborting the request. The default timeout is set to 30 seconds.

### Outputs

- **list[dict]**: The function returns a list of dictionaries, where each dictionary represents a JSON object (record) returned from the API. This format is particularly useful for direct consumption into data processing libraries.

### Key Logic and Design Choices

1. **Error Handling**: The function includes robust error handling, where any exceptions raised during the request process (e.g., network issues, invalid responses, or timeouts) result in a `RuntimeError`. This exception encapsulates the original error, making it easier for calling functions to manage errors gracefully.

2. **Response Parsing**: The function assumes that the JSON response contains a key `response` which itself contains a key `data` holding the actual data list. This is a specific design choice that might need adjustment based on the actual API response structure.

3. **Logging**: Before making the API request, the function logs the action, which is crucial for debugging and monitoring API interactions in production environments. The logging functionality (`log_and_optionally_raise`) is hinted at but not fully detailed in the provided code.

### Integration into Larger Applications

`call_api_core` can serve as a foundational utility in larger applications or modules that require data from external APIs. It abstracts away the complexities of making HTTP requests and handling JSON responses, allowing developers to focus on higher-level application logic. This function can be directly used or extended in various scenarios, such as data synchronization tasks, backend services in web applications, or data ingestion pipelines in data analytics platforms.

### Example Usage

```python
data = call_api_core("https://api.example.com/data", params={"q": "sales"})
print(data)
# Output: [{'id': 1, 'product': 'Widget', 'sales': 100}, {'id': 2, 'product': 'Gadget', 'sales': 150}]
```

This example demonstrates how to call the function with a specific API endpoint and query parameters, illustrating the simplicity and effectiveness of the function in retrieving and parsing API data.

---

### `api_ingestion.py`

#### Function: `prepare_api_reader_kwargs_from_config`

### Function Overview

The function `prepare_api_reader_kwargs_from_config` is designed to facilitate API data ingestion by constructing a set of keyword arguments (kwargs) necessary for making API requests. This function is particularly tailored for integration with Databricks environments, utilizing `dbutils` for secure secret management.

### Inputs

The function accepts two parameters:
1. `config`: A dictionary that includes:
   - `connection_config`: A nested dictionary containing the base URL (`base_url`), query parameters (`query_params`), and optionally additional settings (`extra_config`).
   - `source_path_or_query`: A string specifying the API endpoint or query.
2. `dbutils`: An object provided by Databricks used for accessing securely stored secrets.

### Outputs

The function returns a dictionary containing the following keys:
- `url`: The complete URL constructed for the API request.
- `params`: A dictionary of query parameters for the request.
- `headers`: A dictionary containing request headers, including authentication details.
- `pagination_type`: (Optional) Specifies the pagination method used by the API.
- `page_size`: (Optional) Defines the number of records per page, defaulting to 5000 if not specified.
- `record_path`: (Optional) Indicates the JSON path to the desired records within the API response.

### Key Logic and Design Choices

1. **URL Construction**: The function constructs the full URL by combining the `base_url` and `source_path_or_query`, ensuring no double slashes.
2. **Secret Management**: Utilizes `dbutils` to securely fetch and apply authentication secrets, which are then used to build the request headers.
3. **Flexible Authentication**: Supports different authentication methods (e.g., API keys), and the function dynamically adjusts the authentication headers or query parameters based on the provided secrets.
4. **Pagination and Record Path Handling**: Extracts optional settings for pagination and record paths from `extra_config`, allowing for flexible API interaction based on the API's capabilities and requirements.

### Integration and Usage

This function is a utility designed for use in larger applications or modules that interact with APIs, particularly in data ingestion pipelines within Databricks environments. It abstracts the complexity of setting up API requests, making it easier to configure and execute data ingestion tasks from various APIs by simply providing a configuration dictionary.

### Example Usage

```python
config = {
    "connection_config": {
        "base_url": "https://api.example.com",
        "query_params": {"limit": 100},
        "extra_config": {"pagination_type": "offset", "page_size": 100}
    },
    "source_path_or_query": "data/v1/records"
}
kwargs = prepare_api_reader_kwargs_from_config(config, dbutils)
print(kwargs)
```

This example demonstrates how to prepare the keyword arguments for an API request to fetch data from a specified endpoint with pagination support, using the function to handle the complexities of URL and header construction, as well as secret management.

---

### `api_auth.py`

#### Function: `load_api_secrets`

### Function Overview

The `load_api_secrets` function is designed to retrieve and organize API authentication secrets from a centralized secret management system. It supports multiple authentication schemes, including API key, token, and basic authentication. This function is crucial for securely managing access to external APIs or databases within an application, ensuring that sensitive credentials are handled securely and are easily configurable.

### Inputs

The function accepts a single input parameter, `secret_config`, which is a dictionary containing configuration details necessary for retrieving the secrets. The configuration keys include:

- `secret_scope`: Mandatory. Defines the scope under which the secrets are stored.
- `credential_key`: Optional. Key for retrieving API credentials.
- `identifier_key`: Optional. Key for retrieving identifiers for basic authentication.
- `connection_string_key`: Optional. Key for retrieving database connection strings.
- `database_key`: Optional. Key for retrieving database access credentials.
- `token_header_name`: Optional. Specifies the header name where the token should be placed.
- `auth_type`: Optional. Specifies the type of authentication required; defaults to 'key'.
- `description`: Optional. Provides additional notes or descriptions regarding the API secrets.

### Outputs

The function returns a dictionary containing all the resolved secrets and additional metadata. The keys in the output dictionary include:

- `credential`: Contains the API credentials if `credential_key` is provided.
- `identifier`: Contains the identifier for basic authentication if `identifier_key` is provided.
- `connection_string`: Contains the database connection string if `connection_string_key` is provided.
- `database`: Contains database access credentials if `database_key` is provided.
- `token_header_name`: Directly passed through from the input if provided.
- `auth_type`: The type of authentication required, with a default of 'key'.
- `description`: Description or notes regarding the API secrets, directly passed through from the input.

### Key Logic and Design Choices

The function leverages a helper function `get_secret` to fetch secrets from the secret management system using the provided keys and scope. It conditionally retrieves each secret based on the presence of its corresponding key in the `secret_config` dictionary. This design allows for flexibility in specifying which secrets are needed for a particular API or database connection, making the function adaptable to various use cases.

Metadata such as `auth_type` and `description` are included in the output to provide context about the authentication method used and additional information about the secrets, which can be useful for debugging or auditing purposes.

### Integration into Larger Applications

In a larger application, `load_api_secrets` would typically be part of a module responsible for API integration or database connectivity. It would be called during the initialization or configuration phase of the application to set up connections to external services securely. By centralizing secret retrieval, the function promotes a clean and maintainable codebase, reducing the risk of security mishaps such as hard-coded credentials.

#### Function: `build_api_auth_headers`

### Function Overview

The `build_api_auth_headers` function is designed to generate HTTP headers required for authenticating API requests. It supports three authentication methods: token-based, basic authentication, and key-based. The function dynamically constructs the appropriate headers based on the specified authentication type provided in the input dictionary.

### Inputs

The function accepts a single input parameter:
- `secrets`: A dictionary that contains the necessary details for constructing authentication headers. The expected keys in this dictionary vary based on the authentication type:
  - `auth_type`: Specifies the type of authentication (`token`, `basic`, or `key`).
  - `credential`: Contains the token or password, depending on the authentication type.
  - `identifier`: Required for basic authentication, it represents the username.
  - `token_header_name`: Optional for token-based authentication, it specifies the header name to use for the token.

### Outputs

The function returns a dictionary containing the HTTP headers needed for the specified authentication type. The structure of the returned headers depends on the `auth_type`:
- For `token` authentication, the header typically named "Authorization" (or another name specified by `token_header_name`) will contain the token.
- For `basic` authentication, the "Authorization" header will include a Base64-encoded string of the username and password.
- For `key` authentication, the function returns an empty dictionary since it assumes credentials are passed as query parameters, not in headers.

### Key Logic and Design Choices

- **Dynamic Header Construction**: The function uses conditional logic based on the `auth_type` to construct the appropriate headers. This design allows for easy extension to other authentication methods if needed.
- **Default Values**: For token-based authentication, if no `token_header_name` is provided, it defaults to using "Authorization".
- **Error Handling**: The function assumes that the necessary keys are present in the `secrets` dictionary. If keys are missing (e.g., `credential` for token-based authentication), it could raise a `KeyError`. Implementing error handling or validation could be considered to make the function more robust.

### Integration into Larger Applications

This function is typically used in applications that interact with external APIs requiring authenticated requests. It can be integrated into API client modules or services where constructing headers dynamically based on authentication type is necessary. This utility function simplifies the process of preparing requests by abstracting the details of how different authentication headers are constructed and applied.

---

### `metadata_helpers.py`

#### Function: `add_ingestion_metadata`

### Function Overview: `add_ingestion_metadata`

#### Purpose
The `add_ingestion_metadata` function is designed to enhance a DataFrame by appending two audit metadata columns: `Updated_Timestamp` and `Created_Timestamp`. These columns are used to track the creation and last update times of each record within the DataFrame.

#### Behavior
- **Input**: The function accepts a single argument, `df`, which is a DataFrame expected to contain any number of pre-existing columns.
- **Output**: It returns a new DataFrame that includes the original set of columns plus the two new timestamp columns. Both `Updated_Timestamp` and `Created_Timestamp` are set to the current timestamp at the moment the function is executed.

#### Key Logic
- The function utilizes the `withColumn` method of the DataFrame to add each new column.
- The `current_timestamp()` function is called twice to generate the exact current timestamp for both the `Updated_Timestamp` and `Created_Timestamp` columns.

#### Integration into Larger Applications
- This function is typically used in data ingestion pipelines where tracking the ingestion and update time of records is crucial for data auditing and lineage tracing.
- It can be particularly useful in scenarios involving data synchronization, logging, or when implementing features that rely on timestamp-based filtering or sorting.

#### Example Usage
```python
original_df.show()
# Output:
# +----+-----+
# | id | name|
# +----+-----+
# |  1 | John|
# |  2 | Jane|
# +----+-----+

new_df = add_ingestion_metadata(original_df)
new_df.show()
# Output:
# +----+-----+-------------------+-------------------+
# | id | name| Updated_Timestamp | Created_Timestamp |
# +----+-----+-------------------+-------------------+
# |  1 | John| 2023-12-01 12:00  | 2023-12-01 12:00  |
# |  2 | Jane| 2023-12-01 12:00  | 2023-12-01 12:00  |
# +----+-----+-------------------+-------------------+
```

This function is a straightforward yet powerful tool for adding essential auditability features to data processing workflows, ensuring that each data record can be effectively tracked through its lifecycle within a system.

#### Function: `add_hash_columns`

### Function Overview

The function `add_hash_columns` is designed to enhance a DataFrame by adding two new columns, `Merge_Id` and `Change_Id`, which are SHA-256 hash values computed from specified groups of columns. This function is particularly useful in data processing workflows where tracking and managing unique row identities and detecting changes in data rows are crucial.

### Inputs and Outputs

- **Inputs:**
  - `df (DataFrame)`: The input DataFrame to which the hash columns will be appended.
  - `merge_keys (list of str)`: A list of column names from the DataFrame used to compute the `Merge_Id`. These columns should uniquely identify each row.
  - `change_columns (list of str)`: A list of column names used to compute the `Change_Id`. This hash helps in identifying changes in these columns, which might trigger updates or special handling.

- **Output:**
  - The function returns a new DataFrame that includes the original data along with the two new columns:
    - `Merge_Id`: A SHA-256 hash computed from the values in the `merge_keys` columns.
    - `Change_Id`: A SHA-256 hash computed from the values in the `change_columns`.

### Key Logic and Design Choices

- **Hashing Mechanism:** The function utilizes the SHA-256 hashing algorithm, ensuring a robust method for generating unique identifiers and change detection tokens. This choice helps in minimizing collisions (i.e., different inputs producing the same hash) and enhances the reliability of the identifiers.
- **Column Concatenation:** Before hashing, the values of specified columns are concatenated using a delimiter (`"||"`). This ensures that the hash values are sensitive to both the content and the order of the columns, thereby accurately reflecting the uniqueness and changes in the data.

### Integration into Larger Applications

This function can be a critical component in data integration, migration, and synchronization tasks, especially:
- **Data Deduplication:** By using `Merge_Id`, duplicate records can be easily identified and managed.
- **Change Tracking:** The `Change_Id` helps in identifying rows that have undergone changes, which is vital in incremental data loading and synchronization scenarios.

### Usage Example

The provided example in the documentation demonstrates how to apply the function to a sample DataFrame with a simple structure. It clearly shows how the new hash columns are integrated into the existing DataFrame, making it easy for developers to understand and implement in their own data processing pipelines.

In summary, `add_hash_columns` is a utility function that enhances data management capabilities in applications dealing with large and complex datasets where row uniqueness and change detection are essential.

---
