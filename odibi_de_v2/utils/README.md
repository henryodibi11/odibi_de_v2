# utils Module
The `utils/` module provides a suite of lightweight, framework-agnostic utility functions for use throughout the `odibi_de_v2` data engineering framework. These utilities help enforce consistency, reduce duplication, and enable environment-aware automation.
---
## Modules
| Module                | Description                                                                 |
|-----------------------|-----------------------------------------------------------------------------|
| `type_checks.py`      | Type and structure validation (e.g., is_empty_dict, is_boolean)             |
| `string_utils.py`     | String and column name transformations, normalization, and cleaning         |
| `file_utils.py`       | File path parsing, extension validation, and folder extraction              |
| `validation_utils.py` | Reusable schema and key validation for configs, dataframes, and formats     |
| `env_utils.py`        | Execution context detection (e.g., local vs. Databricks) and env access     |
| `method_chain.py`     | Run chained method calls on objects with optional validation and logging    |
---

## Usage Examples

### `type_checks`
- `is_empty_dict`
   - Checks if the given object is an empty dictionary.

   ```python 
        is_empty_dict({})
            True
        is_empty_dict({'a': 1})
            False
        is_empty_dict([]) # Not a dict
            False
   ```
- `is_valid_type`
   - Checks whether an object is an instance of the expected type(s).
   ```python
         is_valid_type(5, (int, float))
            True
         is_valid_type("5", (int, float))
            False
         is_valid_type(None, (type(None), str))
                True
   ```
- `is_non_empty_string`
   - Checks if a value is a non-empty string.
   ```python
         is_non_empty_string("hello")
            True
         is_non_empty_string("")
            False
         is_non_empty_string(None)
            False
         is_non_empty_string(1)
            False
   ```
- `is_boolean`
   - Checks if a value is a boolean.
   ```python
         is_boolean(True)
            True
         is_boolean("True")
            False
   ```
### `validation_utils`
- `validate_required_keys`
   - Checks whether all required keys exist in the provided dictionary.
   ```python
         validate_required_keys({"a": 1, "b": 2}, ["a", "b"])
            True
   ```
- `validate_columns_exist`
   - Checks whether all required columns exist in a tabular object.
   ```python
      class Dummy:
         columns = ["id", "name", "value"]
      validate_columns_exist(Dummy(), ["id", "value"])
   ```
- `validate_supported_format`
   - Validates whether a file path has a supported extension.
   ```python
         validate_supported_format("data/file.csv", ["csv", "json"])
            True
   ```
- `validate_non_empty_dict`
   - Validates that a dictionary is not empty. Logs and raises if it is.
   ```python
         validate_non_empty_dict({"a": 1}, "config check")
            # No error

         validate_non_empty_dict({}, "config check")
            ValueError: config check cannot be an empty dictionary.
   ```
- `validate_method_exists`
   - Validates that the object has the specified method. Logs a warning if it does not.
   ```python
         class Dummy:
             def my_method(self): pass

         validate_method_exists(Dummy(), "my_method", context="test")
            # No warning

         validate_method_exists(Dummy(), "nonexistent_method", context="test")
            # Logs a warning: Method 'nonexistent_method' not found on Dummy. Skipping...
   ```
- `validate_kwargs_match_signature`
   - Validates that the provided kwargs match the method's signature.
   ```python
         def greet(name, age): pass
         validate_kwargs_match_signature(greet, {"name": "Alice"}, "greet", "example")
            # No error
         validate_kwargs_match_signature(greet, {"foo": 123}, "greet", "example")
            ValueError: Invalid kwargs for 'greet': ['foo']
   ```
### `string_utils`
- `normalize_string`
   - Strips leading/trailing whitespace and converts to a specified case.
   ```python
         normalize_string("  Hello ", case="upper")
            'HELLO'
   ```
- `clean_column_name`
   - Cleans a column name by replacing non-alphanumeric characters with underscores and converting to lowercase snake_case.
   ```python
         clean_column_name("Total Sales ($)")
            'total_sales'
   ```
- `standardize_column_names`
   - Applies `clean_column_name` to a list of column names.
   ```python
         standardize_column_names(["First Name", "Age (Years)"])
            ['first_name', 'age_years']
   ```
- `to_snake_case`
   - Converts CamelCase or PascalCase to snake_case.
   ```python
         to_snake_case("CamelCaseString")
            'camel_case_string'
   ```
- `to_kebab_case`
   - Converts text to kebab-case (lowercase words separated by hyphens).
   ```python
         to_kebab_case("My Variable Name")
            'my-variable-name'
   ```
- `to_camel_case`
   - Converts a string to camelCase.
   ```python
         to_camel_case("my variable name")
            'myVariableName'
   ```
- `to_pascal_case`
   - Converts a string to PascalCase.
   ```python
         to_pascal_case("my variable name")
            'MyVariableName'
   ```
- `remove_extra_whitespace`
   - Removes extra internal whitespace from a string.
   ```python
         remove_extra_whitespace("  Hello   World  ")
            'Hello World'
   ```
- `is_null_or_blank`
   - Checks if a string is None, empty, or whitespace only.
   ```python
         is_null_or_blank("   ")
            True
   ```
- `slugify`
   - Converts a string into a safe format for file names or identifiers.
   ```python
         slugify("Energy Report - 2025!")
            'energy_report_2025'
   ```
### `method_chain`
- `apply_dynamic_method`
   - Dynamically applies a method to an object using various types of arguments.
   ```python
         def add(a, b):
            return a + b

         apply_dynamic_method(None, add, {"a": 2, "b": 3})
            5
         def echo(x): return x
         apply_dynamic_method(None, echo, "hello")
            'hello'
         def combine(a, b, c): return a + b + c
         apply_dynamic_method(None, combine, [1, 2, 3])
            6
   ```
- `run_method_chain`
   - Applies a chain of method calls to a given object based on a dictionary of method names and corresponding arguments.
   ```python
         import pandas as pd
         df = pd.DataFrame({"name": ["Alice", None], "score": [90, None]})
         method_chain = {
                "fillna": {"value": {"name": "unknown", "score": 0}},
                "rename": {"columns": {"score": "final_score"}}
            }
         run_method_chain(df, method_chain)
   ```
### `file_utils`
- `get_file_extension`
   - Extracts the file extension from a path.
   ```python
         get_file_extension("data/file.json")
            'json'
   ```
- `get_stem_name`
   - Returns the file name without the extension.
   ```python
         get_stem_name("folder/data.csv")
            'data'
   ```
- `extract_file_name`
   - Returns the full file name from the path (including extension).
   ```python
         extract_file_name("data/folder/sample.parquet")
            'sample.parquet'
   ```
- `extract_folder_name`
   - Returns the parent folder name from a file path.
   ```python
         extract_folder_name("data/folder/file.csv")
            'folder'
   ```
- `validate_required_keys`
   - is_valid_file_path
   ```python
         is_valid_file_path("folder/data.csv")
               True
   ```
- `is_supported_format`
   - Checks if a file has one of the supported extensions.
   ```python
         is_supported_format("data/file.avro", ["csv", "avro"])
               True
   ```
### `env_utils`
- `is_running_in_databricks`
   - Detects whether the current code is running in a Databricks environment.
   ```python
         is_running_in_databricks()
               True
   ```
- `is_running_in_notebook`
   - Detects whether the code is running in a notebook environment (Jupyter or IPython).
   ```python
         is_running_in_notebook()
               True
   ```
- `is_local_env`
   - Checks if the current environment is local (i.e., not in Databricks or cloud).
   ```python
         is_local_env()
            True
   ```
- `get_current_env`
   - Returns a string label describing the current environment.
   ```python
         get_current_env()
               'local'
   ```
- `get_env_variable`
   -  Safely retrieves an environment variable, with optional default fallback.
   ```python
         get_env_variable("STORAGE_ACCOUNT", "default_account")
               'default_account'
   ```

