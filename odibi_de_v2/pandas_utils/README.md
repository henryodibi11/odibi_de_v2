# Module Overview

### `validation.py`

#### Function: `is_pandas_dataframe`

### Function: `is_pandas_dataframe`

#### Purpose
The `is_pandas_dataframe` function is designed to determine whether a given object is an instance of a pandas DataFrame. This utility function is essential for validating inputs in data processing applications where specific operations are contingent on the object being a DataFrame.

#### Inputs and Outputs
- **Input:**
  - `obj (Any)`: This parameter accepts an object of any type. The function will check if this object is a pandas DataFrame.
- **Output:**
  - `bool`: The function returns a boolean value. It outputs `True` if the input object is a pandas DataFrame, and `False` otherwise.

#### Key Logic
The function utilizes Python's built-in `isinstance()` function to check the type of the object. It compares the type of `obj` against `pd.DataFrame`, which is the class type for pandas DataFrames.

#### Integration in Larger Applications
In larger applications, especially those involving data manipulation and analysis, `is_pandas_dataframe` can be used to ensure that functions expecting a DataFrame are indeed provided with one. This can prevent runtime errors and ensure that data processing pipelines are robust and error-tolerant. It is particularly useful in scenarios where data might come from varied sources or formats, and strict type checking is necessary to maintain the integrity of data workflows.

#### Function: `has_columns`

### Function Overview

The function `has_columns` is designed to verify the presence of specified columns in a pandas DataFrame. This utility function is essential for data validation in data processing and analysis applications, ensuring that dataframes meet expected structural criteria before proceeding with further operations.

### Inputs and Outputs

- **Inputs:**
  - `df (pd.DataFrame)`: The DataFrame to be checked.
  - `required_columns (List[str])`: A list of strings representing the names of the columns that are required to be present in the DataFrame.

- **Output:**
  - `bool`: The function returns `True` if all specified columns are present in the DataFrame, and `False` otherwise.

### Key Logic

The function utilizes Python's `all()` function combined with a generator expression to efficiently check for the presence of each column listed in `required_columns` within the `df.columns`. This approach is both concise and efficient, as `all()` will short-circuit and return `False` as soon as a column is found missing, thereby avoiding unnecessary checks.

### Integration in Larger Applications

`has_columns` can be integrated into data validation frameworks, ETL (Extract, Transform, Load) pipelines, and data preprocessing modules where verifying the structure of incoming or manipulated data is crucial. It helps in early detection of data issues, preventing errors in downstream processes due to missing columns. This function is particularly useful in scenarios where data schemas are expected to be consistent but might vary due to errors in data collection, merging of data sources, or changes in data provider APIs.

#### Function: `is_empty`

### Function Overview

The function `is_empty` is designed to determine whether a given pandas DataFrame is empty, meaning it contains no rows. This utility function is essential for data validation and preprocessing steps in data analysis and processing pipelines.

### Inputs and Outputs

- **Input**: The function accepts a single argument, `df`, which is expected to be a pandas DataFrame.
- **Output**: It returns a boolean value. The output is `True` if the DataFrame is empty (i.e., contains no rows), and `False` otherwise.

### Key Logic

The function leverages the `empty` attribute of the pandas DataFrame, which is a built-in property that directly checks whether the DataFrame is empty. This approach is efficient and concise, as it uses the DataFrame's inherent properties rather than implementing a custom check.

### Integration into Larger Applications

In larger applications or modules that involve data manipulation and analysis, `is_empty` can be used to quickly check dataframes before performing operations that require non-empty datasets. This can help avoid errors and ensure that subsequent functions or methods that depend on non-empty inputs operate correctly. It is particularly useful in scenarios where data may dynamically change or be sourced from external inputs where the presence of actual data cannot be guaranteed. 

By using `is_empty`, developers can implement conditional logic to handle or log empty dataframes appropriately, enhancing the robustness and reliability of data-driven applications.

#### Function: `has_nulls_in_columns`

### Function Overview

The function `has_nulls_in_columns` is designed to determine if any specified columns within a pandas DataFrame contain null values. This utility function is crucial for data validation and preprocessing in data analysis and machine learning workflows.

### Inputs and Outputs

- **Inputs**:
  - `df` (pd.DataFrame): The DataFrame in which to check for null values.
  - `cols` (List[str]): A list of strings representing the column names to check within the DataFrame.

- **Output**:
  - Returns a boolean (`bool`): 
    - `True` if any of the specified columns contain null values.
    - `False` otherwise.

### Key Logic and Design Choices

1. **Column Existence Check**: The function includes a safeguard to check if the column names listed in `cols` actually exist in the DataFrame `df`. This prevents potential errors from trying to access non-existent columns.
   
2. **Use of `any()` Function**: The function utilizes Python's built-in `any()` function to efficiently check for the presence of nulls across multiple columns. This is done by iterating over each column specified in `cols` and checking if any element in these columns is null using the `isnull().any()` method from pandas.

### Integration into Larger Applications

- **Data Cleaning**: This function can be integrated into data cleaning pipelines to quickly identify columns that require handling of missing values before further processing.
- **Data Validation**: Before performing operations that assume complete data (like certain types of statistical analyses or machine learning algorithms), this function can be used to validate that the necessary columns are free of nulls.
- **Conditional Logic**: In scripts or applications, this function can be used to branch logic, executing different code paths depending on whether null values are present in critical columns.

In summary, `has_nulls_in_columns` is a practical utility for managing and validating the integrity of data sets in Python-based data analysis environments.

#### Function: `has_duplicate_columns`

### Function Documentation: `has_duplicate_columns`

#### Purpose
The `has_duplicate_columns` function is designed to determine if a pandas DataFrame contains any duplicate column names. This utility is crucial for data integrity checks, especially before operations that assume unique column names (like pivoting, setting indices, merging, etc.).

#### Inputs and Outputs
- **Input**: 
  - `df` (pd.DataFrame): The DataFrame to be checked for duplicate column names.
- **Output**: 
  - `bool`: Returns `True` if there are duplicate column names in the DataFrame, otherwise `False`.

#### Key Logic
- The function utilizes Python's `collections.Counter` to tally occurrences of each column name in the DataFrame.
- It then checks if any of the column name counts exceed 1, indicating a duplicate.

#### Design Choices
- Using `Counter` from the collections module is an efficient way to count occurrences and simplifies the implementation.
- The function returns a boolean value, providing a straightforward interface that can be easily integrated into conditional statements or assertions in a larger application.

#### Integration into Larger Applications
- This function can be part of a data preprocessing module, ensuring data frames are properly formatted and ready for analysis or processing.
- It is particularly useful in data validation layers or during the initial data loading phase, where ensuring the uniqueness of column names can prevent downstream errors in data manipulation and analysis tasks.

#### Function: `is_flat_dataframe`

### Function Overview

The function `is_flat_dataframe` is designed to determine whether a given pandas DataFrame contains only flat data, meaning it does not have any columns that include nested structures like dictionaries or lists. This is particularly useful in data processing and analytics contexts where nested data structures can complicate or invalidate certain types of operations.

### Inputs and Outputs

- **Input**: The function accepts a single argument, `df`, which is expected to be a pandas DataFrame.
- **Output**: It returns a boolean value. The function returns `True` if the DataFrame is flat (i.e., devoid of nested structures in any of its columns); otherwise, it returns `False`.

### Key Logic and Design Choices

- **Column Iteration**: The function iterates over each column in the DataFrame.
- **Type Checking**: For each column, it applies a lambda function to check if any element in the column is an instance of a dictionary or a list.
- **Early Termination**: If any column contains a nested structure, the function immediately returns `False`. If no such structures are found after checking all columns, it returns `True`.

### Integration into Larger Applications

This function can be a critical component in data validation or preprocessing stages of a data pipeline, ensuring compatibility with operations or algorithms that require flat data structures. It can be used to:
- Validate datasets before processing them with tools or libraries that do not support nested data types.
- Ensure data integrity and format consistency before data transformation or analysis tasks.
- Serve as a utility function within larger data cleaning or preparation modules.

By confirming that all data in a DataFrame is flat, developers can avoid runtime errors and ensure smoother operation of data processing workflows that are not designed to handle nested data structures.

---

### `flatten.py`

#### Function: `flatten_json_columns`

### Function Overview

The `flatten_json_columns` function is designed to handle the flattening of nested JSON structures within specified columns of a Pandas DataFrame. This function is particularly useful in data preprocessing stages where nested JSON fields need to be expanded into a flat table format for easier analysis and processing.

### Inputs and Outputs

- **Inputs**:
  - `df` (pd.DataFrame): The input DataFrame containing one or more columns with nested dictionaries (JSON-like structures).
  - `json_columns` (list[str], optional): A list specifying which columns in the DataFrame contain the nested JSON to be flattened. If this parameter is not provided, the function automatically identifies all DataFrame columns containing dictionary objects.
  - `sep` (str): A string separator used in naming the new columns generated from the nested JSON keys. The default separator is an underscore (`_`).

- **Output**:
  - The function returns a new Pandas DataFrame where all specified JSON columns have been flattened. Each key from the nested JSON becomes a new column in the DataFrame, with the column names created by combining the original JSON column name and the nested keys, separated by the specified `sep`.

### Key Logic and Design Choices

1. **Column Detection**: If no specific columns are provided for flattening, the function identifies columns containing dictionary objects by examining each column in the DataFrame. This is done using a lambda function that checks if any cell in a column is an instance of a dictionary.

2. **Flattening Process**:
   - For each column identified for flattening, the function uses `pd.json_normalize` to convert nested dictionary structures into a flat table (DataFrame).
   - New column names are generated by prefixing the original column name followed by the separator and the nested key names.
   - The original JSON column is removed from the DataFrame, and the newly flattened DataFrame is concatenated with the original DataFrame.

3. **Data Integrity**: The function operates on a copy of the input DataFrame to ensure that the original data is not modified, preserving data integrity.

### Integration into Larger Applications

The `flatten_json_columns` function can be integrated into data processing pipelines where JSON data extracted from databases, APIs, or file systems needs to be transformed into a structured format suitable for data analysis, machine learning models, or reporting tools. This function simplifies the handling of nested data structures, making it easier to integrate JSON data with other data sources and tools in a data processing workflow.

---

### `columns.py`

#### Function: `has_columns`

### Function Overview

The function `has_columns` is designed to verify the presence of specified columns in a pandas DataFrame. It is a utility function that ensures data integrity by checking if all required columns are included in a given DataFrame before proceeding with further data processing or analysis.

### Inputs and Outputs

- **Inputs**:
  - `df (pd.DataFrame)`: The DataFrame to be checked.
  - `required_columns (List[str])`: A list of strings representing the names of the columns that are required to be present in the DataFrame.

- **Output**:
  - `bool`: The function returns `True` if all the columns listed in `required_columns` are found in the DataFrame `df`. It returns `False` if any of the required columns are missing.

### Key Logic and Design Choices

The function utilizes Python's `all()` function combined with a generator expression to efficiently check the presence of each column listed in `required_columns` within the DataFrame's columns. This approach is both concise and efficient, as `all()` will short-circuit and return `False` as soon as a missing column is identified, thus minimizing unnecessary checks.

### Integration into Larger Applications

In larger applications or modules that deal with data manipulation and analysis, `has_columns` can be used as a preliminary check to ensure that all necessary data fields are present before performing operations that depend on these fields. This can help avoid errors and exceptions at later stages of the workflow. It is particularly useful in scenarios where data may come from varied sources or formats, and consistency in DataFrame structure is crucial for the stability and reliability of downstream processes. 

By integrating `has_columns` at the beginning of data processing pipelines, developers can provide clear, early notifications about missing data, which can be critical for debugging and maintaining the robustness of data-driven applications.

#### Function: `drop_columns`

### Function Overview

The function `drop_columns` is designed to remove specified columns from a pandas DataFrame. This utility function is particularly useful in data preprocessing stages where certain columns need to be excluded from analysis or further processing.

### Inputs and Outputs

- **Inputs**:
  - `df` (pd.DataFrame): The input DataFrame from which columns will be removed.
  - `columns_to_drop` (List[str]): A list containing the names of the columns to be dropped from the DataFrame.

- **Output**:
  - The function returns a pandas DataFrame that has been modified to exclude the specified columns listed in `columns_to_drop`.

### Key Logic and Design Choices

- **Column Existence Check**: Before attempting to drop the columns, the function first checks which of the specified columns actually exist in the DataFrame. This is done using a list comprehension that filters out non-existing columns from the `columns_to_drop` list. This step ensures that the function does not attempt to drop columns that do not exist, which would otherwise raise an error.
  
- **Dropping Columns**: The function uses the `drop` method of the DataFrame to remove the columns. It specifies the `columns` parameter with the list of existing columns to be dropped. This method directly returns a new DataFrame without the specified columns, ensuring that the original DataFrame remains unmodified.

### Integration into Larger Applications

- **Modularity and Reusability**: As a standalone function, `drop_columns` can be easily integrated into data processing pipelines or data cleaning modules. It allows for the dynamic removal of columns based on varying requirements across different datasets or stages of analysis.

- **Error Handling**: By checking for column existence before attempting to drop, the function avoids common errors and enhances robustness, making it suitable for automated data processing scripts where manual verification of column names might not be feasible.

This function is a practical tool for data scientists and developers working with large datasets, enabling them to streamline data preparation and ensure that their datasets only contain relevant information.

#### Function: `select_columns`

### Function Overview

The function `select_columns` is designed to filter and return a subset of columns from a given pandas DataFrame based on a specified list of column names. This function is particularly useful in data preprocessing stages where only certain columns are required for further analysis or operations.

### Inputs and Outputs

- **Inputs:**
  - `df (pd.DataFrame)`: The input DataFrame from which columns are to be selected.
  - `columns_to_keep (List[str])`: A list of strings representing the names of the columns that should be retained in the output DataFrame.

- **Output:**
  - `pd.DataFrame`: A new DataFrame containing only the columns specified in `columns_to_keep` that also exist in the input DataFrame.

### Key Logic and Design Choices

1. **Column Existence Check**: The function first creates a list, `existing`, which contains only those column names from `columns_to_keep` that are present in the input DataFrame's columns. This is achieved using a list comprehension that filters out any column names not found in `df.columns`.
   
2. **DataFrame Slicing**: The function then returns a new DataFrame that consists of the columns listed in `existing`. This is done by slicing the original DataFrame `df` using the `existing` list. This approach ensures that the function does not raise an error if some columns listed in `columns_to_keep` do not exist in `df`, making the function robust to missing columns.

### Integration into Larger Applications

The `select_columns` function can be integrated into data processing pipelines or data analysis modules where specific columns of a dataset are needed for tasks such as feature selection, data visualization, or statistical analysis. By providing a way to easily and safely extract a subset of columns, this function helps in maintaining the cleanliness and manageability of code in larger data handling applications. It is especially useful in scenarios where datasets may have a large number of columns, but only a select few are relevant for a particular analysis or processing step.

---

### `datetime_utils.py`

#### Function: `convert_to_datetime`

### Function Overview

The function `convert_to_datetime` is designed to convert a specified column in a pandas DataFrame to datetime format. This is particularly useful in data preprocessing stages of a data analysis or machine learning pipeline where date or time information is crucial for time series analysis, feature engineering, or chronological sorting.

### Inputs and Outputs

- **Inputs**:
  - `df` (pd.DataFrame): The input DataFrame that contains the data.
  - `column_name` (str): The name of the column within the DataFrame that needs to be converted to datetime format.

- **Outputs**:
  - The function returns a pandas DataFrame (`pd.DataFrame`) where the specified column has been converted to datetime format.

### Key Logic and Design Choices

- The function utilizes `pandas.to_datetime()` for the conversion, which is a robust method capable of handling a variety of datetime formats automatically.
- The `errors='coerce'` parameter in `pd.to_datetime()` is a critical design choice. It ensures that if any data in the specified column cannot be converted into a datetime format, those values are replaced with `NaT` (Not a Time), pandas' equivalent for missing or null date/time values. This choice helps in maintaining the integrity of the dataset by not discarding rows with potentially corrupt or non-standard date formats.

### Integration into Larger Applications

- In a larger application or module, `convert_to_datetime` can be part of a data cleaning or preprocessing toolkit. It is typically used in the initial stages of data preparation to ensure that all temporal data is in the correct format for subsequent analysis, which might include sorting events chronologically, computing durations, or resampling time series data.
- This function can be easily integrated into data pipelines and can be used repetitively for different datasets or different columns within the same dataset, making it a versatile tool for data transformation tasks involving datetime information. 

Overall, the `convert_to_datetime` function is a utility function aimed at simplifying the handling of datetime information in data analysis workflows, ensuring data consistency and easing further temporal data manipulations.

#### Function: `extract_date_parts`

### Function Overview

The function `extract_date_parts` is designed to augment a pandas DataFrame by extracting the year, month, and day from a specified datetime column. This function simplifies the process of breaking down datetime information into separate, more granular components, which can be useful for further data analysis or feature engineering tasks.

### Inputs and Outputs

- **Inputs**:
  - `df` (pd.DataFrame): The input DataFrame that must contain at least one column with datetime data.
  - `column_name` (str): The name of the column within `df` that contains datetime data. This column should be of a datetime dtype.

- **Outputs**:
  - The function returns a modified version of the input DataFrame (`df`). This modified DataFrame includes three new columns: `year`, `month`, and `day`, which contain the extracted year, month, and day from the datetime column specified by `column_name`.

### Key Logic and Design Choices

- The function utilizes pandas' built-in datetime properties accessed via `.dt` accessor to extract the year, month, and day from the datetime column. This approach leverages pandas' efficient handling of datetime data types and vectorized operations to ensure performance and simplicity.
- The function directly modifies the input DataFrame by adding new columns. This design choice means that the original DataFrame is changed in-place, which could be beneficial for memory usage in cases where creating a copy of the DataFrame is unnecessary or undesirable. However, users should be aware of this side effect.

### Integration into Larger Applications

- `extract_date_parts` can be a part of a larger data preprocessing module where datetime features need to be decomposed into simpler components for tasks such as trend analysis, seasonal adjustment, or as input features for machine learning models.
- In a typical data processing pipeline, this function would be used after data loading and cleaning stages, ensuring that the datetime data is in the correct format for extraction.
- The simplicity and specificity of the function make it easily integrable with other data transformation processes in a data analysis or machine learning workflow.

This function is a utility for enhancing DataFrame structures by adding detailed temporal data, facilitating more detailed and specific data analysis tasks with minimal additional coding required from the user.

---
