# Module Overview

### `flatten.py`

#### Function: `flatten_nested_structs`

### Function Overview

The function `flatten_nested_structs` is designed to transform a Spark DataFrame by recursively flattening all columns that have a nested `StructType`. This is particularly useful in scenarios where nested data structures complicate data manipulation and analysis tasks in Spark.

### Inputs and Outputs

- **Input:**
  - `df (DataFrame)`: The input Spark DataFrame that potentially contains nested `StructType` columns.
  - `sep (str)`: An optional string separator used to concatenate nested field names into a flat structure. The default separator is an underscore (`_`).

- **Output:**
  - The function returns a new Spark DataFrame where all nested `StructType` columns are flattened into standard columns with no further nesting.

### Key Logic and Design Choices

- **Recursive Flattening:**
  - The function employs a nested helper function `_flatten`, which recursively traverses the schema of the DataFrame. For each field in the DataFrame schema, it checks if the field is of type `StructType`.
  - If a field is a `StructType`, the function recursively processes the nested fields. Otherwise, it renames the field using the provided separator to reflect the hierarchy in the original nested structure.

- **Field Renaming:**
  - Nested fields are renamed by concatenating their names from the top level to the deepest level, separated by the specified `sep`. This renaming is crucial for maintaining a clear and understandable structure in the flattened DataFrame.

- **Usage of Spark DataFrame API:**
  - The function makes extensive use of the Spark DataFrame API, particularly the `select` and `alias` methods. This ensures that the transformation is efficient and leverages Spark's optimized execution engine.

### Integration into Larger Applications

This function is typically used in data preprocessing stages of Spark applications where data needs to be simplified for analysis or before being stored in a format that does not support nested structures (like CSV). It helps in cleaning and preparing data, making it more accessible for analysts and other downstream applications that may not handle complex nested data well.

### Example Usage

```python
from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("Example").getOrCreate()

# Example DataFrame with nested structures
data = [{"id": 1, "info": {"name": "Alice", "age": 30}}]
df = spark.createDataFrame(data)

# Flatten the DataFrame
flat_df = flatten_nested_structs(df)

# Show the result
flat_df.show()
```

This function is a valuable tool in the toolkit of a data engineer or analyst working with complex, nested data structures in Spark, simplifying data structures for further processing or analysis.

---

### `validation.py`

#### Function: `is_spark_dataframe`

### Function Overview

The function `is_spark_dataframe` is designed to determine whether a given object is an instance of a Spark DataFrame. This utility function is essential for applications that interact with Apache Spark data structures, ensuring that operations intended for Spark DataFrames are executed on the correct type of object.

### Inputs and Outputs

- **Input**: The function accepts a single parameter, `obj`, which can be any Python object.
- **Output**: It returns a boolean value. The function outputs `True` if the input object is an instance of a Spark DataFrame, and `False` otherwise.

### Key Logic and Design Choices

The function utilizes Python's built-in `isinstance()` function to check the type of the input object. The simplicity of this approach ensures that the function is both efficient and easy to understand. It directly checks if `obj` is an instance of `DataFrame`, which should be understood in the context of this function as a Spark DataFrame, not to be confused with Pandas DataFrame or other DataFrame types in different libraries.

### Integration into Larger Applications

In a larger application, particularly those involving data processing with Apache Spark, `is_spark_dataframe` can be used to validate objects before performing operations that are specific to Spark DataFrames. This helps in avoiding runtime errors and ensures that the data manipulation functions are called with appropriate data types. For example, it can be used in functions that need to handle different types of data structures dynamically, ensuring that Spark DataFrame methods are only called on Spark DataFrames.

### Assumptions and Dependencies

This function assumes that the `DataFrame` class, presumably from PySpark, is already imported and available in the scope where `is_spark_dataframe` is defined. If the `DataFrame` class is not imported or if the import is referring to a different DataFrame class (like from Pandas), the function will not behave as intended. This should be clearly documented or managed within the codebase to avoid confusion.

#### Function: `has_columns`

### Function Overview

The function `has_columns` is designed to verify the presence of specified columns in a Spark DataFrame. This utility function is crucial for data validation and integrity checks before performing operations that depend on certain data structure requirements.

### Inputs and Outputs

- **Inputs:**
  - `df (DataFrame)`: The Spark DataFrame to be checked. This is the primary data structure on which the column existence check is performed.
  - `required_columns (List[str])`: A list of strings representing the names of the columns that are required to be present in the DataFrame.

- **Output:**
  - `bool`: The function returns a Boolean value. It returns `True` if all specified columns are found in the DataFrame, and `False` otherwise.

### Key Logic and Design Choices

The function employs a straightforward and efficient approach to check for column existence:
- It uses a generator expression within the `all()` function to iterate over each column name in `required_columns`.
- For each column name, it checks if the column is present in the DataFrame's columns list (`df.columns`).
- The `all()` function ensures that the result is `True` only if every column listed in `required_columns` is found in the DataFrame, thereby ensuring complete compliance with the required data structure.

### Integration into Larger Applications

In larger applications or modules, particularly those involving data processing and analysis with PySpark, `has_columns` can serve as a preliminary check to ensure that data frames passed to processing functions or methods meet expected schema criteria. This is essential for:
- Preventing runtime errors that would occur if expected columns are missing when performing data transformations or calculations.
- Ensuring data consistency and reliability across different stages of data processing pipelines.
- Facilitating debugging and maintenance by clearly identifying mismatches in data frame structure before complex operations are performed.

Overall, `has_columns` is a fundamental utility function that enhances robustness and reliability in data-driven applications using Spark DataFrames.

#### Function: `is_empty`

### Function Overview

The function `is_empty` is designed to determine if a given Spark DataFrame contains any rows. This utility function is essential for scenarios where subsequent operations depend on the presence of data in the DataFrame.

### Inputs and Outputs

- **Input**: The function accepts one parameter, `df`, which is expected to be a Spark DataFrame.
- **Output**: It returns a boolean value. The function outputs `True` if the DataFrame is empty (i.e., contains no rows), and `False` otherwise.

### Key Logic and Design Choices

The function leverages Spark's RDD (Resilient Distributed Dataset) API by converting the DataFrame into an RDD using `df.rdd` and then applying the `isEmpty()` method. This method is a straightforward and efficient way to check for the absence of data. Using RDD's `isEmpty()` is preferable in this context due to its direct approach and minimal computational overhead compared to other methods that might involve counting entries.

### Integration into Larger Applications

In larger Spark applications, `is_empty` can be used to guard against errors or unnecessary processing by checking for data presence before executing data transformations, analytics, or exports. This function is particularly useful in data pipelines and ETL (Extract, Transform, Load) processes where conditional logic based on data availability is common. For example, it can prevent the execution of costly data processing steps or the generation of reports when there is no data to process, thereby saving resources and processing time.

#### Function: `has_nulls_in_columns`

### Function Overview

The function `has_nulls_in_columns` is designed to check for the presence of null values in specified columns of a Spark DataFrame. It is a utility function that can be used to ensure data integrity before performing data processing tasks.

### Inputs and Outputs

- **Inputs:**
  - `df (DataFrame)`: A Spark DataFrame in which the presence of null values needs to be checked.
  - `cols (List[str])`: A list of string column names within the DataFrame to inspect for null values.

- **Output:**
  - `bool`: The function returns `True` if any of the specified columns contain null values, otherwise it returns `False`.

### Key Logic and Design Choices

1. **Column Existence Check**: The function first checks if the column exists in the DataFrame to avoid errors related to non-existent columns.
   
2. **Efficient Null Checking**: For each column, the function uses Spark's DataFrame `filter` method combined with `isNull()` to identify rows where the column value is null. To optimize performance, especially in large datasets, the function uses `limit(1)` immediately after filtering. This ensures that the count operation (`count()`) stops as soon as a single null is found, rather than scanning the entire column.

3. **Early Exit**: The function returns `True` as soon as it finds the first column with a null value, which prevents unnecessary checks on subsequent columns once a null is detected.

### Application Context

This function is particularly useful in data preprocessing stages of a data pipeline where data quality and integrity are crucial. Before performing operations like data transformations, aggregations, or model training, it is essential to verify that the data does not contain unexpected null values which could lead to errors or biased results. This function can be part of a larger module of data validation or cleaning tools in a data processing application or ETL pipeline.

#### Function: `has_duplicate_columns`

### Function Overview

The function `has_duplicate_columns` is designed to determine if a Spark DataFrame contains any duplicate column names. This utility function is crucial for data integrity checks, especially before performing operations that require unique column identifiers, such as data transformations or merges.

### Inputs and Outputs

- **Input**: The function accepts a single argument, `df`, which is expected to be a Spark DataFrame.
- **Output**: It returns a boolean value. The function outputs `True` if there are duplicate column names in the DataFrame, and `False` otherwise.

### Key Logic and Design Choices

The function employs a straightforward approach to check for duplicates:
1. It retrieves the list of column names from the DataFrame using `df.columns`.
2. It converts this list into a set, which inherently removes any duplicates due to the properties of sets.
3. It then compares the length of the original list of columns to the length of the set of columns. If the lengths differ, it indicates the presence of duplicate column names, returning `True`. If they are the same, it returns `False`.

This method is efficient and concise, leveraging Python's built-in data structures to perform the check with minimal overhead.

### Integration into Larger Applications

`has_duplicate_columns` can be integrated into data processing pipelines or data validation frameworks within larger Spark-based applications. It is particularly useful:
- Before data merging operations where unique column names might be required to avoid conflicts.
- In data cleaning stages to ensure that the DataFrame conforms to expected structural standards.
- As a preliminary check in functions that manipulate or rely on column names, ensuring that subsequent operations do not fail due to name ambiguities.

This function is a fundamental utility in scenarios involving large and complex data sets where manual checks are impractical, thus enhancing automation and reliability in data handling processes.

#### Function: `is_flat_dataframe`

### Function Overview

The function `is_flat_dataframe` is designed to determine whether a given Spark DataFrame has a flat schema. A flat schema means that all columns in the DataFrame are of simple data types, without any nested structures like structures or arrays.

### Inputs and Outputs

- **Input**: The function accepts a single argument, `df`, which is a Spark DataFrame.
- **Output**: It returns a boolean value. The function outputs `True` if the DataFrame's schema contains only simple data types, and `False` if any column is of type `StructType` or `ArrayType`, indicating a nested or complex structure.

### Key Logic and Design Choices

- The function iterates over each field in the DataFrame's schema.
- It checks the data type of each field using `isinstance` to see if it matches `StructType` or `ArrayType`.
- If any field matches these types, the function immediately returns `False`, indicating the presence of a nested structure.
- If the loop completes without finding any nested types, it returns `True`, indicating that the DataFrame is flat.

### Integration into Larger Applications

This function can be particularly useful in data processing environments where the complexity of data handling and manipulation needs to be managed. For instance, in scenarios where operations or functions require flat schemas for compatibility or performance reasons, `is_flat_dataframe` can be used to validate DataFrames before processing. This helps in ensuring that the data conforms to expected formats, potentially preventing runtime errors and improving efficiency in data pipelines.

---

### `datetime_utils.py`

#### Function: `convert_to_datetime`

### Function Overview

The function `convert_to_datetime` is designed to convert a specified column in a Spark DataFrame to a timestamp format. This function is part of a larger data processing or transformation module where handling and manipulating date and time data is necessary.

### Inputs and Outputs

- **Input Parameters:**
  - `df (DataFrame)`: The input parameter `df` represents a Spark DataFrame containing the data where one of the columns needs to be converted into a timestamp format.
  - `column_name (str)`: This parameter specifies the name of the column within the DataFrame that will be converted to the timestamp format.

- **Output:**
  - The function returns a Spark DataFrame (`DataFrame`) where the specified column has been cast to a timestamp data type. The rest of the DataFrame remains unchanged.

### Key Logic and Design Choices

- The function utilizes Spark SQL functions `to_timestamp` and `col` to perform the conversion. The `to_timestamp` function is used to convert the column values into timestamps, and `col` is used to refer to the column in the DataFrame by name.
- The `withColumn` method of the DataFrame is used to overwrite the existing column with the converted timestamp values. This method ensures that the transformation is applied directly to the specified column without altering any other data in the DataFrame.

### Integration into Larger Applications

- In a larger application, particularly in data engineering or analysis environments using PySpark, this function can be used as part of data preprocessing or cleaning steps. It is crucial for ensuring that datetime data is in the correct format for time-series analysis, trend analysis, or any operations that require date and time calculations.
- This function can be integrated into data ingestion pipelines where raw data is ingested in various formats and needs standardization to a common timestamp format for consistent processing and analysis.

By providing a straightforward and reusable way to convert date and time data to a standardized format, `convert_to_datetime` enhances data integrity and simplifies subsequent data processing tasks in Spark-based applications.

#### Function: `extract_date_parts`

### Function Overview

The function `extract_date_parts` is designed to manipulate a Spark DataFrame by extracting date components from a specified timestamp column. It enriches the DataFrame by adding three new columns: `year`, `month`, and `day`, each representing the respective part of the date extracted from the timestamp.

### Inputs and Outputs

- **Input**:
  - `df` (DataFrame): This is the input Spark DataFrame that contains the data.
  - `column_name` (str): This is the name of the column in the DataFrame that contains timestamp values from which the year, month, and day will be extracted.

- **Output**:
  - The function returns a modified DataFrame that includes the original data along with three new columns (`year`, `month`, `day`) corresponding to the year, month, and day extracted from the timestamp in the specified column.

### Key Logic and Design Choices

- The function utilizes Spark SQL functions `year`, `month`, and `dayofmonth` to extract the respective date parts from the timestamp column.
- It employs the `withColumn` method of the DataFrame to add each new column. This method is called sequentially to add `year`, then `month`, and finally `day` columns.
- The use of `col` function from PySpark SQL functions indicates that the operations are column-wise, which is efficient for handling large datasets typically processed in Spark.

### Integration into Larger Applications

- This function is particularly useful in data preprocessing or transformation stages in data pipelines, where date components are needed for features engineering, reporting, or further time series analysis.
- It can be integrated into a data processing script or module where timestamps need to be decomposed into more granular components for detailed analysis or as part of the data cleaning and preparation process before applying machine learning algorithms or statistical models.
- In a larger application, this function could be part of a utility module for date and time operations, commonly used across different parts of the application that deal with temporal data.

In summary, `extract_date_parts` is a utility function for Spark DataFrame manipulation, focusing on extracting and appending date components as separate columns, facilitating easier and more specific data analysis tasks in large-scale data processing environments.

---

### `columns.py`

#### Function: `has_columns`

### Function Overview

The function `has_columns` is designed to verify the presence of specified columns in a Spark DataFrame. This is particularly useful for schema validation in data processing applications where certain operations depend on the existence of specific columns.

### Inputs and Outputs

- **Inputs**:
  - `df (DataFrame)`: A Spark DataFrame in which the presence of columns is to be checked.
  - `required_columns (List[str])`: A list of strings that specify the names of the columns required in the DataFrame.

- **Output**:
  - `bool`: The function returns `True` if all the columns listed in `required_columns` are present in the DataFrame `df`, otherwise it returns `False`.

### Key Logic

The function employs a straightforward comprehension that iterates over each column name in `required_columns` and checks whether it exists in the DataFrame's columns (`df.columns`). The `all()` function is used to ensure that every required column must be present in the DataFrame for the function to return `True`.

### Integration and Usage

This function is typically used in data processing workflows involving Spark DataFrames to ensure that expected columns are available before proceeding with data transformations or analyses. It helps in avoiding runtime errors that would occur if expected columns are missing. This function can be part of data validation modules or preprocessing steps in larger Spark applications.

### Example Usage

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Example").getOrCreate()
data = [("Alice", 1), ("Bob", 2)]
columns = ["name", "id"]
df = spark.createDataFrame(data, columns)
required_columns = ["name", "id"]
print(has_columns(df, required_columns))  # Output: True

required_columns = ["name", "age"]
print(has_columns(df, required_columns))  # Output: False
```

This example demonstrates how `has_columns` can be used to check for the presence of required columns before proceeding with further data operations.

#### Function: `drop_columns`

### Function Overview

The function `drop_columns` is designed to remove specified columns from a Spark DataFrame. This utility function is particularly useful in data preprocessing stages where certain columns need to be excluded from further analysis or operations.

### Inputs and Outputs

- **Inputs**:
  - `df (DataFrame)`: The input Spark DataFrame from which columns will be removed.
  - `columns_to_drop (List[str])`: A list of strings representing the names of the columns to be dropped from the DataFrame.

- **Output**:
  - The function returns a Spark DataFrame that has had the specified columns removed.

### Key Logic and Design Choices

1. **Column Existence Check**: Before attempting to drop columns, the function first checks which of the specified columns actually exist in the DataFrame. This is done through a list comprehension that filters out any column names not present in the DataFrame's columns. This step ensures that the function does not attempt to drop a column that does not exist, which would lead to an error.

2. **Efficient Column Dropping**: The function uses the `drop` method of the Spark DataFrame, which can accept multiple column names as arguments. By using the unpacking operator `*` on the list of existing columns, all valid columns are dropped in a single operation, making the function efficient and concise.

### Integration into Larger Applications

The `drop_columns` function can be integrated into data processing pipelines in applications that involve data transformation and cleaning. It is particularly useful in scenarios where datasets may have varying structures or when datasets are merged and certain redundant or irrelevant columns need to be removed to streamline the dataset for analysis or machine learning workflows. This function helps maintain the cleanliness and relevance of the data by allowing dynamic removal of specified columns based on the needs of the application or analysis.

#### Function: `select_columns`

### Function Overview

The function `select_columns` is designed to filter out specific columns from a given Spark DataFrame based on a list of column names provided by the user. This function is particularly useful in data processing and transformation tasks where only a subset of all available DataFrame columns is needed for further analysis or operations.

### Inputs and Outputs

- **Inputs:**
  - `df (DataFrame)`: The input Spark DataFrame from which columns are to be selected.
  - `columns_to_keep (List[str])`: A list of strings representing the names of the columns that should be retained in the output DataFrame.

- **Output:**
  - The function returns a new Spark DataFrame (`DataFrame`) that includes only the columns specified in `columns_to_keep` that also exist in the input DataFrame.

### Key Logic and Design Choices

- **Column Existence Check:** Before attempting to select columns from the DataFrame, the function first checks which of the desired columns in `columns_to_keep` actually exist in the input DataFrame. This is done using a list comprehension that filters out any column names not present in `df.columns`.
  
- **Dynamic Column Selection:** The function uses the `select` method of the Spark DataFrame, which is dynamically populated with the existing columns identified in the previous step. The use of the unpacking operator `*` allows for passing a list of column names as individual arguments to the `select` method.

### Integration into Larger Applications

`select_columns` can be integrated into data processing pipelines in applications that utilize PySpark for handling large datasets. It serves as a utility function to streamline the process of reducing a DataFrame to only those columns that are relevant for specific analyses or operations, thereby optimizing memory usage and processing time. This function is especially valuable in scenarios where the structure of incoming data might vary or contain more information than is necessary for particular tasks.

---
