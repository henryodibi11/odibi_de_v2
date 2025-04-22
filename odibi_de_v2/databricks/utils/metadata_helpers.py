from pyspark.sql.functions import sha2, concat_ws, current_timestamp

def add_ingestion_metadata(df):
    """
    Appends audit metadata columns to a DataFrame, specifically 'Updated_Timestamp' and 'Created_Timestamp'.

    This function enhances the input DataFrame by adding two new columns that store the current timestamp. These columns
    are intended to track when each record was created and last updated.

    Args:
        df (DataFrame): The DataFrame to which the audit columns will be added.

    Returns:
        DataFrame: The original DataFrame augmented with two new columns: 'Updated_Timestamp' and 'Created_Timestamp',
        both set to the current timestamp at the time of function execution.

    Example:
        >>> original_df.show()
        +----+-----+
        | id | name|
        +----+-----+
        |  1 | John|
        |  2 | Jane|
        +----+-----+
        >>> new_df = add_ingestion_metadata(original_df)
        >>> new_df.show()
        +----+-----+-------------------+-------------------+
        | id | name| Updated_Timestamp | Created_Timestamp |
        +----+-----+-------------------+-------------------+
        |  1 | John| 2023-12-01 12:00  | 2023-12-01 12:00  |
        |  2 | Jane| 2023-12-01 12:00  | 2023-12-01 12:00  |
        +----+-----+-------------------+-------------------+
    """
    return (
        df.withColumn("Updated_Timestamp", current_timestamp())
          .withColumn("Created_Timestamp", current_timestamp())
    )


def add_hash_columns(df, merge_keys, change_columns):
    """
    Adds 'Merge_Id' and 'Change_Id' columns to a DataFrame based on SHA-256 hashes of specified columns.

    This function computes SHA-256 hashes for specified groups of columns in a DataFrame and adds these hashes as new
    columns. 'Merge_Id' is computed from the columns specified in `merge_keys` and is used to uniquely identify rows.
    'Change_Id' is computed from the columns in `change_columns` and can be used to detect changes in these columns.

    Args:
        df (DataFrame): The input DataFrame to which the hash columns will be added.
        merge_keys (list of str): List of column names used to compute the 'Merge_Id'. These columns should uniquely
            identify a row.
        change_columns (list of str): List of column names used to compute the 'Change_Id'. Changes in these columns
            indicate a need to update or handle the row differently.

    Returns:
        DataFrame: A new DataFrame with the original data and two additional columns: 'Merge_Id' and 'Change_Id'.

    Example:
        >>> df = spark.createDataFrame([(1, "A", 100), (2, "B", 200)], ["id", "category", "value"])
        >>> merge_keys = ["id"]
        >>> change_columns = ["category", "value"]
        >>> result_df = add_hash_columns(df, merge_keys, change_columns)
        >>> result_df.show()
        +---+--------+-----+--------------------+--------------------+
        | id|category|value|            Merge_Id|           Change_Id|
        +---+--------+-----+--------------------+--------------------+
        |  1|       A|  100|c130... (hash value)|5d414... (hash value)|
        |  2|       B|  200|2a21... (hash value)|7d793... (hash value)|
        +---+--------+-----+--------------------+--------------------+
    """
    df = df.withColumn("Merge_Id", sha2(concat_ws("||", *merge_keys), 256))
    df = df.withColumn("Change_Id", sha2(concat_ws("||", *change_columns), 256))
    return df
