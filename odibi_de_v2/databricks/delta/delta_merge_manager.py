from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import sha2, concat_ws, current_timestamp
from pyspark.sql import DataFrame
from typing import List


class DeltaMergeManager:
    """
    Performs an idempotent merge into the target Delta table using Merge_Id (a hash of merge keys)
    and Change_Id (a hash of columns that, if changed, indicate the row must be updated).

    This method ensures that only new or changed data is inserted or updated in the target Delta table,
    preventing duplicates and maintaining data integrity.

    :param source_df: The DataFrame containing new data to be merged into the target table.
    :param merge_keys: A list of column names from `source_df` that uniquely identify each row. These columns
        are used to generate the Merge_Id.
    :param change_columns: A list of column names from `source_df` that, when their values change, should trigger
        an update in the target table. These columns are used to generate the Change_Id.

    :returns: None. The target Delta table is modified in-place by inserting new rows and updating existing rows as
        necessary.

    :raises:
        - ValueError: If any specified column in `merge_keys` or `change_columns` does not exist in `source_df`.
        - RuntimeError: If the target Delta table cannot be accessed or modified.

    Example usage:
        >>> from pyspark.sql.functions import lit
        >>> data = [(1, "NameA", 100), (2, "NameB", 200)]
        >>> columns = ["id", "name", "value"]
        >>> source_df = spark.createDataFrame(data, columns)

        >>> merge_manager = DeltaMergeManager(spark, "my_database.my_table")
        >>> merge_manager.merge(
        ...     source_df=source_df,
        ...     merge_keys=["id"],
        ...     change_columns=["name", "value"]
        ... )
    This will update the target Delta table by inserting new rows for new `id`s and updating the `name` and `value`
    columns for existing `id`s where the values have changed.
    """



    def __init__(self, spark: SparkSession, target_table: str, is_path: bool = False):
        """
        Initializes an instance of DeltaMergeManager to manage Delta table operations.

        This constructor sets up the DeltaMergeManager with a specified Delta table, which can be
        identified either by its path or by its name in the metastore. It initializes the `DeltaTable`
        object based on the `is_path` flag.

        Args:
            spark (SparkSession): An active SparkSession instance to execute operations.
            target_table (str): The name or path of the Delta table depending on the value of `is_path`.
            is_path (bool, optional): A flag to determine how to interpret `target_table`. If True, `target_table`
            is treated as a path to the Delta files. If False, it is treated as a metastore table name.
                Defaults to False.

        Raises:
            AnalysisException: If the provided `target_table` does not exist or is not accessible in the given
            `spark` session.

        Example:
            from pyspark.sql import SparkSession
            from delta.tables import DeltaTable

            spark = SparkSession.builder.appName("DeltaMergeExample").getOrCreate()
            delta_manager = DeltaMergeManager(spark, "/path/to/delta/table", is_path=True)

        """
        self.spark = spark
        self.table_identifier = target_table
        self.is_path = is_path

        self.target_table = (
            DeltaTable.forPath(spark, target_table)
            if is_path else DeltaTable.forName(spark, target_table))

    def _add_hash_columns(
        self,
        df: DataFrame,
        merge_keys: List[str],
        change_columns: List[str]
    ) -> DataFrame:
        """
        Adds 'Merge_Id' and 'Change_Id' hash columns to a DataFrame based on specified keys and
        change-detecting columns.

        This method computes two hash columns in the provided DataFrame:
        - 'Merge_Id': Created by hashing the values of specified `merge_keys` columns. This ID helps in
            identifying unique rows.
        - 'Change_Id': Created by hashing the values in `change_columns`. This ID is useful for detecting
            changes in these columns.

        Args:
            df (DataFrame): The DataFrame to which the hash columns will be added.
            merge_keys (List[str]): List of column names whose values will be concatenated and hashed to
                form the 'Merge_Id'.
            change_columns (List[str]): List of column names whose values will be concatenated and hashed
                to form the 'Change_Id'.

        Returns:
            DataFrame: A new DataFrame with 'Merge_Id' and 'Change_Id' columns added.

        Raises:
            ValueError: If any of the specified columns in `merge_keys` or `change_columns` do not exist in
                the DataFrame.

        Example:
            >>> df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
            >>> df_with_hashes = self._add_hash_columns(df, ["id"], ["value"])
            >>> df_with_hashes.show()
            +---+-----+--------------------+--------------------+
            | id|value|            Merge_Id|           Change_Id|
            +---+-----+--------------------+--------------------+
            |  1|    A|3c6e0b8a9c15224a...|82e3a0e30bc83835...|
            |  2|    B|19ca14e7ea6328a4...|a5b9d5455a9455e9...|
            +---+-----+--------------------+--------------------+
        """
        df = df.withColumn("Merge_Id", sha2(concat_ws("||", *merge_keys), 256))
        df = df.withColumn("Change_Id", sha2(concat_ws("||", *change_columns), 256))
        return df

    def merge(
        self,
        source_df: 'DataFrame',
        merge_keys: List[str],
        change_columns: List[str]
    ):
        """
        Merges a source DataFrame into a target Delta table based on specified keys and columns that determine changes.

        This method performs an idempotent merge operation. It uses a combination of merge keys to identify unique rows
        and change columns to detect modifications that require updates. The process involves adding hash columns for
        merge and change detection, updating existing records if changes are detected, and inserting new records if
        they do not exist in the target table.

        Args:
            source_df (DataFrame): The DataFrame to merge into the target Delta table. This DataFrame should contain
                at least the columns specified in `merge_keys` and `change_columns`.
            merge_keys (List[str]): A list of column names from `source_df` used to uniquely identify each row. These
                are used to generate a hash column `Merge_Id` in the DataFrame.
            change_columns (List[str]): A list of column names that, when their values change, should trigger an update
                to an existing row. These are used to generate a hash column `Change_Id` in the DataFrame.

        Returns:
            None: This method does not return anything but modifies the target Delta table directly.

        Raises:
            ValueError: If any of the specified `merge_keys` or `change_columns` do not exist in `source_df`.

        Example:
            >>> from pyspark.sql import SparkSession
            >>> spark = SparkSession.builder.appName("exampleApp").getOrCreate()
            >>> data = [(1, "NameA", 100), (2, "NameB", 200)]
            >>> columns = ["id", "name", "value"]
            >>> source_df = spark.createDataFrame(data, columns)
            >>> merge_manager = DeltaMergeManager(spark, "my_database.my_table")
            >>> merge_manager.merge(
            ...     source_df=source_df,
            ...     merge_keys=["id"],
            ...     change_columns=["name", "value"]
            ... )
        """
        # Step 1: Add hashes and timestamps
        source_df = self._add_hash_columns(source_df, merge_keys, change_columns)
        source_df = (
            source_df
            .withColumn("Updated_Timestamp", current_timestamp())
            .withColumn("Created_Timestamp", current_timestamp())
        )

        # Step 2: Build join condition using Merge_Id
        join_condition = "source.Merge_Id = target.Merge_Id"

        # Step 3: Define update expression (only update what's changed)
        update_expr = {col: f"source.{col}" for col in change_columns}
        update_expr["Change_Id"] = "source.Change_Id"
        update_expr["Updated_Timestamp"] = "source.Updated_Timestamp"

        # Step 4: Define insert expression (all columns, including the new hash/timestamp columns)

        insert_expr = {self._quote(col): f"source.{self._quote(col)}" for col in source_df.columns}
        # Step 5: Execute merge
        (
            self.target_table.alias("target")
            .merge(
                source_df.alias("source"),
                join_condition
            )
            .whenMatchedUpdate(
                condition="source.Change_Id != target.Change_Id",
                set=update_expr
            )
            .whenNotMatchedInsert(
                values=insert_expr
            )
            .execute()
        )

    def _quote(self,col_name: str) -> str:
        """
        Quotes a column name if it contains special characters or spaces.

        This method checks if the provided column name contains only alphanumeric characters and underscores.
        If the column name includes any other characters, it will be returned enclosed in backticks, which is
        often required by SQL syntax. Otherwise, it returns the column name unchanged.

        Args:
            col_name (str): The column name to potentially quote.

        Returns:
            str: The original column name if it is alphanumeric with underscores; otherwise, the column name
                enclosed in backticks.

        Example:
            >>> _quote("user_id")
            'user_id'
            >>> _quote("first name")
            '`first name`'
        """
        if not col_name.replace("_", "").isalnum():
            return f"`{col_name}`"
        return col_name