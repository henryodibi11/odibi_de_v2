"""
delta_merge_manager.py
----------------------
Performs idempotent, hash-based Delta MERGE operations for Databricks/Spark.

This utility ensures only new or changed rows are inserted or updated, providing
a safe and deterministic merge process for ETL pipelines. It uses SHA-256 hashes
(`Merge_Id`, `Change_Id`) for efficient change detection and lineage tracking.

Pairs seamlessly with:
    - DeltaTableManager → for inspection, optimization, and time travel
    - SparkDataSaverFromConfig → for config-driven Delta writes
    - add_hash_columns / add_ingestion_metadata utilities

----------------------------------------------------------------------
QUICK START (End-to-End Example)
----------------------------------------------------------------------

>>> from pyspark.sql import SparkSession
>>> from odibi_de_v2.databricks.delta.delta_merge_manager import DeltaMergeManager

spark = SparkSession.builder.getOrCreate()

# 1️⃣ Prepare source data
data = [(1, "Alice", 100), (2, "Bob", 200)]
columns = ["id", "name", "value"]
df = spark.createDataFrame(data, columns)

# 2️⃣ Merge into existing Delta table
merge_mgr = DeltaMergeManager(spark, "silver_db.customer_table")

merge_mgr.merge(
    source_df=df,
    merge_keys=["id"],
    change_columns=["name", "value"]
)

# ✅ Behavior
# - Inserts new rows when Merge_Id not found
# - Updates existing rows if Change_Id differs
# - Adds 'Created_Timestamp' and 'Updated_Timestamp' metadata columns
# - Does nothing for identical rows (idempotent)
#
# Works with both table names and paths:
#   DeltaMergeManager(spark, "dbfs:/FileStore/silver/customers", is_path=True)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import sha2, concat_ws, current_timestamp
from delta.tables import DeltaTable
from typing import List
from odibi_de_v2.utils.decorators import log_call


class DeltaMergeManager:
    """Lightweight manager for idempotent Delta MERGE operations using hash-based change detection."""

    def __init__(self, spark: SparkSession, target_table: str, is_path: bool = False):
        """
        Initialize a DeltaMergeManager for a target Delta table.

        Args:
            spark (SparkSession): Active Spark session.
            target_table (str): Name or path of the Delta table.
            is_path (bool): If True, interprets `target_table` as a filesystem path. Defaults to False.

        Example:
            >>> mgr = DeltaMergeManager(spark, "silver_db.energy_efficiency")
            >>> mgr_path = DeltaMergeManager(spark, "dbfs:/FileStore/silver/efficiency", is_path=True)
        """
        self.spark = spark
        self.table_identifier = target_table
        self.is_path = is_path
        self.target_table = (
            DeltaTable.forPath(spark, target_table)
            if is_path
            else DeltaTable.forName(spark, target_table)
        )

    def _add_hash_columns(self, df: DataFrame, merge_keys: List[str], change_columns: List[str]) -> DataFrame:
        """
        Add 'Merge_Id' and 'Change_Id' columns for unique identification and change detection.

        Args:
            df (DataFrame): Input DataFrame.
            merge_keys (List[str]): Columns that uniquely identify a row.
            change_columns (List[str]): Columns to detect modifications.

        Returns:
            DataFrame: A new DataFrame with added 'Merge_Id' and 'Change_Id' columns.
        """
        df = df.withColumn("Merge_Id", sha2(concat_ws("||", *merge_keys), 256))
        df = df.withColumn("Change_Id", sha2(concat_ws("||", *change_columns), 256))
        return df

    @staticmethod
    def _safe_col(col_name: str) -> str:
        """
        Safely quote a column name if it includes special characters or spaces.

        Args:
            col_name (str): Column name to sanitize.
        Returns:
            str: Quoted column name if necessary.
        """
        if not col_name.replace("_", "").isalnum():
            return f"`{col_name}`"
        return col_name

    @log_call(module="DATABRICKS", component="DeltaMergeManager")
    def merge(self, source_df: DataFrame, merge_keys: List[str], change_columns: List[str]) -> None:
        """
        Perform an idempotent Delta MERGE based on hash-based change detection.

        This method:
        - Computes 'Merge_Id' and 'Change_Id' for the source DataFrame
        - Updates records where the Change_Id differs
        - Inserts records where Merge_Id does not exist
        - Preserves unchanged records

        Args:
            source_df (DataFrame): Incoming DataFrame to merge into the target Delta table.
            merge_keys (List[str]): Columns that uniquely identify each row.
            change_columns (List[str]): Columns whose value changes should trigger an update.

        Example:
            >>> from pyspark.sql import SparkSession
            >>> spark = SparkSession.builder.appName("example").getOrCreate()
            >>> data = [(1, "Alice", 100), (2, "Bob", 200)]
            >>> df = spark.createDataFrame(data, ["id", "name", "value"])
            >>> mgr = DeltaMergeManager(spark, "silver_db.people")
            >>> mgr.merge(df, merge_keys=["id"], change_columns=["name", "value"])
        """
        # Step 1: Add hashes and metadata
        df = self._add_hash_columns(source_df, merge_keys, change_columns)
        df = (
            df.withColumn("Updated_Timestamp", current_timestamp())
              .withColumn("Created_Timestamp", current_timestamp())
        )

        # Step 2: Build join and update/insert expressions
        join_condition = "source.Merge_Id = target.Merge_Id"
        update_expr = {col: f"source.{col}" for col in change_columns}
        update_expr.update({
            "Change_Id": "source.Change_Id",
            "Updated_Timestamp": "source.Updated_Timestamp",
        })
        insert_expr = {self._safe_col(col): f"source.{self._safe_col(col)}" for col in df.columns}

        # Step 3: Execute the Delta merge
        (
            self.target_table.alias("target")
            .merge(df.alias("source"), join_condition)
            .whenMatchedUpdate(
                condition="source.Change_Id != target.Change_Id",
                set=update_expr,
            )
            .whenNotMatchedInsert(values=insert_expr)
            .execute()
        )
