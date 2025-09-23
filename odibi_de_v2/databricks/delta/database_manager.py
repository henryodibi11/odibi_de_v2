from pyspark.sql import SparkSession
from typing import Optional, List, Dict


class DatabaseManager:
    """
    Provides database-level operations for Delta tables, such as VACUUM or OPTIMIZE
    across all tables in a given database.

    This class leverages DeltaTableManager for table-level operations, giving you
    both granular and bulk management capabilities.

    Attributes:
        spark (SparkSession): The active Spark session.
        database (str): The name of the database to manage.

    Example:
        >>> from odibi_de_v2.delta.database_manager import DatabaseManager
        >>> db_mgr = DatabaseManager(spark, "bronze_db")
        >>> db_mgr.vacuum_all(retention_hours=168, dry_run=True)
    """

    def __init__(self, spark: SparkSession, database: str):
        """
        Initialize a new DatabaseManager.

        Args:
            spark (SparkSession): The active Spark session.
            database (str): Name of the database to manage.
        """
        self.spark = spark
        self.database = database

    def list_tables(self) -> List[str]:
        """
        List all tables in the database.

        Returns:
            List[str]: Fully-qualified table names (database.table).
        """
        tables = self.spark.sql(f"SHOW TABLES IN {self.database}").collect()
        return [f"{self.database}.{row['tableName']}" for row in tables]

    def vacuum_all(self, retention_hours: int = 168, dry_run: bool = False) -> List[Dict[str, str]]:
        """
        Run VACUUM on all tables in the database.

        Args:
            retention_hours (int, optional): Hours of history to retain. Defaults to 168 (7 days).
            dry_run (bool, optional): If True, only simulate deletions. Defaults to False.

        Returns:
            List[dict]: Summary of vacuum operations with keys:
                - table (str)
                - retention_hours (int)
                - dry_run (bool)
        """
        from odibi_de_v2.databricks import DeltaTableManager
        results = []
        for table in self.list_tables():
            manager = DeltaTableManager(self.spark, table, is_path=False)
            manager.vacuum(retention_hours=retention_hours, dry_run=dry_run)
            results.append({
                "table": table,
                "retention_hours": retention_hours,
                "dry_run": dry_run,
            })
        return results

    def optimize_all(self, zorder_by: Optional[List[str]] = None) -> List[Dict[str, str]]:
        """
        Run OPTIMIZE on all tables in the database.

        Args:
            zorder_by (List[str], optional): Columns for ZORDER. Defaults to None.

        Returns:
            List[dict]: Summary of optimize operations with keys:
                - table (str)
                - zorder_by (list or None)
        """
        from odibi_de_v2.databricks import DeltaTableManager
        results = []
        for table in self.list_tables():
            manager = DeltaTableManager(self.spark, table, is_path=False)
            manager.optimize(zorder_by=zorder_by)
            results.append({
                "table": table,
                "zorder_by": zorder_by,
            })
        return results
