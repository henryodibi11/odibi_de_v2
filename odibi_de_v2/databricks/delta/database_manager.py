from pyspark.sql import SparkSession
from typing import Optional, List, Dict


class DatabaseManager:
    """
    Provides database-level operations for Delta tables, such as VACUUM, OPTIMIZE, CACHE, and UNCACHE
    across all tables in a given database.

    This class leverages DeltaTableManager for table-level operations, giving you both
    granular and bulk management capabilities.

    Attributes:
        spark (SparkSession): The active Spark session.
        database (str): The name of the database to manage.

    Example:
        >>> from odibi_de_v2.delta.database_manager import DatabaseManager
        >>> db_mgr = DatabaseManager(spark, "bronze_db")
        >>> db_mgr.vacuum_all(retention_hours=168, dry_run=True)
    """

    def __init__(self, spark: SparkSession, database: str):
        self.spark = spark
        self.database = database

    def list_tables(self) -> List[str]:
        """Return fully-qualified table names for the database."""
        tables = self.spark.sql(f"SHOW TABLES IN {self.database}").collect()
        return [f"{self.database}.{row['tableName']}" for row in tables]

    def vacuum_all(self, retention_hours: int = 168, dry_run: bool = False) -> List[Dict[str, str]]:
        """Run VACUUM on all Delta tables in the database."""
        from odibi_de_v2.databricks import DeltaTableManager
        results = []
        tables = self.list_tables()
        for i, table in enumerate(tables, 1):
            print(f"\n[{i}/{len(tables)}] Vacuuming {table} "
                  f"(retention={retention_hours}h, dry_run={dry_run})...")
            manager = DeltaTableManager(self.spark, table, is_path=False)
            manager.vacuum(retention_hours=retention_hours, dry_run=dry_run)
            print(f"✅ Done vacuuming {table}")
            results.append({"table": table, "vacuumed": True})
        return results

    def optimize_all(self, zorder_by: Optional[List[str]] = None) -> List[Dict[str, str]]:
        """Run OPTIMIZE on all Delta tables in the database."""
        from odibi_de_v2.databricks import DeltaTableManager
        results = []
        tables = self.list_tables()
        for i, table in enumerate(tables, 1):
            print(f"\n[{i}/{len(tables)}] Optimizing {table} "
                  f"{'(ZORDER BY ' + ', '.join(zorder_by) + ')' if zorder_by else ''}...")
            manager = DeltaTableManager(self.spark, table, is_path=False)
            manager.optimize(zorder_by=zorder_by)
            print(f"✅ Done optimizing {table}")
            results.append({"table": table, "zorder_by": zorder_by})
        return results

    def cache_all(
        self,
        name_filter: Optional[str] = None,
        materialize: bool = True
    ) -> List[Dict[str, str]]:
        """Cache all (or filtered) tables in the database."""
        tables = self.list_tables()
        if name_filter:
            tables = [t for t in tables if name_filter.lower() in t.lower()]
        results = []
        for table in tables:
            print(f"[DatabaseManager] Caching table: {table}")
            self.spark.sql(f"CACHE TABLE {table}")
            if materialize:
                self.spark.sql(f"SELECT COUNT(*) FROM {table}").show()
            results.append({"table": table, "cached": True})
        return results

    def uncache_all(
        self,
        name_filter: Optional[str] = None
    ) -> List[Dict[str, str]]:
        """Uncache all (or filtered) tables in the database."""
        tables = self.list_tables()
        if name_filter:
            tables = [t for t in tables if name_filter.lower() in t.lower()]
        results = []
        for table in tables:
            print(f"[DatabaseManager] Uncaching table: {table}")
            self.spark.sql(f"UNCACHE TABLE {table}")
            results.append({"table": table, "uncached": True})
        return results
