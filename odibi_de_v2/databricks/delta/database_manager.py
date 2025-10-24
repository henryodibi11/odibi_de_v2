"""
database_manager.py
-------------------
Manages all Delta tables within a specified Databricks database.

Provides database-wide operations like:
    - VACUUM      → Clean up old files across all tables
    - OPTIMIZE    → Compact data and optionally ZORDER
    - CACHE       → Cache all (or filtered) tables
    - UNCACHE     → Remove tables from Spark memory

Integrates directly with:
    - DeltaTableManager → for individual table maintenance
    - SparkDataSaverFromConfig → for automatic registration
    - DeltaMergeManager → for merge logic before maintenance

----------------------------------------------------------------------
QUICK START
----------------------------------------------------------------------

>>> from pyspark.sql import SparkSession
>>> from odibi_de_v2.databricks.delta.database_manager import DatabaseManager

spark = SparkSession.builder.getOrCreate()

# 1️⃣ Initialize manager for a given database
db_mgr = DatabaseManager(spark, "silver_db")

# 2️⃣ List tables
tables = db_mgr.list_tables()
print(tables)

# 3️⃣ Run VACUUM on all tables (simulate)
db_mgr.vacuum_all(retention_hours=72, dry_run=True)

# 4️⃣ Run OPTIMIZE on all tables with ZORDER
db_mgr.optimize_all(zorder_by=["Plant", "Asset"])

# 5️⃣ Cache selected tables
db_mgr.cache_all(name_filter="boiler")

# 6️⃣ Uncache all tables
db_mgr.uncache_all()

----------------------------------------------------------------------
NOTES
----------------------------------------------------------------------
- Uses DeltaTableManager internally for Delta operations.
- Logs progress table-by-table for transparency.
- All methods return structured results for programmatic inspection.
"""

from pyspark.sql import SparkSession
from typing import Optional, List, Dict
from odibi_de_v2.utils.decorators import log_call


class DatabaseManager:
    """Provides database-level operations for Delta tables (VACUUM, OPTIMIZE, CACHE, UNCACHE)."""

    def __init__(self, spark: SparkSession, database: str):
        """
        Initialize a DatabaseManager to perform Delta maintenance operations at database scope.

        Args:
            spark (SparkSession): The active Spark session.
            database (str): The name of the database (schema) to manage.

        Example:
            >>> db_mgr = DatabaseManager(spark, "bronze_db")
            >>> db_mgr.vacuum_all(retention_hours=168, dry_run=True)
        """
        self.spark = spark
        self.database = database

    @log_call
    def list_tables(self) -> List[str]:
        """Return fully-qualified Delta table names for the database."""
        tables = self.spark.sql(f"SHOW TABLES IN {self.database}").collect()
        return [f"{self.database}.{row['tableName']}" for row in tables]

    @log_call
    def vacuum_all(self, retention_hours: int = 168, dry_run: bool = False) -> List[Dict[str, str]]:
        """
        Run VACUUM on all Delta tables in the database.

        Args:
            retention_hours (int): Number of hours of history to retain. Default 168 (7 days).
            dry_run (bool): If True, lists files to be deleted without actually removing them.

        Returns:
            List[Dict[str, str]]: List of table results with vacuum status.
        """
        from odibi_de_v2.databricks import DeltaTableManager
        results = []
        tables = self.list_tables()

        for i, table in enumerate(tables, 1):
            print(f"\n[{i}/{len(tables)}] 🧹 Vacuuming {table} (retention={retention_hours}h, dry_run={dry_run})")
            manager = DeltaTableManager(self.spark, table, is_path=False)
            manager.vacuum(retention_hours=retention_hours, dry_run=dry_run)
            print(f"✅ Done vacuuming {table}")
            results.append({"table": table, "vacuumed": True})

        return results

    @log_call
    def optimize_all(self, zorder_by: Optional[List[str]] = None) -> List[Dict[str, str]]:
        """
        Run OPTIMIZE on all Delta tables in the database.

        Args:
            zorder_by (Optional[List[str]]): Optional list of columns to ZORDER by.

        Returns:
            List[Dict[str, str]]: List of table results with optimization info.
        """
        from odibi_de_v2.databricks import DeltaTableManager
        results = []
        tables = self.list_tables()

        for i, table in enumerate(tables, 1):
            zorder_str = f"(ZORDER BY {', '.join(zorder_by)})" if zorder_by else ""
            print(f"\n[{i}/{len(tables)}] ⚙️ Optimizing {table} {zorder_str}...")
            manager = DeltaTableManager(self.spark, table, is_path=False)
            manager.optimize(zorder_by=zorder_by)
            print(f"✅ Done optimizing {table}")
            results.append({"table": table, "zorder_by": zorder_by or []})

        return results

    @log_call
    def cache_all(self, name_filter: Optional[str] = None, materialize: bool = True) -> List[Dict[str, str]]:
        """
        Cache all (or filtered) tables in the database.

        Args:
            name_filter (Optional[str]): Filter to match only certain table names.
            materialize (bool): If True, runs COUNT(*) to populate cache immediately.

        Returns:
            List[Dict[str, str]]: List of cached tables with status.
        """
        tables = self.list_tables()
        if name_filter:
            tables = [t for t in tables if name_filter.lower() in t.lower()]

        results = []
        for i, table in enumerate(tables, 1):
            print(f"[{i}/{len(tables)}] 🧠 Caching table: {table}")
            self.spark.sql(f"CACHE TABLE {table}")
            if materialize:
                self.spark.sql(f"SELECT COUNT(*) FROM {table}").show()
            results.append({"table": table, "cached": True})

        return results

    @log_call
    def uncache_all(self, name_filter: Optional[str] = None) -> List[Dict[str, str]]:
        """
        Uncache all (or filtered) tables in the database.

        Args:
            name_filter (Optional[str]): Filter to match only certain table names.

        Returns:
            List[Dict[str, str]]: List of uncached tables with status.
        """
        tables = self.list_tables()
        if name_filter:
            tables = [t for t in tables if name_filter.lower() in t.lower()]

        results = []
        for i, table in enumerate(tables, 1):
            print(f"[{i}/{len(tables)}] 🧹 Uncaching table: {table}")
            self.spark.sql(f"UNCACHE TABLE {table}")
            results.append({"table": table, "uncached": True})

        return results
