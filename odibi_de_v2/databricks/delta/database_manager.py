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
        self.spark = spark
        self.database = database

    def list_tables(self) -> List[str]:
        """
        List only Delta tables in the database (skips temp views like `_sqldf`).

        Returns:
            List[str]: Fully-qualified Delta table names (database.table).
        """
        tables = self.spark.sql(f"SHOW TABLES IN {self.database}").collect()
        result = []
        for row in tables:
            if row["isTemporary"]:
                print(f"⚠️ Skipping temporary view: {row['tableName']}")
                continue
            full_name = f"{self.database}.{row['tableName']}"
            try:
                detail = self.spark.sql(f"DESCRIBE DETAIL {full_name}").collect()[0]
                if detail["format"] == "delta":
                    result.append(full_name)
                else:
                    print(f"⚠️ Skipping non-Delta table: {full_name} (format={detail['format']})")
            except Exception as e:
                print(f"⚠️ Skipping {full_name}: not a Delta table ({e})")
        print(f"✅ Found {len(result)} Delta tables in {self.database}")
        return result

    def vacuum_all(self, retention_hours: int = 168, dry_run: bool = False) -> List[Dict[str, str]]:
        """
        Run VACUUM on all Delta tables in the database.
        """
        from odibi_de_v2.databricks import DeltaTableManager
        results = []
        tables = self.list_tables()
        for i, table in enumerate(tables, 1):
            print(f"\n[{i}/{len(tables)}] Vacuuming {table} "
                  f"(retention={retention_hours}h, dry_run={dry_run})...")
            manager = DeltaTableManager(self.spark, table, is_path=False)
            manager.vacuum(retention_hours=retention_hours, dry_run=dry_run)
            print(f"✅ Done vacuuming {table}")
            results.append({
                "table": table,
                "retention_hours": retention_hours,
                "dry_run": dry_run,
            })
        return results

    def optimize_all(self, zorder_by: Optional[List[str]] = None) -> List[Dict[str, str]]:
        """
        Run OPTIMIZE on all Delta tables in the database.
        """
        from odibi_de_v2.databricks import DeltaTableManager
        results = []
        tables = self.list_tables()
        for i, table in enumerate(tables, 1):
            print(f"\n[{i}/{len(tables)}] Optimizing {table} "
                  f"{'(ZORDER BY ' + ', '.join(zorder_by) + ')' if zorder_by else ''}...")
            manager = DeltaTableManager(self.spark, table, is_path=False)
            manager.optimize(zorder_by=zorder_by)
            print(f"✅ Done optimizing {table}")
            results.append({
                "table": table,
                "zorder_by": zorder_by,
            })
        return results
