
"""
delta_table_manager.py
----------------------
Utility module for managing Delta Lake tables in Databricks/Spark.

This manager provides a single class, `DeltaTableManager`, that wraps
common Delta operations (metadata, versioning, maintenance, caching)
behind clean, method-level APIs. It supports both metastore tables
and path-based Delta tables (using `is_path=True`).

It pairs seamlessly with:
    - SparkDataReaderFromConfig → for ingestion
    - SparkDataSaverFromConfig  → for batch/stream writes
    - DeltaMergeManager         → for upserts/merges

----------------------------------------------------------------------
QUICK START (Full Lifecycle Example)
----------------------------------------------------------------------

from pyspark.sql import SparkSession
from odibi_de_v2.databricks.delta.delta_table_manager import DeltaTableManager

spark = SparkSession.builder.getOrCreate()

# 1️⃣ Initialize Manager
# For a metastore table
manager = DeltaTableManager(spark, "silver_db.energy_efficiency")

# Or for a path-based table
path_mgr = DeltaTableManager(spark, "dbfs:/FileStore/silver/energy_efficiency", is_path=True)


# 2️⃣ Inspect Metadata
# --------------------------------------------------------------------
# Detail: schema, size, partition info
manager.describe_detail().show(truncate=False)

# Full transaction history (version, operation, user, timestamp)
manager.describe_history().show(5)

# Limited history (most recent operations)
manager.show_history(limit=3).show()


# 3️⃣ Versioning & Time Travel
# --------------------------------------------------------------------
# Read the table as it was at version 5
df_v5 = manager.time_travel(version=5)

# Read as of a specific timestamp
df_time = manager.time_travel(timestamp="2025-01-01")

# Get the current highest version number
latest = manager.get_latest_version()
print("Latest version:", latest)

# Restore the table to a previous version
manager.restore_version(5)


# 4️⃣ Maintenance & Optimization
# --------------------------------------------------------------------
# Compact small files and optionally ZORDER by key columns
manager.optimize(zorder_by=["Plant", "Asset"])

# Remove obsolete data files older than the retention window
manager.vacuum(retention_hours=72, dry_run=True)


# 5️⃣ Register Path-Based Tables
# --------------------------------------------------------------------
# Convert a path-based Delta table into a registered table
path_mgr.register_table("energy_efficiency", database="silver_db")


# 6️⃣ Cache Control & Diagnostics
# --------------------------------------------------------------------
# Cache table in Spark memory for faster reuse
manager.cache(materialize=True)

# Check cache state
print("Cached?", manager.is_cached())

# Show size and memory usage
print(manager.cache_status())

# Refresh metadata and rebuild cache
manager.recache()

# Remove from memory
manager.uncache()


# 7️⃣ Session-Wide Cache Tools
# --------------------------------------------------------------------
# List all cached datasets with memory info
DeltaTableManager.list_cached_tables(spark)

# Clear all caches in session (useful in development)
DeltaTableManager.uncache_all(spark)


----------------------------------------------------------------------
NOTES
----------------------------------------------------------------------
- All operations are Spark SQL–driven; safe to use in Databricks notebooks or pipelines.
- `VACUUM` permanently deletes old files — use `dry_run=True` to preview.
- Works with both catalog tables and raw Delta paths.
- For production pipelines, combine with SparkDataSaverFromConfig to automatically
  register and optimize Delta tables after writes.

"""


from typing import Optional, List
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from odibi_de_v2.utils.decorators import log_call


class DeltaTableManager:
    """Lightweight manager for common Delta Lake operations."""

    def __init__(self, spark: SparkSession, table_or_path: str, is_path: bool = False):
        """Create a manager for a metastore table or a path-based Delta table."""
        self.spark = spark
        self.table_or_path = table_or_path
        self.is_path = is_path
        self.delta_table = None  # lazy

    # ---------- Internals ----------
    def _load_delta_table(self) -> DeltaTable:
        """Lazy-load the DeltaTable handle."""
        if self.delta_table is None:
            self.delta_table = (
                DeltaTable.forPath(self.spark, self.table_or_path)
                if self.is_path else
                DeltaTable.forName(self.spark, self.table_or_path)
            )
        return self.delta_table

    # ---------- Metadata ----------
    @log_call(module="DELTA", component="DeltaTableManager")
    def describe_detail(self) -> DataFrame:
        """Return Delta detail() as a Spark DataFrame."""
        return self._load_delta_table().detail()

    @log_call(module="DELTA", component="DeltaTableManager")
    def describe_history(self) -> DataFrame:
        """Return full Delta history() as a Spark DataFrame."""
        return self._load_delta_table().history()

    @log_call(module="DELTA", component="DeltaTableManager")
    def show_history(self, limit: int = 10) -> DataFrame:
        """Return the most recent `limit` rows of Delta history()."""
        return self._load_delta_table().history(limit)

    # ---------- Versioning ----------
    @log_call(module="DELTA", component="DeltaTableManager")
    def time_travel(self, version: Optional[int] = None, timestamp: Optional[str] = None) -> DataFrame:
        """Read the table at a past version or timestamp."""
        reader = self.spark.read.format("delta")
        if version is not None:
            reader = reader.option("versionAsOf", version)
        elif timestamp is not None:
            reader = reader.option("timestampAsOf", timestamp)
        return reader.load(self.table_or_path) if self.is_path else reader.table(self.table_or_path)

    @log_call(module="DELTA", component="DeltaTableManager")
    def get_latest_version(self) -> int:
        """Return the latest committed version number."""
        history = self.show_history(1)
        return history.collect()[0]["version"]

    @log_call(module="DELTA", component="DeltaTableManager")
    def restore_version(self, version: int):
        """Restore table to a specific version using SQL RESTORE."""
        self.spark.sql(f"RESTORE TABLE {self.table_or_path} TO VERSION AS OF {version}")

    # ---------- Maintenance ----------
    @log_call(module="DELTA", component="DeltaTableManager")
    def optimize(self, zorder_by: Optional[List[str]] = None):
        """Run OPTIMIZE (optionally ZORDER BY the provided columns)."""
        sql = f"OPTIMIZE {self.table_or_path}"
        if zorder_by:
            cols = ", ".join(zorder_by)
            sql += f" ZORDER BY ({cols})"
        self.spark.sql(sql)

    @log_call(module="DELTA", component="DeltaTableManager")
    def vacuum(self, retention_hours: int = 168, dry_run: bool = False):
        """Run VACUUM with an optional DRY RUN; retention_hours affects time travel window."""
        dry = " DRY RUN" if dry_run else ""
        self.spark.sql(f"VACUUM {self.table_or_path} RETAIN {retention_hours} HOURS{dry}")

    @log_call(module="DELTA", component="DeltaTableManager")
    def register_table(self, table_name: str, database: Optional[str] = None):
        """Register a path-based table in the metastore (requires is_path=True)."""
        if not self.is_path:
            raise ValueError("register_table() only works for path-based Delta tables")
        full = f"{database}.{table_name}" if database else table_name
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full}
            USING DELTA
            LOCATION '{self.table_or_path}'
        """)

    # ---------- Cache Control ----------
    @log_call(module="DELTA", component="DeltaTableManager")
    def cache(self, materialize: bool = True):
        """CACHE TABLE; optionally materialize with COUNT(*)."""
        self.spark.sql(f"CACHE TABLE {self.table_or_path}")
        if materialize:
            self.spark.sql(f"SELECT COUNT(*) FROM {self.table_or_path}").show()

    @log_call(module="DELTA", component="DeltaTableManager")
    def uncache(self):
        """UNCACHE TABLE."""
        self.spark.sql(f"UNCACHE TABLE {self.table_or_path}")

    def is_cached(self) -> bool:
        """Return True if table is cached."""
        return self.spark.catalog.isCached(self.table_or_path)

    @log_call(module="DELTA", component="DeltaTableManager")
    def recache(self, materialize: bool = True):
        """Refresh metadata and re-cache (dev-friendly)."""
        if self.is_cached():
            self.uncache()
        self.spark.sql(f"REFRESH TABLE {self.table_or_path}")
        self.cache(materialize=materialize)

    @log_call(module="DELTA", component="DeltaTableManager")
    def cache_status(self) -> dict:
        """Report cached state and sizes (estimated vs actual)."""
        import math

        is_cached = self.is_cached()

        # Estimated size from query plan
        logical_df = (
            self.spark.table(self.table_or_path)
            if not self.is_path else
            self.spark.read.format("delta").load(self.table_or_path)
        )
        size_in_bytes = logical_df._jdf.queryExecution().logical().stats().sizeInBytes()

        def human_readable(n: int) -> str:
            if n <= 0:
                return "0 B"
            units = ["B", "KB", "MB", "GB", "TB"]
            idx = int(math.floor(math.log(n, 1024)))
            return f"{n / (1024**idx):.2f} {units[idx]}"

        cached_memory_bytes = 0
        try:
            storage_status = self.spark._jsparkSession.sharedState().cacheManager().lookupCachedData(
                logical_df._jdf.logicalPlan()
            )
            if storage_status.nonEmpty():
                cached_memory_bytes = storage_status.get().sizeInBytes()
        except Exception:
            cached_memory_bytes = 0

        return {
            "table": self.table_or_path,
            "is_cached": is_cached,
            "estimated_size_bytes": size_in_bytes,
            "estimated_size_readable": human_readable(size_in_bytes),
            "cached_memory_bytes": cached_memory_bytes,
            "cached_memory_readable": human_readable(cached_memory_bytes),
        }

    # ---------- Session-wide helpers ----------
    @staticmethod
    @log_call(module="DELTA", component="DeltaTableManager")
    def uncache_all(spark: SparkSession):
        """Clear all caches in the current Spark session."""
        spark.catalog.clearCache()

    @staticmethod
    @log_call(module="DELTA", component="DeltaTableManager")
    def list_cached_tables(spark: SparkSession) -> list:
        """List all cached datasets with basic storage details."""
        import math
        results = []

        def human_readable(n: int) -> str:
            if n <= 0:
                return "0 B"
            units = ["B", "KB", "MB", "GB", "TB"]
            idx = int(math.floor(math.log(n, 1024)))
            return f"{n / (1024**idx):.2f} {units[idx]}"

        try:
            jsc = spark.sparkContext._jsc
            rdds = jsc.getPersistentRDDs()
            for rdd_id, rdd in rdds.items():
                name = rdd.name() if rdd.name() else f"RDD_{rdd_id}"
                storage = rdd.getStorageLevel().toString()
                mem_size = rdd.memorySize()
                parts_cached = rdd.numCachedPartitions()

                results.append({
                    "table": name,
                    "cached_memory_bytes": mem_size,
                    "cached_memory_readable": human_readable(mem_size),
                    "storage_level": storage,
                    "partitions_cached": parts_cached,
                })
        except Exception as e:
            results.append({"error": str(e)})

        return results
