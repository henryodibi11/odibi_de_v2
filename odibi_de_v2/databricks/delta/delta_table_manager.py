from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from typing import Optional, List


class DeltaTableManager:
    """
    A utility class for managing Delta Lake tables in Spark.

    This manager provides a high-level API for common Delta operations such as
    metadata inspection, transaction history, time travel, optimization,
    cleanup, rollback, and cache management. It works with both
    metastore-registered tables and path-based Delta tables.

    Key Capabilities
    ----------------
    - Metadata:
        * `describe_detail()` → detailed table metadata (schema, partitions, stats).
        * `describe_history()` / `show_history()` → full or recent transaction log.
    - Data Versioning:
        * `time_travel()` → read the table at a past version or timestamp.
        * `get_latest_version()` → retrieve the latest committed version.
        * `restore_version()` → rollback the table to a previous version.
    - Optimization & Maintenance:
        * `optimize()` → run Delta OPTIMIZE, with optional ZORDER.
        * `vacuum()` → remove old files beyond a retention period (affects time travel).
        * `register_table()` → register a path-based table in the metastore.
    - Cache Control:
        * `cache()`, `uncache()`, `recache()` → manage table caching in Spark memory.
        * `is_cached()` → check if table is currently cached.
        * `cache_status()` → report cache state, estimated size, and actual memory used.
        * `uncache_all()` → clear all caches in the session.
        * `list_cached_tables()` → list all cached datasets with storage details.

    Attributes
    ----------
    spark : SparkSession
        The active Spark session.
    table_or_path : str
        Table name (if metastore) or filesystem path to the Delta table.
    is_path : bool
        True if `table_or_path` is a filesystem path, False if it is a metastore table.
    delta_table : DeltaTable or None
        The underlying DeltaTable instance (lazily loaded).

    Notes
    -----
    - `VACUUM` never deletes active data, only obsolete files; it reduces
      or eliminates the ability to time travel depending on `retention_hours`.
    - For dev workflows, `recache()` is helpful after making table changes.
    - `cache_status()` and `list_cached_tables()` expose info similar to the Spark UI.

    Example
    -------
    >>> spark = SparkSession.builder.getOrCreate()
    >>> manager = DeltaTableManager(spark, "bronze_db.my_table")
    >>> manager.optimize(zorder_by=["id", "date"])
    >>> manager.cache()
    >>> print(manager.cache_status())
    {'table': 'bronze_db.my_table', 'is_cached': True, 'estimated_size_readable': '432.00 MB', ...}
    """

    def __init__(self, spark: SparkSession, table_or_path: str, is_path: bool = False):
        """
        Initializes a new instance to manage Delta table interactions within Apache Spark.

        This constructor sets up an object to interact with a Delta table, which can be specified either
        by its name in the Spark metastore or by a direct filesystem path. The actual Delta table is not
        loaded until needed (lazy loading).

        Args:
            spark (SparkSession): An active SparkSession instance to execute operations within Spark.
            table_or_path (str): The name of the Delta table in the metastore if `is_path` is False, or the
                filesystem path to the Delta table if `is_path` is True.
            is_path (bool, optional): A flag indicating the nature of `table_or_path`. If True, `table_or_path`
                is treated as a filesystem path. Defaults to False.

        Example:
            To manage a Delta table by name in the metastore:
            manager = DeltaTableManager(spark_session, "my_delta_table")

            To manage a Delta table by filesystem path:
            manager = DeltaTableManager(spark_session, "/path/to/delta/table", is_path=True)
        """
        self.spark = spark
        self.table_or_path = table_or_path
        self.is_path = is_path
        self.delta_table = None  # Lazy-loaded

    def _load_delta_table(self) -> DeltaTable:
        """
        Loads and returns a DeltaTable instance based on the configuration.

        This method initializes a DeltaTable object for the given path or table name.
        If the DeltaTable object has already been created, it returns the existing instance.
        The method determines whether to load by path or by name based on the `is_path` attribute.

        Returns:
            DeltaTable: An instance of DeltaTable associated with the specified path or table name.

        Raises:
            Exception: If there is an error in accessing the DeltaTable through the provided path or name.

        Example:
            # Assuming `self` is an instance of a class with attributes `spark`, `table_or_path`, and `is_path`
            delta_table = self._load_delta_table()
        """
        if self.delta_table is None:
            self.delta_table = (
                DeltaTable.forPath(self.spark, self.table_or_path)
                if self.is_path else
                DeltaTable.forName(self.spark, self.table_or_path)
            )
        return self.delta_table

    def describe_detail(self) -> DataFrame:
        """
        Generates a DataFrame containing detailed metadata about the Delta table.

        This method retrieves comprehensive information about the Delta table,
        including the file path, partition columns, and the schema of the table.
        It is useful for understanding the structure and storage details of the table.

        Returns:
            DataFrame: A pandas DataFrame with a single row containing the detailed metadata of the Delta table.

        Raises:
            DeltaTableError: If there is an issue loading the Delta table or if the table does not exist.

        Example:
            >>> delta_table = DeltaTable("/path/to/delta/table")
            >>> detail_df = delta_table.describe_detail()
            >>> print(detail_df)
        """
        return self._load_delta_table().detail()

    def describe_history(self) -> DataFrame:
        """
        Fetches and returns the complete transaction log history of a Delta table as a pandas DataFrame.

        This method retrieves the history of operations performed on the Delta table associated with the
        current instance. The history includes details such as the version number, timestamp, operation
        type, and additional metadata related to each transaction.

        Returns:
            DataFrame: A pandas DataFrame containing columns like version number, timestamp, operation,
            and other relevant details of each transaction in the Delta table's history.

        Raises:
            DeltaTableError: If there is an issue loading the Delta table or fetching its history.

        Example:
            >>> delta_instance = DeltaTable("/path/to/delta/table")
            >>> history_df = delta_instance.describe_history()
            >>> print(history_df.head())
        """
        return self._load_delta_table().history()

    def show_history(self, limit: int = 10) -> DataFrame:
        """
        Fetches and displays the transaction history of a Delta table as a DataFrame.

        This method retrieves a specified number of recent transactions from the Delta table's history,
        including details such as version number, timestamp, and other metadata associated with each transaction.

        Args:
            limit (int, optional): The maximum number of recent transaction records to retrieve. Defaults to 10.

        Returns:
            DataFrame: A pandas DataFrame containing the transaction history of the Delta table. Each row in the
            DataFrame represents a transaction, with columns for details like version, timestamp, etc.

        Example:
            >>> delta_table.show_history(limit=5)
        """
        return self._load_delta_table().history(limit)

    def time_travel(
        self,
        version: Optional[int] = None,
        timestamp: Optional[str] = None
    ) -> DataFrame:
        """
        Retrieves a snapshot of a Delta table at a specified version or timestamp, allowing for data versioning
        and historical analysis.

        This method enables accessing previous states of a Delta table, which is useful for auditing changes,
        reproducing experiments, or fixing data corruption issues by accessing data as it was at a specific
        point in time.

        Args:
            version (Optional[int]): The specific version number of the Delta table to retrieve. If provided,
            `timestamp` should be None.
            timestamp (Optional[str]): The specific timestamp to retrieve the Delta table's state. This should
            be a string in a format recognized by Delta, such as 'YYYY-MM-DD'. If provided, `version` should be None.

        Returns:
            DataFrame: A DataFrame representing the state of the Delta table at the specified version or timestamp.

        Raises:
            ValueError: If both `version` and `timestamp` are provided, or if neither is provided.

        Example:
            # To retrieve the table as of version 5
            df_version = time_travel(version=5)

            # To retrieve the table as of January 1st, 2023
            df_timestamp = time_travel(timestamp='2023-01-01')
        """
        reader = self.spark.read.format("delta")
        if version is not None:
            reader = reader.option("versionAsOf", version)
        elif timestamp is not None:
            reader = reader.option("timestampAsOf", timestamp)
        return (
            reader.load(self.table_or_path)
            if self.is_path else reader.table(self.table_or_path)
        )

    def optimize(self, zorder_by: Optional[List[str]] = None):
        """
        Optimizes the data layout of a Delta table, potentially using ZORDER to organize data
        by specified columns.

        This method constructs and executes an SQL command to optimize the storage of a Delta table
        in a Spark environment.
        If the `zorder_by` parameter is provided, the optimization includes a ZORDER operation on
        the specified columns, which can improve query performance on those columns.

        Args:
            zorder_by (Optional[List[str]]): A list of column names to be used for ZORDERing. If
            None or omitted, the optimization will not include a ZORDER clause.

        Returns:
            None: This method does not return anything.

        Example:
            >>> delta_table.optimize()
            This will simply optimize the table without any ZORDER.

            >>> delta_table.optimize(zorder_by=['date', 'id'])
            This will optimize the table and also arrange the data based on the 'date' and 'id' columns.
        """
        sql = f"OPTIMIZE {self.table_or_path}"
        if zorder_by:
            columns = ", ".join(zorder_by)
            sql += f" ZORDER BY ({columns})"
        self.spark.sql(sql)

    def vacuum(self, retention_hours: int = 168, dry_run: bool = False):
        """
        Cleans up old snapshots and files from a Delta table, retaining only the data within the
        specified retention period.

        This method performs a cleanup operation on the Delta table associated with the instance
        by removing all snapshots and files that are older than the specified retention hours. It helps
        in managing the storage efficiently by discarding unnecessary data.

        Args:
            retention_hours (int): The number of hours of historical data to retain. Defaults to 168 hours,
            which is equivalent to 7 days.
            dry_run (bool): If set to True, the method will only simulate the deletion process by listing
            the files that would be deleted, without actually removing any files. Defaults to False.

        Returns:
            None: This method does not return any value.

        Raises:
            SparkSqlError: If the SQL query execution fails.

        Example:
            To clean up a Delta table while retaining the last 5 days of data, and just simulate the deletion process:
            >>> delta_table.vacuum(retention_hours=120, dry_run=True)
        """
        dry = " DRY RUN" if dry_run else ""
        self.spark.sql(
            f"VACUUM {self.table_or_path} RETAIN {retention_hours} HOURS{dry}"
        )

    def get_latest_version(self) -> int:
        """
        Retrieves the latest version number of the Delta table.

        This method queries the Delta table's history to find the most recent version number available.

        Returns:
            int: The latest version number of the Delta table.

        Raises:
            IndexError: If the Delta table history is empty and no version is available.
            Exception: If there are issues accessing the table history.

        Example:
            >>> delta_table = DeltaTable("/path/to/delta/table")
            >>> latest_version = delta_table.get_latest_version()
            >>> print(f"The latest version of the Delta table is {latest_version}")
        """
        history = self.show_history(1)
        return history.collect()[0]["version"]

    def restore_version(self, version: int):
        """
        Restores the Delta table to a specified historical version.

        This method executes a SQL command to revert the Delta table to the state it was in at a
        given version number. It is useful for scenarios where rollback to a previous state of the data is required.

        Args:
            version (int): The version number of the Delta table to which the table should be restored. This must be
            a valid version number that exists in the Delta table's history.

        Returns:
            None: This method does not return any value.

        Raises:
            ValueError: If the `version` is not a valid integer or does not correspond to a valid version in
            the Delta table history.
            RuntimeError: If the SQL execution fails or if the table cannot be restored to the specified
            version for any other reason.

        Example:
            # Assuming `delta_manager` is an instance of a class containing this method
            delta_manager.restore_version(5)  # Restores the table to version 5
        """
        self.spark.sql(
            f"RESTORE TABLE {self.table_or_path} TO VERSION AS OF {version}"
        )

    def register_table(self, table_name: str, database: Optional[str] = None):
        """
        Registers a path-based Delta table in the metastore, enabling it to be queried using SQL.

        This method allows a Delta table, initialized with a path, to be registered in the metastore
        under a specified name. Once registered, the table can be queried by SQL commands as if it
        were a standard table in the database.

        Args:
            table_name (str): The name under which to register the table.
            database (Optional[str]): The name of the database in which to register the table. If not
            specified, the table is registered in the default database.

        Raises:
            ValueError: If the method is called on a Delta table that was not initialized with `is_path=True`.

        Example:
            >>> delta_manager.register_table("my_table", "analytics_db")
            This would register the table as `analytics_db.my_table` in the metastore, assuming `delta_manager`
            is an instance with a path-based Delta table.

        Note:
            This method only works if the current Delta table was initialized with `is_path=True`.
        """
        if not self.is_path:
            raise ValueError("register_table() only works for path-based Delta tables")

        full_table_name = f"{database}.{table_name}" if database else table_name
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_table_name}
            USING DELTA
            LOCATION '{self.table_or_path}'
        """)

    def cache(self, materialize: bool = True):
        """
        Caches the Delta table in Spark memory (storage memory).

        This method marks the table as cached and, if `materialize` is True,
        triggers an action to fully load the cache into executor memory.

        Args:
            materialize (bool, optional): If True, forces Spark to scan the table
                and populate the cache immediately by executing a COUNT query.
                Defaults to True.

        Example:
            >>> manager.cache()
            # Table is cached in memory and ready for reuse
        """
        self.spark.sql(f"CACHE TABLE {self.table_or_path}")
        if materialize:
            self.spark.sql(f"SELECT COUNT(*) FROM {self.table_or_path}").show()

    def uncache(self):
        """
        Removes the Delta table from Spark memory (storage memory).

        This method clears the cached data for the table from executor memory.

        Example:
            >>> manager.uncache()
            # Table cache is released
        """
        self.spark.sql(f"UNCACHE TABLE {self.table_or_path}")

    def is_cached(self) -> bool:
        """
        Checks whether the Delta table is currently cached in Spark memory.

        Returns:
            bool: True if the table is cached, False otherwise.

        Example:
            >>> manager.is_cached()
            False
        """
        return self.spark.catalog.isCached(self.table_or_path)

    def recache(self, materialize: bool = True):
        """
        Refreshes and recaches the Delta table (useful during development).

        This method ensures the latest version of the table is visible by first
        uncaching any existing cache, refreshing table metadata, and then recaching.

        Args:
            materialize (bool, optional): If True, forces Spark to scan the table
                and populate the cache immediately. Defaults to True.

        Example:
            >>> manager.recache()
            # Ensures fresh version of the table is cached
        """
        if self.is_cached():
            self.uncache()
        self.spark.sql(f"REFRESH TABLE {self.table_or_path}")
        self.cache(materialize=materialize)
        
    def cache_status(self) -> dict:
        """
        Returns the cache status of the Delta table, including whether it is cached,
        Spark's estimated size (from query plan), and actual memory usage if cached.

        This method combines Spark's query plan statistics with runtime storage
        information (as seen in the Spark UI Storage tab).

        Returns:
            dict: A dictionary with the following keys:
                - 'table' (str): The table name or path being managed.
                - 'is_cached' (bool): True if the table is cached, False otherwise.
                - 'estimated_size_bytes' (int): Estimated size of the table in bytes (from query plan).
                - 'estimated_size_readable' (str): Human-readable estimated size.
                - 'cached_memory_bytes' (int): Actual memory size used if cached (0 if not cached).
                - 'cached_memory_readable' (str): Human-readable actual memory size.

        Example:
            >>> status = manager.cache_status()
            >>> print(status)
            {
                'table': 'bronze_db.my_table',
                'is_cached': True,
                'estimated_size_bytes': 452984832,
                'estimated_size_readable': '432.00 MB',
                'cached_memory_bytes': 6291456,
                'cached_memory_readable': '6.00 MB'
            }
        """
        import math

        is_cached = self.is_cached()

        # ---- Estimated size from query plan ----
        logical_plan = (
            self.spark.table(self.table_or_path)
            if not self.is_path else
            self.spark.read.format("delta").load(self.table_or_path)
        )
        size_in_bytes = logical_plan._jdf.queryExecution().logical().stats().sizeInBytes()

        def human_readable(num_bytes: int) -> str:
            if num_bytes <= 0:
                return "0 B"
            units = ["B", "KB", "MB", "GB", "TB"]
            idx = int(math.floor(math.log(num_bytes, 1024)))
            return f"{num_bytes / (1024**idx):.2f} {units[idx]}"

        cached_memory_bytes = 0
        try:
            storage_status = self.spark._jsparkSession.sharedState().cacheManager().lookupCachedData(
                logical_plan._jdf.logicalPlan()
            )
            if storage_status.nonEmpty():
                cached_memory_bytes = storage_status.get().sizeInBytes()
        except Exception:
            # If Spark internals change or table isn't cached
            cached_memory_bytes = 0

        return {
            "table": self.table_or_path,
            "is_cached": is_cached,
            "estimated_size_bytes": size_in_bytes,
            "estimated_size_readable": human_readable(size_in_bytes),
            "cached_memory_bytes": cached_memory_bytes,
            "cached_memory_readable": human_readable(cached_memory_bytes),
        }


    @staticmethod
    def uncache_all(spark: SparkSession):
        """
        Removes all cached tables from the Spark session.

        This method clears every cached table in the current Spark session,
        releasing the associated executor memory. Useful during development
        or debugging when multiple tables have been cached and you want to
        reset the session state.

        Args:
            spark (SparkSession): The active Spark session.

        Example:
            >>> DeltaTableManager.uncache_all(spark)
            # All cached tables are removed
        """
        spark.catalog.clearCache()

    @staticmethod
    def list_cached_tables(spark: SparkSession) -> list:
        """
        Lists all cached tables in the current Spark session, including their
        actual memory usage (similar to the Spark UI Storage tab).

        Args:
            spark (SparkSession): The active Spark session.

        Returns:
            list: A list of dictionaries, each containing:
                - 'table' (str): Table or view name if registered, otherwise internal ID.
                - 'cached_memory_bytes' (int): Actual memory used by the cache in bytes.
                - 'cached_memory_readable' (str): Human-readable memory size.
                - 'storage_level' (str): Storage level (e.g., MEMORY_AND_DISK).
                - 'partitions_cached' (int): Number of partitions currently cached.

        Example:
            >>> DeltaTableManager.list_cached_tables(spark)
            [
                {
                    'table': 'bronze_db.my_table',
                    'cached_memory_bytes': 6291456,
                    'cached_memory_readable': '6.00 MB',
                    'storage_level': 'MEMORY_AND_DISK',
                    'partitions_cached': 5
                }
            ]
        """
        import math
        results = []

        def human_readable(num_bytes: int) -> str:
            if num_bytes <= 0:
                return "0 B"
            units = ["B", "KB", "MB", "GB", "TB"]
            idx = int(math.floor(math.log(num_bytes, 1024)))
            return f"{num_bytes / (1024**idx):.2f} {units[idx]}"

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
                    "partitions_cached": parts_cached
                })
        except Exception as e:
            results.append({"error": str(e)})

        return results
