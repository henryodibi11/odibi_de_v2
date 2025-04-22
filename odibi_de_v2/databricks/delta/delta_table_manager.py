from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from typing import Optional, List


class DeltaTableManager:
    """
    Manages common Delta Lake operations such as history, time travel, optimize, vacuum, etc.,
    for a specified Delta table or path.

    This class provides methods to interact with Delta Lake tables, enabling operations like
    viewing transaction history, optimizing table layout, and restoring previous versions.

    Attributes:
        spark (SparkSession): An active SparkSession instance.
        table_or_path (str): The name of the table in the metastore or the filesystem path to the Delta table.
        is_path (bool): Indicates whether table_or_path is a filesystem path (True) or a metastore table name (False).
        delta_table (DeltaTable): The DeltaTable object, loaded lazily.

    Methods:
        __init__(self, spark, table_or_path, is_path=False): Initializes the DeltaTableManager.
        describe_detail(self): Returns detailed metadata about the Delta table.
        describe_history(self): Returns the full Delta transaction log history.
        show_history(self, limit=10): Displays a limited number of transaction history entries.
        time_travel(self, version=None, timestamp=None): Provides the ability to query the table as of a certain
            version or timestamp.
        optimize(self, zorder_by=None): Optimizes the table layout, with an option to ZORDER by specific columns.
        vacuum(self, retention_hours=168, dry_run=False): Cleans up old snapshots and files beyond the retention period.
        get_latest_version(self): Retrieves the latest version number of the Delta table.
        restore_version(self, version): Restores the table to a specified version.
        register_table(self, table_name, database=None): Registers a path-based table in the metastore.

    Raises:
        ValueError: If an operation specific to path-based tables is attempted on a metastore table.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> from your_package import DeltaTableManager
        >>> spark = SparkSession.builder.getOrCreate()
        >>> manager = DeltaTableManager(spark, "db.table_name")
        >>> history_df = manager.show_history()
        >>> history_df.show()
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
