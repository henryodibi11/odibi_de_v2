from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime
import csv
import os


class BaseLogSink(ABC):
    """
    Abstract base class for transformation run log sinks.
    
    All log sinks must implement the write method to persist
    transformation execution records to their target storage.
    
    Example:
        >>> class CustomLogSink(BaseLogSink):
        ...     def write(self, records: List[dict]) -> None:
        ...         for record in records:
        ...             print(record)
    """
    
    @abstractmethod
    def write(self, records: List[dict]) -> None:
        """
        Write transformation run log records to the target storage.
        
        Args:
            records: List of dictionaries with keys:
                - transformation_id (str)
                - project (str)
                - plant (str, optional)
                - asset (str, optional)
                - start_time (datetime)
                - end_time (datetime, optional)
                - status (str)
                - duration_seconds (float, optional)
                - error_message (str, optional)
                - env (str)
        
        Raises:
            Exception: If write operation fails.
        """
        pass


class SparkDeltaLogSink(BaseLogSink):
    """
    Writes transformation run logs to a Delta Lake table using Spark.
    
    Automatically creates the target table if it doesn't exist.
    
    Args:
        spark: Active SparkSession instance
        table_name: Fully qualified Delta table name (e.g., "config_driven.TransformationRunLog")
    
    Example:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.getOrCreate()
        >>> sink = SparkDeltaLogSink(spark, "config_driven.TransformationRunLog")
        >>> sink.write([{
        ...     'transformation_id': 'T001',
        ...     'project': 'my_project',
        ...     'plant': 'plant_a',
        ...     'asset': 'asset_1',
        ...     'start_time': datetime.now(),
        ...     'end_time': datetime.now(),
        ...     'status': 'SUCCESS',
        ...     'duration_seconds': 5.2,
        ...     'error_message': None,
        ...     'env': 'qat'
        ... }])
    """
    
    def __init__(self, spark: Any, table_name: str = "config_driven.TransformationRunLog"):
        try:
            from pyspark.sql import SparkSession, Row
            from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
            from pyspark.sql.utils import AnalysisException
        except ImportError as e:
            raise ImportError(
                "PySpark is required for SparkDeltaLogSink. "
                "Install with: pip install pyspark"
            ) from e
        
        self.spark = spark
        self.table_name = table_name
        self.Row = Row
        self.AnalysisException = AnalysisException
        
        # Define schema
        self.LOG_SCHEMA = StructType([
            StructField("transformation_id", StringType(), False),
            StructField("project", StringType(), False),
            StructField("plant", StringType(), True),
            StructField("asset", StringType(), True),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("duration_seconds", FloatType(), True),
            StructField("error_message", StringType(), True),
            StructField("env", StringType(), True)
        ])
        
        # Create schema if needed
        schema_name = table_name.split('.')[0] if '.' in table_name else 'default'
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    
    def write(self, records: List[dict]) -> None:
        """
        Write records to Delta Lake table via Spark.
        
        Args:
            records: List of transformation run log dictionaries
        
        Raises:
            Exception: If Spark write operation fails
        """
        if not records:
            return
        
        # Convert dicts to Rows
        rows = [
            self.Row(
                transformation_id=str(r.get('transformation_id', '')),
                project=str(r.get('project', '')),
                plant=r.get('plant'),
                asset=r.get('asset'),
                start_time=r.get('start_time'),
                end_time=r.get('end_time'),
                status=r.get('status'),
                duration_seconds=r.get('duration_seconds'),
                error_message=r.get('error_message'),
                env=r.get('env')
            )
            for r in records
        ]
        
        df = self.spark.createDataFrame(rows, schema=self.LOG_SCHEMA)
        
        try:
            (
                df.write
                .format("delta")
                .mode("append")
                .saveAsTable(self.table_name)
            )
        except self.AnalysisException as e:
            if "Table or view not found" in str(e) or "does not exist" in str(e):
                # Create table on first write
                (
                    df.write
                    .format("delta")
                    .mode("overwrite")
                    .saveAsTable(self.table_name)
                )
            else:
                raise


class SQLTableLogSink(BaseLogSink):
    """
    Writes transformation run logs to a SQL database table using INSERT statements.
    
    Automatically creates the target table if it doesn't exist (SQL Server syntax).
    
    Args:
        connection_string: SQLAlchemy-compatible connection string or pyodbc connection string
        table_name: SQL table name (e.g., "TransformationRunLog")
        schema: Database schema (default: "dbo")
    
    Example:
        >>> sink = SQLTableLogSink(
        ...     connection_string="DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=mydb;UID=user;PWD=pass",
        ...     table_name="TransformationRunLog",
        ...     schema="config_driven"
        ... )
        >>> sink.write([{
        ...     'transformation_id': 'T001',
        ...     'project': 'my_project',
        ...     'plant': None,
        ...     'asset': None,
        ...     'start_time': datetime.now(),
        ...     'end_time': None,
        ...     'status': 'RUNNING',
        ...     'duration_seconds': None,
        ...     'error_message': None,
        ...     'env': 'qat'
        ... }])
    """
    
    def __init__(
        self,
        connection_string: str,
        table_name: str = "TransformationRunLog",
        schema: str = "dbo"
    ):
        try:
            import pyodbc
        except ImportError as e:
            raise ImportError(
                "pyodbc is required for SQLTableLogSink. "
                "Install with: pip install pyodbc"
            ) from e
        
        self.connection_string = connection_string
        self.table_name = table_name
        self.schema = schema
        self.full_table_name = f"{schema}.{table_name}"
        self.pyodbc = pyodbc
        
        # Ensure table exists
        self._create_table_if_not_exists()
    
    def _create_table_if_not_exists(self) -> None:
        """Create the log table if it doesn't exist (SQL Server syntax)."""
        create_table_sql = f"""
        IF NOT EXISTS (SELECT * FROM sys.tables t 
                       JOIN sys.schemas s ON t.schema_id = s.schema_id 
                       WHERE s.name = '{self.schema}' AND t.name = '{self.table_name}')
        BEGIN
            CREATE TABLE {self.full_table_name} (
                transformation_id NVARCHAR(255) NOT NULL,
                project NVARCHAR(255) NOT NULL,
                plant NVARCHAR(255) NULL,
                asset NVARCHAR(255) NULL,
                start_time DATETIME2 NOT NULL,
                end_time DATETIME2 NULL,
                status NVARCHAR(50) NULL,
                duration_seconds FLOAT NULL,
                error_message NVARCHAR(MAX) NULL,
                env NVARCHAR(50) NULL
            )
        END
        """
        
        try:
            conn = self.pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            # If table creation fails, it might already exist or schema doesn't exist
            # We'll catch errors during write instead
            pass
    
    def write(self, records: List[dict]) -> None:
        """
        Write records to SQL table using INSERT statements.
        
        Args:
            records: List of transformation run log dictionaries
        
        Raises:
            Exception: If SQL write operation fails
        """
        if not records:
            return
        
        insert_sql = f"""
        INSERT INTO {self.full_table_name} 
        (transformation_id, project, plant, asset, start_time, end_time, 
         status, duration_seconds, error_message, env)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        conn = self.pyodbc.connect(self.connection_string)
        cursor = conn.cursor()
        
        try:
            for record in records:
                cursor.execute(
                    insert_sql,
                    (
                        str(record.get('transformation_id', '')),
                        str(record.get('project', '')),
                        record.get('plant'),
                        record.get('asset'),
                        record.get('start_time'),
                        record.get('end_time'),
                        record.get('status'),
                        record.get('duration_seconds'),
                        record.get('error_message'),
                        record.get('env')
                    )
                )
            conn.commit()
        finally:
            cursor.close()
            conn.close()


class FileLogSink(BaseLogSink):
    """
    Writes transformation run logs to a CSV file as a fallback option.
    
    Useful for local development, testing, or when database connectivity is unavailable.
    
    Args:
        file_path: Path to the CSV log file (will be created if doesn't exist)
        append: If True, append to existing file; if False, overwrite (default: True)
    
    Example:
        >>> sink = FileLogSink(file_path="logs/transformation_runs.csv")
        >>> sink.write([{
        ...     'transformation_id': 'T001',
        ...     'project': 'my_project',
        ...     'plant': 'plant_a',
        ...     'asset': None,
        ...     'start_time': datetime.now(),
        ...     'end_time': datetime.now(),
        ...     'status': 'SUCCESS',
        ...     'duration_seconds': 3.5,
        ...     'error_message': None,
        ...     'env': 'dev'
        ... }])
    """
    
    FIELDNAMES = [
        'transformation_id',
        'project',
        'plant',
        'asset',
        'start_time',
        'end_time',
        'status',
        'duration_seconds',
        'error_message',
        'env'
    ]
    
    def __init__(self, file_path: str, append: bool = True):
        self.file_path = file_path
        self.append = append
        
        # Create directory if it doesn't exist
        dir_path = os.path.dirname(file_path)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path)
        
        # Create file with headers if it doesn't exist
        if not os.path.exists(file_path):
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.FIELDNAMES)
                writer.writeheader()
    
    def write(self, records: List[dict]) -> None:
        """
        Write records to CSV file.
        
        Args:
            records: List of transformation run log dictionaries
        
        Raises:
            Exception: If file write operation fails
        """
        if not records:
            return
        
        mode = 'a' if self.append else 'w'
        
        with open(self.file_path, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.FIELDNAMES, extrasaction='ignore')
            
            # Write header if not appending or file is new
            if not self.append or os.path.getsize(self.file_path) == 0:
                writer.writeheader()
            
            # Convert datetime objects to ISO format strings
            for record in records:
                row = record.copy()
                for key in ['start_time', 'end_time']:
                    if key in row and row[key] is not None:
                        if isinstance(row[key], datetime):
                            row[key] = row[key].isoformat()
                writer.writerow(row)
