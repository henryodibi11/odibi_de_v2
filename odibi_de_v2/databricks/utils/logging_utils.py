from datetime import datetime
import re
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from typing import Optional


def log_to_centralized_table(
    spark,
    base_path: str,
    table_name: str,
    notebook_path: str,
    status: str,
    error_message: Optional[str] = None
):
    """
    Logs the execution status of a notebook to a centralized Delta table using Apache Spark.

    This function creates or appends to a Delta table that records the execution status of notebooks. It includes
    details such as the notebook path, execution status, error message (if any), and the timestamp of the log entry.

    Args:
        spark (SparkSession): The active Spark session used to handle the data.
        base_path (str): The base directory path in ADLS/ABFSS where the logs are stored. This path should not 
            include a trailing slash.
        table_name (str): The name of the Delta table. This name will be sanitized to remove any non-word characters.
        notebook_path (str): The complete path of the notebook that was executed.
        status (str): The execution status of the notebook, typically 'Success' or 'Failure'.
        error_message (Optional[str]): Additional details about the error if the status is 'Failure'. Defaults to None.

    Raises:
        ValueError: If any of the required arguments are not provided or if the `status` is neither
            'Success' nor 'Failure'.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.appName("ExampleApp").getOrCreate()
        >>> log_to_centralized_table(spark, "/mnt/delta/logs", "notebook_logs", "/path/to/notebook.py", "Success")

    Note:
        This function assumes that the Delta Lake library is correctly configured in the Spark session.
    """

    schema = StructType([
        StructField("Notebook_Path", StringType(), True),
        StructField("Status", StringType(), True),
        StructField("Error_Message", StringType(), True),
        StructField("Timestamp", TimestampType(), True)
    ])

    log_df = spark.createDataFrame([
        (notebook_path, status, error_message, datetime.now())
    ], schema)

    # Clean table name just in case (no weird characters)
    sanitized_table_name = re.sub(r'\W+', '_', table_name.strip())

    logging_path = f"{base_path}/{sanitized_table_name}"
    log_df.write.format("delta").mode("append").save(logging_path)

    print(f"[log_to_centralized_table] Logged to {logging_path}")







