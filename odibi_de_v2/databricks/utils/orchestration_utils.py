def run_notebook_with_logging(
    spark,
    notebook_path: str,
    logic_app_url: str,
    base_path: str,
    table_name: str,
    timeout_seconds: int = 3600,
    alert_email: str = None,
    retries: int = 0,
    retry_delay_seconds: int = 60
):
    """
    Executes a Databricks notebook with centralized logging and optional email alerting on failure.

    This function runs a specified Databricks notebook using an active Spark session and logs the execution status to a
    centralized Delta table. If the notebook execution fails, it can optionally send an alert email using a specified
    Logic App URL.

    Args:
        spark (SparkSession): The active Spark session to use for logging.
        notebook_path (str): The path to the Databricks notebook that needs to be executed.
        logic_app_url (str): The URL of the Logic App for sending alert emails in case of failure.
        base_path (str): The base path where the centralized logging Delta table is located.
        table_name (str): The name of the Delta table where execution logs will be stored.
        timeout_seconds (int, optional): The maximum time in seconds to wait for the notebook to complete.
            Defaults to 3600 seconds (1 hour).
        alert_email (str, optional): The email address to send an alert to in case of a failure.
            If None, no email is sent.
        retries (int, optional): Number of retry attempts for transient failures. Defaults to 0 (no retries).
        retry_delay_seconds (int, optional): Delay in seconds between retry attempts. Defaults to 60.

    Raises:
        Exception: Re-raises any exception that occurs during the notebook execution to be handled by the caller.

    Example:
        >>> spark_session = SparkSession.builder.appName("Example").getOrCreate()
        >>> run_notebook_with_logging(
        ... spark=spark_session,
        ... notebook_path="/path/to/notebook",
        ... logic_app_url="https://example.com/logic-app",
        ... base_path="/path/to/logging",
        ... table_name="notebook_logs",
        ... timeout_seconds=1800,
        ... alert_email="alert@example.com"
        )
    """
    import time
    from odibi_de_v2.utils import send_email_using_logic_app
    from odibi_de_v2.databricks import log_to_centralized_table
    from odibi_de_v2.logger import get_logger
    from pyspark.dbutils import DBUtils
    
    logger = get_logger()
    dbutils = DBUtils(spark)
    
    last_error = None
    for attempt in range(retries + 1):
        try:
            if attempt > 0:
                logger.log("info", f"Retry attempt {attempt}/{retries} for {notebook_path} after {retry_delay_seconds}s delay",
                           module="DATABRICKS", component="orchestration_utils", method="run_notebook_with_logging")
                time.sleep(retry_delay_seconds)
            
            start_time = time.time()
            dbutils.notebook.run(notebook_path, timeout_seconds=timeout_seconds)
            end_time = time.time()
            execution_time = end_time - start_time
            logger.log("info", f"Successfully executed {notebook_path} in {execution_time:.2f} seconds (attempt {attempt + 1}/{retries + 1})", 
                       module="DATABRICKS", component="orchestration_utils", method="run_notebook_with_logging")

            log_to_centralized_table(
                spark=spark,
                base_path=base_path,
                table_name=table_name,
                notebook_path=notebook_path,
                status="Success",
                execution_time_seconds=execution_time
            )
            return

        except Exception as e:
            last_error = e
            error_message = str(e)
            if attempt < retries:
                logger.log("warning", f"Attempt {attempt + 1}/{retries + 1} failed for {notebook_path}: {error_message}. Will retry.",
                           module="DATABRICKS", component="orchestration_utils", method="run_notebook_with_logging")
            else:
                logger.log("error", f"All {retries + 1} attempts failed for {notebook_path}: {error_message}",
                           module="DATABRICKS", component="orchestration_utils", method="run_notebook_with_logging", exc_info=True)

    if last_error:
        error_message = str(last_error)
        log_to_centralized_table(
            spark=spark,
            base_path=base_path,
            table_name=table_name,
            notebook_path=notebook_path,
            status="Failure",
            error_message=error_message
        )

        if alert_email:
            send_email_using_logic_app(
                to=alert_email,
                subject=f"Notebook Execution Failure: {notebook_path} (after {retries + 1} attempts)",
                body=error_message,
                logic_app_url=logic_app_url
            )

        raise last_error





