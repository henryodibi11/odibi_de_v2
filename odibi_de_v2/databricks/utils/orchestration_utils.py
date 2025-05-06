def run_notebook_with_logging(
    spark,
    notebook_path: str,
    logic_app_url: str,
    base_path: str,
    table_name: str,
    timeout_seconds: int = 3600,
    alert_email: str = None
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
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    try:
        start_time = time.time()
        dbutils.notebook.run(notebook_path, timeout_seconds=timeout_seconds)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"[run_notebook_with_logging] Successfully executed {notebook_path} in {execution_time:.2f} seconds.")

        log_to_centralized_table(
            spark=spark,
            base_path=base_path,
            table_name=table_name,
            notebook_path=notebook_path,
            status="Success"
        )

    except Exception as e:
        error_message = str(e)
        print(f"[run_notebook_with_logging] Failed to execute {notebook_path}. Error: {error_message}")

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
                subject=f"Notebook Execution Failure: {notebook_path}",
                body=error_message,
                logic_app_url=logic_app_url
            )

        raise  # Re-raise the exception





