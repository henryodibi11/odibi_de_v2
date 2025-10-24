from typing import List, Dict

def run_bronze_pipeline(
    project: str,
    table: str,
    domain: str,
    source_id: str,
    target_id: str,
    app_name: str = None,
    secret_scope: str = "",
    blob_name_key: str = "",
    blob_key_key: str = "",
    sql_server_key: str = "",
    sql_db_key: str = "",
    sql_user_key: str = "",
    sql_pass_key: str = "",
    spark_config: dict = {},
    send_email: bool = False,
    email_to: str = None,
    logic_app_url: str = None
) -> List[Dict]:
    """
    Executes the full Bronze layer pipeline: ingestion from source, optional validation,
    and saving to target based on dynamic configuration tables.

    This function handles the following:
    - Initializes Spark and Azure connection with secrets.
    - Loads source and target ingestion configurations from SQL.
    - Reads source data using Spark.
    - Saves ingested data to the Bronze layer (e.g., Delta or Parquet).
    - Optionally sends email notifications using Logic Apps.

    Args:
        project (str): Name of the ingestion project.
        table (str): Name of the target table.
        domain (str): Domain or subject area (used in logging).
        source_id (str): ID of the source config row in the SQL config table.
        target_id (str): ID of the target config row in the SQL config table.
        app_name (str, optional): Spark app name override. Defaults to `{project}_{table}_bronze_ingestion`.
        secret_scope (str, optional): Databricks Key Vault scope name. Defaults to "GOATKeyVault".
        blob_name_key (str, optional): Key for Azure Blob storage account name. Defaults to "GoatBlobStorageName".
        blob_key_key (str, optional): Key for Azure Blob storage account key. Defaults to "GoatBlobStorageKey".
        sql_server_key (str, optional): Key for SQL Server hostname. Defaults to "goatSQLDBServer".
        sql_db_key (str, optional): Key for SQL database name. Defaults to "goatSQLDB-DM".
        sql_user_key (str, optional): Key for SQL username. Defaults to "goatSQLDBUser".
        sql_pass_key (str, optional): Key for SQL password. Defaults to "goatSQLDBPass".
        spark_config (dict, optional): Additional Spark configuration options to set.
        send_email (bool, optional): Whether to send email notifications on failure. Defaults to False.
        email_to (str, optional): Comma-separated list of recipients.
        logic_app_url (str, optional): Logic App URL for triggering email sends.

    Returns:
        List[Dict]: Structured list of log entries from the logger.

    Raises:
        Exception: If any step in the pipeline fails, the exception is raised after optional email notification.

    Example:
        >>> logs = run_bronze_pipeline(
        ...     project="Sales",
        ...     table="Transactions",
        ...     domain="Retail",
        ...     source_id="SRC123",
        ...     target_id="TGT456",
        ...     send_email=True,
        ...     email_to="data-team@company.com",
        ...     logic_app_url="https://prod-...logic.azure.com"
        ... )
    """
    from odibi_de_v2.databricks import (
        init_spark_with_azure_secrets,
        load_ingestion_configs_from_sql,
        IngestionConfigConstructor,
        get_secret,
        SparkDataReaderFromConfig,
        SparkDataSaverFromConfig
    )
    from odibi_de_v2.logger import get_logger, log_and_optionally_raise
    from odibi_de_v2.core import ErrorType
    from odibi_de_v2.utils import send_email_using_logic_app, build_ingestion_email_html

    logger = get_logger()
    _log_args = dict(module="WORKFLOW", component="BRONZE", method="run_bronze_pipeline")

    try:
        log_and_optionally_raise(**_log_args, error_type=ErrorType.NO_ERROR,
            message=f"Initializing Spark and Azure connector for {project} - {table}", level="DEBUG")

        spark, azure_connector = init_spark_with_azure_secrets(
            app_name=app_name or f"{project}_{table}_bronze_ingestion",
            secret_scope=secret_scope,
            account_name_key=blob_name_key,
            account_key_key=blob_key_key,
            logger_metadata={"project": project, "table": table, "domain": domain})

        if isinstance(spark_config, dict):
            for key, value in spark_config.items():
                spark.conf.set(key, value)

        log_and_optionally_raise(**_log_args, error_type=ErrorType.NO_ERROR,
            message="Loading source and target configs from SQL", level="DEBUG")

        source_df, target_df = load_ingestion_configs_from_sql(
            host=get_secret(secret_scope, sql_server_key),
            database=get_secret(secret_scope, sql_db_key),
            user=get_secret(secret_scope, sql_user_key),
            password=get_secret(secret_scope, sql_pass_key),
            project=project,
            source_id=source_id,
            target_id=target_id,
            spark=spark)

        constructor = IngestionConfigConstructor(source_df, target_df)
        source_config, target_config = constructor.prepare()

        data = SparkDataReaderFromConfig(spark, source_config).read_data()
        SparkDataSaverFromConfig(spark, target_config, azure_connector).save(data)

        log_and_optionally_raise(**_log_args, error_type=ErrorType.NO_ERROR,
            message="Bronze ingestion completed", level="INFO")

        return logger.get_logs()

    except Exception as e:
        log_and_optionally_raise(**_log_args, error_type=ErrorType.Runtime_Error,
            message=f"Error running bronze pipeline for {project} - {table}: {e}", level="ERROR")

        logs = logger.get_logs()
        if send_email and email_to and logic_app_url:
            subject = f"Bronze Ingestion FAILED: {project} - {table}"
            body = build_ingestion_email_html(logs)
            send_email_using_logic_app(email_to, subject, body, logic_app_url)

        raise e
