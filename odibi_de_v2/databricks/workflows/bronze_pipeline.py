from typing import List, Dict, Optional
from odibi_de_v2.databricks.config.pipeline_settings import BronzePipelineSettings

def run_bronze_pipeline(
    project: str,
    table: str,
    domain: str,
    source_id: str,
    target_id: str,
    app_name: str = None,
    settings: Optional[BronzePipelineSettings] = None,
    secret_scope: str = "",
    blob_name_key: str = "",
    blob_key_key: str = "",
    sql_server_key: str = "",
    sql_db_key: str = "",
    sql_user_key: str = "",
    sql_pass_key: str = "",
    spark_config: dict = None,
    send_email: bool = False,
    email_to: str = None,
    logic_app_url: str = None,
    config_environment = "qat"
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
        settings (BronzePipelineSettings, optional): Pipeline configuration settings. If provided, overrides individual secret key parameters.
        secret_scope (str, optional): Databricks Key Vault scope name. Defaults to "GOATKeyVault" (ignored if settings provided).
        blob_name_key (str, optional): Key for Azure Blob storage account name. Defaults to "GoatBlobStorageName" (ignored if settings provided).
        blob_key_key (str, optional): Key for Azure Blob storage account key. Defaults to "GoatBlobStorageKey" (ignored if settings provided).
        sql_server_key (str, optional): Key for SQL Server hostname. Defaults to "goatSQLDBServer" (ignored if settings provided).
        sql_db_key (str, optional): Key for SQL database name. Defaults to "goatSQLDB-DM" (ignored if settings provided).
        sql_user_key (str, optional): Key for SQL username. Defaults to "goatSQLDBUser" (ignored if settings provided).
        sql_pass_key (str, optional): Key for SQL password. Defaults to "goatSQLDBPass" (ignored if settings provided).
        spark_config (dict, optional): Additional Spark configuration options to set.
        send_email (bool, optional): Whether to send email notifications on failure. Defaults to False.
        email_to (str, optional): Comma-separated list of recipients.
        logic_app_url (str, optional): Logic App URL for triggering email sends.
        config_environment (str): Environment to select config from (ignored if settings provided)

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
    import time

    logger = get_logger()
    _log_args = dict(module="WORKFLOW", component="BRONZE", method="run_bronze_pipeline")

    if not project or not project.strip():
        raise ValueError("Parameter 'project' is required and cannot be empty")
    if not table or not table.strip():
        raise ValueError("Parameter 'table' is required and cannot be empty")
    if not source_id or not source_id.strip():
        raise ValueError("Parameter 'source_id' is required and cannot be empty")
    if not target_id or not target_id.strip():
        raise ValueError("Parameter 'target_id' is required and cannot be empty")
    
    if settings is None:
        settings = BronzePipelineSettings(
            secret_scope=secret_scope or "GOATKeyVault",
            blob_name_key=blob_name_key or "GoatBlobStorageName",
            blob_key_key=blob_key_key or "GoatBlobStorageKey",
            sql_server_key=sql_server_key or "goatSQLDBServer",
            sql_db_key=sql_db_key or "goatSQLDB-DM",
            sql_user_key=sql_user_key or "goatSQLDBUser",
            sql_pass_key=sql_pass_key or "goatSQLDBPass",
            environment=config_environment if config_environment else "qat"
        )

    try:
        pipeline_start = time.time()
        
        log_and_optionally_raise(**_log_args, error_type=ErrorType.NO_ERROR,
            message=f"Initializing Spark and Azure connector for {project} - {table}", level="DEBUG")
        
        init_start = time.time()
        spark, azure_connector = init_spark_with_azure_secrets(
            app_name=app_name or f"{project}_{table}_bronze_ingestion",
            secret_scope=settings.secret_scope,
            account_name_key=settings.blob_name_key,
            account_key_key=settings.blob_key_key,
            logger_metadata={"project": project, "table": table, "domain": domain})

        for key, value in (spark_config or {}).items():
            spark.conf.set(key, value)
        
        init_duration_ms = (time.time() - init_start) * 1000
        log_and_optionally_raise(**_log_args, error_type=ErrorType.NO_ERROR,
            message=f"Spark initialization completed in {init_duration_ms:.2f}ms", level="DEBUG")

        log_and_optionally_raise(**_log_args, error_type=ErrorType.NO_ERROR,
            message="Loading source and target configs from SQL", level="DEBUG")
        
        config_start = time.time()

        sql_host = get_secret(settings.secret_scope, settings.sql_server_key)
        sql_database = get_secret(settings.secret_scope, settings.sql_db_key)
        sql_user = get_secret(settings.secret_scope, settings.sql_user_key)
        sql_password = get_secret(settings.secret_scope, settings.sql_pass_key)
        
        source_df, target_df = load_ingestion_configs_from_sql(
            host=sql_host,
            database=sql_database,
            user=sql_user,
            password=sql_password,
            project=project,
            source_id=source_id,
            target_id=target_id,
            spark=spark,
            environment=settings.environment)
        
        if source_df.count() == 0:
            raise ValueError(f"No source config found for source_id='{source_id}', project='{project}', environment='{settings.environment}'")
        if target_df.count() == 0:
            raise ValueError(f"No target config found for target_id='{target_id}', project='{project}', environment='{settings.environment}'")

        constructor = IngestionConfigConstructor(source_df, target_df)
        source_config, target_config = constructor.prepare()
        
        config_duration_ms = (time.time() - config_start) * 1000
        log_and_optionally_raise(**_log_args, error_type=ErrorType.NO_ERROR,
            message=f"Config loading completed in {config_duration_ms:.2f}ms", level="DEBUG")
        
        read_start = time.time()
        data = SparkDataReaderFromConfig(spark, source_config).read_data()
        read_duration_ms = (time.time() - read_start) * 1000
        log_and_optionally_raise(**_log_args, error_type=ErrorType.NO_ERROR,
            message=f"Data reading completed in {read_duration_ms:.2f}ms", level="DEBUG")
        
        write_start = time.time()
        SparkDataSaverFromConfig(spark, target_config, azure_connector).save(data)
        write_duration_ms = (time.time() - write_start) * 1000
        
        total_duration_s = time.time() - pipeline_start
        log_and_optionally_raise(**_log_args, error_type=ErrorType.NO_ERROR,
            message=f"Bronze ingestion completed in {total_duration_s:.2f}s (init={init_duration_ms:.0f}ms, config={config_duration_ms:.0f}ms, read={read_duration_ms:.0f}ms, write={write_duration_ms:.0f}ms)", 
            level="INFO")

        return logger.get_logs()

    except Exception as e:
        log_and_optionally_raise(**_log_args, error_type=ErrorType.Runtime_Error,
            message=f"Error running bronze pipeline for {project} - {table}: {e}", level="ERROR")

        logs = logger.get_logs()
        if send_email and email_to and logic_app_url:
            subject = f"Bronze Ingestion FAILED: {project} - {table}"
            body = build_ingestion_email_html(logs)
            send_email_using_logic_app(email_to, subject, body, logic_app_url)

        raise
