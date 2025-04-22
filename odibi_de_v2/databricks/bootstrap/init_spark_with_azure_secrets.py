from pyspark.sql import SparkSession
from odibi_de_v2.connector.azure.azure_blob_connector import AzureBlobConnection
from odibi_de_v2.core.enums import Framework
from odibi_de_v2.logger import initialize_logger
from odibi_de_v2.databricks.utils import get_secret

def init_spark_with_azure_secrets(
    app_name: str,
    secret_scope: str,
    account_name_key: str,
    account_key_key: str,
    logger_metadata: dict | None = None
    ) -> tuple[SparkSession, AzureBlobConnection]:

    """
    Initializes a Spark session and Azure Blob storage connection using provided secrets.

    This function sets up a Spark session and an authenticated connection to Azure Blob storage,
    facilitated by Databricks utilities. It is intended to be used at the start of a Databricks
    notebook or job to configure the environment for data processing tasks. It also configures
    logging with optional metadata.

    Args:
        app_name (str): Name of the Spark application, which helps in identifying the application in the Spark UI.
        secret_scope (str): The name of the Databricks secret scope containing the Azure storage credentials.
        account_name_key (str): The key within the secret scope for the Azure storage account name.
        account_key_key (str): The key within the secret scope for the Azure storage account key.
        logger_metadata (dict | None, optional): Metadata for the logger to enhance traceability and debugging.
            This should be a dictionary containing key-value pairs that describe the context of the execution,
            such as project name, table name, or domain.

    Returns:
        tuple[SparkSession, AzureBlobConnection]: A tuple containing the initialized Spark session and the
        Azure Blob storage connection, ready for use in data operations.

    Example:
        >>> spark_session, azure_blob_connector = init_spark_with_azure_secrets(
        ...     app_name="DataProcessingApp",
        ...     secret_scope="mySecretScope",
        ...     account_name_key="storageAccountName",
        ...     account_key_key="storageAccountKey",
        ...     logger_metadata={"project": "DataIngestion", "domain": "Sales"}
        ... )
    """
    if logger_metadata:
        initialize_logger(logger_metadata)

    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(SparkSession.builder.getOrCreate())

    spark = SparkSession.builder.appName(app_name).getOrCreate()

    account_name = get_secret(secret_scope, account_name_key)
    account_key = get_secret(secret_scope, account_key_key)

    connector = AzureBlobConnection(
        account_name=account_name,
        account_key=account_key,
        framework=Framework.SPARK
    )

    return spark, connector
