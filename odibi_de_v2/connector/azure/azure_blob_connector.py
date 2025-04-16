from azure.storage.blob import BlobServiceClient
from odibi_de_v2.core import CloudConnector, Framework
from odibi_de_v2.utils import (
    validate_non_empty, enforce_types, log_call, benchmark)
from odibi_de_v2.logger import log_and_optionally_raise
from odibi_de_v2.core.enums import ErrorType
from odibi_de_v2.logger import log_exceptions

class AzureBlobConnector(CloudConnector):
    """
    Connector class for authenticated access to Azure Blob Storage.

    This class provides framework-agnostic integration with Azure Blob Storage,
    supporting both Spark and Pandas. It exposes standardized methods to:
    - Establish an authenticated Azure SDK client.
    - Generate framework-compatible blob paths.
    - Return config dictionaries for use in distributed file access.

    Attributes:
        account_name (str): Azure storage account name.
        account_key (str): Azure storage account key.

    Example:
    >>> connector = AzureBlobConnector("myaccount", "mykey")
    >>> client = connector.get_connection()
    >>> spark_path = connector.get_file_path(
    ... "landing", "data.csv", Framework.SPARK)
    >>> config = connector.get_framework_config(Framework.SPARK)
    """

    @validate_non_empty(["account_name", "account_key"])
    @enforce_types(strict=True)
    def __init__(self, account_name: str, account_key: str):
        """
        Initialize the Azure Blob Storage connector.

        Validates the required credentials and prepares the connector
        for building authenticated URIs and configuration settings.

        Args:
            account_name (str): Azure storage account name.
            account_key (str): Azure storage account key.

        Raises:
            ValueError: If any required input is missing or invalid.

        Example:
        >>> connector = AzureBlobConnector("myaccount", "mykey")
        """
        self.account_name = account_name
        self.account_key = account_key
        self.connector = self.get_connection()
    @log_call(module="CONNECTOR", component="AzureBlobConnector")
    @benchmark(module="CONNECTOR", component="AzureBlobConnector")
    @log_exceptions(
        module="CONNECTOR",
        component="AzureBlobConnector",
        error_type=ErrorType.CONNECTION_ERROR,
        raise_type=RuntimeError
    )
    @enforce_types(strict=True)
    def get_connection(self) -> BlobServiceClient:
        """
        Establish and return a connection to Azure Blob Storage.

        Returns:
            BlobServiceClient: Authenticated client for interacting with the Azure
                Blob service.

        Raises:
            RuntimeError: If the connection fails or credentials are invalid.

        Example:
        >>> client = connector.get_connection()
        >>> containers = client.list_containers()
        """
        url = f"https://{self.account_name}.blob.core.windows.net"
        client = BlobServiceClient(
            account_url=url,
            credential=self.account_key)
        log_and_optionally_raise(
            module="CONNECTOR",
            component="AzureBlobConnector",
            method="get_connection",
            error_type=ErrorType.NO_ERROR,
            message="Successfully connected to Azure Blob Storage.",
            level="INFO"
        )

        return client
    @log_call(module="CONNECTOR", component="AzureBlobConnector")
    @enforce_types(strict=True)
    @log_exceptions(
        module="CONNECTOR",
        component="AzureBlobConnector",
        error_type=ErrorType.VALUE_ERROR,
        raise_type=ValueError)
    def get_file_path(
        self,
        container: str,
        blob_name: str,
        framework: Framework
            ) -> str:
        """
        Generate a fully qualified path for accessing blobs with Spark or Pandas.

        Args:
            container (str): Name of the Azure Blob Storage container.
            blob_name (str): Path to the blob inside the container.
            framework (Framework): Target framework (e.g., Framework.SPARK or
                Framework.PANDAS).

        Returns:
            str: A URI path compatible with the specified framework.

        Raises:
            ValueError: If an unsupported framework is passed.

        Example:
        >>> connector.get_file_path("raw", "data.csv", Framework.SPARK)
        ... 'abfss://raw@myaccount.dfs.core.windows.net/data.csv'
        """

        if framework.value == "spark":
            path = (
                f"abfss://{container}@{self.account_name}."
                f"dfs.core.windows.net/{blob_name}")
            # Log Success
            log_and_optionally_raise(
            module="CONNECTOR",
            component="AzureBlobConnector",
            method="get_file_path",
            error_type=ErrorType.NO_ERROR,
            message="Successfully resolved Spark path",
            level="INFO")
            return path
        elif framework.value == "pandas":
            path = f"az://{container}/{blob_name}"
            # Log Success
            log_and_optionally_raise(
            module="CONNECTOR",
            component="AzureBlobConnector",
            method="get_file_path",
            error_type=ErrorType.NO_ERROR,
            message="Successfully resolved Pandas path",
            level="INFO")
            return path
        else:
            log_and_optionally_raise(
            module="CONNECTOR",
            component="AzureBlobConnector",
            method="get_file_path",
            error_type=ErrorType.VALUE_ERROR,
            message=f"Unsupported framework: {framework.value}",
            raise_exception=True)

    @log_exceptions(
        module="CONNECTOR",
        component="AzureBlobConnector",
        error_type=ErrorType.VALUE_ERROR,
        raise_type=ValueError)
    @enforce_types(strict=True)
    def get_framework_config(self, framework: Framework) -> dict:
        """
        Return framework-specific configuration settings for accessing Azure
            Blob Storage.
        Args:
            framework (Framework): Target framework (e.g., Framework.SPARK or
                Framework.PANDAS).
        Returns:
            dict: Configuration options tailored to the specified framework.
        Raises:
            ValueError: If the framework is unsupported.
        Example:
        >>> connector.get_framework_config(Framework.SPARK)
        ... {'fs.azure.account.key.myaccount.dfs.core.windows.net': 'mykey'}
        """

        if framework == Framework.SPARK:
            config_key = (
                f"fs.azure.account.key.{self.account_name}."
                "dfs.core.windows.net")
            config = {config_key: self.account_key}
            # Log Success
            log_and_optionally_raise(
            module="CONNECTOR",
            component="AzureBlobConnector",
            method="get_framework_config",
            error_type=ErrorType.NO_ERROR,
            message="Successfully resolved Spark config",
            level="INFO")

            return config
        elif framework == Framework.PANDAS:
            config = {
                "account_name": self.account_name,
                "account_key": self.account_key}
            # Log Success
            log_and_optionally_raise(
            module="CONNECTOR",
            component="AzureBlobConnector",
            method="get_framework_config",
            error_type=ErrorType.NO_ERROR,
            message="Successfully resolved Pandas config",
            level="INFO")

            return config
        else:
            log_and_optionally_raise(
                module="CONNECTOR",
                component="AzureBlobConnector",
                method="get_framework_config",
                error_type=ErrorType.VALUE_ERROR,
                message=f"Unsupported framework: {framework.value}",
                raise_exception=True)
