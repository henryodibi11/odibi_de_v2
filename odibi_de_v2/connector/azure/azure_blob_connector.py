from azure.storage.blob import BlobServiceClient
from core import CloudConnector, Framework
from logger import log_info, log_error, format_error
from core.enums import ErrorType


class AzureBlobConnector(CloudConnector):
    """
    Connector class for Azure Blob Storage.

    Provides:
        - Connection via Azure SDK.
        - File paths compatible with Spark and Pandas.
        - Framework-specific config options.

    Attributes:
        account_name (str): Azure storage account name.
        account_key (str): Azure storage account key.
    """

    def __init__(self, account_name: str, account_key: str):
        if not account_name or not isinstance(account_name, str):
            account_name_error_msg = format_error(
                    "CONNECTOR",
                    "AzureBlobConnector", "__init__",
                    ErrorType.VALUE_ERROR,
                    "account_name must be a non-empty string.")
            raise ValueError(account_name_error_msg)

        if not account_key or not isinstance(account_key, str):
            account_key_error_msg = format_error(
                    "CONNECTOR",
                    "AzureBlobConnector", "__init__",
                    ErrorType.VALUE_ERROR,
                    "account_key must be a non-empty string.")
            raise ValueError(account_key_error_msg)

        self.account_name = account_name
        self.account_key = account_key

    def get_connection(self) -> BlobServiceClient:
        """
        Establish connection to Azure Blob Storage.

        Returns:
            BlobServiceClient: Azure SDK client instance.

        Raises:
            RuntimeError: If the connection fails.
        """
        try:
            url = f"https://{self.account_name}.blob.core.windows.net"
            client = BlobServiceClient(
                account_url=url,
                credential=self.account_key)
            log_msg = format_error(
                    "CONNECTOR",
                    "AzureBlobConnector",
                    "get_connection",
                    ErrorType.NO_ERROR,
                    (
                        "Successfully connected to Azure Blob:"
                        f"{self.account_name}"))
            log_info(log_msg)

            return client

        except Exception as e:
            error_msg = format_error(
                "CONNECTOR",
                "AzureBlobConnector",
                "get_connection",
                ErrorType.CONNECTION_ERROR, str(e))
            log_error(error_msg)
            raise RuntimeError(error_msg) from e

    def get_file_path(
        self,
        container: str,
        blob_name: str,
        framework: Framework
            ) -> str:
        """
        Generate a URI for Spark or Pandas access.

        Args:
            container (str): Container name in Azure Blob Storage.
            blob_name (str): Path to the blob (file).
            framework (Framework): Framework enum ('spark' or 'pandas').

        Returns:
            str: Framework-compatible path.
        """
        if framework.value == "spark":
            return (
                f"abfss://{container}@{self.account_name}."
                f"dfs.core.windows.net/{blob_name}")
        elif framework.value == "pandas":
            return f"az://{container}/{blob_name}"
        else:
            error_msg = format_error(
                    "CONNECTOR",
                    "AzureBlobConnector",
                    "get_file_path",
                    ErrorType.VALUE_ERROR,
                    f"Unsupported framework: {framework.value}")
            raise ValueError(error_msg)

    def get_framework_config(self, framework: Framework) -> dict:
        """
        Retrieve storage access config for Spark or Pandas.

        Args:
            framework (Framework): Framework enum ('spark' or 'pandas').

        Returns:
            dict: Framework-specific config.

        Raises:
            ValueError: If an unsupported framework is passed.
        """
        if framework.value == "spark":
            config_key = (
                f"fs.azure.account.key.{self.account_name}."
                "dfs.core.windows.net")
            return {config_key: self.account_key}
        elif framework.value == "pandas":
            return {
                "account_name": self.account_name,
                "account_key": self.account_key
            }
        else:
            error_msg = format_error(
                "CONNECTOR",
                "AzureBlobConnector",
                "get_framework_config",
                ErrorType.VALUE_ERROR,
                f"Unsupported framework: {framework.value}")
            raise ValueError(error_msg)
