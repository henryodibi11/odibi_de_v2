from abc import ABC, abstractmethod


class CloudConnector(ABC):
    """
    Abstract base class connecting to and interacting with cloud storage.

    This abstract class provides a standarized interface to cloud storage
    services (e.g., Azure, AWS, GCP) for use within file readers, savers,
    or ingestion pipelines.

    Attributes:
        account_name (str): The storage account name
        account_key (str): The storage account key

    Methods:
        get_connection():
            Abstract method to establish a connection to the cloud service sdk.
        get_file_path(storage_unit: str, object_name: str) -> str:
            Abstract method to generate the file path for a given storage_unit
            and object.
        get_framework_config(framework: str) -> dict:
            Abstract method to retrieve framework-specific configuration
            details for the cloud service.
    Example:
        >>> from my_module.cloud_connector import CloudConnector
        >>> connector = CloudConnector(
            "account_name",
            "account_key"
        )
        >>> client = connector.get_connection()
        >>> file_name = connector.get_file_path(
        ...    "storage_unit",
        ...    "object_name")
        >>> framework_config = get_framework_config("pandas")
    """

    def __init__(self, account_name: str, account_key: str):
        self.account_name = account_name
        self.account_key = account_key

    @abstractmethod
    def get_connection(self):
        """
        Abstract method to establish a connection to the cloud service sdk.

        Returns:
            Any: A framework specific sdk client instance.

        Raises:
            Exception: If there is an error establishing the connection.
        """
        pass

    @abstractmethod
    def get_file_path(
        self,
        storage_unit: str,
        object_name: str,
        framework: str
            ) -> str:

        """
        Abstract method to generate the file path given the storage_unit
            and object_name.

        Args:
            storage_unit: The container/folder where data is stored.
                For Example:
                    - Azure: `ADLS Gen 2`, `Blob Storage
                    - AWS: `S3`
            object_name: The path to the data
            framework: The framework to generate the file_path for.
                For Example:
                    - Spark: f"abfss://{storage_unit}@{self.account_name}" +
                            f".dfs.core.windows.net/{object_name}"
                    - Pandas: f"az://{storage_unit}/{object_name}"

        Returns:
            str: The full file path
        """
        pass

    @abstractmethod
    def get_framework_config(self, framework: str) -> dict:
        """
        Abstract method to generate the framework config for a given framework

        Args:
            framework: The framework to connect to.
                For Example:
                    - Spark
                    - Pandas

        Returns:
            dict: A dictionary with the config to connect to the given
                framework.
                    For Example:
                        - Spark:  {
                            f"fs.azure.account.key.{self.account_name}.dfs.core.windows.net":
                            self.account_key}
                        - Pandas: {
                            "account_name": self.account_name,
                            "account_key": self.account_key,
                        }
        """
        pass
