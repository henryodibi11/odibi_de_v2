from odibi_de_v2.utils import (
    enforce_types, log_call)
from odibi_de_v2.logger import log_and_optionally_raise
from odibi_de_v2.core.enums import ErrorType
from odibi_de_v2.logger import log_exceptions
from typing import Optional
from odibi_de_v2.core import Framework, BaseConnection


class AzureBlobConnection(BaseConnection):
    """
    Connection class for accessing Azure Blob Storage using Spark or Pandas.

    This connector resolves cloud file paths and constructs the appropriate storage options
    dictionary for Spark and Pandas engines based on the selected `Framework`.

    It supports:
    - ABFS path resolution for both `Spark` (`abfss://...`) and `Pandas` (`abfs://...`)
    - Automatic configuration key generation for `spark.conf.set(...)`
    - Storage options formatted for fsspec compatibility with Pandas
    - Logging for successful and failed path resolution attempts
    - Runtime-safe exceptions using structured decorators

    Decorators:
        - @log_call: Logs method entry/exit
        - @enforce_types: Enforces input type safety
        - @log_exceptions: Logs and optionally raises runtime errors

    Example:
        >>> connector = AzureBlobConnection(
        ...     account_name="myaccount",
        ...     account_key="secret",
        ...     framework=Framework.SPARK
        ... )
        >>> file_path = connector.get_file_path(
        ...     container="bronze",
        ...     path_prefix="raw/events",
        ...     object_name="sales.csv"
        ... )
        >>> storage_options = connector.get_storage_options()
    """

    @log_call(module="CONNECTOR", component="AzureBlobConnection")
    @enforce_types(strict=True)
    def __init__(self, account_name: str, account_key: str, framework: Framework):
        """
        Initialize the Azure connector.

        Args:
            account_name (str): Azure storage account name.
            account_key (str): Corresponding account key.
            framework (Framework): Target framework (PANDAS or SPARK).
        """
        self.account_name = account_name
        self.account_key = account_key
        self.framework = framework


    @log_call(module="CONNECTOR", component="AzureBlobConnection")
    @enforce_types(strict=True)
    @log_exceptions(
        module="CONNECTOR",
        component="AzureBlobConnection",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def get_file_path(
        self,
        container: str,
        path_prefix: str,
        object_name: str
    ) -> str:
        """
        Construct a framework-specific ABFS path to an Azure Blob file.

        Args:
            container (str): Azure Blob container name (e.g., "bronze").
            path_prefix (str): Folder or directory path inside the container.
            object_name (str): Name of the file or blob.

        Returns:
            str: Fully qualified path formatted for the specified engine.
        """
        blob_path = f"{path_prefix}/{object_name}"
        log_and_optionally_raise(
            module="CONNECTOR",
            component="AzureBlobConnector",
            method="get_file_path",
            error_type=ErrorType.NO_ERROR,
            message="Attempting to resolve file path...",
            level="INFO")
        match self.framework:
            case Framework.SPARK:
                file_path = (
                    f"abfss://{container}@{self.account_name}."
                    f"dfs.core.windows.net/{blob_path}")
                log_and_optionally_raise(
                    module="CONNECTOR",
                    component="AzureBlobConnector",
                    method="get_file_path",
                    error_type=ErrorType.NO_ERROR,
                    message=f"Successfully resolved Spark file path: {file_path}",
                    level="INFO")
                return file_path

            case Framework.PANDAS:
                file_path = f"abfs://{container}/{blob_path}"
                log_and_optionally_raise(
                    module="CONNECTOR",
                    component="AzureBlobConnector",
                    method="get_file_path",
                    error_type=ErrorType.NO_ERROR,
                    message=f"Successfully resolved Pandas file path: {file_path}",
                    level="INFO")
                return file_path

            case _:
                raise NotImplementedError(
                    f"AzureBlobConnection does not support framework: {self.framework}")


    @log_call(module="CONNECTOR", component="AzureBlobConnection")
    @enforce_types(strict=True)
    @log_exceptions(
        module="CONNECTOR",
        component="AzureBlobConnection",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def get_storage_options(self) -> Optional[dict]:
        """
        Returns the correct authentication dictionary for the engine in use.

        Returns:
            dict or None: Storage options for Spark or Pandas, or None if unsupported.
        """
        log_and_optionally_raise(
            module="CONNECTOR",
            component="AzureBlobConnector",
            method="get_file_path",
            error_type=ErrorType.NO_ERROR,
            message="Attempting to resolve storage options...",
            level="INFO")
        match self.framework:
            case Framework.PANDAS:
                storage_option = {
                    "account_name": self.account_name,
                    "account_key": self.account_key}
                log_and_optionally_raise(
                    module="CONNECTOR",
                    component="AzureBlobConnector",
                    method="get_file_path",
                    error_type=ErrorType.NO_ERROR,
                    message=(
                        "Successfully resolved Pandas storage optioons: "
                        f"{storage_option}"),
                    level="INFO")
                return storage_option

            case Framework.SPARK:
                storage_option = {
                    f"fs.azure.account.key.{self.account_name}.dfs.core.windows.net": self.account_key
                    }
                log_and_optionally_raise(
                    module="CONNECTOR",
                    component="AzureBlobConnector",
                    method="get_file_path",
                    error_type=ErrorType.NO_ERROR,
                    message=(
                        "Successfully resolved Spark storage optioons: "
                        f"{storage_option}"),
                    level="INFO")
                return storage_option

            case _:
                raise NotImplementedError(
                    f"AzureBlobConnection does not support framework: {self.framework}"
                )
