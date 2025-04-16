from typing import Optional
from odibi_de_v2.core.enums import Framework
from odibi_de_v2.core import BaseConnection
from odibi_de_v2.utils import (
    enforce_types, log_call)
from odibi_de_v2.logger import log_and_optionally_raise
from odibi_de_v2.core.enums import ErrorType




class LocalConnection(BaseConnection):
    """
    Connection class for local or DBFS-based file paths.

    This connector resolves file paths for data stored on local disk, mounted
    volumes, or local filesystems like Databricks DBFS. It assumes no authentication
    or storage options are required.

    Used by default when no cloud connector is specified in Reader or Saver providers.

    Decorators:
        - @log_call: Logs method entry/exit
        - @enforce_types: Enforces input type safety

    Example Usage:
        >>> connector = LocalConnection()
        >>> path = connector.get_file_path(
        ...     container="local",  # Not used, but accepted for interface consistency
        ...     path_prefix="dbfs:/tmp/data",
        ...     object_name="sales.csv"
        ... )
        >>> options = connector.get_storage_options()  # returns None
    """
    def __init__(self):
        self.framework = Framework.LOCAL

    @log_call(module="CONNECTOR", component="AzureBlobConnection")
    @enforce_types(strict=True)
    def get_file_path(
        self,
        container: str = "",
        path_prefix: str = "",
        object_name: str = ""
        ) -> str:
        """
        Constructs a local file path by joining the prefix and object name.

        Args:
            container (str): Placeholder for interface compatibility (ignored).
            path_prefix (str): Directory path (e.g., "/dbfs/tmp").
            object_name (str): File name (e.g., "data.csv").

        Returns:
            str: Fully resolved local file path.
        """
        log_and_optionally_raise(
            module="CONNECTOR",
            component="AzureBlobConnector",
            method="get_file_path",
            error_type=ErrorType.NO_ERROR,
            message="Attempting to resolve local file path...",
            level="INFO")

        file_path = f"{path_prefix}/{object_name}"

        log_and_optionally_raise(
            module="CONNECTOR",
            component="AzureBlobConnector",
            method="get_file_path",
            error_type=ErrorType.NO_ERROR,
            message=f"Successfully resolved local file path: {file_path}",
            level="INFO")
        return file_path

    def get_storage_options(self) -> Optional[dict]:
        """
        Local file systems do not require authentication.

        Returns:
            None
        """
        return None


