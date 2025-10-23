import os
from odibi_de_v2.core import BaseConnection
from odibi_de_v2.core.enums import Framework, ErrorType
from odibi_de_v2.logger import log_and_optionally_raise
from odibi_de_v2.utils import enforce_types, log_call

class LocalConnection(BaseConnection):
    """
    Connection class for accessing **local or Databricks-based file paths**.

    This connector enables the framework to interact with files stored on:
      - The developer’s **local file system** (e.g., in VS Code or unit tests)
      - **Databricks DBFS** (e.g., `/dbfs/FileStore` or `dbfs:/FileStore`)

    It provides a consistent interface with other `odibi_de_v2` connectors 
    (such as AzureBlobConnection or SQLDatabaseConnection) but requires **no authentication** 
    or network access. This makes it ideal for local development, testing, 
    or reading small reference files stored in Databricks `FileStore`.

    **Environment-aware behavior:**
    - ✅ Detects when running inside Databricks and automatically resolves 
      relative paths to `dbfs:/` format (used by Spark).
    - ✅ When running locally, converts relative paths to absolute OS file paths.
    - ✅ Leaves already-prefixed or absolute paths unchanged (`dbfs:/`, `/dbfs/`, or `/Users/...`).

    Example Usage:
        ```python
        from odibi_de_v2.connector import LocalConnection
        connector = LocalConnection()

        # 1️⃣ Inside Databricks Notebook
        # Automatically converts to Spark-friendly path
        connector.get_file_path(object_name="FileStore/data.csv")
        # → 'dbfs:/FileStore/data.csv'

        # 2️⃣ On a Local Computer (e.g., VS Code)
        # Converts relative path to absolute local path
        connector.get_file_path(object_name="data/data.csv")
        # → '/Users/henry/projects/data/data.csv'

        # 3️⃣ Already DBFS-prefixed paths remain unchanged
        connector.get_file_path(object_name="dbfs:/FileStore/data.csv")
        # → 'dbfs:/FileStore/data.csv'

        # 4️⃣ Already absolute local paths remain unchanged
        connector.get_file_path(object_name="/Users/henry/Desktop/data.csv")
        # → '/Users/henry/Desktop/data.csv'

        # 5️⃣ Passing extra args is safe but ignored
        connector.get_file_path(container="local", path_prefix="dbfs:/", object_name="FileStore/test.csv")
        # → 'dbfs:/FileStore/test.csv' (inside Databricks)
        ```

    Notes:
        - `container` and `path_prefix` are ignored but retained for interface consistency.
        - Automatically detects Databricks using the environment variable `DATABRICKS_RUNTIME_VERSION`.
        - Returns a fully resolved, Spark-compatible or OS-compatible path.
    """

    def __init__(self):
        self.framework = Framework.LOCAL

    @log_call(module="CONNECTOR", component="LocalConnection")
    @enforce_types(strict=True)
    def get_file_path(
        self,
        container: str = "",
        path_prefix: str = "",
        object_name: str = ""
    ) -> str:
        """
        Resolves a file path dynamically based on the current runtime environment.

        **Logic Flow:**
        1. If the `object_name` is already absolute (`/Users/...`, `/dbfs/...`) or DBFS-prefixed (`dbfs:/`),
           the path is returned unchanged.
        2. If running inside Databricks, the path is prefixed with `dbfs:/`.
        3. If running locally, a full absolute OS path is constructed from the current working directory.

        Args:
            container (str): Placeholder argument for API consistency.
            path_prefix (str): Placeholder argument for API consistency.
            object_name (str): File name or relative path to resolve.

        Returns:
            str: Fully resolved file path suitable for the current environment.

        Example:
            ```python
            connector = LocalConnection()

            # Local context
            connector.get_file_path(object_name="data/sample.json")
            # → '/Users/henry/.../data/sample.json'

            # Databricks context
            connector.get_file_path(object_name="FileStore/sample.json")
            # → 'dbfs:/FileStore/sample.json'
            ```
        """
        log_and_optionally_raise(
            module="CONNECTOR",
            component="LocalConnection",
            method="get_file_path",
            error_type=ErrorType.NO_ERROR,
            message=f"Resolving local file path for: {object_name}",
            level="INFO",
        )

        # 1️⃣ If already absolute or prefixed, return as-is
        if object_name.startswith(("dbfs:/", "/dbfs/")) or os.path.isabs(object_name):
            return object_name

        # 2️⃣ Detect Databricks runtime
        in_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ

        # 3️⃣ Environment-aware resolution
        if in_databricks:
            file_path = f"dbfs:/{object_name.lstrip('/')}"
        else:
            cwd = os.getcwd()
            file_path = os.path.abspath(os.path.join(cwd, object_name))

        log_and_optionally_raise(
            module="CONNECTOR",
            component="LocalConnection",
            method="get_file_path",
            error_type=ErrorType.NO_ERROR,
            message=f"Resolved path: {file_path}",
            level="INFO",
        )

        return file_path

    def get_storage_options(self) -> dict | None:
        """
        Returns `None` because local or DBFS file systems require no authentication.

        This method exists purely for interface consistency with cloud connectors,
        which may require returning credential options or configuration settings.

        Example:
            ```python
            connector = LocalConnection()
            connector.get_storage_options()
            # → None
            ```
        """
        log_and_optionally_raise(
            module="CONNECTOR",
            component="LocalConnection",
            method="get_storage_options",
            error_type=ErrorType.NO_ERROR,
            message="LocalConnection does not require authentication.",
            level="INFO",
        )
        return None
