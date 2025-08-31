"""
Utilities for managing ADLS Gen2 folders and files.

This module provides the `ADLSFolderUtils` class, which unifies Spark (dbutils.fs)
and Pandas (fsspec/adlfs) backends for common filesystem operations such as listing,
creating, deleting, moving, and retrieving metadata.

The goal is to simplify repetitive file/folder management tasks in data pipelines
and provide a single consistent API, while abstracting away the differences between
Databricks (Spark) and fsspec (Pandas).

Requirements:
    - Databricks runtime (for Spark backend, uses `dbutils.fs`)
    - fsspec[adlfs] (`pip install adlfs`)
    - odibi_de_v2.core.Framework enum
    - odibi_de_v2.databricks.init_spark_with_azure_secrets for Spark initialization
"""

import fsspec
from typing import List, Optional
from odibi_de_v2.core import Framework

# Databricks-only import guard
try:
    from pyspark.sql import SparkSession
    from pyspark.dbutils import DBUtils
except ImportError:
    SparkSession = None
    DBUtils = None


class ADLSFolderUtils:
    """
    Utility class for folder/file management in Azure Data Lake Storage Gen2 (ADLS).

    Each instance is tied to a single framework backend (`Framework.SPARK` or `Framework.PANDAS`).
    All operations will use that backend. If you need both Spark and Pandas utilities in
    the same workflow, instantiate two separate objects.

    Args:
        azure_connector: Azure connector instance with `get_storage_options` and
            `get_file_path` methods.
        framework (Framework): Backend to use (`Framework.SPARK` or `Framework.PANDAS`).
        spark (SparkSession, optional): Active Spark session, required only when
            using the Spark backend.

    Raises:
        ValueError: If Spark framework is selected but no Spark session is provided.

    Example:
        >>> from odibi_de_v2.core import Framework
        >>> from odibi_de_v2.databricks import init_spark_with_azure_secrets
        >>> from odibi_de_v2.utils import ADLSFolderUtils
        >>>
        >>> # --- Spark backend ---
        >>> spark, azure_connector = init_spark_with_azure_secrets(
        ...     app_name="MyApp",
        ...     secret_scope="MyKeyVault",
        ...     account_name_key="BlobAccountName",
        ...     account_key_key="BlobAccountKey"
        ... )
        >>> adls_spark = ADLSFolderUtils(azure_connector, framework=Framework.SPARK, spark=spark)
        >>> files = adls_spark.list_files("my-container", "raw/project/data/", extension=".parquet")
        >>> print(files)
        >>>
        >>> # --- Pandas backend ---
        >>> adls_pandas = ADLSFolderUtils(azure_connector, framework=Framework.PANDAS)
        >>> files_pd = adls_pandas.list_files("my-container", "raw/project/data/")
        >>> print(files_pd)
    """

    def __init__(self, azure_connector, framework: Framework = Framework.SPARK, spark=None):
        self.connector = azure_connector
        self.framework = framework
        self.spark = spark
        
        if self.framework == Framework.SPARK:
            if self.spark is None:
                raise ValueError("SparkSession must be provided when using Framework.SPARK")

            # Apply Spark storage options
            spark_opts = self.connector.get_storage_options()
            for k, v in spark_opts.items():
                self.spark.conf.set(k, v)

            self.dbutils = DBUtils(self.spark)

        elif self.framework == Framework.PANDAS:
            # Setup fsspec filesystem
            pandas_opts = self.connector.get_storage_options()
            self.fs = fsspec.filesystem(
                "abfs",
                account_name=pandas_opts["account_name"],
                account_key=pandas_opts["account_key"],
            )
        else:
            raise ValueError(f"Unsupported framework: {framework}")

    # ---------- Internal ----------
    def _get_path(self, container: str, path_prefix: str, object_name: str = "") -> str:
        """
        Build full abfss:// path using the azure_connector.

        Args:
            container (str): ADLS container name.
            path_prefix (str): Path prefix inside the container.
            object_name (str, optional): Optional file name or wildcard.

        Returns:
            str: Full abfss:// path.
        """
        return self.connector.get_file_path(container, path_prefix, object_name)

    # ---------- Public Methods ----------
    def list_files(self, container: str, path_prefix: str, extension: Optional[str] = None) -> List[str]:
        """
        List files in a given ADLS path.

        Args:
            container (str): ADLS container name.
            path_prefix (str): Path prefix inside the container.
            extension (str, optional): Filter results by file extension (e.g., ".parquet").

        Returns:
            List[str]: List of file paths.
        """
        base_path = self._get_path(container, path_prefix)

        if self.framework == Framework.SPARK:
            files = self.dbutils.fs.ls(base_path)
            out = [f.path for f in files if not f.isDir()]
        elif self.framework == Framework.PANDAS:
            files = self.fs.ls(f"{container}/{path_prefix}")
            out = [f for f in files if not f.endswith("/")]
        else:
            raise ValueError(f"Unsupported framework: {self.framework}")

        if extension:
            out = [f for f in out if f.endswith(extension)]
        return out

    def list_folders(self, container: str, path_prefix: str) -> List[str]:
        """
        List subfolders under a given ADLS path.

        Args:
            container (str): ADLS container name.
            path_prefix (str): Path prefix inside the container.

        Returns:
            List[str]: List of folder paths.
        """
        base_path = self._get_path(container, path_prefix)

        if self.framework == Framework.SPARK:
            return [f.path for f in self.dbutils.fs.ls(base_path) if f.isDir()]
        elif self.framework == Framework.PANDAS:
            return [f for f in self.fs.ls(f"{container}/{path_prefix}") if f.endswith("/")]
        else:
            raise ValueError(f"Unsupported framework: {self.framework}")

    def ensure_folder(self, container: str, path_prefix: str) -> None:
        """
        Ensure that a folder exists (create if missing).

        Args:
            container (str): ADLS container name.
            path_prefix (str): Path prefix inside the container.
        """
        base_path = self._get_path(container, path_prefix)

        if self.framework == Framework.SPARK:
            self.dbutils.fs.mkdirs(base_path)
        elif self.framework == Framework.PANDAS:
            self.fs.mkdir(f"{container}/{path_prefix}")
        else:
            raise ValueError(f"Unsupported framework: {self.framework}")

    def delete_path(self, container: str, path_prefix: str, recursive: bool = True) -> None:
        """
        Delete a file or folder at a given ADLS path.

        Args:
            container (str): ADLS container name.
            path_prefix (str): Path prefix inside the container.
            recursive (bool): Whether to delete recursively. Defaults to True.
        """
        base_path = self._get_path(container, path_prefix)

        if self.framework == Framework.SPARK:
            self.dbutils.fs.rm(base_path, recurse=recursive)
        elif self.framework == Framework.PANDAS:
            self.fs.rm(f"{container}/{path_prefix}", recursive=recursive)
        else:
            raise ValueError(f"Unsupported framework: {self.framework}")

    def move_file(self, container: str, src: str, dst: str) -> None:
        """
        Move a file from one ADLS path to another.

        Args:
            container (str): ADLS container name.
            src (str): Source file path inside the container.
            dst (str): Destination file path inside the container.
        """
        if self.framework == Framework.SPARK:
            self.dbutils.fs.mv(self._get_path(container, src), self._get_path(container, dst))
        elif self.framework == Framework.PANDAS:
            self.fs.mv(f"{container}/{src}", f"{container}/{dst}")
        else:
            raise ValueError(f"Unsupported framework: {self.framework}")

    def get_metadata(self, container: str, path: str) -> dict:
        """
        Get metadata for a file or folder.

        Args:
            container (str): ADLS container name.
            path (str): Path to a file or folder.

        Returns:
            dict: Metadata dictionary containing:
                - path (str): Full path
                - size_bytes (int): Size of the file in bytes
                - is_dir (bool): Whether the path is a directory
                - name (str): File or folder name
        """
        if self.framework == Framework.SPARK:
            info = self.dbutils.fs.ls(self._get_path(container, path))[0]
            return {
                "path": info.path,
                "size_bytes": info.size,
                "is_dir": info.isDir,
                "name": info.name,
            }
        elif self.framework == Framework.PANDAS:
            info = self.fs.info(f"{container}/{path}")
            return {
                "path": info["name"],
                "size_bytes": info["size"],
                "is_dir": info["type"] == "directory",
                "name": info["name"].split("/")[-1],
            }
        else:
            raise ValueError(f"Unsupported framework: {self.framework}")
