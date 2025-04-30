from abc import ABC, abstractmethod
from typing import Optional

class BaseConnection(ABC):
    """
    Abstract base class for all data source connections.

    This interface defines a standard contract for resolving file paths
    and storage options across different storage systems (e.g., Azure, S3, GCS, Local).

    All concrete connection classes must implement:

    - `get_file_path(...)`: Resolves a fully qualified path for the data source
    - `get_storage_options()`: Returns authentication/configuration options required by the engine

    This interface enables Reader and Saver providers to interact with any
    data source in a uniform, engine-agnostic way.

    Example Usage:
        >>> connector = AzureBlobConnection(..., framework=Framework.SPARK)
        >>> path = connector.get_file_path(
        ...     container="bronze",
        ...     path_prefix="raw/events",
        ...     object_name="sales.csv"
        ... )
        >>> options = connector.get_storage_options()
    """
    @abstractmethod
    def get_file_path(
        self,
        storage_unit: Optional[str] = None,
        object_name: Optional[str] = None,
        full_path: Optional[str] = None
    ) -> str:
        """
        Construct a fully qualified file path based on the input components.

        Args:
            container (str): Top-level storage container or bucket name.
            path_prefix (str): Folder or directory path within the container.
            object_name (str): Actual file name or object identifier.

        Returns:
            str: Fully resolved path for the configured engine (e.g., abfss://..., s3://...)
        """
        pass

    @abstractmethod
    def get_storage_options(self) -> Optional[dict]:
        """
        Return any credentials or storage options needed to access the data.

        Returns:
            dict or None: A dictionary of storage authentication options, or None if not required.
        """
        pass