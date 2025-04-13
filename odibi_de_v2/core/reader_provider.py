from abc import ABC, abstractmethod
from core.reader_factory import ReaderFactory
from core.enums import DataType
from core.cloud_connector import CloudConnector


class ReaderProvider(ABC):
    """
    Abstract base class for providing data readers based on the data type
        and cloud connector.

    Attributes:
        factory (ReaderFactory): Instance of a ReaderFactory.
        data_type (DataType): The type of data to be read.
            For Example:
                DataType.CSV
        connector (CloudConnector): Instance of a CloudConnector.

    Methods:
        create_reader(storage_unit: str, object_name: str):
            Abstract method for creating a reader for the specified data type
            and storage location.

    Example:
        >>> from my_module.reader_factory import PandasReaderFactory
        >>> from my_module.enums import DataType
        >>> from my_odule.cloud_connector import CloudConnector
        >>> from my_module import PandasReaderProvider

        >>> factory = PandasReaderFactory()
        >>> connector = CloudConnector(
        ...     "account_name",
        ...     "account_key")
        >>> provider = PandasReaderProvider(
                factory,
                DataType.CSV,
                connector)
            reader = provider.create_reader(
                storage_unit,
                object_name
            )
    """
    def __init__(
        self,
        factory: ReaderFactory,
        data_type: DataType,
        connector: CloudConnector = None
    ):
        self.factory = factory
        self.data_type = data_type
        self.connector = connector

    @abstractmethod
    def create_reader(self, storage_unit: str, object_name: str):
        """
        Creates and returns a reader based on the configured factory,
        data type, and connector.

        This abstract class provides a standarized interface to create readers
        using the data_type, connector, and factory. The full file path will
        be generated dynamically using the connector's logic (e.g., abfss://,
        s3://), based on the given storage unit and object name.

        Args:
            storage_unit: The container/folder where data is stored.
                For Example:
                    - Azure: `ADLS Gen 2`, `Blob Storage
                    - AWS: `S3`
            object_name: The path to the data

        Returns:
            Any: A concrete reader instance that implements the DataReader
                interface.
        """
        pass
