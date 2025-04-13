from abc import ABC, abstractmethod
from .saver_factory import SaverFactory
from .cloud_connector import CloudConnector
from .enums import DataType


class SaverProvider(ABC):
    """
    Abstract base class for providing data savers based on the data type
        and cloud connector.

    Attributes:
        factory (SaverFactory): Instance of a SaverFactory.
        data_type (DataType): The type of data to be read.
            For Example:
                DataType.CSV
        connector (CloudConnector): Instance of a CloudConnector.

    Methods:
        create_saver(storage_unit: str, object_name: str):
            Abstract method for creating a saver for the specified data type
            and storage location.

    Example:
        >>> from my_module.saver_factory import PandasSaverFactory
        >>> from my_module.enums import DataType
        >>> from my_odule.cloud_connector import CloudConnector
        >>> from my_module import PandasSaverProvider

        >>> factory = PandasSaverFactory()
        >>> connector = CloudConnector(
        ...     "account_name",
        ...     "account_key")
        >>> provider = PandasSaverProvider(
                factory,
                DataType.CSV,
                connector)
        >>> df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
            saver = provider.create_saver(
                df,
                storage_unit,
                object_name
            )
            saver.save_data(df, index=False)
    """
    def __init__(
        self, factory: SaverFactory,
        data_type: DataType,
        connector: CloudConnector
    ):
        self.factory = factory
        self.data_type = data_type
        self.connector = connector

    @abstractmethod
    def create_saver(
        self,
        storage_unit: str,
        object_name: str
    ):
        """
        Creates and returns a saver based on the configured factory,
        data type, and connector.

        This abstract class provides a standarized interface to create savers
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
            Any: A concrete saver instance that implements the DataSaver
                interface.
        """
        pass
