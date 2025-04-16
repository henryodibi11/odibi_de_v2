from abc import ABC, abstractmethod
from typing import Any
from .reader import DataReader


class ReaderFactory(ABC):
    """
    Abstract base class for creating data readers for different file formats.

    This factory defines the contract for generating reader instances based on
    the target file format (e.g., CSV, JSON, Avro, Parquet).

    Methods:
        csv_reader(file_path: str):
            Abstract method to create a CSV reader instance.
        json_reader(file_path: str):
            Abstract method to create a JSON reader instance.
        avro_reader(file_path: str):
            Abstract method to create an AVRO reader instance.
        pandas_reader(file_path: str):
            Abstract method to create a Pandas reader instance.

    Example:
        >>> from my_module.reader_factory import PandasReaderFactory
        >>> factory = PandasReaderFactory()
        >>> reader = factory.csv_reader("data.csv",
                    storage_options={
                        "account_name": "xyz",
                        "account_key": "xyx"
                    })
            df = reader.read_data()
    """
    @abstractmethod
    def csv_reader(self, file_path: str, **kwarg) -> DataReader:
        """
        Abstract method to create a CSV reader instance.

        Args:
            file_path (str): The path to the CSV file.
            **kwargs: Optional keyword arguments passed to the concrete
                implementation.
                For Example:
                    - Pandas: `storage_options`
                    - spark: `spark_session`

        Returns:
            DataReader: A concrete reader instance that implements the DataReader
                interface.
        """
        pass

    @abstractmethod
    def json_reader(self, file_path: str, **kwarg) -> DataReader:
        """
        Abstract method to create a JSON reader instance.

        Args:
            file_path (str): The path to the JSON file.
            **kwargs: Optional keyword arguments passed to the concrete
                implementation.
                For Example:
                    - Pandas: `storage_options`
                    - spark: `spark_session`

        Returns:
            DataReader: A concrete reader instance that implements the DataReader
                interface.
        """
        pass

    @abstractmethod
    def avro_reader(self, file_path: str, **kwarg) -> DataReader:
        """
        Abstract method to create a Avro reader instance.

        Args:
            file_path (str): The path to the Avro file.
            **kwargs: Optional keyword arguments passed to the concrete
                implementation.
                For Example:
                    - Pandas: `storage_options`
                    - spark: `spark_session`

        Returns:
            DataReader: A concrete reader instance that implements the DataReader
                interface.
        """
        pass

    @abstractmethod
    def parquet_reader(self, file_path: str, **kwarg) -> DataReader:
        """
        Abstract method to create a Parquet reader instance.

        Args:
            file_path (str): The path to the Parquet file.
            **kwargs: Optional keyword arguments passed to the concrete
                implementation.
                For Example:
                    - Pandas: `storage_options`
                    - spark: `spark_session`

        Returns:
            Any: A concrete reader instance that implements the DataReader
                interface.
        """
        pass
