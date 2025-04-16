from abc import ABC, abstractmethod
from .saver import DataSaver


class SaverFactory(ABC):
    """
    Abstract base class for creating data savers for different file formats.

    This factory defines the contract for generating saver instances based on
    the target file format (e.g., CSV, JSON, Avro, Parquet).

    Methods:
        csv_saver(file_path: str):
            Abstract method to create a CSV saver instance.
        json_saver(file_path: str):
            Abstract method to create a JSON saver instance.
        avro_saver(file_path: str):
            Abstract method to create an AVRO saver instance.
        parquet_saver(file_path: str):
            Abstract method to create a Parquet saver instance.

    Example:
        >>> from my_module.saver_factory import PandasSaverFactory
        >>> factory = PandasSaverFactory()
        >>> saver = factory.csv_saver("data.csv",
        ...           storage_options={
        ...             "account_name": "xyz",
        ...            "account_key": "xyx"
        ...        })
        >>> df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        >>> saver.save_data(df, index=False)
    """

    @abstractmethod
    def csv_saver(self, file_path: str, **kwarg) -> DataSaver:
        """
        Abstract method to create a CSV saver instance.

        Args:
            file_path (str): The path to save the CSV.
            **kwargs: Optional keyword arguments passed to the concrete
                implementation.
                For Example:
                    - Pandas: `storage_options`
                    - spark: `spark_session`

        Returns:
            DataSaver: A concrete saver instance that implements the DataSaver
                interface.
        """
        pass

    @abstractmethod
    def json_saver(self, file_path: str, **kwarg) -> DataSaver:
        """
        Abstract method to create a JSON saver instance.

        Args:
            file_path (str): The path to save the JSON.
            **kwargs: Optional keyword arguments passed to the concrete
                implementation.
                For Example:
                    - Pandas: `storage_options`
                    - spark: `spark_session`

        Returns:
            DataSaver: A concrete saver instance that implements the DataSaver
                interface.
        """
        pass

    @abstractmethod
    def avro_saver(self, file_path: str, **kwarg) -> DataSaver:
        """
        Abstract method to create a Avro saver instance.

        Args:
            file_path (str): The path to save the Avro.
            **kwargs: Optional keyword arguments passed to the concrete
                implementation.
                For Example:
                    - Pandas: `storage_options`
                    - spark: `spark_session`

        Returns:
            DataSaver: A concrete saver instance that implements the DataSaver
                interface.
        """
        pass

    @abstractmethod
    def parquet_saver(self, file_path: str, **kwarg) -> DataSaver:
        """
        Abstract method to create a Parquet saver instance.

        Args:
            file_path (str): The path to save the Parquet.
            **kwargs: Optional keyword arguments passed to the concrete
                implementation.
                For Example:
                    - Pandas: `storage_options`
                    - spark: `spark_session`

        Returns:
            DataSaver: A concrete saver instance that implements the DataSaver
                interface.
        """
        pass
