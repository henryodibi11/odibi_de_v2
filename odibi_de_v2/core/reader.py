from abc import ABC, abstractmethod
from typing import Any


class DataReader(ABC):
    """
    Base class for reading data from various file formats.

    Attributes:
        file_path (str): The path to the file to be read.

    Methods:
        read_data(**kwargs):
            Abstract method for reading the entire dataset from the file.
        read_sample_data(n: int = 100, **kwargs):
            Method for reading a sample of the dataset, typically for schema
            inference.

    Example:
        >>> import pandas as pd
        >>> class CSVReader(DataReader):
        ...     def read_data(self, **kwargs):
        ...         return pd.read_csv(self.file_path)

        >>> reader = CSVReader("example.csv")
        >>> df = reader.read_sample_data(n=10)
    """
    def __init__(self, file_path: str):
        self.file_path = file_path

    @abstractmethod
    def read_data(self, **kwargs) -> Any:
        """
        Abstract method to read full datasets.

        Args:
            **kwargs: Additional keyword arguments to be used by the
            implementation (e.g., `delimiter`, `header`, etc.).

        Returns:
            Any: Dataset object as defined by the concrete implementation
        """
        pass

    def read_sample_data(
        self,
        n: int = 100,
        **kwargs
    ):
        """
        Read the first `n` rows of the dataset for schema inference.

        Args:
            n (int): Number of rows to sample. Defaults to 100.
            **kwargs: Additional keyword arguments passed to `read_data`.

        Returns:
            Any: Sampled dataset objected as defined by the concrete
                implementation.

        Example:
            >>> reader = CSVReader("example.csv")
            >>> sample_df = reader.read_sample_data(n=5)
        """
        try:
            print(
                "info", f"Reading a sample of {n} rows from {self.file_path}")
            data = self.read_data(**kwargs).head(n)
            print(
                "info",
                f"Successfully read a sample of {n} rows from {self.file_path}"
                )
            return data
        except Exception as e:
            print(
                "error",
                f"Failed to read sample data from {self.file_path}: {e}")
            raise
