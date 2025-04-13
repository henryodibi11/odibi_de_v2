from abc import ABC, abstractmethod


class DataSaver(ABC):
    """
    Abstract base class defining the interface for saving data to various
    file formats.

    Attributes:
        file_path (str): The path to the file where data will be saved.

    Methods:
        save_data(data, **kwargs):
            Abstract method for saving the dataset to the configured file path.

    Example:
        >>> import pandas as pd
        >>> class CSVSaver(DataSaver):
        ...     def save_data(self, data, **kwargs):
        ...         return pd.to_csv(self.file_path, **kwargs)

        >>> saver = CSVSaver("example.csv")
        >>> df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        >>> saver.save_data(df, index=False)
    """
    def __init__(self, file_path: str):
        self.file_path = file_path

    @abstractmethod
    def save_data(self, data, **kwargs):
        """
        Abstract method to save datasets.

        Args:
            data (Any): Dataset object to save, as defined by the concrete
                implementation (e.g., a pandas DataFrame).
            **kwargs: Additional keyword arguments to be used by the
            implementation (e.g., `index`, `header`, etc.).

        Returns:
            None
        """
        pass
