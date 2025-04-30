from abc import ABC, abstractmethod
from typing import Any

class IDataTransformer(ABC):
    @abstractmethod
    def transform(self, data: Any, **kwargs) -> Any:
        """
        Apply the transformation to the input DataFrame.

        This abstract method defines the interface for transforming data.
        Concrete implementations should override this method to provide
        specific logic for either Spark or Pandas.

        Args:
            data (Any): The input DataFrame (Spark or Pandas).
            **kwargs: Optional keyword arguments for flexible transformation.

        Returns:
            Any: The transformed DataFrame.
        """
        pass
