from abc import ABC, abstractmethod
from typing import Any, Callable, Dict
from pyspark.sql import DataFrame


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


def node_wrapper(cls: Callable) -> Callable:
    """
    Wrap a transformer class so it can be used directly inside
    a SparkWorkflowNode step.

    This wrapper ensures the step conforms to the convention
    of taking a DataFrame as the first argument, while still
    supporting transformer classes that expect a conversion_query
    or config.

    Args:
        cls (Callable): A transformer class with a .transform() method.

    Returns:
        Callable: A function that accepts (df, **params) and returns
        a DataFrame.

    Example:
        >>> from odibi_de_v2.core.transformer import node_wrapper
        >>> from odibi_de_v2.transformer import SparkWeatherWorkflowTransformer
        >>>
        >>> WeatherNode = node_wrapper(SparkWeatherWorkflowTransformer)
        >>> step = (WeatherNode, {
        ...     "conversion_query": "SELECT * FROM previous_view",
        ...     "humidity_ratio_configs": [
        ...         {
        ...             "input_params": {
        ...                 "Tdb": "Inlet Air Temperature Dry Bulb",
        ...                 "RH": "Inlet Air Relative Humidity",
        ...                 "pressure": "Pressure at Elevation"
        ...             },
        ...             "output_col_name": "Humidity Ratio Inlet"
        ...         }
        ...     ],
        ...     "view_name": "humidity_df",
        ...     "register_view": True
        ... })
    """

    def wrapper(df: DataFrame, **params: Dict[str, Any]) -> DataFrame:
        transformer = cls(**params)
        return transformer.transform()

    return wrapper
