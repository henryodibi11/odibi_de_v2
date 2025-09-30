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


from typing import Callable, Any
from pyspark.sql import DataFrame


def node_wrapper(obj: Callable) -> Callable:
    """
    Wrap a callable so it can be used directly inside a SparkWorkflowNode step.

    This wrapper unifies two scenarios:

    1. **Transformer classes** (with a `.transform()` method):
       - `obj` is a class that implements `.transform()`.
       - The wrapper will instantiate the class with the provided `**params`
         and call its `.transform()` method.
       - Example:
         >>> WeatherNode = node_wrapper(SparkWeatherWorkflowTransformer)
         >>> step = (WeatherNode, {"conversion_query": "SELECT * FROM base_weather"})

    2. **Plain functions** that return a DataFrame:
       - `obj` is a function that directly returns a DataFrame and does not
         require a `df` as its first argument.
       - The wrapper simply calls the function with the provided `**params`.
       - Example:
         >>> EnergyNode = node_wrapper(get_energy_efficiency_data)
         >>> step = (EnergyNode, {"source_table": "my_table", "tags_table": "tags"})

    Parameters
    ----------
    obj : Callable
        Either a transformer class (with `.transform()`) or a plain function
        that returns a DataFrame.

    Returns
    -------
    Callable
        A function with the SparkWorkflowNode-compatible signature:
        `(df: DataFrame, **params) -> DataFrame`.

    Notes
    -----
    - The input `df` argument is preserved for compatibility with the
      SparkWorkflowNode step contract but ignored for plain functions and
      transformer classes that don't consume it.
    - This ensures you can mix both SQL/config steps and Python steps inside
      one `SparkWorkflowNode`.
    """
    def wrapper(df: DataFrame, **params: Any) -> DataFrame:
        if hasattr(obj, "transform"):
            transformer = obj(**params)
            return transformer.transform()
        else:
            return obj(**params)

    return wrapper
