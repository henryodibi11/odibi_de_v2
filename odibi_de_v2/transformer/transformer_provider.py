from typing import Union
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame

from odibi_de_v2.core import Framework, ErrorType
from odibi_de_v2.utils import enforce_types, log_call
from odibi_de_v2.logger import log_exceptions
from odibi_de_v2.transformer import get_transformer_registry


class TransformerProvider:
    """
    Apply a named transformer to the given DataFrame.

    This method retrieves a transformer from a registry based on the provided name and applies it to a DataFrame.
    The method supports both Pandas and Spark DataFrames, and the transformation is performed according to the
    specifications of the transformer class associated with the given name.

    Args:
        transformer_name (str): The key used to retrieve the transformer from the registry.
        data (Union[pd.DataFrame, SparkDataFrame]): The input DataFrame to be transformed.
        **kwargs: Additional keyword arguments that are passed to the transformer class.

    Returns:
        Union[pd.DataFrame, SparkDataFrame]: The DataFrame after applying the specified transformation.

    Raises:
        KeyError: If the transformer name is not found in the registry.

    Example:
        ```python
        provider = TransformerProvider(framework=Framework.PANDAS)
        transformed_data = provider.transform(
            transformer_name="scale_data",
            data=pandas_dataframe,
            scale_factor=0.1
        )
        ```
    """

    @log_call(module="TRANSFORMATION", component="TransformerProvider")
    @enforce_types()
    @log_exceptions(
        module="TRANSFORMATION",
        component="TransformerProvider",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError,
    )
    def __init__(self, framework: Framework):
        """
        Initializes the TransformerProvider with a specified data processing framework.

        This constructor sets up a TransformerProvider to work with either a PANDAS or SPARK framework. It initializes
        the transformer registry specific to the chosen framework. If an unsupported framework is provided,
        it raises a ValueError.

        Args:
            framework (Framework): The data processing engine to be used. Must be either `Framework.PANDAS` or
            `Framework.SPARK`.

        Raises:
            ValueError: If `framework` is not one of the supported types (`Framework.PANDAS` or `Framework.SPARK`).

        Example:
            >>> transformer_provider = TransformerProvider(Framework.PANDAS)
            This will initialize a transformer provider for the PANDAS framework.
        """
        if framework not in [Framework.PANDAS, Framework.SPARK]:
            raise ValueError(f"Unsupported framework: {framework}")
        self.framework = framework
        self.registry = get_transformer_registry()

    @log_call(module="TRANSFORMATION", component="TransformerProvider")
    @enforce_types(strict=False)
    @log_exceptions(
        module="TRANSFORMATION",
        component="TransformerProvider",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError,
    )
    def transform(
        self,
        transformer_name: str,
        data: Union[pd.DataFrame, SparkDataFrame],
        **kwargs
    ) -> Union[pd.DataFrame, SparkDataFrame]:
        """
        Applies a specified transformer to a DataFrame and returns the transformed DataFrame.

        This method retrieves a transformer based on its name from a registry, initializes it with optional keyword
        arguments, and applies its transformation method to the provided DataFrame. It supports both pandas and
        Spark DataFrames.

        Args:
            transformer_name (str): The name of the transformer to retrieve from the registry.
            data (Union[pd.DataFrame, SparkDataFrame]): The DataFrame to be transformed. This can be either a
            pandas DataFrame or a Spark DataFrame.
            **kwargs: Optional keyword arguments that are passed to the transformer upon initialization.

        Returns:
            Union[pd.DataFrame, SparkDataFrame]: The DataFrame after applying the specified transformation.

        Raises:
            KeyError: If the specified transformer_name is not found in the registry.

        Example:
            ```python
            # Assuming 'scaler' is a valid transformer in the registry and df is a pandas DataFrame
            transformed_df = instance.transform('scaler', df, scale=0.5)
            ```
        """
        key = f"{transformer_name}"
        if key not in self.registry:
            raise KeyError(f"Transformer '{key}' not found in registry.")
        transformer_class = self.registry[key]
        transformer = transformer_class(**kwargs)
        return transformer.transform(data)





