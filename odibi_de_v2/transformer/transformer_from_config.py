from typing import Union, Dict, Any, List
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame

from odibi_de_v2.core import Framework, ErrorType
from odibi_de_v2.utils import enforce_types, log_call
from odibi_de_v2.logger import log_exceptions
from odibi_de_v2.transformer import TransformerProvider


class TransformerFromConfig:
    """
    Applies a series of transformations to a DataFrame based on a specified configuration.

    This method processes an input DataFrame (either Pandas or Spark depending on the initialization of the class)
    using a list of transformation specifications. Each specification dictates which transformation to apply and any
    parameters required for that transformation.

    Args:
        data (Union[pd.DataFrame, pyspark.sql.DataFrame]): The DataFrame to be transformed.
        config (list of dict): A list of dictionaries where each dictionary specifies a transformation.
        Each dictionary should contain:
            - "transformer" (str): The name of the transformer class to be used.
            - "params" (dict, optional): A dictionary of parameters to pass to the transformer.

    Returns:
        Union[pd.DataFrame, pyspark.sql.DataFrame]: The DataFrame after applying all specified transformations.

    Raises:
        RuntimeError: If an error occurs during the initialization or transformation process, encapsulated by the
        decorators.

    Example:
        # Assuming the class has been initialized with a Pandas framework
        transformer = TransformerFromConfig(Framework.PANDAS)
        data = pd.DataFrame({"old": [1, 2], "unnecessary_col": [3, 4]})
        config = [
            {"transformer": "PandasColumnRenamer", "params": {"column_map": {"old": "new"}}},
            {"transformer": "PandasColumnDropper", "params": {"columns_to_drop": ["unnecessary_col"]}}
        ]
        transformed_data = transformer.transform(data, config)
    """

    @log_call(module="TRANSFORMATION", component="TransformerFromConfig")
    @enforce_types(strict=True)
    @log_exceptions(
        module="TRANSFORMATION",
        component="TransformerFromConfig",
        error_type=ErrorType.INIT_ERROR,
        raise_type=RuntimeError)
    def __init__(self, framework: Framework):
        """
        Initializes an instance of the class, setting up the framework and transformer provider.

        Args:
            framework (Framework): The framework object that specifies the context or environment
                in which the transformers will operate.

        Returns:
            None

        Example:
            >>> my_framework = Framework()
            >>> my_instance = MyClass(my_framework)
        """
        self.framework = framework
        self.provider = TransformerProvider(framework=framework)

    @log_call(module="TRANSFORMATION", component="TransformerFromConfig")
    @enforce_types(strict=True)
    @log_exceptions(
        module="TRANSFORMATION",
        component="TransformerFromConfig",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError)
    def transform(
        self,
        data: Union[pd.DataFrame, SparkDataFrame],
        config: list
    ) -> Union[pd.DataFrame, SparkDataFrame]:
        """
        Applies a series of transformations to a DataFrame based on a configuration list.

        This method processes an input DataFrame using a sequence of transformation steps defined in the `config`
        parameter. Each step in the configuration should specify a transformer and its corresponding parameters.
        The method supports transformations on both pandas and Spark DataFrames.

        Args:
            data (Union[pd.DataFrame, SparkDataFrame]): The input DataFrame to be transformed.
            config (list): A list of dictionaries, where each dictionary defines a transformation step.
            Each dictionary must include:
                - "transformer" (str): The name of the transformer class to be used.
                - "params" (dict, optional): A dictionary of keyword arguments specific to the transformer.

        Returns:
            Union[pd.DataFrame, SparkDataFrame]: The DataFrame after applying all transformations specified in the config.

        Raises:
            ValueError: If any dictionary in the config list does not contain the "transformer" key.
            TypeError: If the config is not a list or if any of the config elements are not dictionaries.

        Example:
            >>> df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
            >>> config = [
                    {"transformer": "AddColumnTransformer", "params": {"column_name": "c", "value": 5}},
                    {"transformer": "MultiplyColumnTransformer", "params": {"column_name": "a", "factor": 2}}
                ]
            >>> transformed_df = instance.transform(df, config)
            >>> print(transformed_df)
            a  b  c
            0  2  3  5
            1  4  4  5
        """
        if isinstance(config, dict):
            config = [config]

        for step in config:
            transformer_name = step["transformer"]
            transformer_params = step.get("params", {})
            data = self.provider.transform(transformer_name, data, **transformer_params)

        return data
