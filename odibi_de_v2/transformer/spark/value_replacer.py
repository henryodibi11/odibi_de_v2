from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, when
from odibi_de_v2.core import ErrorType
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, log_call, benchmark)
from odibi_de_v2.logger import log_exceptions,log_and_optionally_raise
from odibi_de_v2.core import IDataTransformer


class SparkValueReplacer(IDataTransformer):
    """
    Replace values in specified columns of a Spark DataFrame based on a predefined mapping.

    This class is designed to perform value replacement transformations on Spark DataFrames.
    It uses a dictionary (`value_map`) to define the replacements for specified columns. Each key
    in the `value_map` represents a column in the DataFrame, and the corresponding value is
    another dictionary mapping old values to new values for that column.

    Args:
        value_map (dict): A dictionary where each key is a column name in the DataFrame, and each
        value is another dictionary mapping old values to new values in that column.

    Methods:
        transform(data: DataFrame, **kwargs) -> DataFrame:
            Applies the value replacement transformation to the input DataFrame based on the
            `value_map` provided during initialization.

            Args:
                data (DataFrame): The input Spark DataFrame to be transformed.

            Returns:
                DataFrame: A new DataFrame with values replaced according to the `value_map`.

            Raises:
                KeyError: If any of the columns specified in `value_map` do not exist in the input DataFrame.

    Example:
        ```python
        value_map = {
            'column1': {10: 20, 30: 40},
            'column2': {'old_value': 'new_value'}
        }
        replacer = SparkValueReplacer(value_map)
        transformed_df = replacer.transform(input_df)
        ```
    """

    @enforce_types(strict=True)
    @validate_non_empty(["value_map"])
    @benchmark(module="TRANSFORMER", component="SparkValueReplacer")
    @log_call(module="TRANSFORMER", component="SparkValueReplacer")
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkValueReplacer",
        error_type=ErrorType.INIT_ERROR,
        raise_type=RuntimeError)
    def __init__(self, value_map: dict):
        """
        Initializes a new instance of the SparkValueReplacer class.

        This constructor initializes the SparkValueReplacer with a dictionary mapping values
        for transformation purposes. It logs the initialization details at the INFO level.

        Args:
            value_map (dict): A dictionary mapping original values to their replacements.

        Returns:
            None

        Example:
            >>> replacer = SparkValueReplacer({'a': 'b', 'x': 'y'})
            # This will log: "Initialized with value_map: {'a': 'b', 'x': 'y'}
        """
        self.value_map = value_map
        log_and_optionally_raise(
            module="TRANSFORMER",
            component="SparkValueReplacer",
            method="__init__",
            error_type=ErrorType.NO_ERROR,
            message=f"Initialized with value_map: {value_map}",
            level="INFO")

    @enforce_types(strict=True)
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkValueReplacer",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError)
    def transform(self, data: DataFrame, **kwargs) -> DataFrame:
        """
        Transforms specified columns in a Spark DataFrame by replacing old values with new values
        based on a predefined mapping.

        This method iterates over each column specified in the `value_map` attribute of the instance,
        replacing each old value with its corresponding new value. If a column specified in `value_map`
        does not exist in the input DataFrame, a KeyError is raised.

        Args:
            data (DataFrame): The input Spark DataFrame to be transformed.

        Keyword Args:
            **kwargs: Additional keyword arguments that are not used directly in this method but can
            be passed to other underlying methods or functions.

        Returns:
            DataFrame: A new DataFrame with specified values replaced according to the `value_map`.

        Raises:
            KeyError: If a column specified in `value_map` does not exist in the input DataFrame.

        Example:
            Assuming an instance `replacer` of a class with a `value_map` attribute set to
            `{'age': {30: 29, 45: 44}}`, and a DataFrame `df`:
            ```python
            transformed_df = replacer.transform(df)
            ```
            This will replace all occurrences of 30 with 29, and 45 with 44 in the 'age' column of `df`.
        """
        log_and_optionally_raise(
            module="TRANSFORMER",
            component="SparkValueReplacer",
            method="transform",
            error_type=ErrorType.NO_ERROR,
            message="Starting value replacement transformation.",
            level="INFO")

        transformed_data = data

        for column, replacements in self.value_map.items():
            if column not in transformed_data.columns:
                raise KeyError(f"Column '{column}' not found in DataFrame.")

            log_and_optionally_raise(
                module="TRANSFORMER",
                component="SparkValueReplacer",
                method="transform",
                error_type=ErrorType.NO_ERROR,
                message=f"Replacing values in column '{column}'.",
                level="INFO")

            expr = col(column)
            for old_value, new_value in replacements.items():
                expr = when(col(column) == old_value, new_value).otherwise(expr)

            transformed_data = transformed_data.withColumn(column, expr)

        return transformed_data
