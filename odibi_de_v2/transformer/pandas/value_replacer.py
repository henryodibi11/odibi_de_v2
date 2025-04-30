from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    log_call, benchmark
)
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise
from odibi_de_v2.core.enums import ErrorType
import pandas as pd


class PandasValueReplacer(IDataTransformer):
    """
    Replace values in specified columns of a Pandas DataFrame based on a predefined mapping.

    This class is designed to facilitate the transformation of data within a DataFrame by
    replacing specified old values with new values in designated columns. It utilizes a
    dictionary (`value_map`) to define the replacements for each column.

    Args:
        value_map (dict): A dictionary where each key is a column name in the DataFrame, and each
        value is another dictionary mapping old values to new values for that column.

    Raises:
        KeyError: If a specified column in `value_map` does not exist in the input DataFrame during transformation.

    Returns:
        pd.DataFrame: A DataFrame with the specified values replaced according to `value_map`.

    Example:
        >>> value_replacer = PandasValueReplacer(
                value_map={
                    "column1": {"old_value1": "new_value1", "old_value2": "new_value2"},
                    "column2": {"old_value3": "new_value3"}
                }
            )
        >>> data = pd.DataFrame({
                "column1": ["old_value1", "value_unaffected"],
                "column2": ["old_value3", "another_value"]
            })
        >>> transformed_data = value_replacer.transform(data)
        >>> print(transformed_data)
            column1           column2
        0  new_value1       new_value3
        1  value_unaffected another_value
    """

    @enforce_types(strict=True)
    @validate_non_empty(["value_map"])
    @benchmark(module="TRANSFORMER", component="PandasValueReplacer")
    @log_call(module="TRANSFORMER", component="PandasValueReplacer")
    @log_exceptions(
        module="TRANSFORMER",
        component="PandasValueReplacer",
        error_type=ErrorType.INIT_ERROR,
        raise_type=RuntimeError
    )
    def __init__(self, value_map: dict):
        """
        Initializes an instance of the PandasValueReplacer class.

        This constructor method sets up a new PandasValueReplacer with a specified mapping of columns
        to their new values for replacement purposes. It logs the initialization details using a custom
        logging function.

        Args:
            value_map (dict): A dictionary where keys are column names and values are dictionaries that
            map old values to new values for each column.

        Example:
            >>> replacer = PandasValueReplacer({'age': {30: 31}, 'name': {'John': 'Jonathan'}})
            This creates a replacer instance that will replace the age 30 with 31 and names 'John'
            with 'Jonathan' in a DataFrame.
        """
        self.value_map = value_map
        log_and_optionally_raise(
            module="TRANSFORMER",
            component="PandasValueReplacer",
            method="__init__",
            error_type=ErrorType.NO_ERROR,
            message=f"Initialized with value_map: {value_map}",
            level="INFO"
        )

    @enforce_types(strict=True)
    @ensure_output_type(pd.DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="PandasValueReplacer",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError
    )
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by replacing specified values in certain columns based on a predefined mapping.

        This method applies a series of value replacements across specified columns of a DataFrame. Each column's
        values are replaced according to a mapping dictionary (`value_map`) defined within the class. The method
        logs each step of the process, providing detailed information about the transformation and any potential
        issues encountered.

        Args:
            data (pd.DataFrame): The DataFrame to be transformed.

        Returns:
            pd.DataFrame: A DataFrame with specified values replaced according to `value_map`.

        Raises:
            KeyError: If a column specified in `value_map` does not exist in the input DataFrame.

        Example:
            Assuming an instance `replacer` of a class with a `value_map` attribute set to
            `{'column1': {1: 'One', 2: 'Two'}}`:
            >>> original_data = pd.DataFrame({'column1': [1, 2, 3]})
            >>> transformed_data = replacer.transform(original_data)
            >>> print(transformed_data)
            column1
            0     One
            1     Two
            2       3
        """
        log_and_optionally_raise(
            module="TRANSFORMER",
            component="PandasValueReplacer",
            method="transform",
            error_type=ErrorType.NO_ERROR,
            message="Starting value replacement transformation.",
            level="INFO"
        )

        transformed_data = data.copy()

        for column, replacements in self.value_map.items():
            if column not in transformed_data.columns:
                raise KeyError(f"Column '{column}' not found in DataFrame.")

            log_and_optionally_raise(
                module="TRANSFORMER",
                component="PandasValueReplacer",
                method="transform",
                error_type=ErrorType.NO_ERROR,
                message=f"Replacing values in column '{column}'.",
                level="INFO"
            )

            transformed_data[column] = transformed_data[column].replace(replacements)

        return transformed_data