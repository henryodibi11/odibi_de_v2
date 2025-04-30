from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    log_call, benchmark, validate_kwargs_match_signature
)
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise
from odibi_de_v2.core.enums import ErrorType
import pandas as pd


class PandasColumnDropper(IDataTransformer):
    """
    Drop specified columns from a Pandas DataFrame.

    This transformer class is designed to remove specified columns from a DataFrame, which can be
    useful in data preprocessing steps where certain columns are not needed for further analysis or modeling.

    Args:
        data (pd.DataFrame): The input DataFrame from which columns will be dropped.
        **kwargs: Additional keyword arguments that are passed directly to the `DataFrame.drop()` method.
        This allows for customization such as handling errors when columns do not exist.

    Returns:
        pd.DataFrame: A DataFrame with the specified columns removed.

    Raises:
        KeyError: If any of the specified columns to drop do not exist in the input DataFrame.

    Example:
        >>> df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4], "col3": [5, 6]})
        >>> dropper = PandasColumnDropper(columns_to_drop=["col1", "col3"])
        >>> transformed_df = dropper.transform(df)
        >>> print(transformed_df)
        col2
        0     3
        1     4
"""

    @enforce_types(strict=True)
    @validate_non_empty(["columns_to_drop"])
    @benchmark(module="TRANSFORMER", component="PandasColumnDropper")
    @log_call(module="TRANSFORMER", component="PandasColumnDropper")
    @log_exceptions(
        module="TRANSFORMER",
        component="PandasColumnDropper",
        error_type=ErrorType.INIT_ERROR,
        raise_type=RuntimeError
    )
    def __init__(self, columns_to_drop: list):
        """
        Initializes an instance of the PandasColumnDropper class, setting up the columns to be
        dropped from a DataFrame.

        This constructor stores the list of column names that should be dropped from any DataFrame processed
        by this instance. It also logs the initialization details.

        Args:
            columns_to_drop (list of str): A list containing the names of the columns to be dropped from the DataFrame.

        Raises:
            TypeError: If `columns_to_drop` is not a list.

        Example:
            >>> dropper = PandasColumnDropper(columns_to_drop=['age', 'height'])
            This will create an instance of PandasColumnDropper which, when used, will drop the 'age' and 'height'
            columns from any DataFrame it processes.
        """
        self.columns_to_drop = columns_to_drop
        log_and_optionally_raise(
            module="TRANSFORMER",
            component="PandasColumnDropper",
            method="__init__",
            error_type=ErrorType.NO_ERROR,
            message=f"Initialized with columns_to_drop: {columns_to_drop}",
            level="INFO"
        )

    @enforce_types(strict=True)
    @ensure_output_type(pd.DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="PandasColumnDropper",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError
    )
    def transform(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transforms the input DataFrame by dropping specified columns.

        This method modifies the input DataFrame by removing the columns listed in `self.columns_to_drop`. It allows
        additional parameters that are valid for the `pd.DataFrame.drop()` method to be passed through `**kwargs`.

        Args:
            data (pd.DataFrame): The DataFrame from which columns will be dropped.
            **kwargs: Arbitrary keyword arguments that are passed to `pd.DataFrame.drop()` for additional customization.

        Returns:
            pd.DataFrame: A DataFrame with the specified columns removed.

        Raises:
            KeyError: If any of the columns specified in `self.columns_to_drop` do not exist in the input DataFrame.

        Example:
            Assuming `transformer` is an instance of a class with a `columns_to_drop` attribute set to ['age', 'height']:
            >>> df = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [25, 30], 'height': [165, 175]})
            >>> transformed_df = transformer.transform(df)
            >>> print(transformed_df)
            # Output:    name
                    0  Alice
                    1    Bob
        """
        # Validate the method signature
        validate_kwargs_match_signature(data.drop, kwargs,"TRANSFORMER","PandasColumnDropper")

        # Validate that columns exist
        missing = [col for col in self.columns_to_drop if col not in data.columns]
        if missing:
            raise KeyError(f"The following columns are missing: {missing}")

        log_and_optionally_raise(
            module="TRANSFORMER",
            component="PandasColumnDropper",
            method="transform",
            error_type=ErrorType.NO_ERROR,
            message=f"Dropping columns: {self.columns_to_drop}",
            level="INFO"
        )

        return data.drop(columns=self.columns_to_drop, **kwargs)
