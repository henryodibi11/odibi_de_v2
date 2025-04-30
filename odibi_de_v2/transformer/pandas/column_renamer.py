from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    log_call, benchmark, validate_kwargs_match_signature
)
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise
from odibi_de_v2.core.enums import ErrorType
from odibi_de_v2.pandas_utils.validation import has_columns
import pandas as pd


class PandasColumnRenamer(IDataTransformer):
    """
    Renames columns in a Pandas DataFrame according to a specified mapping.

    This class provides a method to transform a DataFrame by renaming its columns
    based on a provided dictionary mapping of old column names to new column names.
    It includes various decorators to enforce type checks, log method calls and exceptions, and benchmark performance.

    Args:
        column_map (dict): A dictionary mapping from current column names (keys) to new column names (values).

    Attributes:
        column_map (dict): Stores the mapping of column names used for renaming.

    Methods:
        transform(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
            Renames columns of the provided DataFrame according to `column_map`.
            Additional keyword arguments are passed directly to the `rename` method of the DataFrame.

            Args:
                data (pd.DataFrame): The DataFrame whose columns are to be renamed.
                **kwargs: Additional keyword arguments accepted by pandas.DataFrame.rename().

            Returns:
                pd.DataFrame: A new DataFrame with columns renamed as specified in `column_map`.

            Raises:
                RuntimeError: If any specified column in `column_map` does not exist in `data`.
                TypeError: If `data` is not a pandas DataFrame.

    Example:
        column_map = {"old_name": "new_name", "old_age": "new_age"}
        renamer = PandasColumnRenamer(column_map=column_map)
        transformed_df = renamer.transform(df)
    """

    @enforce_types(strict=True)
    @validate_non_empty(["column_map"])
    @benchmark(module="TRANSFORMER", component="PandasColumnRenamer")
    @log_call(module="TRANSFORMER", component="PandasColumnRenamer")
    @log_exceptions(
        module="TRANSFORMER",
        component="PandasColumnRenamer",
        error_type=ErrorType.INIT_ERROR,
        raise_type=RuntimeError)
    def __init__(self, column_map: dict):
        """
        Initializes a new instance of the PandasColumnRenamer class.

        This constructor initializes the PandasColumnRenamer with a mapping of columns, intended
        for renaming DataFrame columns. It logs the initialization details at the INFO level.

        Args:
            column_map (dict): A dictionary mapping from original column names to new column names.

        Raises:
            ValueError: If `column_map` is not a dictionary.

        Example:
            >>> renamer = PandasColumnRenamer({'old_name': 'new_name'})
            >>> print(renamer.column_map)
            {'old_name': 'new_name'}
        """
        self.column_map = column_map
        log_and_optionally_raise(
            module="TRANSFORMER",
            component="PandasColumnRenamer",
            method="__init__",
            error_type=ErrorType.NO_ERROR,
            message=f"Initialized with column_map: {column_map}",
            level="INFO")

    @enforce_types(strict=True)
    @ensure_output_type(pd.DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="PandasColumnRenamer",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError)
    def transform(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Renames columns in a pandas DataFrame according to a predefined mapping.

        This method first checks if all columns specified in the column mapping exist in the
        input DataFrame. It then validates that all keyword arguments provided are applicable
        to the pandas `rename` method. After logging the renaming action, it proceeds to rename the columns.

        Args:
            data (pd.DataFrame): The DataFrame whose columns are to be renamed.
            **kwargs: Arbitrary keyword arguments that are passed directly to the `rename` method of
            pandas DataFrame. These are optional and should match the parameters accepted by `data.rename`.

        Returns:
            pd.DataFrame: A DataFrame with columns renamed as specified in `self.column_map`.

        Raises:
            KeyError: If any column specified in `self.column_map` does not exist in the input DataFrame.
            ValueError: If any provided keyword arguments do not match the signature of the `data.rename` method.

        Example:
            Assuming `self.column_map` is `{'old_name': 'new_name'}`, and the DataFrame `df` contains 'old_name':

            >>> renamer = PandasColumnRenamer(column_map={'old_name': 'new_name'})
            >>> transformed_df = renamer.transform(df)
            >>> 'new_name' in transformed_df.columns
            True
        """
        has_columns(data, list(self.column_map.keys()))
        validate_kwargs_match_signature(data.rename, kwargs,"TRANSFORMER","PandasColumnRenamer")
        log_and_optionally_raise(
            module="TRANSFORMER",
            component="PandasColumnRenamer",
            method="transform",
            error_type=ErrorType.NO_ERROR,
            message=f"Renaming columns: {self.column_map}",
            level="INFO")
        return data.rename(columns=self.column_map, **kwargs)