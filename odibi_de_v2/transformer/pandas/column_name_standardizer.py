import pandas as pd
from odibi_de_v2.core import ErrorType
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, log_call, benchmark
)
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise
from odibi_de_v2.pandas_utils import has_columns, transform_column_name
from odibi_de_v2.core import IDataTransformer


class PandasColumnNameStandardizer(IDataTransformer):
    """
    Standardizes the column names of a Pandas DataFrame according to a specified style, optionally
    excluding certain columns.

    This class provides functionality to transform DataFrame column names to a consistent naming convention
    (e.g., snake_case, camelCase) while allowing specific columns to be excluded from this transformation.

    Args:
        case_style (str): The desired case style for the column names. Supported styles include "snake_case",
            "camelCase", "PascalCase", "lowercase", and "uppercase".
        exclude_columns (list, optional): A list of column names that should not be transformed.
            Defaults to an empty list if not provided.

    Raises:
        ValueError: If the `case_style` provided is not supported.

    Returns:
        pd.DataFrame: A DataFrame with standardized column names, except for those explicitly excluded.

    Example:
        >>> df = pd.DataFrame({
        ...     'First Name': ['Alice', 'Bob'],
        ...     'Last Name': ['Smith', 'Jones'],
        ...     'age': [25, 30]
        ... })
        >>> transformer = PandasColumnNameStandardizer(case_style='snake_case')
        >>> transformed_df = transformer.transform(df)
        >>> transformed_df.columns
        Index(['first_name', 'last_name', 'age'], dtype='object')
    """

    @enforce_types(strict=True)
    @validate_non_empty(["case_style"])
    @benchmark(module="TRANSFORMER", component="PandasColumnNameStandardizer")
    @log_call(module="TRANSFORMER", component="PandasColumnNameStandardizer")
    @log_exceptions(
        module="TRANSFORMER",
        component="PandasColumnNameStandardizer",
        error_type=ErrorType.INIT_ERROR,
        raise_type=ValueError)
    def __init__(self, case_style: str = "snake_case", exclude_columns: list = None):
        """
        Initializes an instance of the PandasColumnNameStandardizer class, setting up the column
        name transformation preferences.

        Args:
            case_style (str, optional): The style to convert column names into. Supported styles
            are "snake_case", "camelCase", "PascalCase", "lowercase", and "uppercase". Defaults to "snake_case".
            exclude_columns (list, optional): A list of column names to exclude from transformation.
                Defaults to an empty list if not provided.

        Raises:
            ValueError: If the `case_style` provided is not one of the valid options.

        Example:
            >>> standardizer = PandasColumnNameStandardizer(case_style="camelCase", exclude_columns=["id", "timestamp"])
            This will create a standardizer that converts all column names to camelCase except for 'id' and 'timestamp'.
        """
        valid_styles = ["snake_case", "camelCase", "PascalCase", "lowercase", "uppercase"]
        if case_style not in valid_styles:
            raise ValueError(f"Invalid case_style: {case_style}. Must be one of {valid_styles}")

        self.case_style = case_style
        self.exclude_columns = exclude_columns or []

        log_and_optionally_raise(
            module="TRANSFORMER",
            component="PandasColumnNameStandardizer",
            method="__init__",
            error_type=ErrorType.NO_ERROR,
            message=f"Initialized with case_style={case_style}, exclude_columns={self.exclude_columns}",
            level="INFO"
        )

    @enforce_types(strict=True)
    @log_exceptions(
        module="TRANSFORMER",
        component="PandasColumnNameStandardizer",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError)
    def transform(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transforms the column names of a pandas DataFrame according to a specified style, excluding specified columns.

        This method standardizes the names of the DataFrame columns based on the `case_style` attribute of the instance,
        while excluding any columns listed in `self.exclude_columns`. It logs the process at various steps.

        Args:
            data (pd.DataFrame): The DataFrame whose column names are to be transformed.
            **kwargs: Arbitrary keyword arguments, potentially used in logging or other extensions.

        Returns:
            pd.DataFrame: A new DataFrame with transformed column names.

        Raises:
            ValueError: If any column specified in `self.exclude_columns` does not exist in the DataFrame.

        Example:
            >>> transformer = PandasColumnNameStandardizer(case_style='snake_case', exclude_columns=['ID'])
            >>> df = pd.DataFrame({'Employee ID': [1, 2], 'Employee Name': ['Alice', 'Bob']})
            >>> transformed_df = transformer.transform(df)
            >>> transformed_df.columns
            Index(['Employee ID', 'employee_name'], dtype='object')
        """
        if self.exclude_columns:
            has_columns(data, self.exclude_columns)

        log_and_optionally_raise(
            module="TRANSFORMER",
            component="PandasColumnNameStandardizer",
            method="transform",
            error_type=ErrorType.NO_ERROR,
            message="Standardizing column names.",
            level="INFO"
        )

        new_columns = [
            col if col in self.exclude_columns else transform_column_name(col, self.case_style)
            for col in data.columns
        ]

        transformed_data = data.copy()
        transformed_data.columns = new_columns

        log_and_optionally_raise(
            module="TRANSFORMER",
            component="PandasColumnNameStandardizer",
            method="transform",
            error_type=ErrorType.NO_ERROR,
            message=f"Final column names: {new_columns}",
            level="INFO"
        )

        return transformed_data