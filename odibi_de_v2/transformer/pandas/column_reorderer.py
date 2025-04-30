from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    log_call, benchmark
)
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise
from odibi_de_v2.core.enums import ErrorType
import pandas as pd


class PandasColumnReorderer(IDataTransformer):
    """
    Reorders the columns of a Pandas DataFrame according to a specified order.

    This class provides functionality to reorder the columns of a DataFrame based on a
    predefined list. If `retain_unspecified` is set to True, any columns not explicitly
    mentioned in the `column_order` list will be appended to the end of the DataFrame in their original order.

    Args:
        column_order (list of str): A list specifying the desired order of the DataFrame columns.
        retain_unspecified (bool, optional): If True, includes columns not specified in `column_order`
        at the end of the DataFrame. Defaults to False.

    Raises:
        KeyError: If any column specified in `column_order` does not exist in the input DataFrame.

    Returns:
        pd.DataFrame: A DataFrame with columns reordered as specified.

    Example:
        >>> reorderer = PandasColumnReorderer(
                column_order=["Plant Name", "Process"],
                retain_unspecified=True
            )
        >>> df = pd.DataFrame({
                "Process": [1, 2],
                "Plant Name": ['Plant A', 'Plant B'],
                "Extra": [5, 6]
            })
        >>> transformed_df = reorderer.transform(df)
        >>> print(transformed_df.columns)
        Index(['Plant Name', 'Process', 'Extra'], dtype='object')
    """

    @enforce_types(strict=True)
    @validate_non_empty(["column_order"])
    @benchmark(module="TRANSFORMER", component="PandasColumnReorderer")
    @log_call(module="TRANSFORMER", component="PandasColumnReorderer")
    @log_exceptions(
        module="TRANSFORMER",
        component="PandasColumnReorderer",
        error_type=ErrorType.INIT_ERROR,
        raise_type=RuntimeError
    )
    def __init__(self, column_order: list, retain_unspecified: bool = False):
        """
        Initializes a new instance of the PandasColumnReorderer class.

        This constructor sets up the column reordering configuration for a pandas DataFrame based on
        specified column order. It can optionally retain columns that are not explicitly specified in
        the column order list.

        Args:
            column_order (list): A list of strings specifying the desired order of columns in the DataFrame.
            retain_unspecified (bool, optional): If True, columns not specified in `column_order` will be
            retained at the end of the DataFrame in their original order. Defaults to False.

        Example:
            >>> reorderer = PandasColumnReorderer(['name', 'age'], retain_unspecified=True)

        Note:
            This method logs the initialization parameters at the INFO level.
        """
        self.column_order = column_order
        self.retain_unspecified = retain_unspecified

        log_and_optionally_raise(
            module="TRANSFORMER",
            component="PandasColumnReorderer",
            method="__init__",
            error_type=ErrorType.NO_ERROR,
            message=f"Initialized with column_order={column_order}, "
                    f"retain_unspecified={retain_unspecified}",
            level="INFO"
        )

    @enforce_types(strict=True)
    @ensure_output_type(pd.DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="PandasColumnReorderer",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError
    )
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Reorders the columns of a pandas DataFrame based on a predefined order, optionally retaining
        unspecified columns.

        This method reorders the columns of the input DataFrame according to the order specified in
        `self.column_order`. If `self.retain_unspecified` is True, any columns in the input DataFrame
        that are not specified in `self.column_order` will be appended to the end of the DataFrame in
        their original order.

        Args:
            data (pd.DataFrame): The DataFrame whose columns are to be reordered.

        Returns:
            pd.DataFrame: A new DataFrame with columns reordered as specified.

        Raises:
            KeyError: If any columns specified in `self.column_order` are not found in the input DataFrame.

        Example:
            Assuming an instance `reorderer` of the class with `column_order` set to ['name', 'age', 'height']
            and `retain_unspecified` set to True:
            >>> df = pd.DataFrame({
            ...     'age': [25, 30],
            ...     'name': ['Alice', 'Bob'],
            ...     'height': [165, 175],
            ...     'weight': [55, 85]
            ... })
            >>> reordered_df = reorderer.transform(df)
            >>> print(reordered_df)
            name  age  height  weight
            0 Alice  25     165      55
            1   Bob  30     175      85
        """
        log_and_optionally_raise(
            module="TRANSFORMER",
            component="PandasColumnReorderer",
            method="transform",
            error_type=ErrorType.NO_ERROR,
            message="Starting column reorder transformation.",
            level="INFO"
        )

        # Validate columns exist
        missing = [col for col in self.column_order if col not in data.columns]
        if missing:
            raise KeyError(f"Columns not found in DataFrame: {missing}")

        # Compose final order
        final_order = self.column_order[:]
        if self.retain_unspecified:
            extras = [col for col in data.columns if col not in self.column_order]
            final_order += extras

        log_and_optionally_raise(
            module="TRANSFORMER",
            component="PandasColumnReorderer",
            method="transform",
            error_type=ErrorType.NO_ERROR,
            message=f"Final column order: {final_order}",
            level="INFO")

        return data[final_order]