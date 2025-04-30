from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    log_call, benchmark
)
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise
from odibi_de_v2.core.enums import ErrorType
import pandas as pd


class PandasColumnAdder(IDataTransformer):
    """
    Adds a new column to a Pandas DataFrame based on specified criteria.

    This class allows for the addition of a new column to a DataFrame by either copying an existing column,
    using a static value, applying a callable function, or aggregating multiple columns using a specified function.

    Args:
        column_name (str): The name of the new column to be added.
        value (any, optional): The static value or callable function to populate the new column.
        If a callable is provided, it should accept a DataFrame and return a Series or a value. Defaults to None.
        aggregation (dict, optional): A dictionary specifying the columns to aggregate and the function to apply
        for aggregation. It should contain the keys 'columns' (a list of column names) and 'agg_func' (a callable).
            Defaults to an empty dictionary.

    Raises:
        ValueError: If neither 'value' nor 'aggregation' is provided, or if 'aggregation' does not contain the
        necessary keys.
        RuntimeError: If an error occurs during the transformation process.

    Returns:
        pd.DataFrame: The DataFrame with the new column added.

    Example:
        >>> df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        >>> adder = PandasColumnAdder(column_name="col_sum", aggregation={"columns": ["a", "b"], "agg_func": sum})
        >>> result = adder.transform(df)
        >>> print(result)
        a  b  col_sum
        0  1  3        4
        1  2  4        6
    """

    @enforce_types(strict=True)
    @validate_non_empty(["column_name"])
    @benchmark(module="TRANSFORMER", component="PandasColumnAdder")
    @log_call(module="TRANSFORMER", component="PandasColumnAdder")
    @log_exceptions(
        module="TRANSFORMER",
        component="PandasColumnAdder",
        error_type=ErrorType.INIT_ERROR,
        raise_type=ValueError
    )
    def __init__(self, column_name: str, value=None, aggregation: dict = {}):
        """
        Initializes a new instance of the PandasColumnAdder class, setting up parameters for column manipulation.

        This constructor sets up the necessary attributes for adding or transforming a column in a pandas DataFrame.
        It requires specifying a column name and either a direct value or an aggregation method to apply.

        Args:
            column_name (str): The name of the column to be added or transformed.
            value (optional): The static value to assign to the column. Default is None.
            aggregation (dict, optional): A dictionary specifying the aggregation operation. It must contain 'columns'
            (list of column names to aggregate) and 'agg_func' (function used for aggregation).
                Default is an empty dictionary.

        Raises:
            ValueError: If neither 'value' nor 'aggregation' is provided, or if 'aggregation' is provided but
            does not contain both 'columns' and 'agg_func' keys.

        Example:
            # Example to add a new column with a static value
            adder = PandasColumnAdder(column_name="new_column", value=10)

            # Example to add a new column by aggregating existing columns
            adder = PandasColumnAdder(
                column_name="average_price", aggregation={'columns': ['price1', 'price2'], 'agg_func': 'mean'})
        """
        if value is None and aggregation is None:
            raise ValueError("Either 'value' or 'aggregation' must be provided.")
        if aggregation and ("columns" not in aggregation or "agg_func" not in aggregation):
            raise ValueError("Aggregation must contain 'columns' and 'agg_func' keys.")

        self.column_name = column_name
        self.value = value
        self.aggregation = aggregation

        log_and_optionally_raise(
            module="TRANSFORMER",
            component="PandasColumnAdder",
            method="__init__",
            error_type=ErrorType.NO_ERROR,
            message=f"Initialized with column_name={column_name}, value={value}, aggregation={aggregation}",
            level="INFO"
        )

    @enforce_types(strict=True)
    @ensure_output_type(pd.DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="PandasColumnAdder",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError
    )
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by adding a new column based on specified rules.

        This method modifies the input DataFrame by adding a new column. The values of this new column are determined
        by the instance's configuration:
        - If `aggregation` is set, it applies the specified aggregation function across the specified columns.
        - If `value` is a callable, it applies this function to the DataFrame.
        - If `value` is a string and exists as a column in the DataFrame, it copies the values from that column.
        - Otherwise, it fills the new column with the static value of `value`.

        Args:
            data (pd.DataFrame): The input DataFrame to be transformed.

        Returns:
            pd.DataFrame: The DataFrame with the new column added.

        Raises:
            RuntimeError: If an error occurs during the addition of the new column.

        Example:
            Assuming an instance `adder` of `PandasColumnAdder` with `column_name='new_col'`
            and `value=lambda x: x['A'] * 2`, you can use this method as follows:

            >>> df = pd.DataFrame({'A': [1, 2, 3]})
            >>> transformed_df = adder.transform(df)
            >>> print(transformed_df)
            A  new_col
            0  1        2
            1  2        4
            2  3        6
        """
        try:
            if self.aggregation:
                cols = self.aggregation["columns"]
                agg_func = self.aggregation["agg_func"]
                log_and_optionally_raise(
                    module="TRANSFORMER",
                    component="PandasColumnAdder",
                    method="transform",
                    error_type=ErrorType.NO_ERROR,
                    message=f"Applying aggregation: {agg_func} on {cols}",
                    level="INFO"
                )
                data[self.column_name] = data[cols].apply(agg_func, axis=1)

            elif callable(self.value):
                data[self.column_name] = self.value(data)

            elif isinstance(self.value, str) and self.value in data.columns:
                data[self.column_name] = data[self.value]

            else:
                data[self.column_name] = self.value

        except Exception as e:
            raise RuntimeError(f"Error while adding column '{self.column_name}': {e}") from e

        log_and_optionally_raise(
            module="TRANSFORMER",
            component="PandasColumnAdder",
            method="transform",
            error_type=ErrorType.NO_ERROR,
            message=f"Column '{self.column_name}' added successfully.",
            level="INFO"
        )
        return data