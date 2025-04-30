from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    log_call, benchmark)
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise
from odibi_de_v2.core.enums import ErrorType
from odibi_de_v2.spark_utils import has_columns
from pyspark.sql import DataFrame



class SparkColumnRenamer(IDataTransformer):
    """
    Rename columns in a Spark DataFrame according to a predefined mapping.

    This method modifies the column names of the input DataFrame based on the `column_map`
    attribute of the SparkColumnRenamer instance. It ensures that all columns specified in
    the `column_map` exist in the input DataFrame before proceeding with the renaming.

    Args:
        data (DataFrame): The input Spark DataFrame to be transformed.
        **kwargs: Arbitrary keyword arguments, included for compatibility with interfaces
            that might require additional parameters.

    Returns:
        DataFrame: A new Spark DataFrame with columns renamed as specified in `column_map`.

    Raises:
        RuntimeError: If any of the columns specified in `column_map` do not exist in the input DataFrame.

    Example:
        >>> column_map = {"old_name": "new_name", "old_date": "new_date"}
        >>> renamer = SparkColumnRenamer(column_map=column_map)
        >>> new_df = renamer.transform(old_df)
        This will rename the 'old_name' column to 'new_name' and 'old_date' to 'new_date' in `old_df`.
    """

    @enforce_types(strict=True)
    @validate_non_empty(["column_map"])
    @benchmark(module="TRANSFORMER", component="SparkColumnRenamer")
    @log_call(module="TRANSFORMER", component="SparkColumnRenamer")
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkColumnRenamer",
        error_type=ErrorType.INIT_ERROR,
        raise_type=RuntimeError
    )
    def __init__(self, column_map: dict):
        """
        Initializes a new instance of the SparkColumnRenamer class, setting up the mapping of columns.

        This constructor stores a dictionary that maps original column names to their new names and
        logs the initialization details.

        Args:
            column_map (dict): A dictionary where keys are original column names and values are the
            new names for these columns.

        Returns:
            None

        Example:
            >>> renamer = SparkColumnRenamer(column_map={'old_name': 'new_name'})
            This will create an instance of SparkColumnRenamer which can be used to rename columns
            from 'old_name' to 'new_name'.
        """
        self.column_map = column_map
        log_and_optionally_raise(
            module="TRANSFORMER",
            component="SparkColumnRenamer",
            method="__init__",
            error_type=ErrorType.NO_ERROR,
            message=f"Initialized with column_map: {column_map}",
            level="INFO"
        )

    @enforce_types(strict=True)
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkColumnRenamer",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError)
    def transform(self, data: DataFrame, **kwargs) -> DataFrame:
        """
        Transforms the input Spark DataFrame by renaming its columns according to a predefined mapping.

        This method modifies the column names of the input DataFrame based on a mapping defined in the
        instance's `column_map` attribute. It logs the transformation process and returns the modified
        DataFrame with new column names.

        Args:
            data (DataFrame): The input Spark DataFrame to be transformed.
            **kwargs: Arbitrary keyword arguments, primarily for maintaining compatibility with interfaces
            expecting additional parameters.

        Returns:
            DataFrame: A new Spark DataFrame with columns renamed as specified in `column_map`.

        Raises:
            KeyError: If any of the original column names specified in `column_map` do not exist in the input DataFrame.

        Example:
            Assuming an instance `renamer` of a class with a `column_map` attribute set to `{'old_name': 'new_name'}`,
            and `df` as an existing Spark DataFrame with a column named `old_name`:

            ```python
            transformed_df = renamer.transform(df)
            print(transformed_df.columns)  # Output should include 'new_name' instead of 'old_name'
            ```
        """
        has_columns(data, list(self.column_map.keys()))

        log_and_optionally_raise(
            module="TRANSFORMER",
            component="SparkColumnRenamer",
            method="transform",
            error_type=ErrorType.NO_ERROR,
            message=f"Renaming columns using column_map: {self.column_map}",
            level="INFO")

        renamed_data = data.selectExpr(
            *[f"`{col}` as `{self.column_map.get(col, col)}`" for col in data.columns]
        )

        return renamed_data
