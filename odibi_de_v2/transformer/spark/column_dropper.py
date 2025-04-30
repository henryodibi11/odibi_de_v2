from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    log_call, benchmark)
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise
from odibi_de_v2.core.enums import ErrorType
from odibi_de_v2.spark_utils import has_columns
from pyspark.sql import DataFrame


class SparkColumnDropper(IDataTransformer):
    """
    Drop specified columns from a Spark DataFrame.

    This method removes the columns listed in `columns_to_drop` from the input Spark DataFrame. It ensures that
    the specified columns exist in the DataFrame before attempting to drop them, logging the operation and any
    exceptions that occur.

    Args:
        data (DataFrame): The input Spark DataFrame from which columns are to be dropped.

    Returns:
        DataFrame: A new Spark DataFrame with the specified columns removed.

    Raises:
        RuntimeError: If any specified column does not exist in the input DataFrame.

    Example:
        # Assuming `df` is an existing Spark DataFrame with columns 'col1', 'col2', 'col3'
        dropper = SparkColumnDropper(columns_to_drop=["col1", "col2"])
        transformed_df = dropper.transform(df)
        # `transformed_df` will now contain only the column 'col3'
    """

    @enforce_types(strict=True)
    @validate_non_empty(["columns_to_drop"])
    @benchmark(module="TRANSFORMER", component="SparkColumnDropper")
    @log_call(module="TRANSFORMER", component="SparkColumnDropper")
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkColumnDropper",
        error_type=ErrorType.INIT_ERROR,
        raise_type=RuntimeError)
    def __init__(self, columns_to_drop: list):
        """
        Initializes an instance of the SparkColumnDropper class.

        This constructor method sets up a new SparkColumnDropper object with specified columns to be
        dropped from a DataFrame during transformation processes. It logs the initialization details at the INFO level.

        Args:
            columns_to_drop (list): A list of column names (strings) that should be dropped from the DataFrame.

        Returns:
            None

        Raises:
            LogAndRaiseException: If there is an issue with logging the initialization details, though this is
            handled internally and typically does not need to be caught by the user.

        Example:
            >>> dropper = SparkColumnDropper(columns_to_drop=['age', 'name'])
            This will create an instance of SparkColumnDropper which will drop 'age' and 'name' columns from a
            DataFrame when applied.
        """
        self.columns_to_drop = columns_to_drop
        log_and_optionally_raise(
            module="TRANSFORMER",
            component="SparkColumnDropper",
            method="__init__",
            error_type=ErrorType.NO_ERROR,
            message=f"Initialized with columns_to_drop: {columns_to_drop}",
            level="INFO")

    @enforce_types(strict=True)
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkColumnDropper",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError)
    def transform(self, data: DataFrame, **kwargs) -> DataFrame:
        """
        Transforms the input DataFrame by dropping specified columns.

        This method modifies the input Spark DataFrame by removing the columns listed in `self.columns_to_drop`.
        It first checks if the DataFrame contains the columns to be dropped and logs the operation. The method
        ensures that only the remaining columns are included in the returned DataFrame.

        Args:
            data (DataFrame): A Spark DataFrame from which columns will be dropped.

        Keyword Args:
            **kwargs: Additional keyword arguments that may be used for future extensions or logging configurations.

        Returns:
            DataFrame: A new Spark DataFrame with the specified columns removed.

        Raises:
            ColumnNotFoundError: If any column specified in `self.columns_to_drop` does not exist in the
            input DataFrame.

        Example:
            Assuming `transformer` is an instance of a class with `columns_to_drop` attribute set to ['age', 'name']:
            >>> original_df = spark.createDataFrame([(1, 'Alice', 30), (2, 'Bob', 25)], ['id', 'name', 'age'])
            >>> transformed_df = transformer.transform(original_df)
            >>> transformed_df.show()
            +---+
            | id|
            +---+
            |  1|
            |  2|
            +---+
        """
        has_columns(data, self.columns_to_drop)
        log_and_optionally_raise(
            module="TRANSFORMER",
            component="SparkColumnDropper",
            method="transform",
            error_type=ErrorType.NO_ERROR,
            message=f"Dropping columns: {self.columns_to_drop}",
            level="INFO")
        return data.select([col for col in data.columns if col not in self.columns_to_drop])
