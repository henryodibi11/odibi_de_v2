from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col
from typing import Any, Callable, Optional

from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    log_call, benchmark
)
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise
from odibi_de_v2.core.enums import ErrorType


class SparkColumnAdder(IDataTransformer):
    """
    Initializes and applies transformations to add a new column to a Spark DataFrame.

    This class provides functionality to add a new column to a Spark DataFrame based on various types of input:
    - Static values
    - Column copying
    - SQL expressions
    - Callable transformations
    - Multi-column aggregation

    Attributes:
        column_name (str): The name of the new column to be added.
        value (Any, optional): The static value, callable, or SQL expression used to generate the new column.
        Default is None.
        aggregation (dict, optional): A dictionary specifying multi-column aggregation with keys 'columns'
        (list of column names) and 'agg_func' (a function to apply for aggregation). Default is None.

    Methods:
        transform(data: DataFrame, **kwargs) -> DataFrame:
            Applies the specified transformation to the input DataFrame and returns a DataFrame with the new
            column added.

    Raises:
        ValueError: If neither `value` nor `aggregation` is provided, or if `aggregation` does not contain the required
        keys 'columns' and 'agg_func'.

    Example:
        >>> adder = SparkColumnAdder(column_name="new_col", value=lambda df: df['old_col'] * 2)
        >>> modified_df = adder.transform(original_df)
        This example creates a new column 'new_col' which is twice the value of 'old_col' in the DataFrame
        'original_df'.
    """

    @enforce_types(strict=True)
    @validate_non_empty(["column_name"])
    @benchmark(module="TRANSFORMER", component="SparkColumnAdder")
    @log_call(module="TRANSFORMER", component="SparkColumnAdder")
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkColumnAdder",
        error_type=ErrorType.INIT_ERROR,
        raise_type=ValueError,
    )
    def __init__(
        self,
        column_name: str,
        value = None,
        aggregation: Optional[dict] = None
    ):
        """
        Initializes a new instance of the SparkColumnAdder class, setting up parameters for column transformation.

        This constructor method sets up the SparkColumnAdder with specific transformation rules defined by either
        a direct value assignment or an aggregation operation.

        Args:
            column_name (str): The name of the column to which the transformation will be applied.
            value (optional): The direct value to assign to the column. Default is None.
            aggregation (Optional[dict], optional): A dictionary specifying the aggregation operation. It must contain
            the keys 'columns' (list of column names to aggregate) and 'agg_func' (the aggregation function to apply).
            Default is None.

        Raises:
            ValueError: If neither `value` nor `aggregation` is provided, or if `aggregation` does not properly contain
            'columns' and 'agg_func' keys.

        Example:
            # Direct value assignment
            adder = SparkColumnAdder(column_name="new_column", value=10)

            # Aggregation assignment
            adder = SparkColumnAdder(
                column_name="average_price",
                aggregation={"columns": ["price"], "agg_func": "mean"}
            )
        """
        if value is None and aggregation is None:
            raise ValueError("Either `value` or `aggregation` must be provided.")
        if aggregation:
            if not isinstance(aggregation, dict) or "columns" not in aggregation or "agg_func" not in aggregation:
                raise ValueError("`aggregation` must contain 'columns' and 'agg_func' keys.")

        self.column_name = column_name
        self.value = value
        self.aggregation = aggregation

        log_and_optionally_raise(
            module="TRANSFORMER",
            component="SparkColumnAdder",
            method="__init__",
            error_type=ErrorType.NO_ERROR,
            message=f"Initialized with column_name={column_name}, value={value}, aggregation={aggregation}",
            level="INFO"
        )

    @enforce_types(strict=True)
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkColumnAdder",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError,
    )
    def transform(self, data: DataFrame, **kwargs) -> DataFrame:
        """
        Transforms the input DataFrame by adding a new column based on specified rules.

        This method adds a new column to the input DataFrame. The new column's values are determined by
        the configuration of the instance:
        - If aggregation is configured, it applies the specified aggregation function to the specified columns.
        - If `self.value` is a callable, it applies this callable to the DataFrame.
        - If `self.value` is a string and matches a column name in the DataFrame, it copies the values from the
            specified column.
        - Otherwise, it fills the new column with the literal value of `self.value`.

        Args:
            data (DataFrame): The input DataFrame to be transformed.
            **kwargs: Additional keyword arguments that are not used but captured for interface compatibility.

        Returns:
            DataFrame: A new DataFrame with the added column.

        Raises:
            ValueError: If there are issues with accessing specified columns or applying functions.

        Example:
            Assuming an instance `transformer` of this class is properly configured with `self.column_name = 'new_col'`
            and `self.value = lambda x: x['existing_col'] * 2`:
            >>> original_df = spark.createDataFrame([(1, 2), (3, 4)], ["id", "existing_col"])
            >>> transformed_df = transformer.transform(original_df)
            >>> transformed_df.show()
            +---+------------+-------+
            | id|existing_col|new_col|
            +---+------------+-------+
            |  1|           2|      4|
            |  3|           4|      8|
            +---+------------+-------+
        """
        if self.aggregation:
            columns = self.aggregation["columns"]
            agg_func = self.aggregation["agg_func"]

            log_and_optionally_raise(
                module="TRANSFORMER",
                component="SparkColumnAdder",
                method="transform",
                error_type=ErrorType.NO_ERROR,
                message=f"Performing aggregation on columns {columns}",
                level="INFO"
            )
            return data.withColumn(self.column_name, agg_func(data))

        if callable(self.value):
            return data.withColumn(self.column_name, self.value(data))

        if isinstance(self.value, str):
            if self.value.strip() in data.columns:
                return data.withColumn(self.column_name, col(self.value.strip()))
            return data.withColumn(self.column_name, lit(self.value))

        return data.withColumn(self.column_name, lit(self.value))