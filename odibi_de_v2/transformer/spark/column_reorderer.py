from pyspark.sql import DataFrame
from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    log_call, benchmark
)
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise
from odibi_de_v2.core.enums import ErrorType
from odibi_de_v2.spark_utils import has_columns


class SparkColumnReorderer(IDataTransformer):
    """
    Reorder columns in the Spark DataFrame according to a specified order.

    This method reorganizes the columns of a Spark DataFrame based on a predefined list. It can optionally
    retain columns that are not specified in the `column_order` list.

    Args:
        data (DataFrame): The Spark DataFrame whose columns are to be reordered.
        **kwargs: Additional keyword arguments are ignored in this method but included for compatibility
        with interfaces that might require them.

    Returns:
        DataFrame: A new DataFrame with columns reordered as specified. If `retain_unspecified` is True,
        columns not listed in `column_order` will be appended at the end in their original order.

    Raises:
        RuntimeError: If an error occurs during the transformation process, such as if specified columns
        do not exist in the input DataFrame.

    Example:
        ```python
        reorderer = SparkColumnReorderer(column_order=["col2", "col1"], retain_unspecified=True)
        df = spark.createDataFrame([(1, 2), (3, 4)], ["col1", "col2"])
        transformed_df = reorderer.transform(df)
        # transformed_df will have columns ["col2", "col1"]
        ```
    """

    @enforce_types(strict=True)
    @validate_non_empty(["column_order"])
    @benchmark(module="TRANSFORMER", component="SparkColumnReorderer")
    @log_call(module="TRANSFORMER", component="SparkColumnReorderer")
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkColumnReorderer",
        error_type=ErrorType.INIT_ERROR,
        raise_type=RuntimeError
    )
    def __init__(self, column_order: list, retain_unspecified: bool = False):
        """
        Initializes a new instance of SparkColumnReorderer, setting up the column order and specifying
        whether to retain columns not explicitly mentioned.

        Args:
            column_order (list): A list of strings specifying the desired order of columns.
            retain_unspecified (bool, optional): A flag to indicate whether columns not listed in
            `column_order` should be retained in the output. Defaults to False.

        Raises:
            ValueError: If `column_order` is not a list or contains non-string elements.
            TypeError: If `retain_unspecified` is not a boolean.

        Example:
            >>> reorderer = SparkColumnReorderer(column_order=['name', 'age', 'email'], retain_unspecified=True)
            This will initialize a reorderer that arranges columns as 'name', 'age', 'email', and retains
            any other columns.
        """
        self.column_order = column_order
        self.retain_unspecified = retain_unspecified
        log_and_optionally_raise(
            module="TRANSFORMER",
            component="SparkColumnReorderer",
            method="__init__",
            error_type=ErrorType.NO_ERROR,
            message=f"Initialized with column_order={column_order}, "
                    f"retain_unspecified={retain_unspecified}",
            level="INFO"
        )

    @enforce_types(strict=True)
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkColumnReorderer",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError
    )
    def transform(self, data: DataFrame, **kwargs) -> DataFrame:
        """
        Reorders columns in a Spark DataFrame according to a specified order, optionally retaining unspecified columns.

        This method adjusts the column order of the input DataFrame based on the predefined `column_order`
        attribute of the instance. If `retain_unspecified` is set to True, columns not explicitly mentioned in
        `column_order` will be appended at the end in their original order.

        Args:
            data (DataFrame): The Spark DataFrame whose columns are to be reordered.
            **kwargs: Additional keyword arguments, ignored in this method but included for compatibility with
                similar interfaces.

        Returns:
            DataFrame: A new DataFrame with columns reordered as specified. If `retain_unspecified` is True and
                there are columns in `data` not listed in `column_order`, these will be included at the end of
                the DataFrame.

        Raises:
            ValueError: If `data` does not contain all the columns specified in `column_order` when
            `retain_unspecified` is False.

        Example:
            Assuming an instance `reorderer` of the class with `column_order=['name', 'age', 'email']`
            and `retain_unspecified=True`:
            >>> original_df = spark.createDataFrame([("John Doe", 30, "johndoe@example.com")], ["name", "age", "email"])
            >>> transformed_df = reorderer.transform(original_df)
            >>> transformed_df.show()
            This will display the DataFrame with columns ordered as ['name', 'age', 'email'].
        """
        has_columns(data, self.column_order)

        existing_cols = [col for col in self.column_order if col in data.columns]
        if self.retain_unspecified:
            unspecified_cols = [col for col in data.columns if col not in self.column_order]
            final_order = existing_cols + unspecified_cols
        else:
            final_order = existing_cols

        log_and_optionally_raise(
            module="TRANSFORMER",
            component="SparkColumnReorderer",
            method="transform",
            error_type=ErrorType.NO_ERROR,
            message=f"Reordering columns to: {final_order}",
            level="INFO"
        )

        return data.select(*final_order
)