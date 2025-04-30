from pyspark.sql import DataFrame
from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, benchmark,
    log_call, transform_column_name
)
from odibi_de_v2.core.enums import ErrorType
from odibi_de_v2.spark_utils import has_columns


class SparkColumnNameStandardizer(IDataTransformer):
    """
    Apply column name standardization to a Spark DataFrame, excluding specified columns.

    This method standardizes the names of the columns in the provided DataFrame according to the `case_style`
    set during the instantiation of the class. It will not alter the names of columns listed in `exclude_columns`.

    Args:
        data (DataFrame): The Spark DataFrame whose column names are to be standardized.

    Returns:
        DataFrame: A new DataFrame with the column names standardized, except for those specified to be excluded.

    Raises:
        RuntimeError: If an error occurs during the transformation process.

    Example:
        ```python
        standardizer = SparkColumnNameStandardizer(case_style="camelCase", exclude_columns=["id", "timestamp"])
        df_standardized = standardizer.transform(df)
        ```
    """

    VALID_CASE_STYLES = {
        "snake_case", "camelCase", "PascalCase", "lowercase", "uppercase"
    }

    @enforce_types(strict=True)
    @benchmark(module="TRANSFORMER", component="SparkColumnNameStandardizer")
    @log_call(module="TRANSFORMER", component="SparkColumnNameStandardizer")
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkColumnNameStandardizer",
        error_type=ErrorType.INIT_ERROR,
        raise_type=RuntimeError)
    def __init__(self, case_style: str = "snake_case", exclude_columns: list = None):
        """
        Initializes an instance of SparkColumnNameStandardizer with specified naming convention and exclusion list.

        This constructor sets the column naming style for a Spark DataFrame and specifies which columns, if any,
        should be excluded from naming standardization. It validates the provided naming style against a
        predefined list of valid styles and logs the initialization details.

        Args:
            case_style (str, optional): The naming convention to apply to DataFrame column names.
                Defaults to "snake_case".
            exclude_columns (list, optional): A list of column names to exclude from naming standardization.
                Defaults to an empty list.

        Raises:
            ValueError: If the `case_style` is not in the predefined list of valid case styles.

        Example:
            >>> standardizer = SparkColumnNameStandardizer(case_style="camelCase", exclude_columns=["id", "timestamp"])
            This will initialize a standardizer that converts all column names to camelCase except for 'id'
            and 'timestamp'.
        """
        self.case_style = case_style
        self.exclude_columns = exclude_columns or []

        if case_style not in self.VALID_CASE_STYLES:
            raise ValueError(
                f"Invalid case_style '{case_style}'. Valid options are: {self.VALID_CASE_STYLES}"
            )

        log_and_optionally_raise(
            module="TRANSFORMER",
            component="SparkColumnNameStandardizer",
            method="__init__",
            error_type=ErrorType.NO_ERROR,
            message=f"Initialized with case_style={case_style}, exclude_columns={self.exclude_columns}",
            level="INFO"
        )

    @enforce_types(strict=True)
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkColumnNameStandardizer",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError)
    def transform(self, data: DataFrame, **kwargs) -> DataFrame:
        """
        Transforms the column names of a Spark DataFrame to a standardized format, optionally excluding
        specified columns.

        This method standardizes the names of the DataFrame columns according to a specified naming
        convention (e.g., camelCase, snake_case), except for the columns explicitly listed to be excluded.
        The transformation respects the case style defined in the instance.

        Args:
            data (DataFrame): The Spark DataFrame whose column names are to be standardized.
            **kwargs: Arbitrary keyword arguments. These are not directly used in the method but can be
            passed to underlying functions or for future extensions.

        Returns:
            DataFrame: A new DataFrame with the column names standardized, except for those specified to be excluded.

        Raises:
            ValueError: If any specified column in `exclude_columns` does not exist in the input DataFrame.

        Example:
            Assuming an instance of this class has been created with `exclude_columns` set to ['id', 'timestamp']
            and `case_style` set to 'snake_case':
            >>> original_data = spark.createDataFrame([(1, "John Doe", "2023-01-01")], ["id", "Name", "Join Date"])
            >>> transformed_data = transformer.transform(original_data)
            >>> transformed_data.columns
            ['id', 'name', 'join_date']
        """
        has_columns(data, self.exclude_columns)

        new_columns = [
            col if col in self.exclude_columns else transform_column_name(col, self.case_style)
            for col in data.columns
        ]

        log_and_optionally_raise(
            module="TRANSFORMER",
            component="SparkColumnNameStandardizer",
            method="transform",
            error_type=ErrorType.NO_ERROR,
            message=f"Column names standardized: {new_columns}",
            level="INFO"
        )

        return data.toDF(*new_columns)
