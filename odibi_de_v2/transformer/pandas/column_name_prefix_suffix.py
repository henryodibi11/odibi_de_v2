from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    log_call, benchmark
)
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise
from odibi_de_v2.core.enums import ErrorType
import pandas as pd


class PandasColumnNamePrefixSuffix(IDataTransformer):
    """
    Adds a prefix and/or suffix to column names in a Pandas DataFrame, optionally excluding
    specified columns.

    This class is designed to be used as a transformer within data processing pipelines,
    particularly where modifications to DataFrame column names are required. It supports the
    addition of prefixes and suffixes while providing the flexibility to exclude certain columns
    from these modifications.

    Attributes:
        prefix (str): The string to prepend to column names. Defaults to an empty string.
        suffix (str): The string to append to column names. Defaults to an empty string.
        exclude_columns (list of str): A list of column names to exclude from prefixing and suffixing.
        Defaults to an empty list.

    Methods:
        transform(data: pd.DataFrame) -> pd.DataFrame:
            Applies the prefix and suffix to the column names of the provided DataFrame, excluding any
            columns specified in `exclude_columns`.

    Args:
        data (pd.DataFrame): The DataFrame whose column names are to be modified.

    Returns:
        pd.DataFrame: A DataFrame with modified column names, where applicable.

    Raises:
        RuntimeError: If an error occurs during the transformation process.

    Example:
        >>> transformer = PandasColumnNamePrefixSuffix(prefix="pre_", suffix="_post")
        >>> df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        >>> transformed_df = transformer.transform(df)
        >>> transformed_df.columns
        Index(['pre_A_post', 'pre_B_post'], dtype='object')
    """

    @enforce_types(strict=True)
    @benchmark(module="TRANSFORMER", component="PandasColumnNamePrefixSuffix")
    @log_call(module="TRANSFORMER", component="PandasColumnNamePrefixSuffix")
    @log_exceptions(
        module="TRANSFORMER",
        component="PandasColumnNamePrefixSuffix",
        error_type=ErrorType.INIT_ERROR,
        raise_type=ValueError
    )
    def __init__(self, prefix: str = "", suffix: str = "", exclude_columns: list = None):
        """
        Initializes a new instance of the PandasColumnNamePrefixSuffix class, setting up optional prefix
        and suffix for column names, and specifying columns to exclude from this modification.

        Args:
            prefix (str, optional): A string to prepend to column names. Defaults to an empty string.
            suffix (str, optional): A string to append to column names. Defaults to an empty string.
            exclude_columns (list, optional): A list of column names that should not have the prefix
            or suffix applied. Defaults to an empty list.

        Returns:
            None

        Raises:
            ErrorType.NO_ERROR: Logs a no-error message indicating successful initialization with specified parameters.

        Example:
            >>> transformer = PandasColumnNamePrefixSuffix(
                prefix="pre_", suffix="_post", exclude_columns=["id", "timestamp"])
            >>> print(transformer.prefix)
            'pre_'
            >>> print(transformer.suffix)
            '_post'
            >>> print(transformer.exclude_columns)
            ['id', 'timestamp']
        """
        self.prefix = prefix or ""
        self.suffix = suffix or ""
        self.exclude_columns = exclude_columns or []

        log_and_optionally_raise(
            module="TRANSFORMER",
            component="PandasColumnNamePrefixSuffix",
            method="__init__",
            error_type=ErrorType.NO_ERROR,
            message=(
                f"Initialized with prefix={self.prefix}, "
                f"suffix={self.suffix}, exclude_columns={self.exclude_columns}"),
            level="INFO"
        )

    @enforce_types(strict=True)
    @ensure_output_type(pd.DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="PandasColumnNamePrefixSuffix",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError
    )
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the column names of a pandas DataFrame by adding specified prefixes and suffixes, excluding
        certain columns.

        This method modifies the column names of the input DataFrame by appending a prefix and suffix to each
        column name, except for those specified in `self.exclude_columns`. The transformation logs the updated
        column names using a custom logging function.

        Args:
            data (pd.DataFrame): The DataFrame whose column names are to be transformed.

        Returns:
            pd.DataFrame: The DataFrame with updated column names.

        Example:
            Assuming an instance `transformer` of a class with properties `prefix='pre_'`, `suffix='_post'`,
            and `exclude_columns=['id']`,
            and a DataFrame `df` with columns ['id', 'name', 'age']:

            >>> transformed_df = transformer.transform(df)
            >>> transformed_df.columns
            Index(['id', 'pre_name_post', 'pre_age_post'], dtype='object')

        Note:
            This method assumes the existence of `self.prefix`, `self.suffix`, and `self.exclude_columns`
            attributes in the class. It also uses a custom logging function `log_and_optionally_raise` which
            needs to be defined elsewhere in the application.
        """
        updated_columns = [
            f"{self.prefix}{col}{self.suffix}" if col not in self.exclude_columns else col
            for col in data.columns
        ]

        log_and_optionally_raise(
            module="TRANSFORMER",
            component="PandasColumnNamePrefixSuffix",
            method="transform",
            error_type=ErrorType.NO_ERROR,
            message=f"Updated column names: {updated_columns}",
            level="INFO"
        )

        data.columns = updated_columns
        return data