from enum import Enum


class DataType(Enum):
    """
    Supported data formats in the odibi_de framework.

    Attributes:
        CSV (str): Represents CSV file format.
        JSON (str): Represents JSON file format.
        AVRO (str): Represents AVRO file format.
        PARQUET (str): Represents Parquet file format.
        DELTA (str): Represents Delta Lake format.

    Example:
        >>> DataType.CSV.value
        'csv'

        >>> DataType.JSON.value
        'json'

        >>> DataType.AVRO.value
        'avro'

        >>> DataType.PARQUET.value
        'parquet'

        >>> DataType.DELTA.value
        'delta'

        >>> DataType.CLOUDFILES.value
        'cloudFiles'

    """
    CSV = 'csv'
    JSON = 'json'
    AVRO = 'avro'
    PARQUET = 'parquet'
    DELTA = 'delta'
    CLOUDFILES = 'cloudFiles'


class CloudService(Enum):
    """
    cloud service providers suppoered by odibi_de framework.

    Attributes:
        AZURE (str): Represents Microsoft Azure.
        AWS (str): Represents Amazon Web Services.
        GCP (str): Represents Google Cloud Platform.

    Example:
        >>> CloudService.AZURE.value
        'azure'
    """

    AZURE = "azure"


class Framework(Enum):
    """
    Enum class representing frameworks supported by the odibi_de framework.

    Attributes:
        PANDAS (str): Represents the Pandas data analysis library.
        SPARK (str): Represents the Apache Spark distributed data
        processing framework.
        SNOWFLAKE (str): Represents the Snowflake cloud data platform.

    Example:
        >>> Framework.PANDAS.value
        'pandas'

        >>> Framework.SPARK.value
        'spark'
    """
    PANDAS = "pandas"
    SPARK = "spark"


class ValidationType(Enum):
    """
    Supported validations that can be performed.

    Attributes:
        MISSING_COLUMNS (str): Checks for missing columns in the dataset.
        EXTRA_COLUMNS (str): Checks for unexpected columns in the dataset.
        DATA_TYPE (str): Validates column data types.
        NULL_VALUES (str): Validates that specified columns do not contain
            null values.
        VALUE_RANGE (str): Checks that column values fall within a specified
            range.
        UNIQUENESS (str): Ensures column values are unique.
        ROW_COUNT (str): Validates the number of rows in the dataset.
        NON_EMPTY (str): Ensures the dataset is not empty.
        VALUE (str): Validates specific values in columns.
        REFERENTIAL_INTEGRITY (str): Checks that values in a column match a
            reference dataset.
        REGEX (str): Validates column values against a regular expression.
        DUPLICATE_ROWS (str): Identifies duplicate rows in the dataset.
        COLUMN_DEPENDENCY (str): Validates dependencies between columns.

    Example:
        >>> ValidationType.MISSING_COLUMNS.value
        'missing_columns'

        >>> ValidationType.EXTRA_COLUMNS.value
        'extra_columns'

        >>> ValidationType.DATA_TYPE.value
        'data_type'
    """
    MISSING_COLUMNS = "missing_columns"
    EXTRA_COLUMNS = "extra_columns"
    DATA_TYPE = "data_type"
    NULL_VALUES = "null_values"
    VALUE_RANGE = "value_range"
    UNIQUENESS = "uniqueness "
    ROW_COUNT = "row_count"
    NON_EMPTY = "non_empty"
    VALUE = "value"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    REGEX = "regex"
    DUPLICATE_ROWS = "duplicate_rows"
    COLUMN_DEPENDENCY = "column_dependency"
    SCHEMA = "schema"
    COLUMN_TYPE = "column_type"


class TransformerType(Enum):
    """
    Supported transformations that can be performed.

    Each member represents a specific transformation type that can be applied
    during ETL, data cleaning, or pipeline execution.

    Attributes:

        COLUMN_ADDER: Adds new columns to the DataFrame.
        COLUMN_ADDER_STATIC: Adds static columns with fixed values.
        COLUMN_DROPPER: Drops specified columns.
        COLUMN_MERGER: Merges multiple columns into one.
        COLUMN_NAME_PREFIX_SUFFIX: Adds prefixes or suffixes to column names.
        COLUMN_NAME_REGEX_RENAMER: Renames columns using regex.
        COLUMN_NAME_STANDARDIZER: Standardizes column names.
        COLUMN_PATTERN_DROPPER: Drops columns matching a pattern.
        COLUMN_RENAMER: Renames columns using a mapping.
        COLUMN_REORDERER: Reorders columns based on a list.
        COLUMN_SPLITTER: Splits a column into multiple columns.
        COLUMN_TYPE_DROPPER: Drops columns by data type.
        EMPTY_COLUMN_DROPPER: Drops empty columns.
        NULL_RATIO_COLUMN_DROPPER: Drops columns with too many nulls.
        ROW_AGGREGATOR: Aggregates rows.
        ROW_APPENDER: Appends new rows.
        ROW_DEDUPLICATOR: Removes duplicates.
        ROW_DUPLICATOR: Duplicates matching rows.
        ROW_EXPANDER: Expands rows based on a condition.
        ROW_FILTER: Filters rows based on a condition.
        ROW_SAMPLER: Samples rows.
        ROW_SORTER: Sorts rows.
        ROW_SPLITTER: Splits rows into multiple.
        VALUE_REPLACER: Replaces values in the DataFrame.

    Example:
        >>> TransformerType.COLUMN_ADDER.value
        'column_adder'

        >>> TransformerType.COLUMN_ADDER_STATIC.value
        'column_adder_static'

        >>> TransformerType.COLUMN_DROPPER.value
        'column_dropper'
    """

    COLUMN_ADDER = "column_adder"
    COLUMN_ADDER_STATIC = "column_adder_static"
    COLUMN_DROPPER = "column_dropper"
    COLUMN_MERGER = "column_merger"
    COLUMN_NAME_PREFIX_SUFFIX = "column_name_prefix_suffix"
    COLUMN_NAME_REGEX_RENAMER = "column_name_regex_renamer"
    COLUMN_NAME_STANDARDIZER = "column_name_standardizer"
    COLUMN_PATTERN_DROPPER = "column_pattern_dropper"
    COLUMN_RENAMER = "column_renamer"
    COLUMN_REORDERER = "column_reorderer"
    COLUMN_SPLITTER = "column_splitter"
    COLUMN_TYPE_DROPPER = "column_type_dropper"
    EMPTY_COLUMN_DROPPER = "empty_column_dropper"
    NULL_RATIO_COLUMN_DROPPER = "null_ratio_column_dropper"
    ROW_AGGREGATOR = "row_aggregator"
    ROW_APPENDER = "row_appender"
    ROW_DEDUPLICATOR = "row_deduplicator"
    ROW_DUPLICATOR = "row_duplicator"
    ROW_EXPANDER = "row_expander"
    ROW_FILTER = "row_filter"
    ROW_SAMPLER = "row_sampler"
    ROW_SORTER = "row_sorter"
    ROW_SPLITTER = "row_splitter"
    VALUE_REPLACER = "value_replacer"


class ErrorType(Enum):

    """
    Standardized error type labels for consistent logging and exception
    handling across the odibi_de framework.

    These enums should be used with the `format_error` function to ensure
    consistent structure and traceability of all error messages.

    Attributes:
        VALUE_ERROR (str): Indicates invalid data values.
        FILE_NOT_FOUND (str): Indicates missing file or resource.
        NOT_IMPLEMENTED (str): Indicates that the feature is not yet supported.
        AUTH_ERROR (str): Indicates authentication or permission failure.
        CONNECTION_ERROR (str): Indicates failure to connect to external
            systems.
        CONFIG_ERROR (str): Indicates invalid or missing configuration.
        VALIDATION_ERROR (str): Indicates schema or data validation failure.
        TYPE_ERROR (str): Indicates mismatched or unexpected data type.
        UNKNOWN (str): Default fallback for uncategorized errors.
        NO_ERROR (str): Indicates no error.
        SCHEMA_ERROR (str): Indicates schema validation failure.

    Example:
        >>> ErrorType.VALUE_ERROR.value
                'VALUE_ERROR'
        >>> ErrorType.FILE_NOT_FOUND.value
                'FILE_NOT_FOUND'

    """
    VALUE_ERROR = "VALUE_ERROR"
    FILE_NOT_FOUND = "FILE_NOT_FOUND"
    NOT_IMPLEMENTED = "NOT_IMPLEMENTED"
    AUTH_ERROR = "AUTH_ERROR"
    CONNECTION_ERROR = "CONNECTION_ERROR"
    CONFIG_ERROR = "CONFIG_ERROR"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    TYPE_ERROR = "TYPE_ERROR"
    UNKNOWN = "UNKNOWN"
    NO_ERROR = "NO_ERROR"
    SCHEMA_ERROR = "SCHEMA_ERROR"
