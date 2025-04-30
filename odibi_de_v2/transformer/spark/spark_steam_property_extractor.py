from typing import Dict, List, Any
import pandas as pd
import re
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, DoubleType

from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    benchmark, log_call
)
from odibi_de_v2.logger import log_exceptions
from odibi_de_v2.core.enums import ErrorType
from odibi_de_v2.utils import compute_steam_properties,safe_eval_lambda


class SparkSteamPropertyExtractor(IDataTransformer):
    """
    Initializes a SparkSteamPropertyExtractor object to compute specific steam properties
    using IAPWS97 formulations on a Spark DataFrame.

    This class is designed to work with Spark DataFrames and utilizes Pandas UDFs to apply
    the computations. The input parameters for the steam properties can be static values,
    dynamic column names, or lambda functions. The computed properties are added to the
    DataFrame with an optional prefix to distinguish them.

    Attributes:
        input_params (Dict[str, object]): A dictionary mapping steam property names to values, column names,
        or lambda functions that define how to compute each property from the DataFrame's rows.
        output_properties (List[str]): A list of property names to compute, such as enthalpy ('h'), entropy ('s'),
        heat capacity at constant pressure ('cp'), and heat capacity at constant volume ('cv').
        prefix (str): An optional prefix used for naming the output columns in the DataFrame. Defaults to 'steam'.

    Methods:
        transform(data: DataFrame, **kwargs) -> DataFrame:
            Applies the steam property calculations to the input DataFrame and returns a new DataFrame with the
            results added as columns.

    Raises:
        ValueError: If the initialization parameters are empty or invalid.
        RuntimeError: If an error occurs during the transformation process.

    Example:
        # Assuming `df` is a Spark DataFrame with the necessary columns
        input_params = {
            "temperature": "T",
            "pressure": "P"
        }
        output_properties = ["h", "s"]
        extractor = SparkSteamPropertyExtractor(input_params, output_properties)
        result_df = extractor.transform(df)
    """

    @enforce_types(strict=True)
    @validate_non_empty(["input_params", "output_properties"])
    @benchmark(module="TRANSFORMER", component="SparkSteamPropertyExtractor")
    @log_call(module="TRANSFORMER", component="SparkSteamPropertyExtractor")
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkSteamPropertyExtractor",
        error_type=ErrorType.INIT_ERROR,
        raise_type=ValueError,
    )
    def __init__(self, input_params: Dict[str, object], output_properties: List[str], prefix: str = "steam"):
        """
        Initializes a new instance of the class with specified input parameters, output properties,
        and an optional prefix.

        This constructor processes input parameters through a `safe_eval_lambda` function, sets default
        output properties if none are provided, and initializes the required input columns based on the
        input parameters.

        Args:
            input_params (Dict[str, object]): A dictionary where keys are parameter names and values are
                expressions (as strings) to be evaluated for those parameters.
            output_properties (List[str]): A list of strings specifying the properties to be output.
                Defaults to ["h", "s", "cp", "cv"] if not provided.
            prefix (str, optional): A prefix string used in naming or logging within the class.
                Defaults to "steam".

        Attributes:
            input_params (Dict[str, object]): Stores the evaluated input parameters.
            output_properties (List[str]): Stores the list of output properties.
            prefix (str): Stores the prefix.
            required_input_cols (List[str]): Stores the list of required input columns derived from `input_params`.

        Raises:
            ValueError: If any of the expressions in `input_params` are invalid or cannot be safely evaluated.

        Example:
            >>> instance = MyClass({
                    'temperature': 'T',
                    'pressure': 'P'
                }, ['enthalpy', 'entropy'])
            Initializes an instance with temperature and pressure as input parameters and enthalpy and entropy
                as output properties.
        """
        self.input_params = {k: safe_eval_lambda(v) for k, v in input_params.items()}
        self.output_properties = output_properties or ["h", "s", "cp", "cv"]
        self.prefix = prefix
        self.required_input_cols = self._extract_required_columns(input_params)

    def _extract_required_columns(self, params: Dict[str, Any]) -> List[str]:
        """
        Extracts the required column names from the provided parameters.

        This method parses a dictionary of parameters to identify and extract column names that
        are either directly referenced or used within lambda expressions. It supports two types of parameter values:
        1. Direct column names as strings.
        2. Lambda expressions as strings, where column names are accessed via the pattern `row['column_name']`.

        Args:
            params (Dict[str, Any]): A dictionary where the keys are parameter names and the values are
            either strings representing column names or lambda expressions as strings that may contain
            column references.

        Returns:
            List[str]: A list of unique column names extracted from the input dictionary. The list does
            not contain duplicates.

        Example:
            >>> params = {
                "filter": "age",
                "calculation": "lambda row: row['salary'] + row['bonus']"
            }
            >>> _extract_required_columns(params)
            ['age', 'salary', 'bonus']
        """
        cols = set()
        for val in params.values():
            if isinstance(val, str):
                if val.strip().startswith("lambda"):
                    # Only extract referenced columns
                    cols.update(re.findall(r"row\[['\"](.*?)['\"]\]", val))
                else:
                    # Treat as direct column
                    cols.add(val)
        return list(cols)


    @enforce_types(strict=True)
    @ensure_output_type(DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkSteamPropertyExtractor",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError,
    )
    def transform(self, data: DataFrame, **kwargs) -> DataFrame:
        """
        Transforms the input DataFrame by applying steam property computations to specified columns.

        This method decorates an internal function with `pandas_udf` to perform complex computations
        on steam properties based on the input DataFrame's columns. It dynamically constructs a DataFrame
        from specified columns, applies a transformation function to compute steam properties,
        and integrates these results back into the original DataFrame with new columns prefixed accordingly.

        Args:
            data (DataFrame): The input DataFrame containing the required columns for computation.
            **kwargs: Arbitrary keyword arguments that can be passed to modify the behavior of the transformation.

        Returns:
            DataFrame: A DataFrame with the original data and new columns added for each steam property computed,
            prefixed as specified by the instance's `prefix` attribute.

        Raises:
            KeyError: If any required columns specified in `self.required_input_cols` are missing from the
            input DataFrame.
            Exception: If any other error occurs during the computation of steam properties.

        Example:
            Assuming an instance `steam_transformer` of a class with this method, and `df` as a pandas DataFrame:

            ```python
            transformed_df = steam_transformer.transform(df)
            print(transformed_df.head())
            ```

        Note:
            This method requires that `self.required_input_cols`, `self.input_params`, `self.output_properties`,
            and `self.prefix` are properly set in the class instance
        """
        input_col_names = self.required_input_cols

        @pandas_udf(self._get_output_schema())
        def apply_steam_props(*cols: pd.Series) -> pd.DataFrame:
            input_df = pd.concat(cols, axis=1)
            input_df.columns = input_col_names

            def row_to_dict(row):
                args = {}
                for k, v in self.input_params.items():
                    if callable(v):
                        args[k] = v(row)
                    elif isinstance(v, str):
                        args[k] = row[v]
                    else:
                        args[k] = v
                return compute_steam_properties(row, args, self.output_properties, self.prefix)

            results = input_df.apply(row_to_dict, axis=1)
            return pd.DataFrame(results.tolist())

        # Apply the UDF
        struct_col = apply_steam_props(*[data[col] for col in input_col_names])
        result = data.withColumn("__steam_struct__", struct_col)

        for prop in self.output_properties:
            result = result.withColumn(
                f"{self.prefix}_{prop}",
                result["__steam_struct__"][f"{self.prefix}_{prop}"]
            )

        return result.drop("__steam_struct__")

    def _get_output_schema(self) -> StructType:
        """
        Generates a Spark DataFrame schema based on prefixed output properties.

        This method constructs a `StructType` schema for a DataFrame where each field corresponds to an
        output property, prefixed with a specified string. Each field in the schema is of type
        `DoubleType` and is nullable.

        Returns:
            StructType: A Spark DataFrame schema with fields named as `<prefix>_<property>`, where each
            field is of type `DoubleType` and nullable.

        Example:
            Assuming `self.prefix` is 'output' and `self.output_properties` includes ['height', 'weight'],
            the method will return a schema equivalent to:
            StructType([
                StructField("output_height", DoubleType(), True),
                StructField("output_weight", DoubleType(), True)
            ])
        """
        return StructType([
            StructField(f"{self.prefix}_{prop}", DoubleType(), True)
            for prop in self.output_properties
        ])
