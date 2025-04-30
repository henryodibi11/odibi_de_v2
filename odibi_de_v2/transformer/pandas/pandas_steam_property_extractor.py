from typing import Dict, List, Any
import pandas as pd
from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import enforce_types, ensure_output_type, validate_non_empty, log_call, benchmark
from odibi_de_v2.logger import log_exceptions
from odibi_de_v2.core.enums import ErrorType
from odibi_de_v2.utils import compute_steam_properties, safe_eval_lambda


class PandasSteamPropertyExtractor(IDataTransformer):
    """
    Transforms a Pandas DataFrame by computing specified IAPWS97 steam properties for each row.

    This class initializes with parameters for steam property calculations and applies these
    calculations to each row of a DataFrame using the `transform` method. The results are added
    as new columns to the DataFrame.

    Attributes:
        input_params (dict): Mapping of IAPWS97 input parameters where keys are parameter names
        and values can be constants, column names from the DataFrame, or lambda functions that
        dynamically compute values based on the DataFrame.
        output_properties (list of str): List of steam properties to be calculated (e.g., "h" for
        enthalpy, "s" for entropy, "cp" for specific heat at constant pressure).
        prefix (str): A prefix for the names of new columns added to the DataFrame. Defaults to "steam".

    Methods:
        transform(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
            Applies the steam property calculations to each row of the input DataFrame and returns a
            new DataFrame with the original data and new columns for each calculated property.

    Args:
        data (pd.DataFrame): The input DataFrame containing the data over which to compute the steam properties.

    Returns:
        pd.DataFrame: A new DataFrame consisting of the original data and additional columns for each
        of the specified steam properties, prefixed as defined in the `prefix` attribute.

    Raises:
        RuntimeError: If an error occurs during the transformation process.

    Example:
        >>> extractor = PandasSteamPropertyExtractor(
                input_params={"T": "temperature", "P": lambda row: row.pressure * 1.01},
                output_properties=["h", "s"]
            )
        >>> input_df = pd.DataFrame({"temperature": [300, 350], "pressure": [100, 150]})
        >>> result_df = extractor.transform(input_df)
        >>> print(result_df.columns)
        Index(['temperature', 'pressure', 'steam_h', 'steam_s'], dtype='object')
    """

    @enforce_types(strict=True)
    @validate_non_empty(["input_params", "output_properties"])
    @benchmark(module="TRANSFORMER", component="PandasSteamPropertyExtractor")
    @log_call(module="TRANSFORMER", component="PandasSteamPropertyExtractor")
    @log_exceptions(
        module="TRANSFORMER",
        component="PandasSteamPropertyExtractor",
        error_type=ErrorType.INIT_ERROR,
        raise_type=ValueError
    )
    def __init__(self, input_params: Dict[str, object], output_properties: List[str], prefix: str = "steam"):
        """
        Initializes a new instance of the class with specified input parameters, output properties,
        and an optional prefix.

        This constructor takes a dictionary of input parameters, processes each value through a
        `safe_eval_lambda` function, and stores the results. It also sets the list of output
        properties and an optional prefix that defaults to "steam".

        Args:
            input_params (Dict[str, object]): A dictionary where keys are parameter names and values are expressions
                to be evaluated safely.
            output_properties (List[str]): A list of strings specifying the names of properties that will be output
                by this instance.
            prefix (str, optional): A prefix string used for naming or tagging purposes. Defaults to "steam".

        Example:
            >>> obj = MyClass({
                    'param1': 'lambda x: x + 1',
                    'param2': 'lambda x: x * 2'
                }, ['result1', 'result2'])
            This creates an instance of MyClass with specific lambda functions as input parameters and designated
            output properties
        """
        self.input_params = {k: safe_eval_lambda(v) for k, v in input_params.items()}
        self.output_properties = output_properties
        self.prefix = prefix

    @enforce_types(strict=True)
    @ensure_output_type(pd.DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="PandasSteamPropertyExtractor",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError
    )
    def transform(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transforms the input DataFrame by computing steam properties for each row and concatenates the results
        with the original DataFrame.

        This method applies a function to each row of the input DataFrame to compute specific steam properties
        based on predefined input parameters and output properties. The results are then combined with the original
        DataFrame to provide a comprehensive view of both the input data and the computed properties.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data for which steam properties need to be computed.
            **kwargs: Additional keyword arguments that are passed to the `compute_steam_properties` function.

        Returns:
            pd.DataFrame: A DataFrame that includes the original data along with the computed steam properties
            in new columns.

        Example:
            >>> input_df = pd.DataFrame({'temperature': [150, 300], 'pressure': [101, 200]})
            >>> transformed_df = instance.transform(input_df)
            >>> print(transformed_df)
            # Output will include original columns along with new columns for each computed steam property.
        """
        result_dicts = data.apply(
            lambda row: compute_steam_properties(row, self.input_params, self.output_properties, self.prefix),
            axis=1
        )
        result_df = pd.DataFrame(list(result_dicts))
        return pd.concat([data.reset_index(drop=True), result_df], axis=1)
