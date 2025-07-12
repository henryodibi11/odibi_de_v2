from typing import Dict
import pandas as pd
import re
from odibi_de_v2.utils import (
    safe_eval_lambda,
    enforce_types,
    validate_non_empty,
    ensure_output_type,
    benchmark,
    log_call,
    compute_humidity_ratio
)
from odibi_de_v2.logger import log_exceptions
from odibi_de_v2.core.enums import ErrorType
from odibi_de_v2.core import IDataTransformer


class PandasHumidityRatioExtractor(IDataTransformer):
    """
    Extracts the humidity ratio (lb water vapor / lb dry air) from a Pandas DataFrame
    using dry bulb temperature (°F), relative humidity (%), and elevation (ft) as inputs.

    This class wraps around the `compute_humidity_ratio` function and dynamically resolves
    inputs using column names, constants, or lambdas. It supports flexible row-wise application
    and automatic column naming with an optional prefix.

    Args:
        input_params (Dict[str, Union[str, float, Callable]]): A mapping of parameter names
            to either column names (str), fixed values (float), or lambda functions.
            Supported keys: `"Tdb"`, `"RH"`, `"Elev_ft"`.
        prefix (str): Optional prefix to prepend to the output column name. Default is "".
        output_col_name (str): Optional name for the output column. Default is humidity_ratio.

    Returns:
        pd.DataFrame: Original DataFrame with a new column for humidity ratio added.

    Example:
    --------
    >>> import pandas as pd
    >>> from odibi_de_v2.utils import PandasHumidityRatioExtractor
    >>> df = pd.DataFrame([
    ...     {"Tdb": 80.0, "RH": 60.0, "Elev_ft": 875},
    ...     {"Tdb": 75.0, "RH": 45.0, "Elev_ft": 500},
    ...     {"Tdb": 90.0, "RH": 70.0, "Elev_ft": 1000}
    ... ])
    >>> extractor = PandasHumidityRatioExtractor(
    ...     input_params={
    ...         "Tdb": "Tdb",
    ...         "RH": "RH",
    ...         "Elev_ft": "Elev_ft"
    ...     },
    ...     prefix="weather"
    ... )
    >>> df_transformed = extractor.transform(df)
    >>> print(df_transformed)

         Tdb    RH  Elev_ft  weather_humidity_ratio
    0  80.0  60.0      875              0.013593
    1  75.0  45.0      500              0.008455
    2  90.0  70.0     1000              0.022244
    """

    @enforce_types(strict=True)
    @validate_non_empty(["input_params"])
    @benchmark(module="TRANSFORMER", component="PandasHumidityRatioExtractor")
    @log_call(module="TRANSFORMER", component="PandasHumidityRatioExtractor")
    @log_exceptions(
        module="TRANSFORMER",
        component="PandasHumidityRatioExtractor",
        error_type=ErrorType.INIT_ERROR,
        raise_type=ValueError,
    )
    def __init__(self, input_params: Dict[str, object], prefix: str = "", output_col_name: str = 'humidity_ratio'):
        self.input_params = {k: safe_eval_lambda(v) for k, v in input_params.items()}
        self.prefix = prefix
        self.output_col_name = output_col_name
        self.required_input_cols = self._extract_required_columns(input_params)

    def _extract_required_columns(self, params: Dict[str, object]) -> list:
        cols = set()
        for val in params.values():
            if isinstance(val, str):
                if val.strip().startswith("lambda"):
                    # Extract column names inside the lambda
                    cols.update(re.findall(r"row\[['\"](.*?)['\"]\]", val))
                else:
                    cols.add(val)
        return list(cols)

    @ensure_output_type(pd.DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="PandasHumidityRatioExtractor",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError,
    )
    def transform(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        def resolve_and_compute(row):
            try:
                resolved = {
                    k: v(row) if callable(v) else row[v] if isinstance(v, str) else v
                    for k, v in self.input_params.items()
                }
                return compute_humidity_ratio(**resolved)
            except Exception:
                return None
        if self.prefix:
            humidity_col = f"{self.prefix}_{self.output_col_name}"
        else:
            humidity_col = self.output_col_name

        data[humidity_col] = data.apply(resolve_and_compute, axis=1)

        return data
