from typing import Dict, List, Any
import pandas as pd
import re
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, DoubleType

from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    benchmark, log_call, compute_steam_properties, safe_eval_lambda
)
from odibi_de_v2.logger import log_exceptions
from odibi_de_v2.core.enums import ErrorType

# Full IAPWS97-supported output properties with metric to imperial conversions
UNIT_CONVERSION_FACTORS = {
    "h": {"imperial": lambda x: x * 0.429922614, "metric": lambda x: x},         # kJ/kg → BTU/lb
    "s": {"imperial": lambda x: x * 0.238845897, "metric": lambda x: x},         # kJ/kg-K → BTU/lb-R
    "cp": {"imperial": lambda x: x * 0.238845897, "metric": lambda x: x},        # kJ/kg-K → BTU/lb-R
    "cv": {"imperial": lambda x: x * 0.238845897, "metric": lambda x: x},
    "rho": {"imperial": lambda x: x * 0.062428, "metric": lambda x: x},          # kg/m³ → lb/ft³
    "v": {"imperial": lambda x: x * 16.0185, "metric": lambda x: x},             # m³/kg → ft³/lb
    "T": {"imperial": lambda x: x * 9/5 - 459.67, "metric": lambda x: x},        # K → °F
    "P": {"imperial": lambda x: x * 145.038, "metric": lambda x: x},             # MPa → psi
    "mu": {"imperial": lambda x: x * 0.000672, "metric": lambda x: x},           # Pa.s → lb/ft-s
    "k": {"imperial": lambda x: x * 0.577789, "metric": lambda x: x},            # W/m-K → BTU/hr-ft-R
    "x": {"imperial": lambda x: x, "metric": lambda x: x},                       # Quality (dimensionless)
}

class SparkSteamPropertyExtractor(IDataTransformer):
    """
    A Spark transformer that computes steam properties using the IAPWS97 standard
    and returns them as new columns in the DataFrame, with unit conversion support.

    This class uses Pandas UDFs to compute thermodynamic properties like enthalpy,
    entropy, specific heat, density, and more for each row in a Spark DataFrame.
    The properties are derived from pressure and temperature inputs using the IAPWS97
    formulation.

    Attributes:
        input_params (Dict[str, object]): Dictionary mapping IAPWS97 input keys to either
            column names, constant values, or stringified lambda expressions.
        output_properties (List[str]): List of IAPWS97 properties to return (e.g., ['h', 's', 'cp']).
        prefix (str): Prefix applied to output column names (default: "steam").
        units (str): Output unit system, either "metric" (default) or "imperial".

    Supported Output Properties:
        - h: Enthalpy (kJ/kg or BTU/lb)
        - s: Entropy (kJ/kg-K or BTU/lb-R)
        - cp: Specific heat at constant pressure
        - cv: Specific heat at constant volume
        - rho: Density (kg/m³ or lb/ft³)
        - v: Specific volume (m³/kg or ft³/lb)
        - T: Temperature (K or °F)
        - P: Pressure (MPa or psi)
        - mu: Dynamic viscosity (Pa.s or lb/ft-s)
        - k: Thermal conductivity (W/m-K or BTU/hr-ft-R)
        - x: Vapor quality (unitless)

    Example:
        >>> df = spark.createDataFrame([
        >>>     {"Boiler_Pressure": 5.0, "Feedwater_Temp": 100.0},
        >>>     {"Boiler_Pressure": 6.0, "Feedwater_Temp": 98.0}
        >>> ])
        >>> input_params = {
        >>>     "P": "Boiler_Pressure",
        >>>     "T": "lambda row: row['Feedwater_Temp'] + 273.15"
        >>> }
        >>> output_properties = ["h", "s", "cp", "rho", "v","mu","k"]
        >>> transformer = SparkSteamPropertyExtractor(input_params, output_properties, prefix="steam", units="imperial")
        >>> result_df = transformer.transform(df)
        >>> result_df.show()

        >>> input_params = {
        >>> "P": "lambda row: (row['Boiler Steam Pressure'] + 14.7) * 0.00689476",
        >>> "T": "lambda row: (row['Boiler Steam Temperature'] - 32) * 5/9 + 273.15"}
        >>> prefix = "keeler boiler"
        >>> output_properties = ["h"]
        >>> transformer = SparkSteamPropertyExtractor(input_params, output_properties, prefix="keeler boiler", units="imperial")
        >>> result_df = transformer.transform(df_with_calculated_columns)
        >>> result_df.display()
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
    def __init__(
        self,
        input_params: Dict[str, object],
        output_properties: List[str],
        prefix: str = "steam",
        units: str = "metric"
    ):
        self.input_params = {k: safe_eval_lambda(v) for k, v in input_params.items()}
        self.output_properties = output_properties or ["h", "s", "cp", "cv"]
        self.prefix = prefix
        self.units = units
        self.required_input_cols = self._extract_required_columns(input_params)

    def _extract_required_columns(self, params: Dict[str, Any]) -> List[str]:
        cols = set()
        for val in params.values():
            if isinstance(val, str):
                if val.strip().startswith("lambda"):
                    cols.update(re.findall(r"row\[['\"](.*?)['\"]\]", val))
                else:
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

                raw_props = compute_steam_properties(row, args, self.output_properties, self.prefix)
                print(raw_props)
                converted_props = {
                    key: UNIT_CONVERSION_FACTORS[prop][self.units](val) if val is not None else None
                    for prop in self.output_properties
                    for key, val in raw_props.items() if key.endswith(f"_{prop}")
                }
                return converted_props

            results = input_df.apply(row_to_dict, axis=1)
            return pd.DataFrame(results.tolist())

        struct_col = apply_steam_props(*[data[col] for col in input_col_names])
        result = data.withColumn("__steam_struct__", struct_col)

        for prop in self.output_properties:
            result = result.withColumn(
                f"{self.prefix}_{prop}",
                result["__steam_struct__"][f"{self.prefix}_{prop}"]
            )

        return result.drop("__steam_struct__")

    def _get_output_schema(self) -> StructType:
        return StructType([
            StructField(f"{self.prefix}_{prop}", DoubleType(), True)
            for prop in self.output_properties
        ])
