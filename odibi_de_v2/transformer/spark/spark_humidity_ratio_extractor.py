from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, DoubleType
import pandas as pd
import re
from typing import Dict, List, Any

from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    benchmark, log_call, safe_eval_lambda,compute_humidity_ratio
)
from odibi_de_v2.logger import log_exceptions
from odibi_de_v2.core.enums import ErrorType


class SparkHumidityRatioExtractor(IDataTransformer):
    """
    Spark-compatible extractor that computes humidity ratio (lb water vapor / lb dry air)
    using dry bulb temperature (°F), relative humidity (%), and elevation (ft).

    Built for use in Spark DataFrames, this class supports constants, column names,
    and lambdas as parameter inputs. Internally, it uses a Pandas UDF and wraps around
    the `compute_humidity_ratio` function, enabling efficient distributed execution.

    Args:
        input_params (Dict[str, Union[str, float, Callable]]): A mapping of input parameter names
            to either column names (str), fixed values (float), or lambdas.
            Supported keys: `"Tdb"`, `"RH"`, `"Elev_ft"`.
        prefix (str): Optional prefix to prepend to the output column name. Default is "".
        output_col_name (str): Optional name for the output column. Default is humidity_ratio.

    Returns:
        pyspark.sql.DataFrame: Transformed Spark DataFrame with a new column for humidity ratio.

    Example:
    --------
    >>> from odibi_de_v2.utils import SparkHumidityRatioExtractor
    >>> from pyspark.sql import SparkSession
    >>> import pandas as pd
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df = spark.createDataFrame(pd.DataFrame({
    ...     "DryBulbTemp_F": [80.0, 75.0, 90.0],
    ...     "RelHum_Percent": [60.0, 45.0, 70.0],
    ...     "timestamp": ["2025-07-01 00:00:00", "2025-07-01 01:00:00", "2025-07-01 02:00:00"]
    ... }))
    >>> extractor = SparkHumidityRatioExtractor(
    ...     input_params={
    ...         "Tdb": "DryBulbTemp_F",
    ...         "RH": "RelHum_Percent",
    ...         "Elev_ft": 875.2
    ...     },
    ...     prefix="weather"
    ... )
    >>> df_transformed = extractor.transform(df)
    >>> df_transformed.show()

    +--------------+---------------+-------------------+------------------------+
    |DryBulbTemp_F |RelHum_Percent |timestamp          |weather_humidity_ratio |
    +--------------+---------------+-------------------+------------------------+
    |80.0          |60.0           |2025-07-01 00:00:00|0.013593                |
    |75.0          |45.0           |2025-07-01 01:00:00|0.008455                |
    |90.0          |70.0           |2025-07-01 02:00:00|0.022244                |
    +--------------+---------------+-------------------+------------------------+
    """

    @enforce_types(strict=True)
    @validate_non_empty(["input_params"])
    @benchmark(module="TRANSFORMER", component="SparkHumidityRatioExtractor")
    @log_call(module="TRANSFORMER", component="SparkHumidityRatioExtractor")
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkHumidityRatioExtractor",
        error_type=ErrorType.INIT_ERROR,
        raise_type=ValueError,
    )
    def __init__(
        self,
        input_params: Dict[str, object],
        prefix: str = "",
        output_col_name: str = "humidity_ratio",
    ):
        self.input_params = {k: safe_eval_lambda(v) for k, v in input_params.items()}
        self.prefix = prefix
        if prefix:
            self.output_col_name = f"{prefix}_{output_col_name}"
        else:
            self.output_col_name = output_col_name
        self.required_input_cols = self._extract_required_columns(input_params)

    def _extract_required_columns(self, params: Dict[str, Any]) -> List[str]:
        cols = set()
        for val in params.values():
            if isinstance(val, str) and val.strip().startswith("lambda"):
                cols.update(re.findall(r"row\[['\"](.*?)['\"]\]", val))
            elif isinstance(val, str):
                cols.add(val)
        return list(cols)

    @enforce_types(strict=True)
    @ensure_output_type(DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkHumidityRatioExtractor",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError,
    )
    def transform(self, data: DataFrame, **kwargs) -> DataFrame:
        input_col_names = self.required_input_cols

        @pandas_udf(self._get_output_schema())
        def apply_humidity_ratio(*cols: pd.Series) -> pd.DataFrame:
            input_df = pd.concat(cols, axis=1)
            input_df.columns = input_col_names

            def row_to_result(row):
                resolved = {}
                for k, resolver in self.input_params.items():
                    if callable(resolver):
                        resolved[k] = resolver(row)
                    elif isinstance(resolver, str):
                        resolved[k] = row[resolver]
                    else:
                        resolved[k] = resolver

                try:
                    result = compute_humidity_ratio(**resolved)
                except Exception:
                    result = None
                return {self.output_col_name: result}

            results = input_df.apply(row_to_result, axis=1)
            return pd.DataFrame(results.tolist())

        struct_col = apply_humidity_ratio(*[data[col] for col in input_col_names])
        result = data.withColumn("__humidity_struct__", struct_col)
        result = result.withColumn(self.output_col_name, result["__humidity_struct__"][self.output_col_name])
        return result.drop("__humidity_struct__")

    def _get_output_schema(self) -> StructType:
        return StructType([StructField(self.output_col_name, DoubleType(), True)])