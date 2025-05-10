from typing import List, Dict, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    benchmark, log_call
)
from odibi_de_v2.logger import log_exceptions
from odibi_de_v2.core.enums import ErrorType
from .spark_pivot_transformer import SparkPivotTransformer
from .spark_unpivot_transformer import SparkUnpivotTransformer
from .spark_steam_property_extractor import SparkSteamPropertyExtractor


@enforce_types(strict=True)
@validate_non_empty(["conversion_query"])
@benchmark(module="TRANSFORMER", component="SparkSteamWorkflowTransformer")
@log_call(module="TRANSFORMER", component="SparkSteamWorkflowTransformer")
@log_exceptions(
    module="TRANSFORMER",
    component="SparkSteamWorkflowTransformer",
    error_type=ErrorType.INIT_ERROR,
    raise_type=ValueError,
)
class SparkSteamWorkflowTransformer(IDataTransformer):
    """
    A flexible transformer that allows optional pre-pivot/unpivot, applies derived column
    logic, performs steam property calculations, and optionally post-pivots/unpivots the result.

    This class supports:
        - Optional pre-processing with pivot or unpivot
        - Arbitrary column derivations using SQL expressions
        - Thermodynamic calculations via SparkSteamPropertyExtractor
        - Optional post-processing with pivot or unpivot
        - Optional registration of final DataFrame as a temp view

    Attributes:
        conversion_query (str): SQL query to select the initial dataset.
        pre_pivot_config (Optional[Dict]): Parameters for SparkPivotTransformer.
        pre_unpivot_config (Optional[Dict]): Parameters for SparkUnpivotTransformer.
        derived_columns (Optional[Dict]): SQL-style expressions to add new calculated columns.
        steam_property_config (Dict): Config for SparkSteamPropertyExtractor.
        post_pivot_config (Optional[Dict]): Optional config to re-pivot the result.
        post_unpivot_config (Optional[Dict]): Optional config to flatten the result.
        register_view (bool): Whether to register the final DataFrame as a temp view.
        view_name (str): View name to register if register_view=True.

    Example 1 (Pre-Pivot → Derive → Steam Props → Post-Unpivot):

        >>> from pyspark.sql import SparkSession
        >>> import pandas as pd
        >>> spark = SparkSession.builder.getOrCreate()
        >>> long_data = [
        ...     {"Time_Stamp": "2025-05-01 00:00:00", "Plant": "A", "Asset": "Boiler1", "Description": "Boiler Steam Temperature", "Value": 495.0},
        ...     {"Time_Stamp": "2025-05-01 00:00:00", "Plant": "A", "Asset": "Boiler1", "Description": "Boiler Steam Pressure", "Value": 180.0},
        ...     {"Time_Stamp": "2025-05-01 01:00:00", "Plant": "A", "Asset": "Boiler1", "Description": "Boiler Steam Temperature", "Value": 500.0},
        ...     {"Time_Stamp": "2025-05-01 01:00:00", "Plant": "A", "Asset": "Boiler1", "Description": "Boiler Steam Pressure", "Value": 185.0}
        ... ]
        >>> spark_df = spark.createDataFrame(pd.DataFrame(long_data))
        >>> spark_df.createOrReplaceTempView("boiler_efficiency_long")

        >>> transformer = SparkSteamWorkflowTransformer(
        ...     conversion_query="SELECT * FROM boiler_efficiency_long",
        ...     pre_pivot_config={
        ...         "group_by": ["Time_Stamp", "Plant", "Asset"],
        ...         "pivot_column": "Description",
        ...         "value_column": "Value",
        ...         "agg_func": "first"
        ...     },
        ...     derived_columns={
        ...         "Temp_K": "`Boiler Steam Temperature` + 273.15",
        ...         "Pressure_MPa": "(`Boiler Steam Pressure` + 14.7) * 0.00689476"
        ...     },
        ...     steam_property_config={
        ...         "input_params": {
        ...             "T": "lambda row: row['Temp_K']",
        ...             "P": "lambda row: row['Pressure_MPa']"
        ...         },
        ...         "output_properties": ["h", "s", "cp"],
        ...         "prefix": "steam",
        ...         "units": "imperial"
        ...     },
        ...     post_unpivot_config={
        ...         "id_columns": ["Time_Stamp", "Plant", "Asset"]
        ...     },
        ...     register_view=True,
        ...     view_name="steam_workflow_pivot_to_unpivot"
        ... )
        >>> df = transformer.transform()
        >>> df.show()

    Example 2 (Pre-Unpivot → Derive → Dummy Steam Props → Post-Pivot):

        >>> wide_data = [
        ...     {"Time_Stamp": "2025-05-01 00:00:00", "Plant": "A", "Asset": "Boiler1", "Boiler Steam Temperature": 495.0, "Boiler Steam Pressure": 180.0},
        ...     {"Time_Stamp": "2025-05-01 01:00:00", "Plant": "A", "Asset": "Boiler1", "Boiler Steam Temperature": 500.0, "Boiler Steam Pressure": 185.0}
        ... ]
        >>> spark.createDataFrame(pd.DataFrame(wide_data)).createOrReplaceTempView("boiler_efficiency_wide")

        >>> transformer2 = SparkSteamWorkflowTransformer(
        ...     conversion_query="SELECT * FROM boiler_efficiency_wide",
        ...     pre_unpivot_config={
        ...         "id_columns": ["Time_Stamp", "Plant", "Asset"]
        ...     },
        ...     derived_columns={
        ...         "Scaled_Value": "Value * 2"
        ...     },
        ...     steam_property_config={
        ...         "input_params": {
        ...             "T": "lambda row: row['Scaled_Value']"
        ...         },
        ...         "output_properties": ["h"],
        ...         "prefix": "steam",
        ...         "units": "metric"
        ...     },
        ...     post_pivot_config={
        ...         "group_by": ["Time_Stamp", "Plant", "Asset"],
        ...         "pivot_column": "Description",
        ...         "value_column": "Value",
        ...         "agg_func": "first"
        ...     },
        ...     register_view=True,
        ...     view_name="steam_workflow_unpivot_to_pivot"
        ... )
        >>> df2 = transformer2.transform()
        >>> df2.show()
"""


    def __init__(
        self,
        conversion_query: str,
        steam_property_config: Dict = {},
        pre_pivot_config: Optional[Dict] = None,
        pre_unpivot_config: Optional[Dict] = None,
        derived_columns: Optional[Dict[str, str]] = None,
        post_pivot_config: Optional[Dict] = None,
        post_unpivot_config: Optional[Dict] = None,
        register_view: bool = True,
        view_name: str = "steam_workflow_view",
    ):
        self.conversion_query = conversion_query
        self.pre_pivot_config = pre_pivot_config
        self.pre_unpivot_config = pre_unpivot_config
        self.derived_columns = derived_columns or {}
        self.steam_property_config = steam_property_config
        self.post_pivot_config = post_pivot_config
        self.post_unpivot_config = post_unpivot_config
        self.register_view = register_view
        self.view_name = view_name

    @enforce_types(strict=True)
    @ensure_output_type(DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkSteamWorkflowTransformer",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError,
    )
    def transform(self, **kwargs) -> DataFrame:
        from odibi_de_v2.spark_utils import get_active_spark
        spark = get_active_spark()
        df = spark.sql(self.conversion_query)

        # Pre-processing: pivot or unpivot
        if self.pre_pivot_config:
            df = SparkPivotTransformer(
                conversion_query=self.conversion_query,
                register_view=False,
                **self.pre_pivot_config
            ).transform()
        elif self.pre_unpivot_config:
            df = SparkUnpivotTransformer(**self.pre_unpivot_config).transform(df)

        # Derived column expressions
        for col_name, col_expr in self.derived_columns.items():
            df = df.withColumn(col_name, expr(col_expr))

        if steam_property_config:
            # Steam property extraction
            df = SparkSteamPropertyExtractor(**self.steam_property_config).transform(df)

        # Post-processing: pivot or unpivot
        if self.post_pivot_config:
            df.createOrReplaceTempView("__intermediate_steam__")
            df = SparkPivotTransformer(
                conversion_query="SELECT * FROM __intermediate_steam__",
                register_view=False,
                **self.post_pivot_config
            ).transform()
        elif self.post_unpivot_config:
            df = SparkUnpivotTransformer(**self.post_unpivot_config).transform(df)

        # Register final view
        if self.register_view:
            df.createOrReplaceTempView(self.view_name)

        return df
