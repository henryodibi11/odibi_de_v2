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
    A flexible Spark transformer that supports optional pivot/unpivot before and after
    column derivation logic and allows one or more steam property extractions using IAPWS97.

    This is designed for boiler or steam-based process data where reshaping and enthalpy/entropy
    calculations are often combined.

    Attributes:
        conversion_query (str): SQL query to pull the input dataset.
        pre_pivot_config (Optional[Dict]): Config for pre-processing with pivot.
        pre_unpivot_config (Optional[Dict]): Config for pre-processing with unpivot.
        derived_columns (Optional[Dict]): Dict of SQL expressions to add new columns.
        steam_property_configs (List[Dict]): List of steam property extractor configs.
        post_pivot_config (Optional[Dict]): Config for post-processing with pivot.
        post_unpivot_config (Optional[Dict]): Config for post-processing with unpivot.
        register_view (bool): Whether to register the final DataFrame as a temp view.
        view_name (str): Name of the temp view to register.

    Example (Pivot → Derive → Multi Steam Props → Unpivot):

        >>> from pyspark.sql import SparkSession
        >>> import pandas as pd
        >>> spark = SparkSession.builder.getOrCreate()
        >>> data = [
        ...     {"Time_Stamp": "2025-05-01 00:00:00", "Plant": "A", "Asset": "Boiler1", "Description": "Boiler Steam Temperature", "Value": 495.0},
        ...     {"Time_Stamp": "2025-05-01 00:00:00", "Plant": "A", "Asset": "Boiler1", "Description": "Boiler Steam Pressure", "Value": 180.0},
        ...     {"Time_Stamp": "2025-05-01 01:00:00", "Plant": "A", "Asset": "Boiler1", "Description": "Boiler Steam Temperature", "Value": 500.0},
        ...     {"Time_Stamp": "2025-05-01 01:00:00", "Plant": "A", "Asset": "Boiler1", "Description": "Boiler Steam Pressure", "Value": 185.0}
        ... ]
        >>> spark.createDataFrame(pd.DataFrame(data)).createOrReplaceTempView("boiler_efficiency_long")

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
        ...     steam_property_configs=[
        ...         {
        ...             "input_params": {
        ...                 "T": "lambda row: row['Temp_K']",
        ...                 "P": "lambda row: row['Pressure_MPa']"
        ...             },
        ...             "output_properties": ["h", "s"],
        ...             "prefix": "main_steam",
        ...             "units": "imperial"
        ...         },
        ...         {
        ...             "input_params": {
        ...                 "T": "lambda row: row['Temp_K']"
        ...             },
        ...             "output_properties": ["cp"],
        ...             "prefix": "aux_steam",
        ...             "units": "metric"
        ...         }
        ...     ],
        ...     post_unpivot_config={
        ...         "id_columns": ["Time_Stamp", "Plant", "Asset"]
        ...     },
        ...     register_view=True,
        ...     view_name="multi_steam_metrics"
        ... )
        >>> df = transformer.transform()
        >>> df.show()
    """

    def __init__(
        self,
        conversion_query: str,
        steam_property_configs: Optional[List[Dict]] = None,
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
        self.steam_property_configs = steam_property_configs or []
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

        # Pre-processing
        if self.pre_pivot_config:
            df = SparkPivotTransformer(
                conversion_query=self.conversion_query,
                register_view=False,
                **self.pre_pivot_config
            ).transform()
        elif self.pre_unpivot_config:
            df = SparkUnpivotTransformer(**self.pre_unpivot_config).transform(df)

        # Derived columns
        for col_name, col_expr in self.derived_columns.items():
            df = df.withColumn(col_name, expr(col_expr))

        # Apply all steam property extractors
        for config in self.steam_property_configs:
            df = SparkSteamPropertyExtractor(**config).transform(df)

        # Post-processing
        if self.post_pivot_config:
            df.createOrReplaceTempView("__intermediate_steam__")
            df = SparkPivotTransformer(
                conversion_query="SELECT * FROM __intermediate_steam__",
                register_view=False,
                **self.post_pivot_config
            ).transform()
        elif self.post_unpivot_config:
            df = SparkUnpivotTransformer(**self.post_unpivot_config).transform(df)

        if self.register_view:
            df.createOrReplaceTempView(self.view_name)

        return df
