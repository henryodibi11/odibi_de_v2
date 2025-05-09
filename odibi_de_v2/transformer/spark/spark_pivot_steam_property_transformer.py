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
from .spark_steam_property_extractor import SparkSteamPropertyExtractor
from .spark_unpivot_transformer import SparkUnpivotTransformer


@enforce_types(strict=True)
@validate_non_empty(["conversion_query", "group_by", "pivot_column", "value_column", "steam_property_config"])
@benchmark(module="TRANSFORMER", component="SparkPivotSteamPropertyTransformer")
@log_call(module="TRANSFORMER", component="SparkPivotSteamPropertyTransformer")
@log_exceptions(
    module="TRANSFORMER",
    component="SparkPivotSteamPropertyTransformer",
    error_type=ErrorType.INIT_ERROR,
    raise_type=ValueError,
)
class SparkPivotSteamPropertyTransformer(IDataTransformer):
    """
    A transformer that performs pivoting on SQL-derived data, computes steam properties
    using the IAPWS97 standard, and optionally unpivots the result back to long format.

    This class is a composition of SparkPivotTransformer, SparkSteamPropertyExtractor, and optionally
    SparkUnpivotTransformer. It is designed to streamline boiler/sensor data transformations.

    Attributes:
        conversion_query (str): SQL query to fetch long-format sensor/tag data.
        group_by (List[str]): Columns to group by during pivot.
        pivot_column (str): Column whose values become new columns.
        value_column (str): Column holding the numeric values to pivot.
        agg_func (str): Aggregation function to apply during pivot (e.g., 'first', 'avg').
        steam_property_config (Dict): Configuration passed to SparkSteamPropertyExtractor.
        unpivot_after (bool): Whether to unpivot the final DataFrame.
        unpivot_id_columns (Optional[List[str]]): Required if unpivot_after is True.
        view_name (str): Optional view name to register the final output.
        register_view (bool): Whether to register the result as a Spark temp view.

    Example:
        >>> pivoted_df = SparkPivotSteamPropertyTransformer(
        >>>     conversion_query=conversion_query,
        >>>     group_by=["Time_Stamp", "Plant", "Asset"],
        >>>     pivot_column="Description",
        >>>     value_column="Value",
        >>>     agg_func="first",
        >>>     steam_property_config={
        >>>         "input_params": {
        >>>             "P": "lambda row: (row['Boiler Steam Pressure'] + 14.7) * 0.00689476",
        >>>             "T": "lambda row: (row['Boiler Steam Temperature'] - 32) * 5/9 + 273.15"
        >>>         },
        >>>         "output_properties": ["h", "s", "cp"],
        >>>         "prefix": "steam",
        >>>         "units": "imperial"
        >>>     },
        >>>     unpivot_after=False,
        >>>     unpivot_id_columns=["Time_Stamp", "Plant", "Asset"],
        >>>     register_view=True,
        >>>     view_name="unpivoted_df"
        >>> ).transform()
        >>> pivoted_df.display()
    """

    def __init__(
        self,
        conversion_query: str,
        group_by: List[str],
        pivot_column: str,
        value_column: str,
        steam_property_config: Dict,
        agg_func: str = "first",
        unpivot_after: bool = False,
        unpivot_id_columns: Optional[List[str]] = None,
        view_name: str = "pivot_steam_view",
        register_view: bool = True,
    ):
        self.conversion_query = conversion_query
        self.group_by = group_by
        self.pivot_column = pivot_column
        self.value_column = value_column
        self.agg_func = agg_func
        self.steam_property_config = steam_property_config
        self.unpivot_after = unpivot_after
        self.unpivot_id_columns = unpivot_id_columns
        self.view_name = view_name
        self.register_view = register_view

    @enforce_types(strict=True)
    @ensure_output_type(DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkPivotSteamPropertyTransformer",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError,
    )
    def transform(self, **kwargs) -> DataFrame:
        pivoted_df = SparkPivotTransformer(
            conversion_query=self.conversion_query,
            group_by=self.group_by,
            pivot_column=self.pivot_column,
            value_column=self.value_column,
            agg_func=self.agg_func,
            view_name="internal_pivot_view",
            register_view=False,
        ).transform()

        steam_df = SparkSteamPropertyExtractor(**self.steam_property_config).transform(pivoted_df)

        if self.unpivot_after:
            if not self.unpivot_id_columns:
                raise ValueError("unpivot_id_columns must be provided if unpivot_after=True")
            steam_df = SparkUnpivotTransformer(id_columns=self.unpivot_id_columns).transform(steam_df)

        if self.register_view:
            steam_df.createOrReplaceTempView(self.view_name)

        return steam_df
