from typing import List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr

from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    benchmark, log_call)
from odibi_de_v2.logger import log_exceptions
from odibi_de_v2.core.enums import ErrorType

from odibi_de_v2.spark_utils import get_active_spark


@enforce_types(strict=True)
@validate_non_empty(["conversion_query", "group_by", "pivot_column", "value_column"])
@benchmark(module="TRANSFORMER", component="SparkPivotTransformer")
@log_call(module="TRANSFORMER", component="SparkPivotTransformer")
@log_exceptions(
    module="TRANSFORMER",
    component="SparkPivotTransformer",
    error_type=ErrorType.INIT_ERROR,
    raise_type=ValueError,
)
class SparkPivotTransformer(IDataTransformer):
    """
    A Spark transformer that pivots the results of a SQL query using configurable grouping,
    pivoting, and aggregation logic, and optionally registers the output as a temporary view.

    This transformer is useful for converting long-format tag or sensor data into a wide-format
    representation, where each unique value in a pivot column becomes a separate column.

    It is fully metadata-driven and designed to work with the odibi_de_v2 transformation framework.

    Attributes:
        conversion_query (str): A valid Spark SQL query string that returns the long-format data.
        group_by (List[str]): A list of column names to group by before pivoting.
        pivot_column (str): The column whose unique values will become pivoted columns.
        value_column (str): The column containing the values to aggregate (e.g., Value_Metric).
        agg_func (str): The aggregation function to use during pivot (e.g., "first", "avg", "sum").
        view_name (str): Name of the Spark SQL temporary view to register the result under.
        register_view (bool): Whether to register the output DataFrame as a view (default: True).

    Example:
        >>> transformer = SparkPivotTransformer(
        >>>     conversion_query="SELECT Tag, Timestamp, Value_Metric FROM boiler_data",
        >>>     group_by=["Timestamp"],
        >>>     pivot_column="Tag",
        >>>     value_column="Value_Metric",
        >>>     agg_func="avg",
        >>>     view_name="pivoted_boiler_avg",
        >>>     register_view=True
        >>> )
        >>> result_df = transformer.transform()
        >>> result_df.display()

    Sample Config (for use with TransformerFromConfig):
        {
            "type": "spark_pivot_transformer",
            "conversion_query": "SELECT Tag, Timestamp, Value_Metric FROM boiler_data",
            "group_by": ["Timestamp"],
            "pivot_column": "Tag",
            "value_column": "Value_Metric",
            "agg_func": "avg",
            "view_name": "pivoted_boiler_avg",
            "register_view": true
        }
    """

    def __init__(
        self,
        conversion_query: str,
        group_by: List[str],
        pivot_column: str,
        value_column: str,
        agg_func: str = "first",
        view_name: str = "pivoted_view",
        register_view: bool = True,
    ):
        self.conversion_query = conversion_query
        self.group_by = group_by
        self.pivot_column = pivot_column
        self.value_column = value_column
        self.agg_func = agg_func
        self.view_name = view_name
        self.register_view = register_view

    @enforce_types(strict=True)
    @ensure_output_type(DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkPivotTransformer",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError,
    )
    def transform(self, **kwargs) -> DataFrame:
        spark: SparkSession = get_active_spark()
        queried_df = spark.sql(self.conversion_query)

        agg_expr = expr(f"{self.agg_func}(`{self.value_column}`)")
        pivoted_df = queried_df.groupBy(*self.group_by).pivot(self.pivot_column).agg(agg_expr)

        if self.register_view:
            pivoted_df.createOrReplaceTempView(self.view_name)

        return pivoted_df