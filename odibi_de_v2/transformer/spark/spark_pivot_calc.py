from typing import List, Dict, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.logger import log_exceptions
from odibi_de_v2.core.enums import ErrorType
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    benchmark, log_call
)
from .spark_pivot_transformer import SparkPivotTransformer
from .spark_unpivot_transformer import SparkUnpivotTransformer


@enforce_types(strict=True)
@validate_non_empty(["conversion_query", "group_by", "pivot_column", "value_column", "derived_columns"])
@benchmark(module="TRANSFORMER", component="SparkPivotWithCalculationTransformer")
@log_call(module="TRANSFORMER", component="SparkPivotWithCalculationTransformer")
@log_exceptions(
    module="TRANSFORMER",
    component="SparkPivotWithCalculationTransformer",
    error_type=ErrorType.INIT_ERROR,
    raise_type=ValueError,
)
class SparkPivotWithCalculationTransformer(IDataTransformer):
    """
    A transformer that pivots long-format Spark data, calculates one or more derived columns,
    and optionally unpivots the result back to long format.

    It composes SparkPivotTransformer and SparkUnpivotTransformer internally to handle 
    metric reshaping and transformation.

    Attributes:
        conversion_query (str): SQL query to fetch the long-format data.
        group_by (List[str]): Columns to group by during pivot.
        pivot_column (str): Column whose unique values become new columns.
        value_column (str): Column holding the values to pivot.
        agg_func (str): Aggregation function to apply (e.g., 'first', 'avg').
        derived_columns (Dict[str, str]): Dict mapping new column names to expressions.
        unpivot_after (bool): Whether to unpivot the final result back to long format.
        unpivot_id_columns (Optional[List[str]]): ID columns to retain when unpivoting.
        view_name (str): Optional view name to register the final result.
        register_view (bool): Whether to register final output as a temp view.
    """

    def __init__(
        self,
        conversion_query: str,
        group_by: List[str],
        pivot_column: str,
        value_column: str,
        derived_columns: Dict[str, str],
        agg_func: str = "first",
        unpivot_after: bool = False,
        unpivot_id_columns: Optional[List[str]] = None,
        view_name: str = "pivot_calc_view",
        register_view: bool = True,
    ):
        self.conversion_query = conversion_query
        self.group_by = group_by
        self.pivot_column = pivot_column
        self.value_column = value_column
        self.agg_func = agg_func
        self.derived_columns = derived_columns
        self.unpivot_after = unpivot_after
        self.unpivot_id_columns = unpivot_id_columns
        self.view_name = view_name
        self.register_view = register_view

    @enforce_types(strict=True)
    @ensure_output_type(DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkPivotWithCalculationTransformer",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError,
    )
    def transform(self, **kwargs) -> DataFrame:
        # Step 1: Run the pivot using existing transformer (no view registration)
        pivot_transformer = SparkPivotTransformer(
            conversion_query=self.conversion_query,
            group_by=self.group_by,
            pivot_column=self.pivot_column,
            value_column=self.value_column,
            agg_func=self.agg_func,
            view_name="internal_pivot_view",
            register_view=False,
        )
        pivoted_df = pivot_transformer.transform()

        # Step 2: Add derived columns using expressions
        for col_name, col_expr in self.derived_columns.items():
            pivoted_df = pivoted_df.withColumn(col_name, expr(col_expr))

        # Step 3 (optional): Unpivot using your existing transformer
        if self.unpivot_after:
            if not self.unpivot_id_columns:
                raise ValueError("unpivot_id_columns must be provided if unpivot_after=True")
            unpivot_transformer = SparkUnpivotTransformer(id_columns=self.unpivot_id_columns)
            pivoted_df = unpivot_transformer.transform(pivoted_df)

        # Step 4 (optional): Register final view
        if self.register_view:
            pivoted_df.createOrReplaceTempView(self.view_name)

        return pivoted_df
