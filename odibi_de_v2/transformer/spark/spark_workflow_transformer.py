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
from odibi_de_v2.spark_utils import get_active_spark

from .spark_pivot_transformer import SparkPivotTransformer
from .spark_unpivot_transformer import SparkUnpivotTransformer



@enforce_types(strict=True)
@validate_non_empty(["conversion_query"])
@benchmark(module="TRANSFORMER", component="SparkWorkflowTransformer")
@log_call(module="TRANSFORMER", component="SparkWorkflowTransformer")
@log_exceptions(
    module="TRANSFORMER",
    component="SparkWorkflowTransformer",
    error_type=ErrorType.INIT_ERROR,
    raise_type=ValueError,
)
class SparkWorkflowTransformer(IDataTransformer):
    """
    A flexible transformer that supports optional pivoting/unpivoting before and after
    derived column transformations using SQL expressions.

    This class is useful for general reshaping, enrichment, and flattening of Spark DataFrames
    without any steam-specific logic.

    Attributes:
        conversion_query (str): SQL query to retrieve the input data.
        pre_pivot_config (Optional[Dict]): Config for pre-processing with pivot.
        pre_unpivot_config (Optional[Dict]): Config for pre-processing with unpivot.
        derived_columns (Optional[Dict]): SQL-style expressions to create new columns.
        post_pivot_config (Optional[Dict]): Config for post-processing with pivot.
        post_unpivot_config (Optional[Dict]): Config for post-processing with unpivot.
        register_view (bool): Whether to register the output as a temp view.
        view_name (str): Name of the temp view to register.
    """

    def __init__(
        self,
        conversion_query: str,
        pre_pivot_config: Optional[Dict] = None,
        pre_unpivot_config: Optional[Dict] = None,
        derived_columns: Optional[Dict[str, str]] = None,
        post_pivot_config: Optional[Dict] = None,
        post_unpivot_config: Optional[Dict] = None,
        register_view: bool = True,
        view_name: str = "workflow_output"
    ):
        self.conversion_query = conversion_query
        self.pre_pivot_config = pre_pivot_config
        self.pre_unpivot_config = pre_unpivot_config
        self.derived_columns = derived_columns or {}
        self.post_pivot_config = post_pivot_config
        self.post_unpivot_config = post_unpivot_config
        self.register_view = register_view
        self.view_name = view_name

    @enforce_types(strict=True)
    @ensure_output_type(DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkWorkflowTransformer",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError,
    )
    def transform(self, **kwargs) -> DataFrame:
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

        # Post-processing
        if self.post_pivot_config:
            df.createOrReplaceTempView("__intermediate_workflow__")
            df = SparkPivotTransformer(
                conversion_query="SELECT * FROM __intermediate_workflow__",
                register_view=False,
                **self.post_pivot_config
            ).transform()
        elif self.post_unpivot_config:
            df = SparkUnpivotTransformer(**self.post_unpivot_config).transform(df)

        # Register output
        if self.register_view:
            df.createOrReplaceTempView(self.view_name)

        return df
