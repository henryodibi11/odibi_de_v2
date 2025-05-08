from typing import List
from pyspark.sql import DataFrame

from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    benchmark, log_call
)
from odibi_de_v2.logger import log_exceptions
from odibi_de_v2.core.enums import ErrorType


@enforce_types(strict=True)
@validate_non_empty(["id_columns"])
@benchmark(module="TRANSFORMER", component="SparkUnpivotTransformer")
@log_call(module="TRANSFORMER", component="SparkUnpivotTransformer")
@log_exceptions(
    module="TRANSFORMER",
    component="SparkUnpivotTransformer",
    error_type=ErrorType.INIT_ERROR,
    raise_type=ValueError,
)
class SparkUnpivotTransformer(IDataTransformer):
    """
    A Spark transformer that performs unpivoting (stacking) on wide-format DataFrames.

    It converts multiple metric columns into a normalized long-format structure, mapping
    column names to a 'Description' field and their values to 'Value_Metric'.

    This transformer is useful for preparing metric tables for modeling or visualization.

    Attributes:
        id_columns (List[str]): Columns to retain as row identifiers. All other columns will be unpivoted.

    Example:
        >>> transformer = SparkUnpivotTransformer(
        >>>     id_columns=["Time_Stamp", "Year", "Month", "Plant", "Asset"]
        >>> )
        >>> result_df = transformer.transform(efficiency_df)
        >>> result_df.display()

    Sample Config:
        {
            "type": "spark_unpivot_transformer",
            "id_columns": ["Time_Stamp", "Year", "Month", "Plant", "Asset"]
        }
    """
    def __init__(self, id_columns: List[str]):
        self.id_columns = id_columns

    @enforce_types(strict=True)
    @ensure_output_type(DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkUnpivotTransformer",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError,
    )
    def transform(self, data: DataFrame, **kwargs) -> DataFrame:
        all_columns = data.columns
        value_columns = [col for col in all_columns if col not in self.id_columns]

        stack_expr = ", ".join([f"'{col}', `{col}`" for col in value_columns])
        num_cols = len(value_columns)

        unpivoted_df = data.selectExpr(
            *self.id_columns,
            f"stack({num_cols}, {stack_expr}) as (Description, Value)"
        )

        return unpivoted_df

